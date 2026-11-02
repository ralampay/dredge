"""Routing and dependency contracts for independently extensible modules/modes."""
import ast
from contextlib import nullcontext, redirect_stderr
from dataclasses import FrozenInstanceError
from importlib import import_module
from importlib.util import resolve_name
import io
from pathlib import Path
import subprocess
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch

from dredge.cli import build_parser, main
from dredge.cli_routing import ROUTES, RouteDescriptor, resolve_runner
from dredge.core.cli_options import CliOption
from dredge.modules.discover.requests import CrawlOptions, CrawlRequest
from dredge.modules.discover.runner import run_discovery
from test_discover import MemorySource


ROOT = Path(__file__).resolve().parents[1]


def imports(path):
    package = ".".join(path.relative_to(ROOT).parts[:-1])
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            yield resolve_name("." * node.level + (node.module or ""), package) if node.level else node.module


class ArchitectureTests(unittest.TestCase):
    def test_dependency_direction(self):
        for path in (ROOT / "dredge/modules").rglob("*.py"):
            if path.name in ("runner.py", "presentation.py", "cli_options.py"):
                continue
            with self.subTest(path=path):
                for name in imports(path):
                    self.assertFalse(name.startswith(("dredge.cli", "dredge.modes", "rich", "boto3", "botocore")), name)
                    self.assertNotIn(name.rsplit(".", 1)[-1], ("runner", "presentation", "ui"))
        for path in (ROOT / "dredge/formats").rglob("*.py"):
            with self.subTest(path=path):
                for name in imports(path):
                    self.assertFalse(name.startswith(("dredge.modules", "dredge.modes", "dredge.cli", "rich")), name)
        for path in (ROOT / "dredge/modes").rglob("*.py"):
            with self.subTest(path=path):
                for name in imports(path):
                    self.assertFalse(name.startswith(("dredge.modules", "dredge.features", "dredge.cli")), name)
        for name in imports(ROOT / "dredge/core/sources.py"):
            self.assertFalse(name.startswith(("dredge.modules", "dredge.modes", "boto3", "botocore")), name)
        for path in (ROOT / "dredge").rglob("*.py"):
            if "features" not in path.parts:
                self.assertFalse(any(name.startswith("dredge.features") for name in imports(path)), str(path))

    def test_help_doctor_and_runner_import_are_lazy(self):
        script = '''
import importlib.abc
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

class BlockProviders(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"boto3", "botocore", "pyarrow", "ijson", "yaml"}:
            raise AssertionError("Unexpected import: " + fullname)

sys.meta_path.insert(0, BlockProviders())
from dredge.cli import main
from dredge.cli_routing import ROUTES, resolve_runner
from dredge.core.configuration import EnvironmentConfig
with TemporaryDirectory() as directory:
    config = EnvironmentConfig(Path(directory) / ".env", False, ())
    with patch("dredge.cli.load_environment", return_value=config):
        assert main([]) == 0
        assert main(["--doctor"]) == 0
        for args in (["--help"], ["--module", "discover", "--mode", "s3", "--help"]):
            try:
                main(args)
            except SystemExit as exc:
                assert exc.code == 0
            else:
                raise AssertionError("Expected help exit")
assert "dredge.modules.discover.runner" not in sys.modules
assert callable(resolve_runner(ROUTES[0]))
assert "dredge.modules.discover.commands" not in sys.modules
'''
        result = subprocess.run([sys.executable, "-c", script], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_compatibility_imports_are_identical(self):
        for old, new, names in (
            ("commands", "dredge.modules.discover.commands", ("CrawlSchemas",)),
            ("adapters", "dredge.modes.s3.adapters", ("S3Source",)),
            ("models", "dredge.core.sources", ("ObjectInfo", "ObjectSource", "SourceAccessError")),
            ("models", "dredge.modules.discover.requests", ("CrawlOptions",)),
            ("models", "dredge.modules.discover.models", ("DiscoveryReport",)),
            ("schema", "dredge.modules.discover.schema", ("infer", "merge")),
            ("serialization", "dredge.modules.discover.serialization", ("write_report",)),
        ):
            for name in names:
                with self.subTest(name=name):
                    self.assertIs(getattr(import_module("dredge.features.discover." + old), name),
                                  getattr(import_module(new), name))

    def test_typed_request_defaults_are_preserved(self):
        request = CrawlRequest.from_config({"bucket": "test", "output": "schema.yml", "include": ["*.json"]})
        self.assertEqual(request.options, CrawlOptions(bucket="test", include=("*.json",)))
        self.assertFalse(request.overwrite)
        with self.assertRaises(FrozenInstanceError):
            request.output = "other.yml"

    def test_runner_returns_report_and_closes_source_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            config = dict(module="discover", mode="s3", action="crawl", bucket="test",
                          output=str(Path(directory) / "schema.yml"))
            context = nullcontext(MemorySource({"x.json": b'{"x":1}'}))
            with patch("dredge.modes.s3.clients.open_source", return_value=context), \
                    patch("dredge.modules.discover.presentation.present") as present:
                report = run_discovery(config)
                self.assertFalse(report.partial)
                present.assert_called_once_with(report, config["output"])
            # A failed crawl must still release the source client's resources.
            config["overwrite"] = True
            with patch("boto3.Session") as session, \
                    patch("dredge.modes.s3.adapters.S3Source", return_value=MemorySource({"x.json": b"bad"})), \
                    patch("dredge.modules.discover.presentation.present") as present:
                from dredge.core.exceptions import UserError
                with self.assertRaises(UserError):
                    run_discovery(config)
                session.return_value.client.return_value.close.assert_called_once()
                present.assert_not_called()

    def test_route_metadata_drives_options_validation_and_dispatch(self):
        module = ModuleType("dredge._test_route")
        module.OPTIONS = (CliOption(("--test-value",), {"help": "Test route only."}),)
        called = []
        module.run = lambda config: called.append(config) or type("Result", (), {"partial": False})()
        extra = RouteDescriptor("example", "fake", ("check",), "._test_route:run",
                                (("Example", "._test_route:OPTIONS"),), ())
        with patch.dict(sys.modules, {module.__name__: module}), patch("dredge.cli_routing.ROUTES", (*ROUTES, extra)):
            help_text = build_parser().format_help()
            self.assertIn("example", help_text)
            self.assertIn("--test-value", help_text)
            with patch("dredge.cli.load_environment"):
                self.assertEqual(main(["--module", "example", "--mode", "fake", "--action", "check", "--test-value", "ok"]), 0)
            self.assertEqual(called[0]["test_value"], "ok")
            for args in (
                ["--module", "example", "--mode", "s3", "--action", "check"],
                ["--module", "example", "--mode", "fake", "--action", "crawl"],
                ["--module", "example", "--mode", "fake", "--action", "check", "--bucket", "test"],
                ["--module", "discover", "--mode", "s3", "--action", "crawl", "--test-value", "wrong"],
            ):
                with self.subTest(args=args), redirect_stderr(io.StringIO()), \
                        patch("dredge.cli.load_environment") as environment, self.assertRaises(SystemExit) as error:
                    main(args)
                self.assertEqual(error.exception.code, 2)
                environment.assert_not_called()

    def test_option_metadata_is_immutable(self):
        settings = {"help": "Original"}
        option = CliOption(("--example",), settings)
        settings["help"] = "Changed"
        self.assertEqual(option.settings["help"], "Original")
        with self.assertRaises(TypeError):
            option.settings["help"] = "Changed"
