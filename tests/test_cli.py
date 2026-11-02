import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from rich.console import Console

from dredge.core.configuration import default_env_path, load_environment
from dredge.core.doctor import InspectEnvironment
from dredge.core.exceptions import UserError
from dredge.core.ui import print_environment_report


ROOT = Path(__file__).resolve().parents[1]


class EnvironmentTests(unittest.TestCase):
    def test_values_precedence_and_masking(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(
                "APP_NAME=file\nAPI_TOKEN=file-secret\nEMPTY=\nBARE\n"
                "MARKUP=[bold]literal[/bold]\nREFERENCE=${APP_NAME}\n",
                encoding="utf-8",
            )
            environment = {"APP_NAME": "shell", "API_TOKEN": "shell-secret", "UNRELATED": "private"}
            config = load_environment(path, environment)
            report = InspectEnvironment(config).execute()
            entries = {entry.name: entry for entry in report.entries}
            self.assertEqual(entries["APP_NAME"].display_value, "shell")
            self.assertEqual(entries["APP_NAME"].source, "shell override")
            self.assertEqual(entries["API_TOKEN"].display_value, "********")
            self.assertEqual(entries["EMPTY"].display_value, "<empty>")
            self.assertEqual(entries["BARE"].display_value, "<unset>")
            self.assertNotIn("BARE", environment)
            self.assertNotIn("UNRELATED", entries)
            self.assertEqual(environment["REFERENCE"], "${APP_NAME}")
            output = io.StringIO()
            print_environment_report(report, Console(file=output, width=120, color_system=None))
            self.assertIn("[bold]literal[/bold]", output.getvalue())
            self.assertNotIn("shell-secret", output.getvalue())
            self.assertNotIn("file-secret", output.getvalue())

    def test_missing_empty_and_shell_empty_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            self.assertFalse(load_environment(path, {}).exists)
            path.write_text("", encoding="utf-8")
            config = load_environment(path, {})
            self.assertTrue(config.exists)
            self.assertEqual(config.entries, ())
            path.write_text("NAME=file\n", encoding="utf-8")
            config = load_environment(path, {"NAME": ""})
            self.assertEqual(config.entries[0].value, "")
            self.assertEqual(config.entries[0].source, "shell override")

    def test_unreadable_file_is_actionable(self):
        with patch.object(Path, "open", side_effect=PermissionError("denied")):
            with self.assertRaisesRegex(UserError, "Cannot read"):
                load_environment(Path(".env"), {})

    def test_path_is_relative_to_package(self):
        self.assertEqual(default_env_path(), ROOT / ".env")


class CliTests(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "dredge", *args], cwd=ROOT,
            capture_output=True, text=True, check=False,
        )

    def test_help_and_no_arguments(self):
        for args in [(), ("--help",)]:
            with self.subTest(args=args):
                result = self.run_cli(*args)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("python -m dredge", result.stdout)
                self.assertIn("--doctor", result.stdout)

    def test_invalid_option(self):
        result = self.run_cli("--unknown-option")
        self.assertEqual(result.returncode, 2)

    def test_doctor_from_another_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(os.environ, PYTHONPATH=str(ROOT))
            result = subprocess.run(
                [sys.executable, "-m", "dredge", "--doctor"], cwd=directory,
                env=environment, capture_output=True, text=True, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            # Rich may wrap the path at the terminal width.
            self.assertIn(str(ROOT / ".env"), result.stdout.replace("\n", ""))

    def test_import_does_not_load_environment(self):
        result = subprocess.run(
            [sys.executable, "-c", "from unittest.mock import patch; "
             "import os; before = dict(os.environ); "
             "guard = patch('dotenv.dotenv_values', side_effect=AssertionError('import read dotenv')); "
             "guard.start(); import dredge.cli; assert dict(os.environ) == before"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
