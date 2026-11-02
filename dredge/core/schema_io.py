"""Atomic local report publication without implicit replacement."""
import os
from pathlib import Path
import tempfile

import yaml

from .exceptions import UserError
from .schema_manifest import SchemaManifest


def validate_output(path, overwrite=False):
    path = Path(path)
    if not path.parent.is_dir():
        raise UserError(f"Output directory does not exist: {path.parent}")
    if path.is_symlink() or (path.exists() and (not path.is_file() or not overwrite)):
        raise UserError(f"Output already exists or is not a regular file: {path}. Choose a new output path.")


def write_report(report, path, overwrite=False):
    SchemaManifest(report.document)
    path = Path(path)
    validate_output(path, overwrite)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            # Convert tuples to plain YAML sequences using safe_dump's supported representation.
            yaml.safe_dump(report.document, stream, sort_keys=False, allow_unicode=True)
            stream.flush()
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
    except OSError as exc:
        raise UserError(f"Cannot write report to {path}. Check permissions and whether the destination exists.") from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
