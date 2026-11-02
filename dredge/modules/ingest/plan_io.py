"""Version dispatch and atomic persistence shared by ingestion plan contracts."""
from datetime import datetime, timezone
import os
from pathlib import Path
import tempfile

import yaml

from ...core.exceptions import UserError
from ...core.schema_io import validate_output


def write_plan(plan, estimate, path):
    validate_output(path)
    path = Path(path)
    document = {**plan.document(), 'created_at': datetime.now(timezone.utc).isoformat(), 'estimate': estimate}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         prefix='.' + path.name, suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            yaml.safe_dump(document, stream, sort_keys=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    except OSError as exc:
        raise UserError('Cannot publish ingestion plan; choose a new writable output path.') from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def read_plan_document(path):
    try:
        return yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise UserError('Cannot read ingestion plan; check its path and YAML syntax.') from exc


def load_plan(path):
    document = read_plan_document(path)
    if isinstance(document, dict) and type(document.get('plan_version')) is int and document['plan_version'] == 2:
        from .source_plan import SourcePlan
        return SourcePlan.from_document(document)
    from .filter_plan import FilterPlan
    return FilterPlan.from_document(document)
