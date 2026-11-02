"""Read untrusted edited YAML without accepting it as an executable contract."""
from pathlib import Path

from ...core.exceptions import UserError


def load_draft(path):
    import yaml
    try:
        with Path(path).open(encoding='utf-8') as stream:
            return yaml.safe_load(stream)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise UserError('Cannot load edited schema; check its path and YAML syntax.') from exc
