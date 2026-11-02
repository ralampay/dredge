"""Shared workflow policies; each route retains its own publication semantics."""
from .cli_options import CliOption

PREVIEW_OPTIONS = (CliOption(('--dry-run',), {'action': 'store_true', 'help': 'Preview source metadata without publishing output.'}),)
POLICY_OPTIONS = (
    CliOption(('--on-error',), {'choices': ('fail', 'continue'), 'help': 'Per-item error policy (default: fail).'}),
    CliOption(('--overwrite',), {'action': 'store_true', 'help': 'Allow replacement of conflicting managed artifacts or regular reports.'}),
)
