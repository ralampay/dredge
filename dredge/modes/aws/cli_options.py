"""AWS flags declared once and reused by source adapters."""
from ...core.cli_options import CliOption

OPTIONS = (
    CliOption(('--profile',), {'help': 'AWS named profile; otherwise use SDK defaults.'}),
    CliOption(('--region',), {'help': 'AWS region override.'}),
)
DATABASE_OPTIONS = (CliOption(('--database',), {'help': 'Database name within the selected catalog.'}),)
