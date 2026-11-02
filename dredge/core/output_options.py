"""Shared local-output argument; each workflow validates its own destination."""
from .cli_options import CliOption

OPTIONS = (CliOption(('--output',), {'help': 'Local output path: schema YAML or new ingestion directory.'}),)
