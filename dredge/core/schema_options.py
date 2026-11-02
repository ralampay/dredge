"""Shared schema input and dataset-selection CLI arguments."""
from .cli_options import CliOption

OPTIONS = (
    CliOption(('--schema',), {'help': 'Schema YAML: ingestion contract or edited resolution draft.'}),
    CliOption(('--dataset',), {'action': 'append', 'help': 'Dataset ID; repeatable (default: all).'}),
)
