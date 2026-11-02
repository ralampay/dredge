"""Shared bounded sampling CLI arguments."""
from .cli_options import CliOption, positive

OPTIONS = (
    CliOption(('--sample-files',), {'type': positive, 'help': 'Files per dataset (default: 10 for discovery, 50 for resolution).'}),
    CliOption(('--sample-records',), {'type': positive, 'help': 'Records per sampled file (default: 1000).'}),
    CliOption(('--sample-bytes',), {'type': positive, 'help': 'Transferred/decompressed bytes per sampled file (default: 8388608).'}),
)
