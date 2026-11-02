"""Ingestion flags; imports do not initialize readers or providers."""

from ...core.cli_options import CliOption, positive


OPTIONS = (
    CliOption(('--dry-run',), {'action': 'store_true', 'help': 'List source sizes without reading object contents or writing output.'}),
    CliOption(('--batch-rows',), {'type': positive, 'help': 'Maximum records per write batch (default: 10000).'}),
    CliOption(('--batch-bytes',), {'type': positive, 'help': 'Estimated record payload per batch (default: 16777216 bytes).'}),
    CliOption(('--file-bytes',), {'type': positive, 'help': 'Target uncompressed Arrow bytes per output file (default: 134217728).'}),
)


def validate_options(parser, config):
    for key in ('schema', 'output'):
        if not config.get(key):
            parser.error('--' + key + ' is required for ingestion')
    if config.get('prefix') is not None:
        parser.error('Ingestion uses the schema selector; --prefix cannot override it')
