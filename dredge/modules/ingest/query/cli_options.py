"""SQL export options; parsing does not import the SDK or SQL parser."""
from ....core.cli_options import CliOption, positive

OPTIONS = (
    CliOption(('--query',), {'help': 'One SELECT or WITH ... SELECT statement.'}),
    CliOption(('--query-file',), {'help': 'UTF-8 file containing one SELECT statement.'}),
    CliOption(('--query-timeout',), {'type': positive, 'help': 'Maximum query wait in seconds (default: 1800).'}),
)


def validate_options(parser, config):
    if bool(config.get('query')) == bool(config.get('query_file')):
        parser.error('Athena export requires exactly one of --query and --query-file')
    for key in ('database', 'output'):
        if not config.get(key):
            parser.error('--' + key + ' is required for Athena export')
