"""Resolution-only flags and lightweight argument validation."""
from ...core.cli_options import CliOption

OPTIONS = (CliOption(('--baseline',), {'help': 'Original unedited version-2/3 discovery YAML.'}),)


def validate_options(parser, config):
    for key in ('baseline', 'schema', 'output'):
        if not config.get(key):
            parser.error('--' + key + ' is required for schema resolution')
    if config.get('prefix') is not None:
        parser.error('Resolution uses baseline selectors; --prefix cannot override them')
