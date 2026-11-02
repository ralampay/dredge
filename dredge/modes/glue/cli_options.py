"""Glue metadata navigation; no provider imports."""
from ...core.cli_options import CliOption

OPTIONS = (
    CliOption(('--catalog-id',), {'help': 'Glue catalog ID (default: account catalog); parent for catalogs action.'}),
    CliOption(('--table',), {'help': 'Glue table name to describe.'}),
)


def validate_options(parser, config):
    action = config['action']
    if action in ('tables', 'describe') and not config.get('database'):
        parser.error('--database is required for Glue tables/describe')
    if action == 'describe' and not config.get('table'):
        parser.error('--table is required for Glue describe')
    if action != 'describe' and config.get('table'):
        parser.error('--table applies only to Glue describe')
    if action in ('catalogs', 'databases') and config.get('database'):
        parser.error('--database applies only to Glue tables/describe')
