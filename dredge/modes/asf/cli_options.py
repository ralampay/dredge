"""Lightweight ASF flags and argument validation."""
from ...core.cli_options import CliOption, positive
from ...core.exceptions import UserError
from ...core.imagery import SceneQuery

OPTIONS = (
    CliOption(('--bbox',), {'help': 'WGS84 west,south,east,north; west > east crosses the antimeridian.'}),
    CliOption(('--start-date',), {'help': 'Inclusive UTC acquisition start date or timezone-aware timestamp.'}),
    CliOption(('--end-date',), {'help': 'Inclusive acquisition end; calendar dates include the entire UTC day.'}),
    CliOption(('--polarization',), {'type': str.upper, 'choices': ('VV', 'VH', 'HH', 'HV', 'VV+VH', 'HH+HV')}),
    CliOption(('--flight-direction',), {'type': str.upper, 'choices': ('ASCENDING', 'DESCENDING')}),
    CliOption(('--relative-orbit',), {'type': positive}),
    CliOption(('--acquisition-mode',), {'type': str.upper, 'choices': ('IW', 'EW', 'SM', 'WV')}),
    CliOption(('--max-results',), {'type': positive, 'help': 'Limit selected scenes; full catalog metadata is still enumerated.'}),
)


def validate_options(parser, config):
    for name in ('bbox', 'start_date', 'end_date'):
        if not config.get(name):
            parser.error('--' + name.replace('_', '-') + ' is required for ASF')
    try:
        SceneQuery.from_config(config)
    except UserError as exc:
        parser.error(str(exc))
    if not config.get('dry_run') and not config.get('output'):
        parser.error('--output is required for ASF downloads')
