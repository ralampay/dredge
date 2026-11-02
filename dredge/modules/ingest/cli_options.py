"""Ingestion flags; imports do not initialize readers or providers."""

from ...core.cli_options import CliOption, positive


OPTIONS = (
    CliOption(('--partition',), {'action': 'append', 'help': 'Literal Hive key=value filter; repeat values for OR, keys for AND.'}),
    CliOption(('--plan',), {'help': 'Saved ingestion plan to execute or preview.'}),
    CliOption(('--partition-date',), {'help': 'Plan using three partition names in year,month,day order.'}),
    CliOption(('--time-field',), {'help': 'Plan using a literal top-level date/timestamp field.'}),
    CliOption(('--from',), {'dest': 'time_from', 'help': 'Inclusive ISO time/date bound for a plan.'}),
    CliOption(('--to',), {'dest': 'time_to', 'help': 'Exclusive ISO time/date bound for a plan.'}),
    CliOption(('--timezone',), {'help': 'IANA timezone for naive timestamps (default: UTC).'}),
    CliOption(('--inspect-parquet-metadata',), {'action': 'store_true',
              'help': 'Permit Parquet footer downloads for record-time planning and plan previews.'}),
    CliOption(('--batch-rows',), {'type': positive, 'help': 'Maximum records per write batch (default: 10000).'}),
    CliOption(('--batch-bytes',), {'type': positive, 'help': 'Estimated record payload per batch (default: 16777216 bytes).'}),
    CliOption(('--file-bytes',), {'type': positive, 'help': 'Target uncompressed Arrow bytes per output file (default: 134217728).'}),
)


def validate_options(parser, config):
    from .requests import source_selection_requested

    planning = config['action'] == 'plan'
    source_selection = source_selection_requested(config)
    filters = ('partition', 'partition_date', 'time_field', 'time_from', 'time_to', 'timezone', 'inspect_parquet_metadata')
    if config.get('plan') and (config.get('prefix') is not None or any(config.get(k) for k in filters)):
        parser.error('--plan cannot be combined with selection overrides')
    if config.get('dataset') and not config.get('schema'):
        parser.error('--dataset requires --schema')
    if config.get('time_field') and config.get('partition_date'):
        parser.error('Choose --time-field or --partition-date, not both')
    if planning:
        if any(config.get(k) for k in ('plan', 'dry_run', 'batch_rows', 'batch_bytes', 'file_bytes')):
            parser.error('Download options do not apply to --action plan')
    else:
        if not config.get('dry_run'):
            if not config.get('schema') or not config.get('output'):
                parser.error('Downloading requires --schema and --output')
        if config.get('time_field') or config.get('inspect_parquet_metadata'):
            parser.error('Record-time options require --action plan')
    if source_selection:
        if any(config.get(k) for k in ('time_field', 'inspect_parquet_metadata', 'timezone')):
            parser.error('Source selection cannot be combined with record-time options')
        if config.get('partition_date'):
            if not config.get('time_from') or not config.get('time_to'):
                parser.error('--partition-date requires --from and --to')
        elif config.get('time_from') or config.get('time_to'):
            parser.error('Source date bounds require --partition-date')
        return
    if config.get('plan'):
        return  # Version-specific schema and dataset requirements are checked after loading.
    if not config.get('schema'):
        parser.error('--schema is required for record-time planning')
    if not planning:
        if any(config.get(k) for k in filters):
            parser.error('Time filter options require --action plan')
        return
    has_filter = any(config.get(k) for k in filters)
    if not has_filter:
        return  # Existing offline schema-field inspection.
    if not config.get('partition_date') and not config.get('time_field'):
        parser.error('Choose --partition-date or --time-field')
    if not all(config.get(k) for k in ('time_from', 'time_to', 'output')):
        parser.error('Saving a schema-bound time plan requires --from, --to, and --output')
    if len(config.get('dataset') or []) != 1:
        parser.error('Saving a schema-bound time plan requires exactly one --dataset ID')
