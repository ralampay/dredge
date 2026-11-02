"""Build ingestion contracts from discovery evidence, without provider imports."""
from ...core.schema_manifest import schema_fingerprint

from ...formats.validation import SUPPORTED_TYPES


def unresolved(schema):
    if schema['type'] not in SUPPORTED_TYPES:
        return True
    return any(unresolved(f) for f in schema.get('fields', [])) or (
        'items' in schema and unresolved(schema['items']))


def attach_contract(dataset, options):
    dataset['name'] = (dataset['location_pattern'].rstrip('/') or 'root') + ':' + dataset['format']
    parser = {"adapter": dataset['format'], "version": 1}
    if dataset['format'] != 'parquet':
        parser['encoding'] = options.encoding
    if dataset['format'] == 'csv':
        parser.update(csv_delimiter=options.csv_delimiter, csv_header=options.csv_header)
    if dataset['format'] == 's3-access-log':
        parser.update(null_marker='-', timestamp_output='utc-datetime',
                      optional_trailing_fields='null', unknown_trailing_fields='extra_fields',
                      url_decode=False)
    selector = {
        'version': 1, 'bucket': options.bucket, 'prefix': options.prefix, 'include': list(options.include),
        'exclude': list(options.exclude), 'format_policy': options.format,
        'format': dataset['format'], 'partition_style': options.partition_style,
        'location_pattern': dataset['location_pattern'], 'partitions': [dict(p) for p in dataset['partitions']],
    }
    blockers = []
    if dataset['failures']:
        blockers.append('sample_failures')
    if not dataset['samples'] or dataset['schema']['type'] != 'struct':
        blockers.append('no_record_schema')
    if unresolved(dataset['schema']):
        blockers.append('unresolved_types')
    warnings = []
    if dataset['sample_files_limited']:
        warnings.append('unsampled_objects')
    if any(s['limits'] for s in dataset['samples']):
        warnings.append('limited_record_coverage')
    dataset['ingestion'] = {'adapter': 's3-objects', 'selector': selector, 'parser': parser,
                            'ready': not blockers, 'blockers': blockers, 'warnings': warnings,
                            'schema_policy': 'validate-records', 'snapshot': False}
    dataset['schema_fingerprint'] = schema_fingerprint(dataset['schema'], parser)
    dataset['schema_origin'] = 'format-contract' if dataset['format'] == 's3-access-log' else (
        'parquet-metadata' if dataset['format'] == 'parquet' else 'sample-inference')
