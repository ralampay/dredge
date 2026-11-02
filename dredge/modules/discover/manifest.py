"""Build ingestion contracts from discovery evidence, without provider imports."""
from ...core.schema_manifest import schema_fingerprint

from ...formats.validation import SUPPORTED_TYPES
from ...formats.evidence import annotate
from ...formats.s3_access_log import FIELD_NAMES


def unresolved(schema):
    if schema['type'] not in SUPPORTED_TYPES:
        return True
    return any(unresolved(f) for f in schema.get('fields', [])) or (
        'items' in schema and unresolved(schema['items']))


def attach_contract(dataset, options, evidence, layouts):
    dataset['name'] = (dataset['location_pattern'].rstrip('/') or 'root') + ':' + dataset['format']
    parser = {"adapter": dataset['format'], "version": 1}
    if dataset['format'] != 'parquet':
        parser['encoding'] = options.encoding
    header_policies = {layout['csv_header'] for layout in layouts if 'csv_header' in layout}
    if dataset['format'] == 'csv':
        parser.update(csv_delimiter=options.csv_delimiter,
                      csv_header=next(iter(header_policies)) if len(header_policies) == 1 else 'absent')
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
    origin = 'format-contract' if dataset['format'] == 's3-access-log' else (
        'parquet-metadata' if dataset['format'] == 'parquet' else 'sample-inference')
    source_kind = {'csv': 'csv-header' if parser.get('csv_header') == 'present' else 'generated-position',
                   'json': 'json-key', 'jsonl': 'json-key', 'parquet': 'parquet-field',
                   's3-access-log': 'format-field'}[dataset['format']]
    positions = {}
    if dataset['format'] in ('csv', 'parquet'):
        for layout in layouts:
            for index, name in enumerate(layout.get('columns', [])):
                positions.setdefault(name, set()).add(index)
    elif dataset['format'] == 's3-access-log':
        positions = {name: {index} for index, name in enumerate(FIELD_NAMES)}
    dataset['schema'] = annotate(dataset['schema'], None if dataset['format'] == 'parquet' else evidence,
                                 origin=origin, temporal_types=options.temporal_types if origin == 'sample-inference' else 'off',
                                 source_kind=source_kind, positions=positions)
    if dataset['format'] == 'csv':
        for field in dataset['schema'].get('fields', []):
            field['source']['assumed'] = options.csv_header == 'auto'
    blockers = []
    if any(layout.get('header_ambiguous') for layout in layouts):
        blockers.append('ambiguous_csv_header_use_explicit_override')
    if len(header_policies) > 1:
        blockers.append('inconsistent_csv_header_presence')
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
    dataset['schema_fingerprint'] = schema_fingerprint(dataset['schema'], parser, version=3)
    dataset['schema_origin'] = 'format-contract' if dataset['format'] == 's3-access-log' else (
        'parquet-metadata' if dataset['format'] == 'parquet' else 'sample-inference')

    # Contract first; evidence and crawl diagnostics follow the executable fields.
    order = ('id', 'name', 'location_pattern', 'format', 'compression', 'partitions',
             'ingestion', 'schema_origin', 'schema', 'schema_fingerprint', 'matched_objects',
             'sample_files_limited', 'conflicts', 'failures', 'samples')
    ordered = {key: dataset[key] for key in order}
    dataset.clear()
    dataset.update(ordered)
