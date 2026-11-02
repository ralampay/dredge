"""Versioned discovery contract loader shared by discovery and ingestion."""
from copy import deepcopy
import codecs
import hashlib
import json
from pathlib import Path

from .exceptions import UserError


FORMATS = {'csv', 'json', 'jsonl', 'parquet', 's3-access-log'}


def require(condition, message):
    if not condition:
        raise UserError('Invalid schema manifest: ' + message)


def schema_fingerprint(schema, parser, version=2, resolutions=None):
    def executable(node):
        if isinstance(node, dict):
            return {key: executable(value) for key, value in node.items() if key not in ('inference', 'source')}
        if isinstance(node, list):
            return [executable(value) for value in node]
        return node
    contract = executable(schema) if version >= 3 else schema
    executable_contract = {'schema': contract, 'parser': parser}
    if version == 4:
        executable_contract['resolutions'] = resolutions or []
    return hashlib.sha256(json.dumps(executable_contract, sort_keys=True).encode()).hexdigest()


def validate_node(node, depth=0, version=2):
    require(depth < 50 and isinstance(node, dict), 'invalid or overly deep field schema')
    require(isinstance(node.get('type'), str) and isinstance(node.get('nullable'), bool), 'field type/nullable required')
    allowed = {'name', 'type', 'nullable', 'fields', 'items', 'alternatives', 'precision', 'scale', 'unit', 'timezone', 'storage_type'}
    if version >= 3:
        allowed.update(('source', 'inference', 'input_format'))
    require(set(node) <= allowed, 'unknown field-schema attributes')
    for key in ('name', 'unit', 'storage_type'):
        require(key not in node or isinstance(node[key], str), 'invalid field ' + key)
    require(node.get('timezone') is None or isinstance(node['timezone'], str), 'invalid timezone')
    if 'input_format' in node:
        require((node['type'], node['input_format']) in (('date', 'iso-date'), ('timestamp', 'iso-timestamp-offset')),
                'invalid temporal conversion rule')
    if version >= 3 and 'inference' in node:
        info = node['inference']
        require(isinstance(info, dict) and info.get('origin') in ('sample-inference', 'parquet-metadata', 'format-contract'),
                'invalid inference origin')
        for key in ('present_count', 'null_count', 'missing_count'):
            require(key not in info or type(info[key]) is int and info[key] >= 0, 'invalid evidence count')
        if info['origin'] == 'parquet-metadata':
            require(not any(key in info for key in ('present_count', 'null_count', 'missing_count', 'observed_types')),
                    'metadata-only inference cannot claim sampled record counts')
        else:
            observed = info.get('observed_types')
            require(isinstance(observed, dict) and all(isinstance(k, str) and type(v) is int and v >= 0 for k, v in observed.items()),
                    'invalid observed type counts')
            require(info.get('present_count') == sum(observed.values()) and info.get('null_count') == observed.get('null', 0),
                    'inconsistent evidence counts')
    if 'source' in node:
        require(isinstance(node['source'], dict) and isinstance(node['source'].get('kind'), str)
                and isinstance(node['source'].get('path'), str), 'invalid field source')
        positions = node['source'].get('positions', [])
        require(isinstance(positions, list) and all(type(p) is int and p >= 0 for p in positions), 'invalid source positions')
    if node['type'] == 'decimal':
        require(type(node.get('precision')) is int and node['precision'] > 0 and type(node.get('scale')) is int,
                'decimal precision and scale required')
    if node['type'] == 'timestamp':
        require(node.get('unit') in ('s', 'ms', 'us', 'ns'), 'invalid timestamp unit')
    if node['type'] == 'struct':
        fields = node.get('fields')
        require(isinstance(fields, list), 'struct fields must be a list')
        names = set()
        for field in fields:
            require(isinstance(field, dict) and isinstance(field.get('name'), str), 'field name required')
            require(field['name'] not in names, 'duplicate field name')
            names.add(field['name'])
            validate_node(field, depth + 1, version)
    elif node['type'] == 'list':
        validate_node(node.get('items'), depth + 1, version)
    elif node['type'] == 'union':
        require(isinstance(node.get('alternatives'), list) and len(node['alternatives']) > 1, 'invalid union')
        for alternative in node['alternatives']:
            require(isinstance(alternative, dict), 'invalid union alternative')
            validate_node({**alternative, 'nullable': False}, depth + 1, version)


def _validate_parser(parser):
    require(isinstance(parser, dict) and type(parser.get('version')) is int and parser['version'] == 1
            and isinstance(parser.get('adapter'), str) and parser['adapter'] in FORMATS, 'unsupported parser adapter/version')
    if parser['adapter'] != 'parquet':
        require(isinstance(parser.get('encoding'), str), 'text encoding required')
        try:
            codecs.lookup(parser['encoding'])
        except LookupError as exc:
            raise UserError('Invalid schema manifest: unknown encoding') from exc
    if parser['adapter'] == 'csv':
        delimiter = parser.get('csv_delimiter')
        require(isinstance(delimiter, str) and len(delimiter) == 1 and delimiter not in '\r\n\0', 'invalid CSV delimiter')
        require(parser.get('csv_header') in ('present', 'absent'), 'invalid CSV header policy')
    if parser['adapter'] == 's3-access-log':
        expected = dict(null_marker='-', timestamp_output='utc-datetime', optional_trailing_fields='null',
                        unknown_trailing_fields='extra_fields', url_decode=False)
        require(all(parser.get(k) == v for k, v in expected.items()), 'unsupported access-log parser policy')


def _validate_selector(selector, source, parser):
    require(isinstance(selector, dict) and type(selector.get('version')) is int and selector['version'] == 1,
            'unsupported selector version')
    require(selector.get('bucket') == source['bucket'], 'selector/source bucket mismatch')
    require(selector.get('format') == parser['adapter'], 'selector/parser format mismatch')
    require(isinstance(selector.get('format_policy'), str) and selector['format_policy'] in FORMATS | {'auto'},
            'unsupported format policy')
    require(selector.get('partition_style') in ('auto', 'hive', 'none'), 'invalid partition style')
    for key in ('prefix', 'location_pattern'):
        require(isinstance(selector.get(key), str), 'selector ' + key + ' required')
    for key in ('include', 'exclude'):
        require(isinstance(selector.get(key), list) and all(isinstance(s, str) for s in selector[key]), 'invalid selector filters')
    require(isinstance(selector.get('partitions'), list), 'partitions required')
    positions = set()
    for partition in selector['partitions']:
        require(isinstance(partition, dict) and isinstance(partition.get('name'), str), 'invalid partition definition')
        position = partition.get('segment')
        require(type(position) is int and position >= 0 and position not in positions, 'invalid partition position')
        positions.add(position)
        require(partition.get('layout') in ('hive', 'YYYY/MM/DD', 'YYYY-MM-DD'), 'unknown partition layout')


def _validate_dataset(dataset, source, version):
    validate_node(dataset.get('schema'), version=version)
    contract = dataset.get('ingestion')
    require(isinstance(contract, dict) and contract.get('adapter') == 's3-objects', 'unsupported source adapter')
    require(isinstance(contract.get('ready'), bool), 'readiness required')
    parser = contract.get('parser')
    _validate_parser(parser)
    _validate_selector(contract.get('selector'), source, parser)
    require(parser['adapter'] == dataset.get('format'), 'dataset/parser format mismatch')
    require(contract.get('schema_policy') == 'validate-records' and contract.get('snapshot') is False, 'unsupported ingestion policy')
    for key in ('blockers', 'warnings'):
        require(isinstance(contract.get(key), list) and all(isinstance(v, str) for v in contract[key]), 'invalid readiness diagnostics')
    require(contract['ready'] == (not contract['blockers']), 'inconsistent readiness')
    require(isinstance(dataset.get('samples'), list) and all(isinstance(s, dict) and isinstance(s.get('key'), str)
            for s in dataset['samples']), 'sample object keys required')
    if contract['ready']:
        from ..formats.validation import SUPPORTED_TYPES
        pending = [dataset['schema']]
        while pending:
            node = pending.pop()
            require(node['type'] in SUPPORTED_TYPES, 'ready dataset has unresolved or unsupported types')
            pending.extend(node.get('fields', []))
            if 'items' in node:
                pending.append(node['items'])
        require(dataset['schema']['type'] == 'struct' and bool(dataset['samples']) and not dataset.get('failures'),
                'ready dataset lacks successful discovery evidence')
    require(dataset.get('schema_fingerprint') == schema_fingerprint(dataset['schema'], parser, version, dataset.get('resolutions', [])), 'schema/parser fingerprint mismatch')


class SchemaManifest:
    def __init__(self, document):
        require(isinstance(document, dict), 'document must be a mapping')
        require(type(document.get('schema_version')) is int and document['schema_version'] in (2, 3, 4),
                'requires schema_version 2, 3, or 4; rerun discovery for legacy reports')
        require(document.get('status') in ('complete', 'partial'), 'invalid completion status')
        source = document.get('source')
        require(isinstance(source, dict) and source.get('mode') == 's3' and isinstance(source.get('bucket'), str)
                and bool(source['bucket']), 'S3 source bucket required')
        require(source.get('region') is None or isinstance(source['region'], str), 'invalid source region')
        require(isinstance(document.get('datasets'), list), 'datasets must be a list')
        ids = set()
        for dataset in document['datasets']:
            require(isinstance(dataset, dict) and isinstance(dataset.get('id'), str), 'dataset id required')
            require(dataset['id'] not in ids, 'duplicate dataset id')
            ids.add(dataset['id'])
            _validate_dataset(dataset, source, document['schema_version'])
        if document['schema_version'] == 4:
            from .resolution_contract import validate_resolution_document
            validate_resolution_document(document)
        else:
            require('resolution' not in document and all('resolutions' not in d for d in document['datasets']),
                    'resolution rules require a validated version-4 contract')
        self.document = deepcopy(document)

    @classmethod
    def load(cls, path):
        import yaml
        try:
            with Path(path).open(encoding='utf-8') as stream:
                document = yaml.safe_load(stream)
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            raise UserError('Cannot load schema manifest; check its path and YAML syntax.') from exc
        return cls(document)

    def dataset(self, dataset_id):
        for dataset in self.document['datasets']:
            if dataset['id'] == dataset_id:
                return deepcopy(dataset)
        raise UserError('Dataset id is not present in the schema manifest.')
