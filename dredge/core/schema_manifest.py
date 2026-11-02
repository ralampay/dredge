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


def schema_fingerprint(schema, parser):
    return hashlib.sha256(json.dumps({'schema': schema, 'parser': parser}, sort_keys=True).encode()).hexdigest()


def validate_node(node, depth=0):
    require(depth < 50 and isinstance(node, dict), 'invalid or overly deep field schema')
    require(isinstance(node.get('type'), str) and isinstance(node.get('nullable'), bool), 'field type/nullable required')
    allowed = {'name', 'type', 'nullable', 'fields', 'items', 'alternatives', 'precision', 'scale', 'unit', 'timezone', 'storage_type'}
    require(set(node) <= allowed, 'unknown field-schema attributes')
    for key in ('name', 'unit', 'storage_type'):
        require(key not in node or isinstance(node[key], str), 'invalid field ' + key)
    require(node.get('timezone') is None or isinstance(node['timezone'], str), 'invalid timezone')
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
            validate_node(field, depth + 1)
    elif node['type'] == 'list':
        validate_node(node.get('items'), depth + 1)
    elif node['type'] == 'union':
        require(isinstance(node.get('alternatives'), list) and len(node['alternatives']) > 1, 'invalid union')
        for alternative in node['alternatives']:
            require(isinstance(alternative, dict), 'invalid union alternative')
            validate_node({**alternative, 'nullable': False}, depth + 1)


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


def _validate_dataset(dataset, source):
    validate_node(dataset.get('schema'))
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
    require(dataset.get('schema_fingerprint') == schema_fingerprint(dataset['schema'], parser), 'schema/parser fingerprint mismatch')


class SchemaManifest:
    def __init__(self, document):
        require(isinstance(document, dict), 'document must be a mapping')
        require(document.get('schema_version') == 2, 'requires schema_version 2; rerun discovery for legacy reports')
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
            _validate_dataset(dataset, source)
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
