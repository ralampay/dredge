"""Pure field-path traversal and explicit resolution transformations."""
from copy import deepcopy
from decimal import Decimal
import json
import math

from ..core.exceptions import UserError
from .json_fallback import encode_json, fallback_target


def path_key(path):
    if not isinstance(path, list) or not path:
        raise UserError('Resolution paths must contain field/items segments.')
    result = []
    for segment in path:
        if isinstance(segment, dict) and set(segment) == {'field'} and isinstance(segment['field'], str):
            result.append(('field', segment['field']))
        elif isinstance(segment, dict) and set(segment) == {'items'} and segment['items'] is True:
            result.append(('items', True))
        else:
            raise UserError('Invalid resolution path segment.')
    return tuple(result)


def path_label(path):
    return '$' + ''.join('[' + json.dumps(s['field']) + ']' if 'field' in s else '[*]' for s in path)


def validate_rules(rules):
    if not isinstance(rules, list):
        raise UserError('Resolutions must be a list.')
    indexed = {}
    for rule in rules:
        if not isinstance(rule, dict) or rule.get('op') not in ('assert_type', 'extract_number', 'encode_json'):
            raise UserError('Unsupported resolution operation.')
        keys = {'op', 'path', 'members'} if rule['op'] == 'extract_number' else {'op', 'path'}
        if set(rule) != keys:
            raise UserError('Unexpected or missing resolution attributes.')
        key = path_key(rule['path'])
        if any(key[:len(other)] == other or other[:len(key)] == key for other in indexed):
            raise UserError('Resolution paths must not overlap or repeat.')
        if rule['op'] == 'extract_number':
            members = rule['members']
            if (not isinstance(members, list) or not members or
                    not all(isinstance(m, str) and m for m in members) or len(set(members)) != len(members)):
                raise UserError('Numeric extraction requires unique member names.')
        indexed[key] = rule
    return indexed


def meaningful(value):
    if isinstance(value, dict):
        return any(meaningful(v) for v in value.values())
    if isinstance(value, list):
        return any(meaningful(v) for v in value)
    return value is not None


def extract_number(value, members, label):
    if value is None:
        return None
    if isinstance(value, dict):
        if set(value) - set(members):
            raise UserError('Unexpected numeric-wrapper members at ' + label)
        populated = [v for v in value.values() if v is not None]
        if len(populated) != 1:
            raise UserError('Numeric extraction requires exactly one non-null member at ' + label)
        value = populated[0]
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise UserError('Expected a numeric scalar at ' + label)
    try:
        converted = float(value)
    except (OverflowError, ValueError) as exc:
        raise UserError('Numeric overflow at ' + label) from exc
    if not math.isfinite(converted) or converted != value:
        raise UserError('Non-finite or lossy numeric extraction at ' + label)
    return converted


def apply_resolutions(record, rules, on_applied=None):
    """Transform only declared paths; leave structural drift to validation."""
    def walk(value, path, rule, index):
        if not path:
            if rule['op'] == 'extract_number':
                result = extract_number(value, rule['members'], path_label(rule['path']))
            elif rule['op'] == 'encode_json':
                result = encode_json(value, path_label(rule['path']))
            else:
                result = value
            if on_applied and rule['op'] != 'encode_json' and meaningful(result):
                on_applied(index)
            return result
        segment, *rest = path
        if 'field' in segment and isinstance(value, dict) and segment['field'] in value:
            return {**value, segment['field']: walk(value[segment['field']], rest, rule, index)}
        if 'items' in segment and isinstance(value, list):
            return [walk(item, rest, rule, index) for item in value]
        return value
    for index, rule in enumerate(rules):
        record = walk(record, rule['path'], rule, index)
        # JSON is an encoding policy, not a source-type assertion. Null or
        # absent nullable fields do not require non-null type evidence.
        if on_applied and rule['op'] == 'encode_json':
            on_applied(index)
    return record


def compile_schema(original, edited, rules):
    """Only unknown assertions and explicit numeric-union resolution may edit schemas."""
    from ..core.schema_manifest import validate_node
    indexed = validate_rules(rules)
    operations = []
    metadata = {'inference', 'source'}

    def visit(before, after, path):
        if not isinstance(after, dict):
            raise UserError('Invalid edited schema at ' + path_label(path))
        if before == after:
            return deepcopy(before)
        kind = before['type']
        rule = indexed.get(path_key(path)) if path else None
        if rule is not None and rule['op'] == 'encode_json':
            if not fallback_target(before):
                raise UserError('JSON fallback requires an unresolved field or array at ' + path_label(path))
            clean = {k: deepcopy(v) for k,v in before.items() if k in ('name', 'nullable', 'source')}
            clean['type'] = 'string'
            for key in metadata:
                if key in after and after[key] != before.get(key):
                    raise UserError('Discovery evidence cannot be edited at ' + path_label(path))
            if {k:v for k,v in after.items() if k not in metadata} != {k:v for k,v in clean.items() if k not in metadata}:
                raise UserError('JSON fallback must preserve names/nullability and output a string at ' + path_label(path))
            operations.append(deepcopy(rule))
            return clean
        if kind in ('unknown', 'union'):
            if after.get('name') != before.get('name') or after.get('nullable') != before['nullable']:
                raise UserError('Resolution must preserve field names and nullability at ' + path_label(path))
            for name in metadata:
                if name in after and after[name] != before.get(name):
                    raise UserError('Discovery evidence cannot be edited at ' + path_label(path))
            rule = indexed.get(path_key(path))
            if kind == 'unknown':
                rule = rule or {'op': 'assert_type', 'path': path}
                if rule['op'] != 'assert_type':
                    raise UserError('Unknown types require a type assertion.')
            elif rule is None or rule['op'] != 'extract_number' or after.get('type') != 'float':
                raise UserError('Union resolution requires explicit numeric extraction to float at ' + path_label(path))
            clean = {k: deepcopy(v) for k, v in after.items() if k not in metadata}
            validate_node(clean, version=3)
            check_shape(clean)
            if kind == 'union':
                check_numeric_union(before, rule['members'])
                if clean.get('storage_type', 'double') != 'double':
                    raise UserError('Numeric extraction requires float64 output.')
            if 'source' in before:
                clean['source'] = deepcopy(before['source'])
            operations.append(deepcopy(rule))
            return clean
        children = {'fields'} if kind == 'struct' else {'items'} if kind == 'list' else set()
        if {k:v for k,v in before.items() if k not in children} != {k:v for k,v in after.items() if k not in children}:
            raise UserError('Only unresolved field types may be edited at ' + path_label(path))
        result = deepcopy(before)
        if kind == 'struct':
            fields = after.get('fields')
            if not isinstance(fields, list) or [f.get('name') for f in fields if isinstance(f, dict)] != [f['name'] for f in before['fields']] or len(fields) != len(before['fields']):
                raise UserError('Field names and ordering must be preserved.')
            result['fields'] = [visit(a, b, path + [{'field': a['name']}]) for a,b in zip(before['fields'], fields)]
        elif kind == 'list':
            result['items'] = visit(before['items'], after.get('items'), path + [{'items': True}])
        return result

    output = visit(original, edited, [])
    used = {path_key(rule['path']) for rule in operations}
    if set(indexed) - used:
        raise UserError('Resolution rules must target an edited unresolved field.')
    validate_rules(operations)
    return output, operations


def check_shape(node):
    """Reject stale union attributes and implicit conversions in asserted schemas."""
    extras = {'struct': {'fields'}, 'list': {'items'}, 'decimal': {'precision', 'scale'},
              'timestamp': {'unit', 'timezone'}, 'integer': {'storage_type'}, 'float': {'storage_type'},
              'string': set(), 'boolean': set(), 'date': set(), 'binary': set()}
    kind = node['type']
    if kind not in extras or set(node) - ({'type', 'name', 'nullable'} | extras[kind]):
        raise UserError('Asserted schemas must use supported types and matching attributes only.')
    for field in node.get('fields', []):
        check_shape(field)
    if 'items' in node:
        check_shape(node['items'])


def check_numeric_union(node, members):
    for alternative in node['alternatives']:
        kind = alternative['type']
        if kind in ('integer', 'float', 'decimal'):
            continue
        if kind != 'struct' or not alternative.get('fields'):
            raise UserError('Numeric extraction can resolve only numeric scalars and numeric wrappers.')
        for field in alternative['fields']:
            null_placeholder = field['type'] == 'unknown' and field.get('nullable') is True
            if field['name'] not in members or (field['type'] not in ('integer', 'float', 'decimal') and not null_placeholder):
                raise UserError('Configured members must cover the numeric wrapper fields.')
