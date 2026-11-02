"""Validate records against a discovered schema without coercing or dropping data."""
from datetime import date, datetime
from decimal import Decimal
import math

from ..core.exceptions import UserError


SUPPORTED_TYPES = {'string', 'integer', 'float', 'boolean', 'timestamp', 'date', 'binary', 'decimal', 'struct', 'list'}


def validate_record(value, node, path='$'):
    kind = node['type']
    if value is None:
        if node['nullable']:
            return
        raise UserError(f'Schema drift at {path}: unexpected null or missing field.')
    valid = True
    if kind == 'struct':
        valid = isinstance(value, dict)
        if valid:
            known = {f['name'] for f in node['fields']}
            unexpected = set(value) - known
            if unexpected:
                names = ', '.join(repr(name) for name in sorted(unexpected, key=str)[:20])
                if len(unexpected) > 20:
                    names += f', ... ({len(unexpected)} total)'
                raise UserError(f'Schema drift at {path}: unexpected fields: {names}; '
                                'expand discovery sampling and regenerate the schema.')
            for field in node['fields']:
                validate_record(value.get(field['name']), field, path + '/' + field['name'])
    elif kind == 'list':
        valid = isinstance(value, list)
        if valid:
            for item in value:
                validate_record(item, node['items'], path + '/*')
    elif kind == 'integer':
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif kind == 'float':
        valid = isinstance(value, (int, float, Decimal)) and not isinstance(value, bool) and math.isfinite(value)
    elif kind == 'string':
        valid = isinstance(value, str)
    elif kind == 'boolean':
        valid = isinstance(value, bool)
    elif kind == 'timestamp':
        valid = isinstance(value, datetime)
        if valid and node.get('timezone'):
            valid = value.tzinfo is not None
    elif kind == 'date':
        valid = isinstance(value, date) and not isinstance(value, datetime)
    elif kind == 'binary':
        valid = isinstance(value, bytes)
    elif kind == 'decimal':
        valid = isinstance(value, Decimal)
    else:
        raise UserError(f'Unresolved or unsupported schema type at {path}; rerun discovery or resolve the schema.')
    if not valid:
        raise UserError(f'Schema drift at {path}: expected {kind}; rerun discovery.')
