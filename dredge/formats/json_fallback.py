"""Explicit JSON-string fallback without guessing unresolved source types."""
from decimal import Decimal
import json
import math

from ..core.exceptions import UserError


def has_unresolved(node):
    return (node['type'] in ('unknown', 'union') or
            (node['type'] == 'struct' and not node['fields']) or
            any(has_unresolved(field) for field in node.get('fields', [])) or
            ('items' in node and has_unresolved(node['items'])))


def fallback_target(node):
    return (node['type'] in ('unknown', 'union') or
            (node['type'] == 'struct' and not node['fields']) or
            (node['type'] == 'list' and has_unresolved(node)))


def encode_json(value, label):
    """Keep top-level nulls native; encode JSON values, including exact Decimal numbers."""
    if value is None:
        return None

    def encode(item):
        if item is None or isinstance(item, (str, bool, int)):
            return json.dumps(item, ensure_ascii=True, allow_nan=False, separators=(',', ':'))
        if isinstance(item, Decimal):
            if not item.is_finite():
                raise ValueError('non-finite decimal')
            return str(item)
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError('non-finite float')
            return json.dumps(item, allow_nan=False)
        if isinstance(item, list):
            return '[' + ','.join(encode(element) for element in item) + ']'
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            return '{' + ','.join(json.dumps(key, ensure_ascii=True) + ':' + encode(item[key])
                                  for key in sorted(item)) + '}'
        raise ValueError('unsupported JSON value')

    try:
        return encode(value)
    except (ValueError, TypeError, OverflowError, RecursionError) as exc:
        raise UserError('Cannot JSON-encode field at ' + label + '; expected finite JSON-compatible values.') from exc
