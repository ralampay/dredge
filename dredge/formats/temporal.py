"""Strict, locale-independent temporal assumptions and declared conversions."""
from datetime import date, datetime, timezone
import re

from ..core.exceptions import UserError


def temporal_kind(value):
    if not isinstance(value, str):
        return None
    try:
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
            date.fromisoformat(value)
            return 'iso-date'
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})', value):
            datetime.fromisoformat(value.replace('Z', '+00:00'))
            return 'iso-timestamp-offset'
    except ValueError:
        pass
    return None


def convert_record(value, schema, path='$'):
    """Convert only schema-declared temporal strings; leave other values intact."""
    if value is None:
        return value
    rule = schema.get('input_format')
    if rule:
        if temporal_kind(value) != rule:
            raise UserError(f'Schema drift at {path}: expected {rule}.')
        if rule == 'iso-date':
            return date.fromisoformat(value)
        return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
    if schema['type'] == 'struct' and isinstance(value, dict):
        fields = {field['name']: field for field in schema['fields']}
        return {key: convert_record(item, fields[key], path + '/' + key) if key in fields else item
                for key, item in value.items()}
    if schema['type'] == 'list' and isinstance(value, list):
        return [convert_record(item, schema['items'], path + '/*') for item in value]
    return value
