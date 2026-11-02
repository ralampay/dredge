"""Translate executable contracts to Arrow without inferring batch schemas."""
from decimal import Decimal
import math
import struct

from ..core.exceptions import UserError


def arrow_type(node):
    import pyarrow as pa
    kind = node['type']
    if kind == 'struct':
        if not node['fields']:
            raise UserError('Empty structs cannot be stored in Parquet; resolve the schema first.')
        return pa.struct([arrow_field(f) for f in node['fields']])
    if kind == 'list':
        return pa.list_(pa.field('element', arrow_type(node['items']), nullable=node['items']['nullable']))
    if kind == 'decimal':
        precision = node['precision']
        return (pa.decimal128 if precision <= 38 else pa.decimal256)(precision, node['scale'])
    if kind == 'timestamp':
        return pa.timestamp(node['unit'], tz=node.get('timezone'))
    if kind in ('integer', 'float'):
        storage = node.get('storage_type', 'int64' if kind == 'integer' else 'double')
        allowed = {'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64'} if kind == 'integer' else {'halffloat', 'float', 'double'}
        if storage not in allowed:
            raise UserError('Unsupported numeric storage type: ' + storage)
        if storage == 'halffloat':
            # Parquet cannot represent half precision natively; widening is lossless.
            return pa.float32()
        return pa.type_for_alias(storage)
    factories = {'string': pa.string, 'boolean': pa.bool_, 'date': pa.date32, 'binary': pa.binary}
    if kind not in factories:
        raise UserError('Unresolved or unsupported output type: ' + kind)
    return factories[kind]()


def arrow_field(node):
    import pyarrow as pa
    return pa.field(node['name'], arrow_type(node), nullable=node['nullable'])


def output_schema(dataset):
    import pyarrow as pa
    try:
        fields = [arrow_field(f) for f in dataset['schema']['fields']]
        names = {f.name for f in fields}
        mapping = []
        for partition in dataset['ingestion']['selector']['partitions']:
            name = '__dredge_partition_' + str(partition['segment'])
            if name in names:
                raise UserError('Partition output column collides with record field: ' + name)
            names.add(name)
            fields.append(pa.field(name, pa.string(), nullable=False))
            mapping.append({**partition, 'column': name})
        if not fields:
            raise UserError('Parquet output requires at least one field.')
        return pa.schema(fields), mapping
    except (ValueError, TypeError, OverflowError) as exc:
        raise UserError('Schema cannot be represented safely in Parquet.') from exc


def normalize_value(value, node, path='$'):
    """Reject lossy numeric/temporal conversions before Arrow constructs arrays."""
    if value is None:
        return None
    kind = node['type']
    if kind == 'struct':
        return {f['name']: normalize_value(value.get(f['name']), f, path + '/' + f['name']) for f in node['fields']}
    if kind == 'list':
        return [normalize_value(v, node['items'], path + '/*') for v in value]
    if kind == 'float':
        result = float(value)
        storage = node.get('storage_type', 'double')
        try:
            if storage in ('float', 'halffloat'):
                result = struct.unpack('f', struct.pack('f', result))[0]
        except OverflowError as exc:
            raise UserError('Numeric overflow at ' + path) from exc
        # Decimal JSON numbers are intentionally represented as IEEE floats by
        # the discovery contract. Integers and native floats must not lose bits.
        if not math.isfinite(result) or (not isinstance(value, Decimal) and result != value):
            raise UserError('Lossy numeric conversion at ' + path)
        return result
    if kind == 'timestamp':
        aware = value.utcoffset() is not None
        if aware != bool(node.get('timezone')):
            raise UserError('Timestamp timezone differs from contract at ' + path)
        nanos = value.microsecond * 1000 + getattr(value, 'nanosecond', 0)
        divisor = {'s': 10**9, 'ms': 10**6, 'us': 1000, 'ns': 1}[node['unit']]
        if nanos % divisor:
            raise UserError('Timestamp precision would be lost at ' + path)
    return value
