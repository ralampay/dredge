"""Pure recursive schema inference and merging; no record values are retained."""
from decimal import Decimal
import json
import math
import re


def infer(value):
    if value is None:
        return {"type": "unknown", "nullable": True}
    if isinstance(value, bool):
        kind = "boolean"
    elif isinstance(value, int):
        kind = "integer"
    elif isinstance(value, (float, Decimal)):
        kind = "float"
    elif isinstance(value, str):
        kind = "string"
    elif isinstance(value, dict):
        return {"type": "struct", "nullable": False,
                "fields": [{"name": k, **infer(v)} for k, v in value.items()]}
    elif isinstance(value, list):
        item = {"type": "unknown", "nullable": False}
        for element in value:
            item = merge(item, infer(element))
        return {"type": "list", "nullable": False, "items": item}
    else:
        raise TypeError(f"Unsupported inference type: {type(value).__name__}")
    return {"type": kind, "nullable": False}


def merge(left, right):
    nullable = left.get("nullable", False) or right.get("nullable", False)
    a, b = left["type"], right["type"]
    if a == "unknown" or b == "unknown":
        return {**(right if a == "unknown" else left), "nullable": nullable}
    if a == b and a in ("integer", "float") and left.get("storage_type") == right.get("storage_type"):
        return {**left, "nullable": nullable}
    if {a, b} <= {"integer", "float"}:
        return {"type": "float" if "float" in (a, b) else "integer", "nullable": nullable}
    if a == b == "struct":
        first = {f["name"]: {k: v for k, v in f.items() if k != "name"} for f in left["fields"]}
        second = {f["name"]: {k: v for k, v in f.items() if k != "name"} for f in right["fields"]}
        fields = []
        for name in dict.fromkeys([*first, *second]):
            value = merge(first[name], second[name]) if name in first and name in second else {
                **(first.get(name) or second[name]), "nullable": True}
            fields.append({"name": name, **value})
        return {"type": "struct", "nullable": nullable, "fields": fields}
    if a == b == "list":
        return {"type": "list", "nullable": nullable, "items": merge(left["items"], right["items"])}
    l = {k: v for k, v in left.items() if k != "nullable"}
    r = {k: v for k, v in right.items() if k != "nullable"}
    if l == r:
        return {**l, "nullable": nullable}
    alternatives = []
    for node in (l, r):
        alternatives.extend(node["alternatives"] if node["type"] == "union" else [node])
    unique = {json.dumps(node, sort_keys=True): node for node in alternatives}
    return {"type": "union", "nullable": nullable, "alternatives": [unique[k] for k in sorted(unique)]}


def conflicts(schema, path=""):
    result = []
    if schema["type"] == "union":
        result.append({"path": path, "reason": "incompatible_types"})
    for field in schema.get("fields", []):
        result.extend(conflicts(field, path + "/" + field["name"].replace("~", "~0").replace("/", "~1")))
    if "items" in schema:
        result.extend(conflicts(schema["items"], path + "/*"))
    return result


def csv_value(value):
    if value == "":
        return None
    if value.lower() in ("true", "false"):
        return value.lower() == "true"
    if re.fullmatch(r"-?(0|[1-9]\d*)", value):
        return int(value)
    if re.fullmatch(r"-?(0|[1-9]\d*)(\.\d+([eE][+-]?\d+)?|[eE][+-]?\d+)", value):
        number = float(value)
        if math.isfinite(number):
            return number
    return value


def arrow_schema(schema):
    import pyarrow as pa

    def node(t, nullable):
        result = {"type": str(t), "nullable": nullable}
        if pa.types.is_struct(t):
            result.update(type="struct", fields=[field(f) for f in t])
        elif pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
            result.update(type="list", items=node(t.value_type, t.value_field.nullable))
        elif pa.types.is_decimal(t):
            result.update(type="decimal", precision=t.precision, scale=t.scale)
        elif pa.types.is_timestamp(t):
            result.update(type="timestamp", unit=t.unit, timezone=t.tz)
        elif pa.types.is_integer(t):
            result.update(type="integer", storage_type=str(t))
        elif pa.types.is_floating(t):
            result.update(type="float", storage_type=str(t))
        elif pa.types.is_boolean(t):
            result["type"] = "boolean"
        elif pa.types.is_string(t) or pa.types.is_large_string(t):
            result["type"] = "string"
        elif pa.types.is_date(t):
            result['type'] = 'date'
        elif pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
            result['type'] = 'binary'
        elif pa.types.is_null(t):
            result["type"] = "unknown"
        return result

    def field(f):
        return {"name": f.name, **node(f.type, f.nullable)}

    return {"type": "struct", "nullable": False, "fields": [field(f) for f in schema]}
