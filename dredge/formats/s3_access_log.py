"""S3 server-access-log grammar and declared field schema.

Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
No source values are included in parsing errors.
"""
from datetime import datetime, timedelta, timezone
import re


FIELD_NAMES = (
    "bucket_owner", "bucket", "request_time", "remote_ip", "requester", "request_id",
    "operation", "key", "request_uri", "http_status", "error_code", "bytes_sent",
    "object_size", "total_time_ms", "turn_around_time_ms", "referer", "user_agent",
    "version_id", "host_id", "signature_version", "cipher_suite", "authentication_type",
    "host_header", "tls_version", "access_point_arn", "acl_required", "source_region",
)
INTEGERS = {"http_status", "bytes_sent", "object_size", "total_time_ms", "turn_around_time_ms"}
# Anchor quoted fields by their surrounding fixed-position fields. AWS permits
# unescaped quotes inside request URI, referer, and user-agent.
TOKEN = r'(\S+)'
QUOTED = r'(".*"|-)'
RECORD = re.compile(r'^' + r'\s+'.join(
    [TOKEN, TOKEN, r'(\[[^\]]+\]|-)'] + [TOKEN] * 5 + [QUOTED] +
    [TOKEN] * 6 + [r'(".*?"|-)', QUOTED] + [r'([^"\s]+(?:\s+.*)?)']) + r'$')
MONTHS = {name: i + 1 for i, name in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))}


def parse_timestamp(value):
    match = re.fullmatch(r'\[(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})\]', value)
    if not match or match[2] not in MONTHS:
        raise ValueError("Invalid access-log timestamp")
    day, month, year, hour, minute, second, sign, zh, zm = match.groups()
    if int(zh) > 23 or int(zm) > 59:
        raise ValueError("Invalid timestamp offset")
    offset = timedelta(hours=int(zh), minutes=int(zm)) * (-1 if sign == '-' else 1)
    return datetime(int(year), MONTHS[month], int(day), int(hour), int(minute), int(second),
                    tzinfo=timezone(offset)).astimezone(timezone.utc)


def parse_record(line):
    match = RECORD.fullmatch(line.strip())
    if not match:
        raise ValueError("Invalid S3 access-log record structure")
    values = list(match.groups()[:-1]) + match.groups()[-1].split()
    if len(values) < 18:
        raise ValueError("Access-log record has fewer than 18 fields")
    record = {}
    for index, name in enumerate(FIELD_NAMES):
        value = values[index] if index < len(values) else '-'
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value == '-':
            value = None
        elif name in INTEGERS:
            if not re.fullmatch(r'\d+', value):
                raise ValueError("Invalid numeric access-log field")
            value = int(value)
        elif name == 'request_time':
            value = parse_timestamp(value)
        record[name] = value
    if record['operation'] is not None and not re.fullmatch(r'(?:SOAP|REST|WEBSITE|BATCH|S3)\.\S+', record['operation']):
        raise ValueError('Invalid access-log operation')
    if record['signature_version'] is not None and not re.fullmatch(r'SigV\d+', record['signature_version']):
        raise ValueError('Invalid access-log signature version or shifted fields')
    record['extra_fields'] = values[len(FIELD_NAMES):]
    return record


def access_log_schema():
    fields = []
    for name in FIELD_NAMES:
        node = {"name": name, "type": "integer" if name in INTEGERS else "string", "nullable": True}
        if name == 'request_time':
            node.update(type="timestamp", unit="us", timezone="UTC")
        fields.append(node)
    fields.append({"name": "extra_fields", "type": "list", "nullable": False,
                   "items": {"type": "string", "nullable": False}})
    return {"type": "struct", "nullable": False, "fields": sorted(fields, key=lambda f: f['name'])}
