"""Conservative CSV header detection with explicit ambiguity reporting."""
import re
from .schema import csv_value
from .temporal import temporal_kind


def value_kind(value):
    converted = csv_value(value)
    return temporal_kind(value) or type(converted).__name__


def detect_header(rows):
    if len(rows) < 2 or any(len(row) != len(rows[0]) for row in rows):
        return 'absent', True
    first, rest = rows[0], rows[1:]
    labels = len(set(first)) == len(first) and all(
        re.fullmatch(r'[A-Za-z_][A-Za-z0-9_ .-]*', value) and value_kind(value) == 'str' for value in first)
    columns = list(zip(*rest))
    column_kinds = [{value_kind(v) for v in column} - {'NoneType'} for column in columns]
    stable_typed = [len(kinds) == 1 and 'str' not in kinds for kinds in column_kinds]
    if labels and any(stable_typed):
        return 'present', False
    same_types = all(len({value_kind(row[index]) for row in rows}) == 1 for index in range(len(first)))
    if same_types and any(value_kind(v) not in ('str', 'NoneType') for v in first):
        return 'absent', False
    return 'absent', True
