"""Stable directory partition recognition shared by discovery and selection."""
from datetime import date
from dataclasses import dataclass
from functools import cached_property
import re

from .exceptions import UserError


def group_key(key: str, style: str):
    directories = key.split("/")[:-1]
    normalized, partitions = [], []
    index = 0
    while index < len(directories):
        part = directories[index]
        if style != "none" and re.fullmatch(r"[^=]+=[^=]+", part):
            name, _ = part.split("=", 1)
            normalized.append(name + "={value}")
            partitions.append({"name": name, "type": "string", "layout": "hive", "segment": index})
        elif style == "auto" and re.fullmatch(r"\d{4}-\d{2}-\d{2}", part) and valid_date(part):
            normalized.append("{date}")
            partitions.append({"name": "date", "type": "date", "layout": "YYYY-MM-DD", "segment": index})
        elif (style == "auto" and index + 2 < len(directories)
              and re.fullmatch(r"\d{4}/\d{2}/\d{2}", "/".join(directories[index:index + 3]))
              and valid_date("-".join(directories[index:index + 3]))):
            normalized.extend(["{year}", "{month}", "{day}"])
            for offset, name in enumerate(("year", "month", "day")):
                partitions.append({"name": name, "type": "string", "layout": "YYYY/MM/DD", "segment": index + offset})
            index += 2
        else:
            normalized.append(part)
        index += 1
    return "/".join(normalized), partitions


def valid_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def hive_values(key, names):
    """Extract requested literal Hive values, rejecting ambiguous directory keys."""
    values = {}
    for segment in key.split('/')[:-1]:
        name, separator, value = segment.partition('=')
        if separator and name in names:
            if name in values:
                raise UserError(f'Ambiguous duplicate partition {name!r} in object {key!r}.')
            values[name] = value
    return values


@dataclass(frozen=True)
class HiveFilter:
    partitions: tuple[str, ...] = ()
    date_names: tuple[str, ...] = ()
    start: str | None = None
    end: str | None = None

    def __post_init__(self):
        for expression in self.partitions:
            if not isinstance(expression, str):
                raise UserError('Partition predicates must be key=value strings.')
            name, separator, value = expression.partition('=')
            if not separator or not name or not value or '/' in expression:
                raise UserError('Partition predicates require nonempty key=value directory segments.')
        if self.date_names:
            if (len(self.date_names) != 3
                    or any(not isinstance(n, str) or not n or '/' in n or '=' in n for n in self.date_names)
                    or len(set(self.date_names)) != 3):
                raise UserError('Partition dates require three distinct names in year,month,day order.')
            for bound in (self.start, self.end):
                if not isinstance(bound, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', bound) or not valid_date(bound):
                    raise UserError('Partition date bounds must be valid YYYY-MM-DD dates.')
            if self.start >= self.end:
                raise UserError('Partition --from must precede exclusive --to.')
        elif self.start is not None or self.end is not None:
            raise UserError('Date bounds require --partition-date.')

    @cached_property
    def values(self):
        values = {}
        for expression in self.partitions:
            name, value = expression.split('=', 1)
            values.setdefault(name, set()).add(value)
        return values

    def matches(self, key):
        values = hive_values(key, self.values.keys() | set(self.date_names))
        if any(values.get(name) not in allowed for name, allowed in self.values.items()):
            return False
        if not self.date_names:
            return True
        if any(name not in values for name in self.date_names):
            return False
        try:
            value = calendar_partition_date(values, self.date_names)
        except UserError as exc:
            raise UserError(f'Invalid calendar partition date in object {key!r}.') from exc
        return date.fromisoformat(self.start) <= value < date.fromisoformat(self.end)


def calendar_partition_date(values, names):
    """Normalize numeric calendar components shared by schema and source filters."""
    try:
        parts = [values[name] for name in names]
        if not all(isinstance(value, str) and re.fullmatch(r'\d+', value) for value in parts):
            raise ValueError()
        return date(*map(int, parts))
    except (KeyError, ValueError, TypeError, OverflowError) as exc:
        raise UserError('Invalid calendar date in source partitions.') from exc
