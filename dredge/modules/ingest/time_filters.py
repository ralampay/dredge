"""Explicit temporal predicates and provider-neutral partition selection."""
from dataclasses import dataclass
from functools import cached_property
from datetime import date, datetime, timezone
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ...core.exceptions import UserError
from ...core.partitioning import calendar_partition_date


def temporal_fields(dataset):
    """V1 filters address literal top-level names; no ambiguous dotted paths."""
    return [field for field in dataset['schema'].get('fields', [])
            if field['type'] in ('date', 'timestamp')]


@dataclass(frozen=True)
class TimeFilter:
    kind: str
    field: str
    start: str
    end: str
    timezone: str = 'UTC'

    def __post_init__(self):
        if not all(isinstance(v, str) and v for v in (self.kind, self.field, self.start, self.end, self.timezone)):
            raise UserError('Filter values must be nonempty strings.')
        if self.kind not in ('partition', 'record'):
            raise UserError('Unknown time filter kind.')
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise UserError('Unknown filter timezone; use an IANA timezone such as UTC.') from exc
        if self.kind == 'partition':
            if len(self.field.split(',')) != 3 or len(set(self.field.split(','))) != 3:
                raise UserError('Partition date requires three distinct names: year,month,day.')
            lower, upper = self._date(self.start), self._date(self.end)
        else:
            lower, upper = self.instant(self.start), self.instant(self.end)
        if lower >= upper:
            raise UserError('Filter --from must precede exclusive --to.')

    @cached_property
    def bounds(self):
        parse = self._date if self.kind == 'partition' else self.instant
        return parse(self.start), parse(self.end)

    @staticmethod
    def _date(value):
        try:
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
                raise ValueError()
            return date.fromisoformat(value)
        except ValueError as exc:
            raise UserError('Partition/date bounds must use YYYY-MM-DD.') from exc

    def instant(self, value):
        if isinstance(value, str):
            # Python datetime truncates finer precision; refuse rather than shift a boundary.
            if re.search(r'[.,]\d{7,}', value):
                raise UserError('Filter bounds support at most microsecond precision.')
            try:
                value = datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError as exc:
                raise UserError('Time bounds must be ISO dates or timestamps.') from exc
        if isinstance(value, date) and not isinstance(value, datetime):
            value = datetime.combine(value, datetime.min.time())
        if not isinstance(value, datetime):
            raise UserError('Time filter encountered a non-temporal value.')
        if value.tzinfo is None:
            zone = ZoneInfo(self.timezone)
            first, second = value.replace(tzinfo=zone, fold=0), value.replace(tzinfo=zone, fold=1)
            if first.utcoffset() != second.utcoffset():
                raise UserError('Ambiguous or nonexistent local time; use UTC or offset-aware timestamps.')
            value = first
        return value.astimezone(timezone.utc)

    def validate_dataset(self, dataset):
        if self.kind == 'partition':
            definitions = dataset['ingestion']['selector']['partitions']
            for name in self.field.split(','):
                if sum(p['name'] == name for p in definitions) != 1:
                    raise UserError(f'Partition date component {name!r} must identify one partition.')
        else:
            fields = {f['name']: f for f in temporal_fields(dataset)}
            if self.field not in fields:
                raise UserError('Time field must be a top-level date/timestamp field in the schema.')
            if fields[self.field]['type'] == 'date':
                self._date(self.start)
                self._date(self.end)

    def matches_record(self, record):
        value = record.get(self.field)
        return value is not None and self.bounds[0] <= self.instant(value) < self.bounds[1]

    def matches_partitions(self, partitions):
        values = {p['name']: p['value'] for p in partitions}
        value = calendar_partition_date(values, self.field.split(','))
        return self.bounds[0] <= value < self.bounds[1]


class FilteredSelection:
    """Compose a manifest selector with a partition predicate."""
    def __init__(self, selection, predicate):
        self.selection, self.predicate = selection, predicate

    @property
    def prefix(self):
        return self.selection.prefix

    def matches(self, obj):
        return self.selection.matches(obj) and self.accepts(obj)

    def accepts(self, obj):
        return self.predicate.kind != 'partition' or self.predicate.matches_partitions(self.partition_values(obj))

    def objects(self):
        return (obj for obj in self.selection.objects() if self.accepts(obj))

    def partition_values(self, obj):
        return self.selection.partition_values(obj)
