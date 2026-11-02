"""Reusable record consumption of a discovery manifest; no CLI or S3 imports."""
from dataclasses import dataclass
from types import SimpleNamespace
from ...core.exceptions import UserError
from ...formats.records import RangeReader, SampleLimit, open_records
from ...formats.validation import validate_record


@dataclass(frozen=True)
class DatasetRecord:
    key: str
    etag: str
    partitions: list[dict]
    values: dict


class ReadDatasetRecords:
    """Read selected objects with manifest parsers and strict schema checks.

    Limits are opt-in validation controls. Byte exhaustion is an error, never a
    successful truncated object. Callers stopping iteration early should close
    the returned generator to release streams immediately.
    """
    def __init__(self, dataset, source, selection, *, max_files=None, max_records_per_file=None,
                 max_bytes_per_file=None):
        self.dataset, self.source, self.selection = dataset, source, selection
        self.max_files, self.max_records = max_files, max_records_per_file
        self.max_bytes = max_bytes_per_file

    def execute(self):
        contract = self.dataset['ingestion']
        if not contract['ready'] or contract.get('blockers'):
            raise UserError('Dataset is not ingestion-ready; resolve discovery blockers first.')
        for value in (self.max_files, self.max_records, self.max_bytes):
            if value is not None and value <= 0:
                raise UserError('Ingestion validation limits must be positive.')
        parser = contract['parser']
        for index, obj in enumerate(self.selection.objects()):
            if self.max_files is not None and index >= self.max_files:
                break
            # Gzip expansion can exceed object size. Unbounded ingestion uses an
            # effectively unlimited budget; callers can impose an explicit cap.
            budget = self.max_bytes if self.max_bytes is not None else 2**63 - 1
            options = SimpleNamespace(sample_bytes=budget, encoding=parser.get('encoding', 'utf-8-sig'),
                                      csv_delimiter=parser.get('csv_delimiter', ','),
                                      csv_header=parser.get('csv_header', 'present'))
            compression = 'gzip' if obj.key.lower().endswith('.gz') else 'none'
            raw = RangeReader(self.source, obj, budget)
            try:
                with open_records(raw, parser['adapter'], compression, options) as records:
                    for count, record in enumerate(records, 1):
                        validate_record(record, self.dataset['schema'])
                        yield DatasetRecord(obj.key, obj.etag, self.selection.partition_values(obj), record)
                        if self.max_records is not None and count >= self.max_records:
                            break
            except SampleLimit as exc:
                raise UserError(f'Reading {obj.key!r} exceeded the configured byte limit.') from exc
