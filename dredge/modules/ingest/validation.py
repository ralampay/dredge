"""Read-only held-out validation of a manifest against source records."""
import hashlib

from ...core.exceptions import UserError
from ...core.sources import SourceAccessError
from .commands import ReadDatasetRecords


class _SelectedObjects:
    def __init__(self, selection, objects):
        self.selection, self.selected = selection, objects

    def objects(self):
        return iter(self.selected)

    def partition_values(self, obj):
        return self.selection.partition_values(obj)


class ValidateDatasetRecords:
    def __init__(self, dataset, source, selection, *, sample_files=50, sample_records=1000,
                 sample_bytes=8 * 1024 * 1024):
        self.dataset, self.source, self.selection = dataset, source, selection
        self.sample_files, self.sample_records, self.sample_bytes = sample_files, sample_records, sample_bytes

    def execute(self):
        if any(v <= 0 for v in (self.sample_files, self.sample_records, self.sample_bytes)):
            raise UserError('Validation sample limits must be positive.')
        if not self.dataset['ingestion']['ready']:
            raise UserError('Dataset is not ingestion-ready; resolve discovery blockers first.')
        discovered = {sample['key'] for sample in self.dataset['samples']}
        candidates, matched, eligible = [], 0, 0
        for obj in self.selection.objects():
            matched += 1
            if obj.key in discovered:
                continue
            eligible += 1
            candidates.append((hashlib.sha256(('validate:' + obj.key).encode()).hexdigest(), obj))
            candidates.sort(key=lambda item: item[0])
            del candidates[self.sample_files:]
        results = []
        for _, obj in candidates:
            count, error = 0, None
            try:
                command = ReadDatasetRecords(self.dataset, self.source, _SelectedObjects(self.selection, [obj]),
                                             max_records_per_file=self.sample_records,
                                             max_bytes_per_file=self.sample_bytes)
                for record in command.execute():
                    count += 1
            except SourceAccessError:
                raise
            except UserError as exc:
                error = str(exc)
            results.append({'key': obj.key, 'etag': obj.etag, 'records_validated': count,
                            'record_limit_reached': count >= self.sample_records, 'error': error})
        failures = sum(result['error'] is not None for result in results)
        return {'dataset_id': self.dataset['id'], 'schema_fingerprint': self.dataset['schema_fingerprint'],
                'status': 'failed' if failures else 'passed' if results else 'not_validated',
                'matched_objects': matched, 'eligible_holdout_objects': eligible,
                'validated_objects': len(results) - failures, 'failed_objects': failures,
                'validated_records': sum(r['records_validated'] for r in results),
                'sample_files': self.sample_files, 'sample_records': self.sample_records,
                'sample_bytes': self.sample_bytes, 'objects': results}
