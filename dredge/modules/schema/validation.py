"""Validate compiled resolution rules using bounded source samples."""
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timezone

from ...core.exceptions import UserError
from ...core.sampling import sample_objects, SelectedObjects
from ...formats.arrow_contract import arrow_type, normalize_value
from ...formats.resolution import path_label
from ..ingest.commands import ReadDatasetRecords
from .models import ResolutionProgress


class ValidateResolution:
    def __init__(self, dataset, source, selection, limits, on_progress=None):
        self.dataset, self.source, self.selection = dataset, source, selection
        self.limits, self.on_progress = limits, on_progress

    def execute(self):
        import pyarrow as pa
        self.limits.validate()
        dataset = self.dataset
        discovered = {sample['key'] for sample in dataset['samples']}
        objects, _, _ = sample_objects(self.selection, discovered, self.limits.sample_files, fallback=True)
        coverage = [0] * len(dataset.get('resolutions', []))
        def observed(index):
            coverage[index] += 1
        output_type = arrow_type(dataset['schema'])
        results, total = [], 0
        for obj in objects:
            count = 0
            reader = ReadDatasetRecords(dataset, self.source, SelectedObjects(self.selection, [obj]),
                                        max_records_per_file=self.limits.sample_records,
                                        max_bytes_per_file=self.limits.sample_bytes, on_resolution=observed)
            try:
                with closing(reader.execute()) as records:
                    for record in records:
                        values = normalize_value(record.values, dataset['schema'])
                        pa.scalar(values, type=output_type)
                        count += 1
            except (UserError, pa.ArrowException, OverflowError, ValueError) as exc:
                detail = str(exc) if isinstance(exc, UserError) else 'record cannot be represented safely in the output schema'
                raise UserError(f"Resolution validation failed for dataset {dataset['id']}, object {obj.key!r}: {detail}") from exc
            total += count
            results.append({'key': obj.key, 'etag': obj.etag, 'reused_discovery_sample': obj.key in discovered,
                            'records': count, 'record_limit_reached': count >= self.limits.sample_records})
            if self.on_progress:
                self.on_progress(ResolutionProgress(dataset['id'], len(results), total))
        if not total:
            raise UserError(f"Dataset {dataset['id']}: validation found no records; increase sampling or provide representative data.")
        missing = [path_label(rule['path']) for rule, seen in zip(dataset.get('resolutions', []), coverage) if not seen]
        if missing:
            raise UserError(f"Dataset {dataset['id']}: insufficient non-null validation evidence for {', '.join(missing)}; increase sampling or provide representative data.")
        return {'dataset_id': dataset['id'], 'schema_fingerprint': dataset['schema_fingerprint'], 'status': 'passed',
                'validated_at': datetime.now(timezone.utc).isoformat(), 'limits': asdict(self.limits),
                'validated_records': total, 'rule_coverage': coverage, 'objects': results}
