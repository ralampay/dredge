"""Inspect filter capabilities and estimate a reusable ingestion strategy."""
from ...core.schema_io import validate_output
from ...formats.parquet_filter import inspect_parquet
from .plan_io import write_plan
from .models import IngestResult
from .planning import blockers
from .time_filters import FilteredSelection, temporal_fields


class InspectIngestionFields:
    def __init__(self, datasets):
        self.datasets = datasets

    def execute(self):
        return IngestResult({'filter_fields': [
            {'id': d['id'], 'name': d['name'], 'fields': temporal_fields(d),
             'partitions': d['ingestion']['selector']['partitions']}
            for d in self.datasets]})


class PlanIngestion:
    """Refresh inventory and optionally inspect bounded Parquet metadata.

    Estimates never claim row selectivity or exact network/output sizes.
    Metadata reads are capped at 16 MiB per file and do not scan records.
    """
    def __init__(self, source, dataset, selection, plan, output=None, on_progress=None):
        self.source, self.dataset, self.selection = source, dataset, selection
        self.plan, self.output, self.on_progress = plan, output, on_progress

    def execute(self):
        self.plan.validate(self.plan.bucket, self.dataset)
        if self.output is not None:
            validate_output(self.output)
        selection = FilteredSelection(self.selection, self.plan.predicate)
        estimate = dict(candidate_objects=0, candidate_bytes=0, selected_objects=0,
                        selected_bytes=0, excluded_objects=0, estimated_data_bytes=0,
                        metadata_bytes=0, row_groups=0, selected_row_groups=0, uncertain_row_groups=0)
        for obj in self.selection.objects():
            estimate['candidate_objects'] += 1
            estimate['candidate_bytes'] += obj.size
            if selection.accepts(obj):
                keep, size = True, obj.size
                if self.plan.inspect_metadata:
                    inspection = inspect_parquet(self.source, obj, self.plan.predicate)
                    groups = inspection.groups
                    estimate['metadata_bytes'] += inspection.bytes_read
                    estimate['row_groups'] += inspection.total_groups
                    estimate['selected_row_groups'] += len(groups.indices)
                    estimate['uncertain_row_groups'] += groups.uncertain_groups
                    keep, size = bool(groups.indices), groups.compressed_bytes
                if keep:
                    estimate['selected_objects'] += 1
                    estimate['selected_bytes'] += obj.size
                    estimate['estimated_data_bytes'] += size
                else:
                    estimate['excluded_objects'] += 1
            else:
                estimate['excluded_objects'] += 1
            if self.on_progress:
                self.on_progress(estimate['candidate_objects'])
        document = {'filter_plan': self.plan.document(), 'estimate': estimate,
                    'blockers': blockers(self.dataset), 'saved': self.output is not None}
        if self.output is not None:
            write_plan(self.plan, estimate, self.output)
        return IngestResult(document, str(self.output) if self.output is not None else None,
                            partial=bool(document['blockers']))
