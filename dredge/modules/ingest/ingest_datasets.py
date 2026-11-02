"""Materialize validated records into a published dataset collection."""
from dataclasses import replace
from contextlib import closing

from ...core.exceptions import UserError
from ...formats.arrow_contract import output_schema
from ...core.sources import CountedSource
from .commands import ReadDatasetRecords
from .models import IngestProgress, IngestResult
from .notebook import write_notebook
from .planning import pair_selections, require_ready
from .publication import staged_output, validate_output
from .requests import IngestOptions
from .writer import ParquetDatasetWriter
from .serialization import create_manifest, dataset_entry, dataset_path, write_manifests


class IngestDatasets:
    """Write caller-selected datasets; selection_plan records source provenance.

    Schema-independent selection and assignment preflight are coordinated by
    IngestSelectedObjects before this writer receives its selectors.
    """
    def __init__(self, manifest, source, datasets, selections, output, options=None, on_progress=None, plan=None, selection_plan=None):
        self.manifest, self.source = manifest, source
        self.datasets, self.selections, self.output = datasets, selections, output
        self.options = options or IngestOptions()
        self.on_progress = on_progress
        self.plan, self.selection_plan = plan, selection_plan
        self.progress = IngestProgress()

    def _advance(self, **increments):
        self.progress = replace(self.progress, **{
            key: getattr(self.progress, key) + amount for key, amount in increments.items()})
        if self.on_progress:
            self.on_progress(self.progress)

    def execute(self):
        self.options.validate()
        if self.plan is not None and self.selection_plan is not None:
            raise UserError('Choose either a record-time plan or a source-selection plan.')
        pairs = (() if self.selection_plan is not None and not self.datasets and not self.selections
                 else pair_selections(self.datasets, self.selections))
        require_ready(self.datasets)
        if self.plan is not None:
            if len(self.datasets) != 1:
                raise UserError('An ingestion plan selects exactly one dataset.')
            self.plan.validate(self.manifest.document['source']['bucket'], self.datasets[0])
            from .time_filters import FilteredSelection
            pairs = tuple((d, FilteredSelection(s, self.plan.predicate)) for d, s in pairs)
        validate_output(self.output)
        self.progress = IngestProgress()
        document = create_manifest(self.manifest.document, self.options)
        if self.plan is not None:
            document['filter_plan'] = self.plan.document()
        if self.selection_plan is not None:
            document['source_plan'] = self.selection_plan
        counted = CountedSource(self.source, lambda size: self._advance(bytes_read=size))
        try:
            with staged_output(self.output) as stage:
                for dataset, selection in pairs:
                    document['datasets'].append(self._write_dataset(stage, dataset, selection, counted))
                document['execution'] = {'objects': self.progress.objects, 'records_written': self.progress.records,
                                         'bytes_read': self.progress.bytes_read}
                write_manifests(stage, self.manifest.document, document)
        except OSError as exc:
            raise UserError('Cannot write ingestion output; check disk space and directory permissions.') from exc
        return IngestResult(document, str(self.output))

    def _write_dataset(self, stage, dataset, selection, source):
        schema, partitions = output_schema(dataset)
        directory = stage / dataset_path(dataset['id'])
        with ParquetDatasetWriter(directory / 'data', dataset, schema, self.options,
                                  on_file=lambda: self._advance(files=1)) as writer:
            reader = ReadDatasetRecords(dataset, source, selection,
                                         on_object=lambda obj: self._advance(objects=1),
                                         predicate=self.plan.predicate if self.plan else None,
                                         inspect_metadata=self.plan.inspect_metadata if self.plan else False)
            records = reader.execute()
            with closing(records):
                for record in records:
                    writer.append(record)
                    self._advance(records=1)
        write_notebook(directory / 'preview.ipynb', dataset, schema, partitions,
                       self.plan.document() if self.plan else self.selection_plan)
        entry = dataset_entry(dataset, schema, partitions, writer.files, writer.rows)
        entry.update(records_scanned=reader.records_scanned, records_filtered=reader.records_filtered)
        return entry
