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
    def __init__(self, manifest, source, datasets, selections, output, options=None, on_progress=None):
        self.manifest, self.source = manifest, source
        self.datasets, self.selections, self.output = datasets, selections, output
        self.options = options or IngestOptions()
        self.on_progress = on_progress
        self.progress = IngestProgress()

    def _advance(self, **increments):
        self.progress = replace(self.progress, **{
            key: getattr(self.progress, key) + amount for key, amount in increments.items()})
        if self.on_progress:
            self.on_progress(self.progress)

    def execute(self):
        self.options.validate()
        pairs = pair_selections(self.datasets, self.selections)
        require_ready(self.datasets)
        validate_output(self.output)
        self.progress = IngestProgress()
        document = create_manifest(self.manifest.document, self.options)
        counted = CountedSource(self.source, lambda size: self._advance(bytes_read=size))
        try:
            with staged_output(self.output) as stage:
                for dataset, selection in pairs:
                    document['datasets'].append(self._write_dataset(stage, dataset, selection, counted))
                write_manifests(stage, self.manifest.document, document)
        except OSError as exc:
            raise UserError('Cannot write ingestion output; check disk space and directory permissions.') from exc
        return IngestResult(document, str(self.output))

    def _write_dataset(self, stage, dataset, selection, source):
        schema, partitions = output_schema(dataset)
        directory = stage / dataset_path(dataset['id'])
        with ParquetDatasetWriter(directory / 'data', dataset, schema, self.options,
                                  on_file=lambda: self._advance(files=1)) as writer:
            records = ReadDatasetRecords(dataset, source, selection,
                                         on_object=lambda obj: self._advance(objects=1)).execute()
            with closing(records):
                for record in records:
                    writer.append(record)
                    self._advance(records=1)
        write_notebook(directory / 'preview.ipynb', dataset, schema, partitions)
        return dataset_entry(dataset, schema, partitions, writer.files, writer.rows)
