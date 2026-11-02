"""Regression coverage for reusable workflow inputs and resource ownership."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from dredge.core.exceptions import UserError
from dredge.core.sources import CountedSource
from dredge.modules.ingest.ingest_datasets import IngestDatasets
from dredge.modules.ingest.preview import PreviewIngestion
from dredge.modules.ingest.publication import staged_output
from dredge.modules.ingest.requests import IngestOptions
from dredge.modules.ingest.writer import ParquetDatasetWriter
from test_ingestion import setup


class WorkflowBoundaryTests(unittest.TestCase):
    def test_both_commands_reject_mismatched_or_duplicate_selections_before_io(self):
        manifest, source, datasets, selections = setup({'a.json': b'{"x":1}'})
        with tempfile.TemporaryDirectory() as parent, patch.object(source, 'objects') as listing:
            for items, selectors in ((datasets, []), ([], []), (datasets * 2, selections * 2)):
                commands = [PreviewIngestion(source, items, selectors),
                            IngestDatasets(manifest, source, items, selectors, Path(parent) / 'run')]
                for command in commands:
                    with self.subTest(command=type(command).__name__), self.assertRaises(UserError):
                        command.execute()
            listing.assert_not_called()
            self.assertEqual(source.reads, [])
            self.assertEqual(list(Path(parent).iterdir()), [])

    def test_preview_uses_neutral_selector_interface(self):
        _, source, datasets, selections = setup({'a.json': b'{"x":1}'})
        # Deliberately expose no S3 selector dictionary.
        class Selection:
            prefix = ''
            def matches(self, obj):
                return selections[0].matches(obj)
        result = PreviewIngestion(source, datasets, [Selection()]).execute()
        self.assertEqual(result.document['objects'], 1)
        self.assertFalse(result.partial)
        self.assertEqual(source.reads, [])

    def test_shared_read_accounting_preserves_compatibility(self):
        from dredge.modules.discover.progress import CountedSource as PreviousCountedSource
        from dredge.modules.ingest.commands import DatasetRecord as PreviousRecord
        from dredge.modules.ingest.models import DatasetRecord
        self.assertIs(PreviousCountedSource, CountedSource)
        self.assertIs(PreviousRecord, DatasetRecord)
        _, source, _, _ = setup({'a.json': b'{"x":1}'})
        counts = []
        counted = CountedSource(source, counts.append)
        obj = next(counted.objects(''))
        self.assertEqual(counted.read_range(obj, 0, 2), b'{"x')
        self.assertEqual(counts, [3])
        with patch.object(source, 'read_range', side_effect=UserError('unavailable')), self.assertRaises(UserError):
            counted.read_range(obj, 0, 2)
        self.assertEqual(counts, [3])

    def test_abort_closes_writer_without_reporting_success(self):
        with tempfile.TemporaryDirectory() as parent:
            callback = Mock()
            writer = ParquetDatasetWriter(Path(parent) / 'data', {}, pa.schema([]), IngestOptions(), callback)
            handle = Mock()
            writer.writer = handle
            writer.__exit__(KeyboardInterrupt, KeyboardInterrupt(), None)
            handle.close.assert_called_once()
            self.assertIsNone(writer.writer)
            self.assertEqual(writer.files, [])
            callback.assert_not_called()

    def test_close_failure_preserves_original_cancellation(self):
        with tempfile.TemporaryDirectory() as parent:
            writer = ParquetDatasetWriter(Path(parent), {}, pa.schema([]), IngestOptions())
            writer.writer = Mock()
            writer.writer.close.side_effect = OSError('cannot close')
            cancelled = KeyboardInterrupt()
            with self.assertRaises(KeyboardInterrupt) as error:
                writer.__exit__(KeyboardInterrupt, cancelled, None)
            self.assertIs(error.exception, cancelled)
            self.assertIsInstance(error.exception.__cause__, OSError)
            self.assertIsNone(writer.writer)

    def test_staging_cleanup_failure_still_releases_lock(self):
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'run'
            with patch('dredge.modules.ingest.publication.shutil.rmtree', side_effect=OSError('cannot remove')):
                with self.assertRaisesRegex(OSError, 'cannot remove'):
                    with staged_output(output):
                        raise UserError('original failure')
            self.assertFalse((Path(parent) / '.run.ingest.lock').exists())
            self.assertFalse(output.exists())

    def test_staging_rechecks_destination_before_publish(self):
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'run'
            with self.assertRaisesRegex(UserError, 'already exists'):
                with staged_output(output) as stage:
                    (stage / 'ours').write_text('ours')
                    output.mkdir()
                    (output / 'theirs').write_text('theirs')
            self.assertEqual([p.name for p in output.iterdir()], ['theirs'])
            self.assertEqual(list(Path(parent).iterdir()), [output])

    def test_notebook_failure_rolls_back_completed_parquet_files(self):
        manifest, source, datasets, selections = setup({'a.json': b'{"x":1}'})
        with tempfile.TemporaryDirectory() as parent:
            with patch('dredge.modules.ingest.ingest_datasets.write_notebook', side_effect=OSError('disk full')):
                with self.assertRaisesRegex(UserError, 'Cannot write ingestion output'):
                    IngestDatasets(manifest, source, datasets, selections, Path(parent) / 'run').execute()
            self.assertTrue(source.reads)
            self.assertEqual(list(Path(parent).iterdir()), [])

    def test_opaque_dataset_ids_cannot_escape_output(self):
        import json
        manifest, source, datasets, selections = setup({'a.json': b'{"x":1}'})
        datasets[0]['id'] = '../outside'
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'run'
            IngestDatasets(manifest, source, datasets, selections, output).execute()
            document = json.loads((output / 'manifest.json').read_text())
            entry = document['datasets'][0]
            self.assertEqual(entry['id'], '../outside')
            self.assertTrue((output / entry['notebook']).is_file())
            self.assertTrue((output / entry['data_path']).resolve().is_relative_to(output))
            self.assertEqual(list(Path(parent).iterdir()), [output])
