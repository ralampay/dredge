"""Portable ingestion integration tests, including generated notebook execution."""
from contextlib import nullcontext, redirect_stderr
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
import gzip
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dredge.cli import main
from dredge.core.exceptions import UserError
from dredge.core.schema_manifest import SchemaManifest
from dredge.formats.arrow_contract import normalize_value, output_schema
from dredge.modes.s3.datasets import S3DatasetAdapter
from dredge.modules.discover.serialization import write_report
from dredge.modules.ingest.ingest_datasets import IngestDatasets
from dredge.modules.ingest.planning import select_datasets
from dredge.modules.ingest.preview import PreviewIngestion
from dredge.modules.ingest.requests import IngestOptions
from dredge.modules.ingest.runner import run_ingestion
from dredge.modules.ingest.writer import ParquetDatasetWriter
from test_discover import MemorySource, crawl
from test_schema_manifest import KEY, LOG


def setup(data, **options):
    manifest = SchemaManifest(crawl(data, **options).document)
    source = MemorySource(data)
    datasets = manifest.document['datasets']
    selections = [S3DatasetAdapter(source, d) for d in datasets]
    return manifest, source, datasets, selections


def run(data, output, options=None, source_data=None, **discovery):
    manifest, source, datasets, selections = setup(data, **discovery)
    if source_data is not None:
        source.data = source_data
    return IngestDatasets(manifest, source, datasets, selections, output, options).execute()


class IngestionTests(unittest.TestCase):
    def test_multi_partition_shards_and_notebook(self):
        data = {'events/year=2025/a.jsonl': b'{"id":1,"nested":{"x":true},"items":[1]}\n',
                'events/year=2026/b.jsonl': b'{"id":2,"nested":{"x":false},"items":[2]}\n'}
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'run'
            result = run(data, output, IngestOptions(batch_rows=1, file_bytes=1))
            self.assertFalse(result.partial)
            manifest = json.loads((output / 'manifest.json').read_text())
            self.assertEqual(manifest['dataset_version'], 1)
            item = manifest['datasets'][0]
            self.assertEqual(item['rows'], 2)
            self.assertEqual(len(item['files']), 2)
            table = pq.read_table(output / item['data_path'])
            self.assertEqual(table['id'].to_pylist(), [1, 2])
            self.assertEqual(table['__dredge_partition_1'].to_pylist(), ['2025', '2026'])
            self.assertEqual(table['nested'].to_pylist(), [{'x': True}, {'x': False}])
            notebook = json.loads((output / item['notebook']).read_text())
            self.assertEqual([c['cell_type'] for c in notebook['cells']], ['markdown', 'code'])
            self.assertEqual(notebook['cells'][1]['outputs'], [])
            self.assertIn('nested.x', notebook['cells'][0]['source'])
            self.assertIn('__dredge_partition_1', notebook['cells'][0]['source'])
            try:
                import pandas  # noqa: F401 -- optional notebook dependency
            except ImportError:
                self.skipTest('Notebook execution requires pandas')
            namespace = {}
            previous = Path.cwd()
            try:
                os.chdir((output / item['notebook']).parent)
                exec(notebook['cells'][1]['source'], namespace)
            finally:
                os.chdir(previous)
            self.assertEqual(list(namespace['df']['id']), [1, 2])
            self.assertEqual(set(namespace['df']['__dredge_partition_1']), {'2025', '2026'})

    def test_supported_formats_and_temporal_decimal_types(self):
        buffer = io.BytesIO()
        pq.write_table(pa.table({'amount': pa.array([Decimal('1.23')], type=pa.decimal128(8, 2)),
                                 'day': [date(2026, 1, 2)],
                                 'at': pa.array([datetime(2026, 1, 2, tzinfo=timezone.utc)], type=pa.timestamp('us', 'UTC')),
                                 'number': pa.array([2**63 + 1], type=pa.uint64())}), buffer)
        fixtures = [({'a.csv': b'id,name\n001,A\n002,B\n'}, {'csv_header': 'present'}, 2),
                    ({'a.json': b'[{"id":1},{"id":2}]'}, {}, 2),
                    ({'a.jsonl.gz': gzip.compress(b'{"id":1}\n{"id":2}\n')}, {}, 2),
                    ({KEY: (LOG + '\n').encode()}, {}, 1),
                    ({'a.parquet': buffer.getvalue()}, {}, 1),
                    ({'a.jsonl': b'{"day":"2026-01-02","at":"2026-01-02T00:00:00Z"}\n'}, {}, 1)]
        for data, kwargs, rows in fixtures:
            with self.subTest(format=list(data)), tempfile.TemporaryDirectory() as parent:
                output = Path(parent) / 'run'
                result = run(data, output, **kwargs)
                item = result.document['datasets'][0]
                table = pq.read_table(output / item['data_path'])
                self.assertEqual(table.num_rows, rows)
                if 'a.parquet' in data:
                    self.assertEqual(table['amount'].to_pylist(), [Decimal('1.23')])
                    self.assertEqual(table['number'].to_pylist(), [2**63 + 1])
                    self.assertEqual(table.schema.field('at').type, pa.timestamp('us', 'UTC'))

    def test_empty_multiple_and_nullable_datasets(self):
        data = {'one/a.jsonl': b'{"x":1}\n{"x":null}\n', 'two/a.csv': b'id\n2\n'}
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'run'
            result = run(data, output, csv_header='present', source_data={})
            self.assertEqual(len(result.document['datasets']), 2)
            for dataset in result.document['datasets']:
                table = pq.read_table(output / dataset['data_path'])
                self.assertEqual(table.num_rows, 0)
                self.assertEqual(len(dataset['files']), 1)
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'run'
            result = run({'a.jsonl': data['one/a.jsonl']}, output)
            self.assertEqual(pq.read_table(output / result.document['datasets'][0]['data_path'])['x'].to_pylist(), [1, None])

    def test_failures_never_publish_or_leave_staging(self):
        original = {'a.jsonl': b'{"x":1}\n'}
        cases = [({'a.jsonl': b'{"x":1}\n{"x":"private"}\n'}, None),
                 ({'a.jsonl': b'{"x":9223372036854775808}\n'}, None)]
        for changed, _ in cases:
            with tempfile.TemporaryDirectory() as parent:
                with self.assertRaises(UserError):
                    run(original, Path(parent) / 'run', IngestOptions(batch_rows=1), source_data=changed)
                self.assertEqual(list(Path(parent).iterdir()), [])
        for error in (OSError('disk full'), KeyboardInterrupt(), UserError('object changed')):
            with tempfile.TemporaryDirectory() as parent:
                target = 'dredge.modules.ingest.writer.ParquetDatasetWriter.append'
                with patch(target, side_effect=error), self.assertRaises(type(error) if not isinstance(error, OSError) else UserError):
                    run(original, Path(parent) / 'run')
                self.assertEqual(list(Path(parent).iterdir()), [])

    def test_preflight_blocked_existing_and_bucket_mismatch(self):
        manifest, source, datasets, selections = setup({'a.json': b'{"x":[]} '})
        with tempfile.TemporaryDirectory() as parent:
            with self.assertRaisesRegex(UserError, 'not ingestion-ready'):
                IngestDatasets(manifest, source, datasets, selections, Path(parent) / 'run').execute()
            self.assertEqual(source.reads, [])
            self.assertEqual(list(Path(parent).iterdir()), [])
        with self.assertRaisesRegex(UserError, 'bucket'):
            select_datasets(manifest, 'other')
        with self.assertRaisesRegex(UserError, 'Dataset id'):
            select_datasets(manifest, 'test', ['missing'])
        with tempfile.TemporaryDirectory() as parent:
            path = Path(parent) / 'run'
            path.mkdir()
            (path / 'keep').write_text('original')
            with self.assertRaisesRegex(UserError, 'already exists'):
                run({'a.json': b'{"x":1}'}, path)
            self.assertEqual((path / 'keep').read_text(), 'original')

    def test_batch_bounds_and_progress(self):
        data = {'a.jsonl': b'{"x":"a long string"}\n' * 25}
        manifest, source, datasets, selections = setup(data)
        events, batch_lengths = [], []
        original = ParquetDatasetWriter.flush
        def capture(writer):
            batch_lengths.append(len(writer.batch))
            return original(writer)
        with tempfile.TemporaryDirectory() as parent, patch.object(ParquetDatasetWriter, 'flush', capture):
            IngestDatasets(manifest, source, datasets, selections, Path(parent) / 'run',
                           IngestOptions(batch_rows=3, batch_bytes=1000), events.append).execute()
        self.assertLessEqual(max(batch_lengths), 3)
        self.assertGreater(len(batch_lengths), 2)
        self.assertEqual(events[-1].records, 25)
        self.assertEqual(events[-1].objects, 1)
        self.assertGreater(events[-1].bytes_read, 0)
        self.assertGreater(events[-1].files, 0)

    def test_partition_collision_and_lossy_conversions(self):
        dataset = setup({'a/year=2026/a.json': b'{"__dredge_partition_1":1}'})[2][0]
        with self.assertRaisesRegex(UserError, 'collides'):
            output_schema(dataset)
        with self.assertRaisesRegex(UserError, 'Lossy'):
            normalize_value(2**53 + 1, {'type': 'float'})
        with self.assertRaisesRegex(UserError, 'precision'):
            normalize_value(datetime(2026, 1, 1, microsecond=1), {'type': 'timestamp', 'unit': 'ms'})


class PreviewTests(unittest.TestCase):
    def test_listing_only_filters_totals_and_blockers(self):
        data = {'a/a.json': b'{"x":1}', 'b/a.json': b'{"x":[]}',
                'b/skip.json': b'{"x":1}', 'unsupported.txt': b'ignore', 'empty.json': b''}
        manifest, source, datasets, selections = setup(data, exclude=('*/skip.json',))
        with patch.object(source, 'read_range', side_effect=AssertionError('content read')), \
                patch.object(source, 'objects', wraps=source.objects) as listings:
            result = PreviewIngestion(source, datasets, selections).execute()
        listings.assert_called_once_with('')
        self.assertTrue(result.partial)
        self.assertEqual(result.document['objects'], 2)
        self.assertEqual(result.document['bytes'], len(data['a/a.json']) + len(data['b/a.json']))
        self.assertEqual(result.document['datasets'][1]['largest'], len(data['b/a.json']))

    def test_cli_dry_run_no_output_and_dispatch(self):
        report = crawl({'a.json': b'{"x":1}'})
        source = MemorySource({'a.json': b'{"x":1}'})
        with tempfile.TemporaryDirectory() as parent:
            schema = Path(parent) / 'schema.yaml'
            write_report(report, schema)
            output = Path(parent) / 'nonexistent' / 'run'
            args = ['--module', 'ingest', '--mode', 's3', '--action', 'get', '--bucket', 'test',
                    '--schema', str(schema), '--output', str(output), '--dry-run']
            with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(source)), \
                    patch('dredge.modules.ingest.presentation.present') as present:
                self.assertEqual(main(args), 0)
                self.assertEqual(present.call_args.args[0].document['objects'], 1)
            self.assertFalse(output.parent.exists())
            self.assertEqual(source.reads, [])
            for extra in (['--sample-files', '1'], ['--batch-rows', '0']):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    main(args + extra)
                self.assertEqual(error.exception.code, 2)
            with patch('dredge.modes.s3.clients.open_source') as factory, self.assertRaises(UserError):
                run_ingestion(dict(schema=str(schema), output=str(output), bucket='wrong', dry_run=True))
            factory.assert_not_called()

    def test_cli_blocked_preview_exit_one_and_no_writes(self):
        data = {'a.json': b'{"x":[]}'}
        with tempfile.TemporaryDirectory() as parent:
            schema = Path(parent) / 'schema.yaml'
            write_report(crawl(data), schema)
            with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(MemorySource(data))), \
                    patch('dredge.modules.ingest.presentation.present'):
                status = main(['--module', 'ingest', '--mode', 's3', '--action', 'get', '--bucket', 'test',
                               '--schema', str(schema), '--output', str(Path(parent) / 'run'), '--dry-run'])
            self.assertEqual(status, 1)
            self.assertEqual(list(Path(parent).iterdir()), [schema])

    def test_paginated_s3_listing_never_gets_objects(self):
        import boto3
        from botocore.stub import Stubber
        from dredge.modes.s3.adapters import S3Source
        manifest, _, datasets, _ = setup({'a.json': b'{"x":1}'})
        client = boto3.client('s3', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
        try:
            with Stubber(client) as stub:
                stub.add_response('list_objects_v2', {'IsTruncated': True, 'NextContinuationToken': 'page2',
                    'Contents': [{'Key': 'a.json', 'Size': 10}]}, {'Bucket': 'test', 'Prefix': ''})
                stub.add_response('list_objects_v2', {'IsTruncated': False,
                    'Contents': [{'Key': 'b.json', 'Size': 30}]}, {'Bucket': 'test', 'Prefix': '', 'ContinuationToken': 'page2'})
                source = S3Source(client, 'test')
                result = PreviewIngestion(source, datasets, [S3DatasetAdapter(source, datasets[0])]).execute()
                self.assertEqual(result.document['objects'], 2)
                self.assertEqual(result.document['bytes'], 40)
                self.assertEqual(result.document['datasets'][0]['largest'], 30)
                stub.assert_no_pending_responses()
        finally:
            client.close()

    def test_ingest_cli_success_and_byte_batch_limit(self):
        data = {'a.jsonl': b'{"id":1}\n{"id":2}\n'}
        with tempfile.TemporaryDirectory() as parent:
            schema = Path(parent) / 'schema.yaml'
            write_report(crawl(data), schema)
            output = Path(parent) / 'run'
            batches = []
            original = ParquetDatasetWriter.flush
            def capture(writer):
                batches.append(len(writer.batch))
                return original(writer)
            with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(MemorySource(data))), \
                    patch('dredge.modules.ingest.presentation.present'), \
                    patch.object(ParquetDatasetWriter, 'flush', capture):
                status = main(['--module', 'ingest', '--mode', 's3', '--action', 'get', '--bucket', 'test',
                               '--schema', str(schema), '--output', str(output), '--batch-bytes', '1'])
            self.assertEqual(status, 0)
            self.assertTrue((output / 'manifest.json').exists())
            self.assertEqual(max(batches), 1)

    def test_compatibility_arguments_include_output(self):
        import argparse
        from dredge.features.discover.arguments import add_arguments
        parser = argparse.ArgumentParser()
        add_arguments(parser)
        self.assertEqual(parser.parse_args(['--output', 'schema.yaml']).output, 'schema.yaml')

    def test_version_two_manifest_and_selected_dataset(self):
        from dredge.core.schema_manifest import schema_fingerprint
        from dredge.formats.schema import arrow_schema
        manifest, source, datasets, _ = setup({'one/a.json': b'{"x":1}', 'two/b.json': b'{"y":2}'})
        document = deepcopy(manifest.document)
        document['schema_version'] = 2
        for dataset in document['datasets']:
            schema, _ = output_schema(dataset)
            dataset['schema'] = arrow_schema(schema)
            dataset['schema_fingerprint'] = schema_fingerprint(dataset['schema'], dataset['ingestion']['parser'], 2)
        manifest = SchemaManifest(document)
        selected = select_datasets(manifest, 'test', [datasets[0]['id'], datasets[0]['id']])
        self.assertEqual(len(selected), 1)
        with tempfile.TemporaryDirectory() as parent:
            result = IngestDatasets(manifest, source, selected, [S3DatasetAdapter(source, selected[0])],
                                    Path(parent) / 'run').execute()
        self.assertEqual(len(result.document['datasets']), 1)
        self.assertEqual({r[0] for r in source.reads}, {'one/a.json'})

    def test_large_text_reads_are_buffered(self):
        data = {'a.jsonl': b'{"x":1}\n' * 200_000}
        manifest, source, datasets, selections = setup(data, sample_records=2)
        from dredge.modules.ingest.commands import ReadDatasetRecords
        self.assertEqual(sum(1 for _ in ReadDatasetRecords(datasets[0], source, selections[0]).execute()), 200_000)
        self.assertLess(len(source.reads), 4)
