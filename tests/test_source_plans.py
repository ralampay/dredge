"""Prefix/Hive selection without schemas, and strict schema intersections."""
from contextlib import nullcontext, redirect_stderr
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
import yaml

from dredge.cli import main
from dredge.core.exceptions import UserError
from dredge.core.partitioning import HiveFilter
from dredge.modes.s3.datasets import S3ObjectSelection
from dredge.modules.ingest.filter_plan import load_plan, write_plan
from dredge.modules.ingest.requests import IngestOptions
from dredge.modules.ingest.runner import run_ingestion
from dredge.modules.ingest.source_plan import SourcePlan
from dredge.modules.ingest.source_selection import PreviewSourceSelection, IngestSelectedObjects
from test_discover import MemorySource
from test_ingestion import setup


def fixture():
    return {'json/year=2026/month=10/day=22/a.jsonl': b'{"id":1}\n',
            'json/year=2026/month=10/day=23/b.jsonl': b'{"id":2}\n',
            'json/year=2026/month=11/day=1/c.jsonl': b'{"id":3}\n'}


class HiveFilterTests(unittest.TestCase):
    def test_exact_segments_reordered_keys_and_or_and(self):
        predicate = HiveFilter(('year=2026', 'month=01', 'month=02'))
        self.assertTrue(predicate.matches('json/month=01/year=2026/a.json'))
        self.assertTrue(predicate.matches('json/year=2026/month=02/a.json'))
        for key in ('json/year=2026/month=1/a.json', 'json/year=2026/month=010/a.json',
                    'json/year=2025/month=01/a.json', 'json/year=2026/month=01',
                    'json/year=2026/xmonth=01/a.json'):
            self.assertFalse(predicate.matches(key), key)
        self.assertTrue(HiveFilter(('tag=a=b',)).matches('tag=a=b/a.json'))

    def test_missing_and_duplicate_filtered_keys(self):
        predicate = HiveFilter(('year=2026',))
        self.assertFalse(predicate.matches('json/a.json'))
        with self.assertRaisesRegex(UserError, 'duplicate'):
            predicate.matches('year=2026/year=2026/a.json')
        self.assertTrue(predicate.matches('year=2026/tag=a/tag=b/a.json'))

    def test_calendar_normalization_boundaries_and_missing_keys(self):
        predicate = HiveFilter((), ('year', 'month', 'day'), '2024-02-29', '2024-03-01')
        self.assertTrue(predicate.matches('year=2024/month=2/day=29/a.json'))
        self.assertTrue(predicate.matches('year=2024/month=02/day=29/a.json'))
        self.assertFalse(predicate.matches('year=2024/month=3/day=1/a.json'))
        self.assertFalse(predicate.matches('year=2024/month=2/a.json'))
        with self.assertRaisesRegex(UserError, 'calendar'):
            predicate.matches('year=2024/month=2/day=30/a.json')
        combined = HiveFilter(('kind=events',), predicate.date_names, predicate.start, predicate.end)
        self.assertFalse(combined.matches('kind=logs/year=2024/month=2/day=29/a.json'))

    def test_invalid_predicates_and_dates(self):
        for expression in ('year', '=2026', 'year=', 'year=2026/month=10', 123):
            with self.subTest(expression=expression), self.assertRaises(UserError):
                HiveFilter((expression,))
        for names, start, end in [(('y', 'm'), '2024-01-01', '2025-01-01'),
                                   (('y', 'y', 'd'), '2024-01-01', '2025-01-01'),
                                   (('y', 'm', 'd'), '2023-02-29', '2023-03-01'),
                                   (('y', 'm', 'd'), '2024-01-01', '2024-01-01')]:
            with self.subTest(names=names, start=start), self.assertRaises(UserError):
                HiveFilter((), names, start, end)


class SourcePlanTests(unittest.TestCase):
    def test_schema_free_listing_zero_files_markers_and_literal_prefix(self):
        data = fixture()
        data.update({'json/year=2026/month=10/day=22/empty.jsonl': b'',
                     'json/year=2026/month=10/day=22/': b'',
                     'json-backup/year=2026/month=10/a.jsonl': b'ignored'})
        source = MemorySource(data)
        plan = SourcePlan('test', 'json/', HiveFilter(('year=2026', 'month=10')))
        with patch.object(source, 'objects', wraps=source.objects) as listing, \
                patch.object(source, 'read_range', side_effect=AssertionError('content read')):
            result = PreviewSourceSelection(S3ObjectSelection(source, plan.prefix, plan.partitions), plan).execute()
        listing.assert_called_once_with('json/')
        self.assertFalse(result.partial)
        self.assertFalse(result.document['schema_checked'])
        self.assertEqual(result.document['source_estimate']['objects'], 3)
        self.assertEqual(result.document['source_estimate']['bytes'], 18)
        # Literal prefixes are not rewritten as directory prefixes.
        broad = S3ObjectSelection(source, 'json', HiveFilter())
        self.assertIn('json-backup/year=2026/month=10/a.jsonl', [o.key for o in broad.objects()])

    def test_plan_roundtrip_refresh_and_bucket_mismatch(self):
        source = MemorySource(fixture())
        plan = SourcePlan('test', 'json/', HiveFilter(('day=22',)))
        with tempfile.TemporaryDirectory() as parent:
            path = Path(parent) / 'plan.yml'
            PreviewSourceSelection(S3ObjectSelection(source, plan.prefix, plan.partitions), plan, output=path).execute()
            restored = load_plan(path)
            self.assertEqual(plan, restored)
            document = yaml.safe_load(path.read_text())
            self.assertEqual(document['plan_version'], 2)
            self.assertFalse({'schema_binding', 'schema_fingerprint', 'dataset_id'} & document.keys())
            source.data['json/year=2026/month=11/day=22/new.jsonl'] = b'{"id":4}\n'
            result = PreviewSourceSelection(S3ObjectSelection(source, restored.prefix, restored.partitions), restored).execute()
            self.assertEqual(result.document['source_estimate']['objects'], 2)
            with self.assertRaisesRegex(UserError, 'bucket'):
                restored.validate('other')
            with self.assertRaises(UserError):
                write_plan(restored, {}, path)

    def test_invalid_version_two_documents(self):
        plan = SourcePlan('test').document()
        for change in ({'plan_version': True}, {'prefix': None}, {'partitions': 'day=22'},
                       {'partitions': [1]}, {'calendar': {'names': [], 'from': None, 'to': None}}, {'calendar': {'names': [[], 'm', 'd'], 'from': '2024-01-01', 'to': '2025-01-01'}},
                       {'schema_binding': 'unexpected'}):
            with self.subTest(change=change), self.assertRaises(UserError):
                SourcePlan.from_document({**plan, **change})

    def test_direct_cli_and_saved_preview_require_no_schema_or_output(self):
        source = MemorySource(fixture())
        args = ['--module', 'ingest', '--mode', 's3', '--bucket', 'test']
        with tempfile.TemporaryDirectory() as parent:
            path = Path(parent) / 'plan.yml'
            with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(source)), \
                    patch('dredge.modules.ingest.presentation.present') as present:
                self.assertEqual(main(args + ['--action', 'get', '--prefix', 'json/', '--partition', 'day=22', '--dry-run']), 0)
                self.assertEqual(present.call_args.args[0].document['source_estimate']['objects'], 1)
                self.assertEqual(main(args + ['--action', 'plan', '--prefix', 'json/', '--output', str(path)]), 0)
                self.assertEqual(main(args + ['--action', 'get', '--plan', str(path), '--dry-run']), 0)
                self.assertEqual(present.call_args.args[0].document['source_estimate']['objects'], 3)
            self.assertEqual(list(Path(parent).iterdir()), [path])
            self.assertFalse(source.reads)

    def test_schema_free_calendar_cli(self):
        source = MemorySource(fixture())
        with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(source)), \
                patch('dredge.modules.ingest.presentation.present') as present:
            status = main(['--module', 'ingest', '--mode', 's3', '--bucket', 'test', '--action', 'plan',
                           '--prefix', 'json/', '--partition-date', 'year,month,day',
                           '--from', '2026-10-22', '--to', '2026-10-23'])
        self.assertEqual(status, 0)
        self.assertEqual(present.call_args.args[0].document['source_estimate']['objects'], 1)
        self.assertFalse(source.reads)

    def test_s3_pagination_uses_literal_prefix_without_get_requests(self):
        import boto3
        from botocore.stub import Stubber
        from dredge.modes.s3.adapters import S3Source
        client = boto3.client('s3', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
        try:
            with Stubber(client) as stub:
                stub.add_response('list_objects_v2', {'IsTruncated': True, 'NextContinuationToken': 'next',
                                  'Contents': [{'Key': 'json/year=2026/a.jsonl', 'Size': 10}]},
                                  {'Bucket': 'test', 'Prefix': 'json/'})
                stub.add_response('list_objects_v2', {'IsTruncated': False,
                                  'Contents': [{'Key': 'json/year=2026/empty.jsonl', 'Size': 0},
                                               {'Key': 'json/year=2025/b.jsonl', 'Size': 20}]},
                                  {'Bucket': 'test', 'Prefix': 'json/', 'ContinuationToken': 'next'})
                source = S3Source(client, 'test')
                plan = SourcePlan('test', 'json/', HiveFilter(('year=2026',)))
                result = PreviewSourceSelection(S3ObjectSelection(source, plan.prefix, plan.partitions), plan).execute()
                self.assertEqual(result.document['source_estimate']['objects'], 2)
                self.assertEqual(result.document['source_estimate']['bytes'], 10)
                stub.assert_no_pending_responses()
        finally:
            client.close()

    def test_conflicting_cli_options(self):
        base = ['--module', 'ingest', '--mode', 's3', '--bucket', 'test']
        cases = [['--action', 'get', '--prefix', 'json/', '--output', 'out'],
                 ['--action', 'get', '--dry-run', '--plan', 'p', '--partition', 'year=2026'],
                 ['--action', 'plan', '--prefix', 'json/', '--time-field', 'at'],
                 ['--action', 'plan', '--prefix', 'json/', '--from', '2026-01-01'],
                 ['--action', 'plan', '--prefix', 'json/', '--partition-date', 'year,month,day'],
                 ['--action', 'plan', '--dataset', 'id']]
        for args in cases:
            with self.subTest(args=args), redirect_stderr(io.StringIO()), patch('dredge.cli.load_environment') as config:
                with self.assertRaises(SystemExit) as error:
                    main(base + args)
                self.assertEqual(error.exception.code, 2)
                config.assert_not_called()


class SelectedIngestionTests(unittest.TestCase):
    def test_schema_intersection_download_and_provenance(self):
        manifest, source, datasets, selections = setup(fixture())
        plan = SourcePlan('test', 'json/', HiveFilter(('day=22',)))
        selection = S3ObjectSelection(source, plan.prefix, plan.partitions)
        preview = PreviewSourceSelection(selection, plan, datasets, selections).execute()
        self.assertEqual(preview.document['source_estimate']['consumable_objects'], 1)
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'out'
            result = IngestSelectedObjects(manifest, source, datasets, selections, selection, plan,
                                           output, IngestOptions()).execute()
            row = result.document['datasets'][0]
            self.assertEqual(pq.read_table(output / row['data_path'])['id'].to_pylist(), [1])
            self.assertEqual(len({r[0] for r in source.reads}), 1)
            self.assertEqual(result.document['source_plan'], plan.document())
            notebook = json.loads((output / row['notebook']).read_text())
            self.assertIn('day=22', notebook['cells'][0]['source'])
            self.assertEqual(len(notebook['cells']), 2)

    def test_unmatched_zero_and_unsupported_files_fail_before_reads(self):
        for suffix, payload in [('unknown.txt', b'unknown'), ('empty.jsonl', b'')]:
            manifest, source, datasets, selections = setup(fixture())
            source.data['json/year=2026/month=10/day=22/' + suffix] = payload
            plan = SourcePlan('test', 'json/')
            selection = S3ObjectSelection(source, plan.prefix, plan.partitions)
            preview = PreviewSourceSelection(selection, plan, datasets, selections).execute()
            self.assertTrue(preview.partial)
            self.assertEqual(preview.document['source_estimate']['unmatched_objects'], 1)
            with tempfile.TemporaryDirectory() as parent, self.assertRaisesRegex(UserError, 'unmatched'):
                IngestSelectedObjects(manifest, source, datasets, selections, selection, plan,
                                      Path(parent) / 'out', IngestOptions()).execute()
            self.assertFalse(source.reads)

    def test_ambiguous_schema_assignment_fails(self):
        manifest, source, datasets, selections = setup(fixture())
        duplicate = deepcopy(datasets[0])
        duplicate['id'] = 'other'
        datasets.append(duplicate)
        selections.append(selections[0])
        plan = SourcePlan('test', 'json/')
        selection = S3ObjectSelection(source, plan.prefix, plan.partitions)
        with tempfile.TemporaryDirectory() as parent, self.assertRaisesRegex(UserError, 'ambiguous'):
            IngestSelectedObjects(manifest, source, datasets, selections, selection, plan,
                                  Path(parent) / 'out', IngestOptions()).execute()
        self.assertFalse(source.reads)

    def test_unselected_blocked_dataset_does_not_block_and_empty_is_valid(self):
        data = fixture()
        data['blocked/a.jsonl'] = b'{"x":[]}\n'
        manifest, source, datasets, selections = setup(data)
        for prefix, count in [('json/', 1), ('missing/', 0)]:
            plan = SourcePlan('test', prefix)
            selection = S3ObjectSelection(source, prefix, plan.partitions)
            with tempfile.TemporaryDirectory() as parent:
                result = IngestSelectedObjects(manifest, source, datasets, selections, selection, plan,
                                              Path(parent) / 'out', IngestOptions()).execute()
                self.assertEqual(len(result.document['datasets']), count)
        plan = SourcePlan('test', 'blocked/')
        selection = S3ObjectSelection(source, plan.prefix, plan.partitions)
        source.reads.clear()
        with tempfile.TemporaryDirectory() as parent, self.assertRaisesRegex(UserError, 'unresolved_types'):
            IngestSelectedObjects(manifest, source, datasets, selections, selection, plan,
                                  Path(parent) / 'out', IngestOptions()).execute()
        self.assertFalse(source.reads)

    def test_new_unmatched_object_during_ingestion_is_not_silently_dropped(self):
        manifest, source, datasets, selections = setup(fixture())
        plan = SourcePlan('test', 'json/')
        selection = S3ObjectSelection(source, plan.prefix, plan.partitions)
        original = source.objects
        calls = 0
        def changing(prefix):
            nonlocal calls
            calls += 1
            if calls == 2:
                source.data['json/new.txt'] = b'unknown'
            yield from original(prefix)
        with tempfile.TemporaryDirectory() as parent, patch.object(source, 'objects', changing):
            output = Path(parent) / 'out'
            with self.assertRaisesRegex(UserError, 'inventory changed'):
                IngestSelectedObjects(manifest, source, datasets, selections, selection, plan,
                                      output, IngestOptions()).execute()
            self.assertFalse(output.exists())
            self.assertEqual(list(Path(parent).iterdir()), [])

    def test_saved_source_plan_accepts_schema_and_explicit_dataset(self):
        manifest, source, datasets, _ = setup(fixture())
        plan = SourcePlan('test', 'json/', HiveFilter(('day=22',)))
        with tempfile.TemporaryDirectory() as parent:
            schema, plan_path, output = [Path(parent) / name for name in ('schema.yml', 'plan.yml', 'out')]
            schema.write_text(yaml.safe_dump(manifest.document))
            write_plan(plan, {}, plan_path)
            with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(source)), \
                    patch('dredge.modules.ingest.presentation.present'):
                result = run_ingestion(dict(action='get', bucket='test', plan=str(plan_path),
                                            schema=str(schema), dataset=[datasets[0]['id']], output=str(output)))
            self.assertEqual(result.document['datasets'][0]['rows'], 1)

    def test_source_plan_survives_schema_regeneration(self):
        plan = SourcePlan('test', 'json/')
        original = setup(fixture())[2][0]['schema_fingerprint']
        updated = {key: b'{"id":1,"new_field":"value"}\n' for key in fixture()}
        manifest, source, datasets, selections = setup(updated)
        self.assertNotEqual(original, datasets[0]['schema_fingerprint'])
        with tempfile.TemporaryDirectory() as parent:
            result = IngestSelectedObjects(manifest, source, datasets, selections,
                                          S3ObjectSelection(source, plan.prefix, plan.partitions),
                                          plan, Path(parent) / 'out', IngestOptions()).execute()
        self.assertEqual(result.document['datasets'][0]['rows'], 3)
