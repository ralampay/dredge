"""Offline resolution-to-ingestion tests using original and edited contracts."""
from contextlib import nullcontext, redirect_stderr
from copy import deepcopy
from decimal import Decimal
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from dredge.cli import main
from dredge.core.exceptions import UserError
from dredge.core.resolution_contract import compile_draft
from dredge.core.schema_manifest import SchemaManifest, schema_fingerprint
from dredge.core.schema_io import write_report
from dredge.formats.resolution import apply_resolutions, extract_number
from dredge.modes.s3.datasets import S3DatasetAdapter
from dredge.modules.schema.commands import ResolveSchema
from dredge.modules.schema.requests import ValidationLimits
from dredge.modules.ingest.ingest_datasets import IngestDatasets
from dredge.modules.ingest.commands import ReadDatasetRecords
from test_discover import crawl, MemorySource


def array_draft(name='ids'):
    baseline = SchemaManifest(crawl({'a.jsonl': (json.dumps({name: []}) + '\n').encode()}).document)
    draft = deepcopy(baseline.document)
    draft['datasets'][0]['schema']['fields'][0]['items']['type'] = 'integer'
    return baseline, draft


def numeric_draft():
    data = {'a.jsonl': b'{"score":1.5}\n{"score":{"double":2.5,"int":null}}\n{"score":{"double":null,"int":3}}\n'}
    baseline = SchemaManifest(crawl(data).document)
    draft = deepcopy(baseline.document)
    field = draft['datasets'][0]['schema']['fields'][0]
    field['type'] = 'float'
    del field['alternatives']
    draft['datasets'][0]['resolutions'] = [{'op': 'extract_number', 'path': [{'field': 'score'}], 'members': ['double', 'int']}]
    return baseline, draft, data


class ResolutionTests(unittest.TestCase):
    def test_assertion_roundtrip_and_full_ingestion(self):
        baseline, draft = array_draft()
        original = deepcopy(baseline.document)
        source = MemorySource({'a.jsonl': b'{"ids":[]}\n', 'heldout.jsonl': b'{"ids":[1,2]}\n'})
        with tempfile.TemporaryDirectory() as parent:
            schema = Path(parent) / 'resolved.yml'
            result = ResolveSchema(baseline, draft, source, S3DatasetAdapter, schema).execute()
            manifest = SchemaManifest.load(schema)
            self.assertEqual(manifest.document['schema_version'], 4)
            self.assertEqual(baseline.document, original)
            self.assertEqual(result.document['resolution']['baseline'], original)
            report = result.document['resolution']['validation'][0]
            self.assertEqual(report['rule_coverage'], [2])
            self.assertEqual([o['reused_discovery_sample'] for o in report['objects']], [False, True])
            datasets = manifest.document['datasets']
            output = Path(parent) / 'ingested'
            ingested = IngestDatasets(manifest, source, datasets, [S3DatasetAdapter(source, d) for d in datasets], output).execute()
            entry = ingested.document['datasets'][0]
            self.assertEqual(pq.read_table(output / entry['data_path'])['ids'].to_pylist(), [[], [1, 2]])
            self.assertEqual(entry['resolutions'][0]['op'], 'assert_type')
            notebook = json.loads((output / entry['notebook']).read_text())
            self.assertEqual(len(notebook['cells']), 2)
            self.assertIn('assert_type', notebook['cells'][0]['source'])

    def test_numeric_wrappers_are_transformed_before_validation(self):
        baseline, draft, data = numeric_draft()
        source = MemorySource(data)
        with tempfile.TemporaryDirectory() as parent:
            result = ResolveSchema(baseline, draft, source, S3DatasetAdapter, Path(parent) / 'resolved.yml').execute()
            dataset = result.document['datasets'][0]
            records = list(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute())
            self.assertEqual([r.values['score'] for r in records], [1.5, 2.5, 3.0])
            self.assertEqual(result.document['resolution']['validation'][0]['rule_coverage'], [3])
            manifest = SchemaManifest(result.document)
            output = Path(parent) / 'data'
            written = IngestDatasets(manifest, source, [dataset], [S3DatasetAdapter(source, dataset)], output).execute()
            self.assertEqual(pq.read_table(output / written.document['datasets'][0]['data_path'])['score'].to_pylist(), [1.5, 2.5, 3.0])

    def test_real_parquet_union_shapes(self):
        files = {}
        for key, table in [('a.parquet', pa.table({'score': [1.5]})),
                           ('b.parquet', pa.table({'score': pa.array([{'double': 2.5, 'int': None}],
                                type=pa.struct([('double', pa.float64()), ('int', pa.int32())]))}))]:
            stream = io.BytesIO(); pq.write_table(table, stream); files[key] = stream.getvalue()
        baseline = SchemaManifest(crawl(files).document)
        draft = deepcopy(baseline.document)
        field = draft['datasets'][0]['schema']['fields'][0]
        field['type'] = 'float'; del field['alternatives']
        draft['datasets'][0]['resolutions'] = [{'op': 'extract_number', 'path': [{'field': 'score'}], 'members': ['double', 'int']}]
        with tempfile.TemporaryDirectory() as parent:
            result = ResolveSchema(baseline, draft, MemorySource(files), S3DatasetAdapter, Path(parent) / 'resolved.yml').execute()
            self.assertEqual(result.document['resolution']['validation'][0]['validated_records'], 2)

    def test_nested_arrays_and_literal_path_names(self):
        name = 'a/b.*'
        baseline, draft = array_draft(name)
        source = MemorySource({'a.jsonl': (json.dumps({name: [4]}) + '\n').encode()})
        with tempfile.TemporaryDirectory() as parent:
            result = ResolveSchema(baseline, draft, source, S3DatasetAdapter, Path(parent) / 'schema.yml').execute()
        self.assertEqual(result.document['datasets'][0]['resolutions'][0]['path'], [{'field': name}, {'items': True}])
        rules = [{'op': 'extract_number', 'path': [{'field': 'rows'}, {'items': True}, {'field': name}], 'members': ['int']}]
        record = {'rows': [{name: {'int': 3}}, {name: 2.5}]}
        original = deepcopy(record)
        self.assertEqual(apply_resolutions(record, rules), {'rows': [{name: 3.0}, {name: 2.5}]})
        self.assertEqual(record, original)

    def test_insufficient_evidence_byte_limit_and_drift_leave_no_output(self):
        baseline, draft = array_draft()
        for data, limits in [({'a.jsonl': b'{"ids":[]}\n'}, ValidationLimits()),
                             ({}, ValidationLimits()),
                             ({'a.jsonl': b'{"ids":[1]}\n'}, ValidationLimits(sample_bytes=4)),
                             ({'a.jsonl': b'{"ids":["private"]}\n'}, ValidationLimits()),
                             ({'a.jsonl': b'{"ids":[9223372036854775808]}\n'}, ValidationLimits())]:
            with self.subTest(data=data), tempfile.TemporaryDirectory() as parent:
                with self.assertRaises(UserError) as error:
                    ResolveSchema(baseline, draft, MemorySource(data), S3DatasetAdapter, Path(parent) / 'schema.yml', limits=limits).execute()
                self.assertNotIn('private', str(error.exception))
                self.assertEqual(list(Path(parent).iterdir()), [])

    def test_heldout_preference_and_discovery_fallback(self):
        baseline, draft = array_draft()
        for data, reused in [({'a.jsonl': b'{"ids":[1]}\n'}, True),
                             ({'a.jsonl': b'{"ids":[1]}\n', 'z.jsonl': b'{"ids":[2]}\n'}, False)]:
            with tempfile.TemporaryDirectory() as parent:
                result = ResolveSchema(baseline, draft, MemorySource(data), S3DatasetAdapter, Path(parent) / 'resolved.yml',
                                       limits=ValidationLimits(sample_files=1)).execute()
                objects = result.document['resolution']['validation'][0]['objects']
                self.assertEqual(len(objects), 1)
                self.assertEqual(objects[0]['reused_discovery_sample'], reused)

    def test_strict_numeric_semantics(self):
        for value in ({'int': 1, 'double': 2.0}, {'int': None}, {'other': 1}, True, '1', float('inf'), 2**53+1, Decimal('0.1')):
            with self.subTest(value=value), self.assertRaises(UserError):
                extract_number(value, ['int', 'double'], '$["score"]')
        self.assertIsNone(extract_number(None, ['int'], '$'))
        self.assertEqual(extract_number({'int': 4}, ['int'], '$'), 4.0)

    def test_forbidden_edits_rejected_before_source_reads(self):
        baseline, draft = array_draft()
        mutations = [lambda d: d['source'].update(bucket='other'),
                     lambda d: d['datasets'][0]['ingestion'].update(ready=True),
                     lambda d: d['datasets'][0]['ingestion']['selector'].update(prefix='other/'),
                     lambda d: d['datasets'][0].update(schema_fingerprint='edited'),
                     lambda d: d['datasets'][0]['schema']['fields'][0].update(name='renamed'),
                     lambda d: d['datasets'][0]['schema']['fields'][0]['items'].update(nullable=True),
                     lambda d: d['datasets'][0]['schema']['fields'][0]['items']['inference'].update(present_count=5)]
        for change in mutations:
            edited = deepcopy(draft); change(edited)
            source = MemorySource({'a.jsonl': b'{"ids":[1]}\n'})
            with tempfile.TemporaryDirectory() as parent, patch.object(source, 'objects') as listing:
                with self.assertRaises(UserError):
                    ResolveSchema(baseline, edited, source, S3DatasetAdapter, Path(parent) / 'resolved.yml').execute()
                listing.assert_not_called()
                self.assertEqual(list(Path(parent).iterdir()), [])

    def test_reject_duplicate_overlapping_unsupported_and_unused_rules(self):
        baseline, draft = array_draft()
        rule = {'op': 'assert_type', 'path': [{'field': 'ids'}, {'items': True}]}
        for rules in ([rule, rule], [rule, {'op': 'assert_type', 'path': [{'field': 'ids'}]}],
                      [{'op': 'eval', 'path': [{'field': 'ids'}]}],
                      [{'op': 'assert_type', 'path': [{'field': 'missing'}]}]):
            edited = deepcopy(draft); edited['datasets'][0]['resolutions'] = rules
            with self.assertRaises(UserError):
                compile_draft(baseline.document, edited)

    def test_non_numeric_union_and_unconfigured_wrapper_rejected(self):
        baseline, draft, _ = numeric_draft()
        draft['datasets'][0]['resolutions'][0]['members'] = ['double']
        with self.assertRaisesRegex(UserError, 'members'):
            compile_draft(baseline.document, draft)
        base = SchemaManifest(crawl({'a.json': b'[{"x":1},{"x":"text"}]'}).document)
        edited = deepcopy(base.document)
        field = edited['datasets'][0]['schema']['fields'][0]
        field['type'] = 'float'; del field['alternatives']
        edited['datasets'][0]['resolutions'] = [{'op': 'extract_number', 'path': [{'field': 'x'}], 'members': ['int']}]
        with self.assertRaises(UserError):
            compile_draft(base.document, edited)

    def test_tampering_with_rules_schema_or_evidence_is_rejected(self):
        baseline, draft = array_draft()
        with tempfile.TemporaryDirectory() as parent:
            document = ResolveSchema(baseline, draft, MemorySource({'a.jsonl': b'{"ids":[1]}\n'}), S3DatasetAdapter,
                                     Path(parent) / 'resolved.yml').execute().document
        mutations = [lambda d: d.pop('resolution'),
                     lambda d: d['resolution'].update(baseline_digest='wrong'),
                     lambda d: d['resolution']['validation'][0].update(status='failed'),
                     lambda d: d['resolution']['validation'][0].update(rule_coverage=[0]),
                     lambda d: d['resolution']['validation'][0].update(schema_fingerprint='wrong'),
                     lambda d: d['datasets'][0]['resolutions'][0]['path'].append({'items': True}),
                     lambda d: d['datasets'][0]['schema']['fields'][0]['items'].update(type='string')]
        for change in mutations:
            edited = deepcopy(document); change(edited)
            with self.assertRaises(UserError):
                SchemaManifest(edited)
        downgraded = deepcopy(document); downgraded['schema_version'] = 3
        dataset = downgraded['datasets'][0]
        dataset['schema_fingerprint'] = schema_fingerprint(dataset['schema'], dataset['ingestion']['parser'], 3)
        with self.assertRaisesRegex(UserError, 'version-4'):
            SchemaManifest(downgraded)

    def test_unselected_datasets_remain_blocked(self):
        baseline = SchemaManifest(crawl({'one/a.json': b'{"x":[]}', 'two/a.json': b'{"y":[]}'}).document)
        draft = deepcopy(baseline.document)
        draft['datasets'][0]['schema']['fields'][0]['items']['type'] = 'integer'
        source = MemorySource({'one/a.json': b'{"x":[1]}', 'two/a.json': b'{"y":[]}'})
        with tempfile.TemporaryDirectory() as parent:
            result = ResolveSchema(baseline, draft, source, S3DatasetAdapter, Path(parent) / 'resolved.yml',
                                   dataset_ids=[draft['datasets'][0]['id']]).execute()
        self.assertTrue(result.document['datasets'][0]['ingestion']['ready'])
        self.assertFalse(result.document['datasets'][1]['ingestion']['ready'])
        self.assertEqual(result.document['datasets'][1]['schema'], baseline.document['datasets'][1]['schema'])

    def test_cancellation_and_existing_output(self):
        baseline, draft = array_draft()
        source = MemorySource({'a.jsonl': b'{"ids":[1]}\n'})
        with tempfile.TemporaryDirectory() as parent:
            path = Path(parent) / 'resolved.yml'
            with patch.object(source, 'read_range', side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
                ResolveSchema(baseline, draft, source, S3DatasetAdapter, path).execute()
            self.assertEqual(list(Path(parent).iterdir()), [])
            path.write_text('original')
            with self.assertRaises(UserError):
                ResolveSchema(baseline, draft, source, S3DatasetAdapter, path).execute()
            self.assertEqual(path.read_text(), 'original')

    def test_cli_dispatch_and_reject_flags(self):
        baseline, draft = array_draft()
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            write_report(baseline, root / 'baseline.yml')
            (root / 'draft.yml').write_text(yaml.safe_dump(draft))
            args = ['--module', 'schema', '--mode', 's3', '--action', 'resolve', '--bucket', 'test',
                    '--baseline', str(root / 'baseline.yml'), '--schema', str(root / 'draft.yml'), '--output', str(root / 'resolved.yml')]
            source = MemorySource({'a.jsonl': b'{"ids":[1]}\n'})
            with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(source)), patch('dredge.modules.schema.presentation.present'):
                self.assertEqual(main(args), 0)
            SchemaManifest.load(root / 'resolved.yml')
            for extra in (['--dry-run'], ['--overwrite'], ['--prefix', 'other'], ['--sample-files', '0']):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    main(args + extra)
                self.assertEqual(error.exception.code, 2)

    def test_version_two_baseline_is_supported(self):
        baseline, draft = array_draft()
        def strip(node):
            if isinstance(node, dict):
                return {k: strip(v) for k,v in node.items() if k not in ('inference', 'source')}
            if isinstance(node, list):
                return [strip(v) for v in node]
            return node
        old = deepcopy(baseline.document)
        old['schema_version'] = 2
        for dataset in old['datasets']:
            dataset['schema'] = strip(dataset['schema'])
            dataset['schema_fingerprint'] = schema_fingerprint(dataset['schema'], dataset['ingestion']['parser'], 2)
        baseline = SchemaManifest(old)
        draft = deepcopy(old)
        draft['datasets'][0]['schema']['fields'][0]['items']['type'] = 'integer'
        with tempfile.TemporaryDirectory() as parent:
            path = Path(parent) / 'schema.yml'
            ResolveSchema(baseline, draft, MemorySource({'a.jsonl': b'{"ids":[1]}\n'}), S3DatasetAdapter, path).execute()
            self.assertEqual(SchemaManifest.load(path).document['resolution']['baseline']['schema_version'], 2)

    def test_null_preservation_and_runtime_drift_after_resolution(self):
        baseline, draft, data = numeric_draft()
        # Discover a nullable union by including a real null record.
        data['a.jsonl'] += b'{"score":null}\n'
        baseline = SchemaManifest(crawl(data).document)
        draft = deepcopy(baseline.document)
        field = draft['datasets'][0]['schema']['fields'][0]
        field['type'] = 'float'; del field['alternatives']
        draft['datasets'][0]['resolutions'] = [{'op': 'extract_number', 'path': [{'field': 'score'}], 'members': ['double', 'int']}]
        with tempfile.TemporaryDirectory() as parent:
            result = ResolveSchema(baseline, draft, MemorySource(data), S3DatasetAdapter, Path(parent) / 'schema.yml').execute()
        dataset = result.document['datasets'][0]
        source = MemorySource({'a.jsonl': b'{"score":null}\n'})
        self.assertIsNone(next(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute()).values['score'])
        source = MemorySource({'a.jsonl': b'{"score":{"double":1.5,"int":3}}\n'})
        with self.assertRaisesRegex(UserError, 'exactly one'):
            list(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute())

    def test_atomic_publication_failure_and_other_discovery_blockers(self):
        baseline, draft = array_draft()
        with tempfile.TemporaryDirectory() as parent:
            path = Path(parent) / 'schema.yml'
            with patch('dredge.core.schema_io.os.link', side_effect=OSError('failure')), self.assertRaises(UserError):
                ResolveSchema(baseline, draft, MemorySource({'a.jsonl': b'{"ids":[1]}\n'}), S3DatasetAdapter, path).execute()
            self.assertEqual(list(Path(parent).iterdir()), [])
        original = deepcopy(baseline.document)
        original['datasets'][0]['ingestion']['blockers'].append('sample_failures')
        baseline = SchemaManifest(original)
        draft = deepcopy(original)
        draft['datasets'][0]['schema']['fields'][0]['items']['type'] = 'integer'
        with self.assertRaisesRegex(UserError, 'sample_failures'):
            compile_draft(baseline.document, draft)

    def test_invalid_draft_and_bucket_do_not_open_provider(self):
        from dredge.modules.schema.runner import run_resolution
        baseline, draft = array_draft()
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            write_report(baseline, root / 'base.yml')
            (root / 'draft.yml').write_text(yaml.safe_dump(draft))
            config = dict(bucket='other', baseline=str(root / 'base.yml'), schema=str(root / 'draft.yml'), output=str(root / 'resolved.yml'))
            with patch('dredge.modes.s3.clients.open_source') as provider, self.assertRaisesRegex(UserError, 'bucket'):
                run_resolution(config)
            provider.assert_not_called()
            config['bucket'] = 'test'
            draft['source']['bucket'] = 'wrong'
            (root / 'draft.yml').write_text(yaml.safe_dump(draft))
            with patch('dredge.modes.s3.clients.open_source') as provider, self.assertRaises(UserError):
                run_resolution(config)
            provider.assert_not_called()

    def test_runner_releases_source_after_failed_validation(self):
        from contextlib import contextmanager
        from dredge.modules.schema.runner import run_resolution
        baseline, draft = array_draft()
        closed = []
        @contextmanager
        def source_context(settings):
            try:
                yield MemorySource({'a.jsonl': b'{"ids":[]}\n'})
            finally:
                closed.append(True)
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            write_report(baseline, root / 'base.yml')
            (root / 'draft.yml').write_text(yaml.safe_dump(draft))
            config = dict(bucket='test', baseline=str(root / 'base.yml'), schema=str(root / 'draft.yml'), output=str(root / 'resolved.yml'))
            with patch('dredge.modes.s3.clients.open_source', source_context), self.assertRaisesRegex(UserError, 'insufficient'):
                run_resolution(config)
            self.assertEqual(closed, [True])
            self.assertFalse((root / 'resolved.yml').exists())
