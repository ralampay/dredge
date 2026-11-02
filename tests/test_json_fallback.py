"""Opt-in JSON fallback from discovery through Parquet and notebook output."""
from contextlib import nullcontext, redirect_stderr
from copy import deepcopy
from dataclasses import replace
from datetime import date
from decimal import Decimal
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from dredge.cli import main
from dredge.core.exceptions import UserError
from dredge.core.schema_manifest import SchemaManifest, schema_fingerprint
from dredge.formats.json_fallback import encode_json
from dredge.modules.schema.json_fallback import apply_json_fallback
from dredge.modules.discover.crawl_and_resolve import CrawlAndResolveSchemas
from dredge.modules.discover.requests import CrawlOptions, CrawlRequest, CrawlResolutionRequest
from dredge.modules.discover.presentation import present
from dredge.modules.ingest.ingest_datasets import IngestDatasets
from dredge.modules.ingest.commands import ReadDatasetRecords
from dredge.modes.s3.datasets import S3DatasetAdapter
from test_discover import MemorySource, crawl


DATA = {'a.jsonl': b'{"id":1,"ids":[],"score":1.5,"nothing":null}\n'
                    b'{"id":2,"ids":[],"score":{"double":2.5,"int":null},"nothing":null}\n'}


def resolve(data, root, **options):
    request = CrawlResolutionRequest(CrawlRequest(CrawlOptions(bucket='test', **options), str(root / 'schema.yml')),
                                     None, unresolved_as='json')
    source = MemorySource(data)
    result = CrawlAndResolveSchemas(source, S3DatasetAdapter, request, None).execute()
    return result, source


class JsonFallbackTests(unittest.TestCase):
    def test_complete_discovery_ingestion_and_notebook_roundtrip(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            result, source = resolve(DATA, root)
            manifest = SchemaManifest.load(root / 'schema.yml')
            dataset = manifest.document['datasets'][0]
            types = {f['name']: f['type'] for f in dataset['schema']['fields']}
            self.assertEqual(types, {'id': 'integer', 'ids': 'string', 'score': 'string', 'nothing': 'string'})
            self.assertEqual([r['path'] for r in dataset['resolutions']], [[{'field': 'ids'}], [{'field': 'score'}], [{'field': 'nothing'}]])
            self.assertEqual(result.document['resolution']['validation'][0]['rule_coverage'], [2, 2, 2])
            self.assertFalse(result.document['resolution']['baseline']['datasets'][0]['ingestion']['ready'])
            output = root / 'ingested'
            written = IngestDatasets(manifest, source, [dataset], [S3DatasetAdapter(source, dataset)], output).execute()
            entry = written.document['datasets'][0]
            rows = pq.read_table(output / entry['data_path']).to_pylist()
            self.assertEqual(rows[0], {'id': 1, 'ids': '[]', 'score': '1.5', 'nothing': None})
            self.assertEqual(json.loads(rows[1]['score']), {'double': 2.5, 'int': None})
            self.assertEqual(json.loads(rows[0]['ids']), [])
            notebook = json.loads((output / entry['notebook']).read_text())
            self.assertEqual(len(notebook['cells']), 2)
            self.assertIn('json.loads', notebook['cells'][0]['source'])
            self.assertTrue(all(rule['op'] == 'encode_json' for rule in entry['resolutions']))
            display = io.StringIO()
            present(result, str(root / 'schema.yml'), console=Console(file=display, width=120))
            self.assertIn('JSON-encoded', display.getvalue())

    def test_future_values_are_encoded_but_known_fields_remain_strict(self):
        with tempfile.TemporaryDirectory() as parent:
            result, _ = resolve(DATA, Path(parent))
        dataset = result.document['datasets'][0]
        for value in ([1, 'a', {'nested': True}], 'literal string', {'different': [1]}, []):
            data = {'a.jsonl': (json.dumps({'id': 3, 'ids': value, 'score': 'text', 'nothing': None}) + '\n').encode()}
            source = MemorySource(data)
            record = next(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute())
            self.assertEqual(json.loads(record.values['ids']), value)
            self.assertEqual(json.loads(record.values['score']), 'text')
        for line in (b'{"id":"wrong","ids":[],"score":1,"nothing":null}',
                     b'{"id":3,"ids":[],"score":1,"nothing":null,"new":5}', b'not json'):
            source = MemorySource({'a.jsonl': line})
            with self.assertRaises(UserError):
                list(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute())

    def test_nested_structs_and_arrays_choose_nonoverlapping_targets(self):
        data = {'a.json': b'{"outer":{"keep":1,"unknown":null},"rows":[{"keep":2,"ids":[]}]}' }
        baseline = SchemaManifest(crawl(data).document)
        original = deepcopy(baseline.document)
        draft, selected, statuses = apply_json_fallback(baseline)
        self.assertEqual(baseline.document, original)
        self.assertEqual([s['path'] for s in statuses], [[{'field': 'outer'}, {'field': 'unknown'}], [{'field': 'rows'}]])
        with tempfile.TemporaryDirectory() as parent:
            result, source = resolve(data, Path(parent))
        dataset = result.document['datasets'][0]
        row = next(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute()).values
        self.assertEqual(row['outer'], {'keep': 1, 'unknown': None})
        self.assertEqual(json.loads(row['rows']), [{'keep': 2, 'ids': []}])

    def test_exact_decimal_unicode_and_null_encoding(self):
        value = {'decimal': Decimal('123456789.123456789123456789'), 'huge': 2**80,
                 'text': 'a\n雪"', 'bool': True, 'null': None}
        encoded = encode_json(value, '$')
        self.assertEqual(json.loads(encoded, parse_float=Decimal), value)
        self.assertEqual(encode_json([], '$'), '[]')
        self.assertEqual(encode_json('[]', '$'), '"[]"')
        self.assertIsNone(encode_json(None, '$'))
        for bad in (float('nan'), float('inf'), Decimal('NaN'), b'private', date(2026, 1, 1), {1: 'key'}, [b'private']):
            with self.assertRaises(UserError) as error:
                encode_json(bad, '$["field"]')
            self.assertIn('$["field"]', str(error.exception))
            self.assertNotIn('private', str(error.exception))

    def test_parquet_scalar_struct_conflict(self):
        data = {}
        for key, table in [('a.parquet', pa.table({'score': [1.5]})),
                           ('b.parquet', pa.table({'score': pa.array([{'double': 2.5, 'int': None}],
                             type=pa.struct([('double', pa.float64()), ('int', pa.int32())]))}))]:
            stream = io.BytesIO(); pq.write_table(table, stream); data[key] = stream.getvalue()
        with tempfile.TemporaryDirectory() as parent:
            result, source = resolve(data, Path(parent))
        dataset = result.document['datasets'][0]
        rows = list(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute())
        self.assertEqual([json.loads(row.values['score']) for row in rows], [1.5, {'double': 2.5, 'int': None}])

    def test_no_unresolved_types_and_empty_only_values(self):
        for data, expected in [({'a.jsonl': b'{"id":1}\n'}, 0), ({'a.jsonl': b'{"ids":[],"x":null}\n'}, 2)]:
            with tempfile.TemporaryDirectory() as parent:
                result, _ = resolve(data, Path(parent))
                self.assertEqual(len(result.document['datasets'][0]['resolutions']), expected)
                self.assertTrue(result.document['datasets'][0]['ingestion']['ready'])
                SchemaManifest.load(Path(parent) / 'schema.yml')

    def test_errors_preserve_output_and_do_not_clear_other_blockers(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            (root / 'schema.yml').write_text('keep')
            request = CrawlResolutionRequest(CrawlRequest(CrawlOptions(bucket='test'), str(root / 'schema.yml'), True), None,
                                              sample_bytes=4, unresolved_as='json')
            with self.assertRaises(UserError):
                CrawlAndResolveSchemas(MemorySource(DATA), S3DatasetAdapter, request, None).execute()
            self.assertEqual((root / 'schema.yml').read_text(), 'keep')
            bad = MemorySource({'a.csv': b'one,two\nthree,four\n'})
            with self.assertRaisesRegex(UserError, 'header'):
                CrawlAndResolveSchemas(bad, S3DatasetAdapter, replace(request, sample_bytes=10000), None).execute()
            self.assertEqual((root / 'schema.yml').read_text(), 'keep')

    def test_contract_cannot_hide_edits_to_known_fields(self):
        with tempfile.TemporaryDirectory() as parent:
            result, _ = resolve(DATA, Path(parent))
        document = deepcopy(result.document)
        dataset = document['datasets'][0]
        dataset['schema']['fields'][0]['type'] = 'string'
        dataset['resolutions'].insert(0, {'op': 'encode_json', 'path': [{'field': 'id'}]})
        dataset['schema_fingerprint'] = schema_fingerprint(dataset['schema'], dataset['ingestion']['parser'], 4, dataset['resolutions'])
        report = document['resolution']['validation'][0]
        report['schema_fingerprint'] = dataset['schema_fingerprint']; report['rule_coverage'].insert(0, 2)
        with self.assertRaisesRegex(UserError, 'requires an unresolved'):
            SchemaManifest(document)

    def test_cli_flag_without_rules_and_mutual_exclusion(self):
        with tempfile.TemporaryDirectory() as parent:
            output = str(Path(parent) / 'schema.yml')
            args = ['--module', 'discover', '--mode', 's3', '--action', 'crawl', '--bucket', 'test',
                    '--output', output, '--unresolved-as', 'json']
            with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(MemorySource(DATA))), \
                    patch('dredge.modules.discover.presentation.present_combined'):
                self.assertEqual(main(args), 0)
            SchemaManifest.load(output)
            for extra in (['--resolve-with', 'rules.yml'], ['--unresolved-as', 'object']):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    main(args + extra)
                self.assertEqual(error.exception.code, 2)
            # Strict discovery still reports unresolved types by default.
            self.assertFalse(crawl(DATA).document['datasets'][0]['ingestion']['ready'])

    def test_absent_nullable_fallback_in_heldout_samples_is_valid(self):
        from dredge.modules.schema.commands import BuildResolvedSchema
        from dredge.modules.schema.requests import ValidationLimits
        baseline = SchemaManifest(crawl({'a.jsonl': b'{"id":1,"optional":null}\n'}).document)
        draft, selected, _ = apply_json_fallback(baseline)
        source = MemorySource({'a.jsonl': b'{"id":1,"optional":null}\n', 'b.jsonl': b'{"id":2}\n'})
        result = BuildResolvedSchema(baseline, draft, source, S3DatasetAdapter, dataset_ids=selected,
                                     limits=ValidationLimits(sample_files=1)).execute()
        validation = result.document['resolution']['validation'][0]
        self.assertEqual(validation['objects'][0]['key'], 'b.jsonl')
        self.assertEqual(validation['rule_coverage'], [1])

    def test_literal_paths_and_nested_empty_arrays(self):
        data = {'a.json': b'{"a/b.*":[[]],"stable":[1,2]}' }
        with tempfile.TemporaryDirectory() as parent:
            result, source = resolve(data, Path(parent))
        dataset = result.document['datasets'][0]
        self.assertEqual(dataset['resolutions'], [{'op': 'encode_json', 'path': [{'field': 'a/b.*'}]}])
        row = next(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute()).values
        self.assertEqual(row, {'a/b.*': '[[]]', 'stable': [1, 2]})

    def test_empty_structs_and_arrays_of_empty_structs_are_encoded(self):
        data = {'a.json': b'{"body":{},"rows":[{}],"known":{"id":1}}'}
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            result, source = resolve(data, root)
            manifest = SchemaManifest(result.document)
            dataset = manifest.document['datasets'][0]
            written = IngestDatasets(manifest, source, [dataset], [S3DatasetAdapter(source, dataset)], root / 'data').execute()
            rows = pq.read_table(root / 'data' / written.document['datasets'][0]['data_path']).to_pylist()
            self.assertEqual(rows, [{'body': '{}', 'rows': '[{}]', 'known': {'id': 1}}])
