"""Assumed structure, evidence, and executable temporal contracts."""
from copy import deepcopy
from datetime import date, datetime, timezone
import io
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from dredge.core.exceptions import UserError
from dredge.core.schema_manifest import SchemaManifest, schema_fingerprint
from dredge.modes.s3.datasets import S3DatasetAdapter
from dredge.modules.ingest.commands import ReadDatasetRecords
from test_discover import crawl, MemorySource
from test_schema_manifest import LOG, KEY


def dataset(data, **options):
    return crawl(data, **options).document['datasets'][0]


def consume(definition, data):
    source = MemorySource(data)
    return list(ReadDatasetRecords(definition, source, S3DatasetAdapter(source, definition)).execute())


class AssumedContractTests(unittest.TestCase):
    def test_header_detection_and_source_order(self):
        data = {'a.csv': b'name,id,day\nAlice,1,2026-11-02\nBob,2,2026-11-03\n'}
        d = dataset(data)
        fields = d['schema']['fields']
        self.assertEqual([f['name'] for f in fields], ['name', 'id', 'day'])
        self.assertEqual(d['ingestion']['parser']['csv_header'], 'present')
        self.assertTrue(d['ingestion']['ready'])
        self.assertEqual(fields[0]['source'], {'kind': 'csv-header', 'path': '/name', 'positions': [0], 'assumed': True})
        self.assertEqual(fields[2]['type'], 'date')
        self.assertEqual(consume(d, data)[0].values['day'], date(2026, 11, 2))

    def test_headerless_ambiguous_and_explicit_override(self):
        no_header = dataset({'a.csv': b'1,Alice\n2,Bob\n'})
        self.assertTrue(no_header['ingestion']['ready'])
        self.assertEqual(no_header['schema']['fields'][0]['name'], 'column_1')
        self.assertEqual(no_header['schema']['fields'][0]['source']['kind'], 'generated-position')
        for value in (b'name,code\nAlice,001\nBob,002\n', b'name,id\n', b'Alice,example\nBob,other\n'):
            with self.subTest(value=value):
                d = dataset({'a.csv': value})
                self.assertFalse(d['ingestion']['ready'])
                self.assertIn('ambiguous_csv_header_use_explicit_override', d['ingestion']['blockers'])
                # The first row is retained as a data record, never silently discarded.
                self.assertEqual(d['samples'][0]['records'], len(value.splitlines()))
        explicit = dataset({'a.csv': b'name,code\nAlice,001\n'}, csv_header='present')
        self.assertTrue(explicit['ingestion']['ready'])
        self.assertEqual(explicit['schema']['fields'][1]['type'], 'string')

    def test_header_modes_must_agree_but_order_can_vary(self):
        mixed = dataset({'a.csv': b'id,name\n1,A\n', 'b.csv': b'2,B\n3,C\n'})
        self.assertIn('inconsistent_csv_header_presence', mixed['ingestion']['blockers'])
        data = {'a.csv': b'id,name\n1,A\n', 'b.csv': b'name,id\nB,2\n'}
        d = dataset(data)
        self.assertTrue(d['ingestion']['ready'])
        for field in d['schema']['fields']:
            self.assertEqual(field['source']['positions'], [0, 1])
        self.assertEqual({r.values['id'] for r in consume(d, data)}, {1, 2})

    def test_temporal_rules_nested_paths_and_drift(self):
        data = {'a.json': b'[{"nested":{"when":"2026-11-02T09:00:00+09:00"},"days":["2026-11-02"]}]'}
        d = dataset(data)
        records = consume(d, data)
        self.assertEqual(records[0].values['nested']['when'], datetime(2026,11,2,tzinfo=timezone.utc))
        self.assertEqual(records[0].values['days'], [date(2026,11,2)])
        nested = d['schema']['fields'][0]['fields'][0]
        self.assertEqual(nested['source']['path'], '/nested/when')
        with self.assertRaisesRegex(UserError, 'iso-timestamp-offset'):
            consume(d, {'a.json': b'[{"nested":{"when":"2026-11-02"},"days":["2026-11-02"]}]'})
        off = dataset(data, temporal_types='off')
        self.assertEqual(off['schema']['fields'][0]['fields'][0]['type'], 'string')

    def test_conservative_temporal_inference(self):
        for raw in (b'[{"x":"2026-11-02"},{"x":"other"}]',
                    b'[{"x":"2026-11-02"},{"x":"2026-02-30"}]',
                    b'[{"x":"11/02/2026"}]', b'[{"x":"2026-11-02T01:02:03"}]',
                    b'[{"x":"001"}]', b'[{"x":"123"}]'):
            with self.subTest(raw=raw):
                field = dataset({'a.json': raw})['schema']['fields'][0]
                self.assertEqual(field['type'], 'string')
        d = dataset({'a.json': b'[{"x":"2026-11-02"},{"x":"other"}]'})
        self.assertEqual(d['schema']['fields'][0]['inference']['diagnostic'], 'mixed_temporal_and_other_strings')

    def test_evidence_null_missing_and_fingerprint(self):
        d = dataset({'a.json': b'[{"z":1,"a":null},{"z":2},{"a":"value"}]'})
        self.assertEqual([f['name'] for f in d['schema']['fields']], ['z', 'a'])
        info = d['schema']['fields'][1]['inference']
        self.assertEqual((info['present_count'], info['null_count'], info['missing_count']), (2, 1, 1))
        self.assertEqual(info['observed_types'], {'null': 1, 'string': 1})
        a = dataset({'a.json': b'[{"x":1}]'})
        b = dataset({'a.json': b'[{"x":1},{"x":2}]'})
        self.assertEqual(a['schema_fingerprint'], b['schema_fingerprint'])

    def test_declared_order_and_metadata_evidence(self):
        log = dataset({KEY: LOG.encode()})
        self.assertEqual([f['name'] for f in log['schema']['fields'][:3]], ['bucket_owner', 'bucket', 'request_time'])
        self.assertEqual(log['schema']['fields'][2]['source']['positions'], [2])
        self.assertEqual(log['schema']['fields'][2]['inference']['origin'], 'format-contract')
        output = io.BytesIO()
        pq.write_table(pa.table({'z': [1], 'a': ['value']}), output)
        d = dataset({'a.parquet': output.getvalue()})
        self.assertEqual([f['name'] for f in d['schema']['fields']], ['z', 'a'])
        self.assertEqual(d['schema']['fields'][0]['inference'], {'origin': 'parquet-metadata'})

    def test_header_only_and_budget_limited_lookahead(self):
        header_only = dataset({'a.csv': b'name,id\n'}, csv_header='present')
        self.assertEqual([f['name'] for f in header_only['schema']['fields']], ['name', 'id'])
        self.assertFalse(header_only['ingestion']['ready'])
        limited = dataset({'a.csv': b'id,name\n1,Alice\n2,' + b'x' * 1000}, sample_bytes=32)
        self.assertEqual(limited['samples'][0]['records'], 1)
        self.assertIn('bytes', limited['samples'][0]['limits'])

    def test_invalid_evidence_is_rejected_without_affecting_fingerprint(self):
        document = crawl({'a.json': b'{"x":1}'}).document
        field = document['datasets'][0]['schema']['fields'][0]
        field['inference']['present_count'] = 2
        with self.assertRaisesRegex(UserError, 'evidence'):
            SchemaManifest(document)

    def test_v2_compatibility(self):
        document = crawl({'a.json': b'{"x":1}'}).document
        legacy = deepcopy(document)
        legacy['schema_version'] = 2
        def strip(node):
            node.pop('source', None); node.pop('inference', None)
            for field in node.get('fields', []):
                strip(field)
            if 'items' in node:
                strip(node['items'])
        d = legacy['datasets'][0]
        strip(d['schema'])
        d['schema_fingerprint'] = schema_fingerprint(d['schema'], d['ingestion']['parser'])
        manifest = SchemaManifest(legacy)
        self.assertEqual(consume(manifest.dataset(d['id']), {'a.json': b'{"x":1}'})[0].values, {'x': 1})
        self.assertEqual(SchemaManifest(document).document['schema_version'], 3)
