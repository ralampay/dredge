"""Synthetic contract tests; no bucket names, credentials, or live records."""
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import io
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from dredge.core.exceptions import UserError
from dredge.core.schema_manifest import SchemaManifest
from dredge.core.sources import ObjectInfo
from dredge.formats.s3_access_log import parse_record
from dredge.modes.s3.datasets import S3DatasetAdapter
from dredge.modules.discover.serialization import write_report
from dredge.modules.ingest.commands import ReadDatasetRecords
from dredge.modules.ingest.validation import ValidateDatasetRecords
from test_discover import MemorySource, crawl


LOG = ('owner example [02/Nov/2026:01:02:03 +0000] 192.0.2.1 requester request-id '
       'REST.GET.OBJECT folder/item "GET /folder/item HTTP/1.1" 200 - 12 20 3 2 '
       '"-" "Example Agent/1.0" - host SigV4 cipher AuthHeader example.s3.amazonaws.com '
       'TLSv1.3 - -')
KEY = '2026-11-02-01-02-03-0123456789ABCDEF'


class AccessLogTests(unittest.TestCase):
    def test_old_current_and_future_fields(self):
        for suffix, region, extras in (('', None, []), (' ap-northeast-2', 'ap-northeast-2', []),
                                      (' ap-northeast-2 future value', 'ap-northeast-2', ['future', 'value'])):
            with self.subTest(suffix=suffix):
                record = parse_record(LOG + suffix)
                self.assertEqual(record['request_time'], datetime(2026, 11, 2, 1, 2, 3, tzinfo=timezone.utc))
                self.assertEqual(record['http_status'], 200)
                self.assertEqual(record['total_time_ms'], 3)
                self.assertIsNone(record['error_code'])
                self.assertIsNone(record['referer'])
                self.assertEqual(record['source_region'], region)
                self.assertEqual(record['extra_fields'], extras)
        legacy = LOG.split(' host ')[0]
        self.assertIsNone(parse_record(legacy)['host_id'])

    def test_quotes_offsets_and_missing_fields(self):
        line = LOG.replace('GET /folder/item', 'GET /folder/"quoted"').replace('Example Agent/1.0', 'Example "Quoted" Agent')
        result = parse_record(line)
        self.assertIn('"quoted"', result['request_uri'])
        self.assertEqual(result['user_agent'], 'Example "Quoted" Agent')
        offset = parse_record(LOG.replace('01:02:03 +0000', '10:02:03 +0900'))
        self.assertEqual(offset['request_time'].hour, 1)
        missing = LOG.replace('[02/Nov/2026:01:02:03 +0000]', '-').replace(' 200 - 12 20 3 2 ', ' - - - - - - ')
        self.assertIsNone(parse_record(missing)['request_time'])
        self.assertIsNone(parse_record(missing)['http_status'])

    def test_invalid_records(self):
        for line in ('bad private payload', LOG.replace(' 200 ', ' error '),
                     LOG.replace('02/Nov', '32/Nov'), LOG.replace('+0000', '+2500'), LOG[:100]):
            with self.subTest(line=line), self.assertRaises(ValueError):
                parse_record(line)

    def test_auto_discovery_and_declared_schema(self):
        report = crawl({KEY: (LOG + '\n').encode(), KEY + '.gz': gzip.compress((LOG + '\n').encode())})
        self.assertEqual(report.document['schema_version'], 3)
        dataset = report.document['datasets'][0]
        self.assertEqual(dataset['format'], 's3-access-log')
        self.assertEqual(dataset['schema_origin'], 'format-contract')
        self.assertTrue(dataset['ingestion']['ready'])
        self.assertEqual(len(dataset['schema']['fields']), 28)
        self.assertNotIn('192.0.2.1', str(report.document))
        self.assertEqual(dataset['matched_objects'], 2)


class ManifestTests(unittest.TestCase):
    def test_roundtrip_and_consumer_parsers(self):
        parquet = io.BytesIO()
        pq.write_table(pa.table({'id': [1, 2]}), parquet)
        fixtures = [
            ({KEY: (LOG + '\n').encode()}, {}),
            ({'a.json': b'[{"id":1},{"id":2}]'}, {}),
            ({'a.jsonl.gz': gzip.compress(b'{"id":1}\n{"id":2}\n')}, {}),
            ({'a.csv': b'id;name\n001;A\n002;B\n'}, {'csv_delimiter': ';', 'csv_header': 'present'}),
            ({'a.parquet': parquet.getvalue()}, {}),
        ]
        for data, options in fixtures:
            with self.subTest(data=list(data)):
                report = crawl(data, **options)
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / 'schema.yml'
                    write_report(report, path)
                    manifest = SchemaManifest.load(path)
                dataset = manifest.dataset(report.document['datasets'][0]['id'])
                source = MemorySource(data)
                selection = S3DatasetAdapter(source, dataset)
                records = list(ReadDatasetRecords(dataset, source, selection).execute())
                self.assertGreater(len(records), 0)
                self.assertTrue(all(record.key in data for record in records))

    def test_selectors_are_exact_and_extract_partitions(self):
        data = {'events/year=2026/a.json': b'{"x":1}', 'events/year=2025/a.json': b'{"x":2}',
                'events/year=2026/a.csv': b'x\n1\n', 'other/a.json': b'{"x":3}'}
        report = crawl(data, include=('events/*',), exclude=('*/a.csv',))
        dataset = SchemaManifest(report.document).dataset(report.document['datasets'][0]['id'])
        source = MemorySource(data)
        selector = S3DatasetAdapter(source, dataset)
        self.assertEqual(len(list(selector.objects())), 2)
        records = list(ReadDatasetRecords(dataset, source, selector).execute())
        self.assertEqual({r.partitions[0]['value'] for r in records}, {'2025', '2026'})
        self.assertFalse(selector.matches(ObjectInfo('events/year=2026/nested/a.json', 10)))
        self.assertFalse(selector.matches(ObjectInfo('events/year=2026/a.csv', 10)))

    def test_readiness_and_drift(self):
        conflict = crawl({'a.json': b'[{"x":1},{"x":"one"}]'}).document['datasets'][0]
        self.assertFalse(conflict['ingestion']['ready'])
        source = MemorySource({'a.json': b'{"x":1}'})
        with self.assertRaises(UserError):
            list(ReadDatasetRecords(conflict, source, S3DatasetAdapter(source, conflict)).execute())
        dataset = crawl({'a.json': b'{"x":1}'}).document['datasets'][0]
        for data in (b'{"x":"private"}', b'{"x":1,"new":2}', b'{}'):
            source = MemorySource({'a.json': data})
            with self.assertRaises(UserError) as error:
                list(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset)).execute())
            self.assertNotIn('private', str(error.exception))

    def test_byte_limit_fails_instead_of_silent_truncation(self):
        source = MemorySource({'a.jsonl': b'{"x":1}\n' * 100})
        dataset = crawl(source.data).document['datasets'][0]
        with self.assertRaisesRegex(UserError, 'byte limit'):
            list(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset), max_bytes_per_file=32).execute())
        records = list(ReadDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset), max_records_per_file=3).execute())
        self.assertEqual(len(records), 3)

    def test_holdout_validation_excludes_discovery_samples(self):
        data = {f'part-{i}.json': b'{"x":1}' for i in range(20)}
        dataset = crawl(data, sample_files=3).document['datasets'][0]
        source = MemorySource(data)
        result = ValidateDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset), sample_files=5).execute()
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['eligible_holdout_objects'], 17)
        self.assertEqual(result['validated_records'], 5)
        self.assertFalse({s['key'] for s in dataset['samples']} & {s['key'] for s in result['objects']})
        for item in result['objects']:
            data[item['key']] = b'{"x":"different"}'
        failed = ValidateDatasetRecords(dataset, source, S3DatasetAdapter(source, dataset), sample_files=5).execute()
        self.assertEqual(failed['failed_objects'], 5)
        self.assertEqual(failed['status'], 'failed')
        all_sampled = crawl({'a.json': b'{"x":1}'}).document['datasets'][0]
        small_source = MemorySource({'a.json': b'{"x":1}'})
        no_holdout = ValidateDatasetRecords(all_sampled, small_source, S3DatasetAdapter(small_source, all_sampled)).execute()
        self.assertEqual(no_holdout['status'], 'not_validated')

    def test_selector_rejects_different_bucket(self):
        dataset = crawl({'a.json': b'{"x":1}'}).document['datasets'][0]
        source = MemorySource({'a.json': b'{"x":1}'})
        source.bucket = 'different'
        with self.assertRaisesRegex(UserError, 'bucket'):
            S3DatasetAdapter(source, dataset)

    def test_invalid_manifests(self):
        document = crawl({'a.json': b'{"x":1}'}).document
        bad = []
        legacy = deepcopy(document); legacy['schema_version'] = 1; bad.append(legacy)
        duplicate = deepcopy(document); duplicate['datasets'] *= 2; bad.append(duplicate)
        version = deepcopy(document); version['datasets'][0]['ingestion']['parser']['version'] = 99; bad.append(version)
        wrong = deepcopy(document); wrong['datasets'][0]['ingestion']['selector']['format'] = 'csv'; bad.append(wrong)
        schema = deepcopy(document); schema['datasets'][0]['schema']['fields'][0]['nullable'] = 'false'; bad.append(schema)
        fingerprint = deepcopy(document); fingerprint['datasets'][0]['schema_fingerprint'] = 'tampered'; bad.append(fingerprint)
        partition = deepcopy(document); partition['datasets'][0]['ingestion']['selector']['partitions'] = [{'name': 'day', 'segment': -1}]; bad.append(partition)
        encoding = deepcopy(document); encoding['datasets'][0]['ingestion']['parser']['encoding'] = 'not-an-encoding'; bad.append(encoding)
        for value in bad:
            with self.subTest(value=value), self.assertRaises(UserError):
                SchemaManifest(value)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'schema.yml'
            path.write_text('!!python/object:os.system {}')
            with self.assertRaises(UserError):
                SchemaManifest.load(path)
