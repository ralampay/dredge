"""Combined discovery/resolution behavior and publication guarantees."""
from contextlib import contextmanager, redirect_stderr
from copy import deepcopy
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
import yaml
from rich.console import Console

from dredge.cli import main
from dredge.core.exceptions import UserError
from dredge.core.schema_manifest import SchemaManifest
from dredge.modules.discover.crawl_and_resolve import CrawlAndResolveSchemas, validate_paths
from dredge.modules.discover.requests import CrawlOptions, CrawlRequest, CrawlResolutionRequest
from dredge.modules.discover.presentation import CrawlResolutionDisplay, present_combined
from dredge.modules.schema.rules import apply_rules, validate_rules_document
from dredge.modules.ingest.ingest_datasets import IngestDatasets
from dredge.modes.s3.datasets import S3DatasetAdapter
from test_discover import MemorySource, crawl


DATA = {'a.jsonl': b'{"ids":[]}\n{"ids":[1,2]}\n'}


def rules_for(data=DATA):
    identity = crawl(data, sample_records=1).document['datasets'][0]['id']
    return {'rules_version': 1, 'datasets': [{'id': identity, 'rules': [
        {'op': 'assert_type', 'path': [{'field': 'ids'}, {'items': True}], 'schema': {'type': 'integer'}}]}]}


def request(root, *, overwrite=False, raw=True, **options):
    return CrawlResolutionRequest(CrawlRequest(CrawlOptions(bucket='test', sample_records=1, **options),
                                               str(root / 'schema.yml'), overwrite),
                                  str(root / 'rules.yml'), str(root / 'raw.yml') if raw else None)


class CrawlResolutionTests(unittest.TestCase):
    def test_combined_output_can_be_ingested_and_repeated(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            events = []
            source = MemorySource(DATA)
            result = CrawlAndResolveSchemas(source, S3DatasetAdapter, request(root), rules_for(), events.append).execute()
            self.assertEqual(result.rules[0]['status'], 'applied')
            baseline = SchemaManifest.load(root / 'raw.yml')
            self.assertEqual(baseline.document['schema_version'], 3)
            self.assertFalse(baseline.document['datasets'][0]['ingestion']['ready'])
            resolved = SchemaManifest.load(root / 'schema.yml')
            self.assertEqual(resolved.document['schema_version'], 4)
            self.assertEqual(resolved.document['resolution']['baseline'], baseline.document)
            datasets = resolved.document['datasets']
            written = IngestDatasets(resolved, source, datasets, [S3DatasetAdapter(source, d) for d in datasets], root / 'data').execute()
            self.assertEqual(pq.read_table(root / 'data' / written.document['datasets'][0]['data_path'])['ids'].to_pylist(), [[], [1, 2]])
            self.assertEqual({e.phase for e in events}, {'discovery', 'discovery_output', 'rules', 'rules_applied', 'validation', 'publication'})
            again = CrawlAndResolveSchemas(source, S3DatasetAdapter, request(root, overwrite=True), rules_for()).execute()
            self.assertEqual(again.rules[0]['status'], 'applied')
            output = io.StringIO()
            present_combined(result, str(root / 'schema.yml'), console=Console(file=output, width=100))
            self.assertIn('applied', output.getvalue())

    def test_already_resolved_type_is_satisfied_without_transformation(self):
        source = MemorySource({'a.jsonl': b'{"ids":[1]}\n'})
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            result = CrawlAndResolveSchemas(source, S3DatasetAdapter, request(root, raw=False), rules_for()).execute()
            self.assertEqual(result.rules[0]['status'], 'satisfied')
            self.assertEqual(result.document['datasets'][0]['resolutions'], [])
            self.assertEqual(result.document['resolution']['validation'][0]['validated_records'], 1)
            self.assertFalse((root / 'raw.yml').exists())
            SchemaManifest.load(root / 'schema.yml')

    def test_incompatible_or_missing_targets_preserve_existing_final(self):
        for data in ({'a.jsonl': b'{"ids":["wrong"]}\n'}, {'a.jsonl': b'{"other":1}\n'},
                     {'a.jsonl': b'{"ids":[]}\n'}):
            with self.subTest(data=data), tempfile.TemporaryDirectory() as parent:
                root = Path(parent)
                (root / 'schema.yml').write_text('keep me')
                with self.assertRaises(UserError):
                    CrawlAndResolveSchemas(MemorySource(data), S3DatasetAdapter, request(root, overwrite=True), rules_for()).execute()
                self.assertEqual((root / 'schema.yml').read_text(), 'keep me')
                self.assertEqual(SchemaManifest.load(root / 'raw.yml').document['schema_version'], 3)
                self.assertEqual({p.name for p in root.iterdir()}, {'schema.yml', 'raw.yml'})

    def test_blocked_unruled_dataset_and_partial_discovery_fail(self):
        fixtures = [({'a.jsonl': DATA['a.jsonl'], 'other/b.jsonl': b'{"unknown":null}\n'}, {}),
                    ({'a.jsonl': DATA['a.jsonl'], 'bad.jsonl': b'bad'}, {'on_error': 'continue'})]
        for data, options in fixtures:
            with tempfile.TemporaryDirectory() as parent:
                root = Path(parent)
                with self.assertRaises(UserError):
                    CrawlAndResolveSchemas(MemorySource(data), S3DatasetAdapter, request(root, **options), rules_for()).execute()
                self.assertFalse((root / 'schema.yml').exists())
                self.assertTrue((root / 'raw.yml').exists())

    def test_cancellation_and_publication_error_preserve_output(self):
        for error in (KeyboardInterrupt(), UserError('validation failed')):
            with tempfile.TemporaryDirectory() as parent:
                root = Path(parent)
                (root / 'schema.yml').write_text('original')
                with patch('dredge.modules.schema.commands.ValidateResolution.execute', side_effect=error), self.assertRaises(type(error)):
                    CrawlAndResolveSchemas(MemorySource(DATA), S3DatasetAdapter, request(root, overwrite=True), rules_for()).execute()
                self.assertEqual((root / 'schema.yml').read_text(), 'original')
                self.assertTrue((root / 'raw.yml').exists())
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            (root / 'schema.yml').write_text('original')
            with patch('dredge.core.schema_io.os.replace', side_effect=OSError('disk error')), self.assertRaises(UserError):
                CrawlAndResolveSchemas(MemorySource(DATA), S3DatasetAdapter, request(root, raw=False, overwrite=True), rules_for()).execute()
            self.assertEqual((root / 'schema.yml').read_text(), 'original')
            self.assertEqual(len(list(root.iterdir())), 1)

    def test_preflight_aliases_and_overwrite(self):
        from dataclasses import replace
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            req = request(root, overwrite=True)
            (root / 'rules.yml').write_text('rules')
            invalid = [replace(req, discovery_output=req.crawl.output),
                       replace(req, discovery_output=req.rules_path),
                       replace(req, crawl=replace(req.crawl, output=req.rules_path))]
            for candidate in invalid:
                with self.assertRaisesRegex(UserError, 'distinct'):
                    validate_paths(candidate)
            (root / 'alias.yml').hardlink_to(root / 'rules.yml')
            with self.assertRaisesRegex(UserError, 'distinct'):
                validate_paths(replace(req, discovery_output=str(root / 'alias.yml')))
            (root / 'raw.yml').write_text('old')
            source = MemorySource(DATA)
            with patch.object(source, 'objects') as listing, self.assertRaises(UserError):
                CrawlAndResolveSchemas(source, S3DatasetAdapter, request(root), rules_for()).execute()
            listing.assert_not_called()
            self.assertEqual((root / 'raw.yml').read_text(), 'old')

    def test_rules_validation(self):
        rules = rules_for()
        cases = [{}, {**rules, 'rules_version': 2}, {**rules, 'datasets': []}]
        duplicate = deepcopy(rules); duplicate['datasets'] *= 2; cases.append(duplicate)
        overlapping = deepcopy(rules); overlapping['datasets'][0]['rules'] *= 2; cases.append(overlapping)
        invalid = deepcopy(rules); invalid['datasets'][0]['rules'][0]['schema']['nullable'] = True; cases.append(invalid)
        invalid = deepcopy(rules); invalid['datasets'][0]['rules'][0]['schema']['type'] = 'unknown'; cases.append(invalid)
        for value in cases:
            with self.subTest(value=value), self.assertRaises(UserError):
                validate_rules_document(value)
        unknown = deepcopy(rules); unknown['datasets'][0]['id'] = 'missing'
        with self.assertRaisesRegex(UserError, 'absent'):
            apply_rules(SchemaManifest(crawl(DATA).document), unknown)

    def test_numeric_rules_and_type_parameters(self):
        data = {'a.jsonl': b'{"score":1.5}\n{"score":{"int":3}}\n'}
        baseline = SchemaManifest(crawl(data).document)
        rule = {'rules_version': 1, 'datasets': [{'id': baseline.document['datasets'][0]['id'], 'rules': [
            {'op': 'extract_number', 'path': [{'field': 'score'}], 'members': ['int']}]}]}
        from dataclasses import replace
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            req = request(root)
            req = replace(req, crawl=replace(req.crawl, options=CrawlOptions(bucket='test', sample_records=100)))
            result = CrawlAndResolveSchemas(MemorySource(data), S3DatasetAdapter, req, rule).execute()
            self.assertEqual(result.rules[0]['status'], 'applied')
        satisfied = SchemaManifest(crawl({'a.jsonl': b'{"score":1.5}\n'}).document)
        self.assertEqual(apply_rules(satisfied, rule)[2][0]['status'], 'satisfied')
        assert_rules = rules_for()
        assert_rules['datasets'][0]['rules'][0]['schema']['storage_type'] = 'int32'
        known = SchemaManifest(crawl({'a.jsonl': b'{"ids":[1]}\n'}).document)
        with self.assertRaisesRegex(UserError, 'conflicts'):
            apply_rules(known, assert_rules)

    def test_cli_dispatch_source_cleanup_and_flag_isolation(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent)
            (root / 'rules.yml').write_text(yaml.safe_dump(rules_for()))
            args = ['--module', 'discover', '--mode', 's3', '--action', 'crawl', '--bucket', 'test',
                    '--sample-records', '1', '--resolve-with', str(root / 'rules.yml'),
                    '--output', str(root / 'schema.yml'), '--discovery-output', str(root / 'raw.yml')]
            closed = []
            @contextmanager
            def opened(settings):
                try:
                    yield MemorySource(DATA)
                finally:
                    closed.append(True)
            with patch('dredge.modes.s3.clients.open_source', opened), patch('dredge.modules.discover.presentation.present_combined'):
                self.assertEqual(main(args), 0)
            self.assertEqual(closed, [True])
            SchemaManifest.load(root / 'schema.yml')
            plain = ['--module', 'discover', '--mode', 's3', '--action', 'crawl', '--bucket', 'test', '--output', str(root / 'new.yml')]
            for extra in (['--discovery-output', 'raw.yml'], ['--resolve-sample-files', '2'], ['--resolve-with', '']):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    main(plain + extra)
                self.assertEqual(error.exception.code, 2)
            (root / 'rules.yml').write_text('rules_version: 999')
            with patch('dredge.modes.s3.clients.open_source') as provider:
                self.assertEqual(main(args + ['--overwrite']), 1)
                provider.assert_not_called()

    def test_progress_cleans_up_terminal_display(self):
        from dredge.modules.discover.crawl_and_resolve import CrawlResolutionProgress
        from dredge.modules.discover.progress import DiscoveryProgress
        stream = io.StringIO()
        display = CrawlResolutionDisplay(Console(file=stream, force_terminal=True))
        with self.assertRaises(KeyboardInterrupt):
            with display:
                display(CrawlResolutionProgress('discovery', DiscoveryProgress()))
                display(CrawlResolutionProgress('rules'))
                display(CrawlResolutionProgress('validation'))
                raise KeyboardInterrupt()
        self.assertIsNone(display.active)
        self.assertIn('final output was not published', stream.getvalue())
