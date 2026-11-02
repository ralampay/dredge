"""Filter planning and execution agree without requiring credentials or network."""
from contextlib import nullcontext, redirect_stderr
from copy import deepcopy
from datetime import date, datetime, timezone
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
from dredge.modules.ingest.filter_plan import FilterPlan, write_plan
from dredge.modules.ingest.ingest_datasets import IngestDatasets
from dredge.modules.ingest.plan_ingestion import PlanIngestion
from dredge.modules.ingest.presentation import present
from dredge.modules.ingest.runner import run_ingestion
from dredge.modules.ingest.time_filters import TimeFilter
from test_ingestion import setup


def fixture():
    return {'events/year=2024/month=2/day=28/a.jsonl': b'{"at":"2024-02-28T00:00:00Z","id":1}\n',
            'events/year=2024/month=02/day=29/b.jsonl': b'{"at":"2024-02-29T00:00:00Z","id":2}\n',
            'events/year=2024/month=3/day=1/c.jsonl': b'{"at":"2024-03-01T00:00:00Z","id":3}\n'}


def make_plan(dataset, kind='partition', field='year,month,day', metadata=False):
    return FilterPlan.create('test', dataset, TimeFilter(kind, field, '2024-02-29', '2024-03-01'), metadata)


class TimePredicateTests(unittest.TestCase):
    def test_boundaries_nulls_timezone_and_dates(self):
        predicate = TimeFilter('record', 'at', '2024-02-29', '2024-03-01')
        for value, expected in [(None, False), (datetime(2024, 2, 29), True),
                                (datetime(2024, 3, 1), False), (date(2024, 2, 28), False),
                                (datetime.fromisoformat('2024-03-01T07:00:00+08:00'), True)]:
            self.assertEqual(predicate.matches_record({'at': value}), expected)
        local = TimeFilter('record', 'at', '2024-02-29', '2024-03-01', 'Asia/Manila')
        self.assertTrue(local.matches_record({'at': datetime(2024, 2, 28, 16, tzinfo=timezone.utc)}))
        self.assertFalse(local.matches_record({'at': datetime(2024, 2, 29, 16, tzinfo=timezone.utc)}))

    def test_invalid_ranges_timezones_and_dst(self):
        cases = [('record', 'at', 'bad', '2024-03-01', 'UTC'),
                 ('record', 'at', '2024-03-01', '2024-02-29', 'UTC'),
                 ('partition', 'year,month,day', '2023-02-29', '2023-03-01', 'UTC'),
                 ('record', 'at', '2024-01-01', '2024-03-01', 'Not/AZone'),
                 ('record', 'at', '2024-01-01T00:00:00.1234567', '2024-03-01', 'UTC'),
                 ('record', 'at', '2024-11-03T01:30:00', '2024-11-04', 'America/New_York')]
        for args in cases:
            with self.subTest(args=args), self.assertRaises(UserError):
                TimeFilter(*args)

    def test_ns_values_do_not_round_into_range(self):
        # Arrow returns pandas Timestamp for ns precision when pandas is installed.
        try:
            import pandas as pd
        except ImportError:
            self.skipTest('Optional pandas ns representation')
        predicate = TimeFilter('record', 'at', '2024-02-29', '2024-03-01')
        self.assertFalse(predicate.matches_record({'at': pd.Timestamp('2024-02-28T23:59:59.999999999')}))
        self.assertTrue(predicate.matches_record({'at': pd.Timestamp('2024-02-29T23:59:59.999999999')}))


class FilterPlanTests(unittest.TestCase):
    def test_partition_preview_and_download_match_without_content_reads(self):
        manifest, source, datasets, selections = setup(fixture())
        plan = make_plan(datasets[0])
        preview = PlanIngestion(source, datasets[0], selections[0], plan).execute()
        self.assertEqual(source.reads, [])
        self.assertEqual(preview.document['estimate']['selected_objects'], 1)
        self.assertEqual(preview.document['estimate']['excluded_objects'], 2)
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'data'
            result = IngestDatasets(manifest, source, datasets, selections, output, plan=plan).execute()
            dataset = result.document['datasets'][0]
            self.assertEqual(pq.read_table(output / dataset['data_path'])['id'].to_pylist(), [2])
            self.assertEqual(len({r[0] for r in source.reads}), 1)
            self.assertEqual(result.document['filter_plan'], plan.document())
            notebook = json.loads((output / dataset['notebook']).read_text())
            self.assertEqual(len(notebook['cells']), 2)
            self.assertIn('Ingestion filter', notebook['cells'][0]['source'])

    def test_json_record_filter_ignores_folder_date_and_scans_candidates(self):
        data = fixture()
        data['events/year=2024/month=2/day=28/a.jsonl'] = b'{"at":"2024-02-29T01:00:00Z","id":1}\n'
        manifest, source, datasets, selections = setup(data)
        plan = make_plan(datasets[0], 'record', 'at')
        preview = PlanIngestion(source, datasets[0], selections[0], plan).execute()
        self.assertEqual(preview.document['estimate']['selected_objects'], 3)
        self.assertEqual(preview.document['estimate']['estimated_data_bytes'], sum(map(len, data.values())))
        self.assertFalse(source.reads)
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'data'
            result = IngestDatasets(manifest, source, datasets, selections, output, plan=plan).execute()
            self.assertEqual(pq.read_table(output / result.document['datasets'][0]['data_path'])['id'].to_pylist(), [1, 2])
            self.assertEqual(len({r[0] for r in source.reads}), 3)
            self.assertEqual(result.document['datasets'][0]['records_scanned'], 3)
            self.assertEqual(result.document['datasets'][0]['records_filtered'], 1)

    def test_invalid_partition_fails_without_publishing(self):
        manifest, source, datasets, selections = setup(fixture())
        source.data['events/year=2024/month=2/day=30/x.jsonl'] = b'{"id":4}'
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'plan.yml'
            with self.assertRaisesRegex(UserError, 'calendar'):
                PlanIngestion(source, datasets[0], selections[0], make_plan(datasets[0]), output).execute()
            self.assertFalse(output.exists())
            self.assertFalse(source.reads)

    def test_plan_round_trip_binding_and_refreshed_inventory(self):
        manifest, source, datasets, selections = setup(fixture())
        plan = make_plan(datasets[0])
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'plan.yml'
            PlanIngestion(source, datasets[0], selections[0], plan, output).execute()
            restored = FilterPlan.load(output)
            self.assertEqual(restored, plan)
            source.data['events/year=2024/month=2/day=29/new.jsonl'] = b'{"id":4}'
            result = PlanIngestion(source, datasets[0], selections[0], restored).execute()
            self.assertEqual(result.document['estimate']['selected_objects'], 2)
            with self.assertRaises(UserError):
                write_plan(plan, {}, output)
            for bucket, dataset in [('other', datasets[0]), ('test', deepcopy(datasets[0]))]:
                if bucket == 'test':
                    dataset['ingestion']['selector']['prefix'] = 'changed/'
                with self.assertRaises(UserError):
                    restored.validate(bucket, dataset)
            document = yaml.safe_load(output.read_text())
            document['inspect_parquet_metadata'] = 'false'
            output.write_text(yaml.safe_dump(document))
            with self.assertRaises(UserError):
                FilterPlan.load(output)

    def test_unsupported_fields_and_metadata_modes(self):
        _, _, datasets, _ = setup(fixture())
        for kind, field, metadata in [('record', 'id', False), ('partition', 'y,m,d', False),
                                      ('record', 'at', True), ('partition', 'year,month,day', True)]:
            with self.subTest(field=field, metadata=metadata), self.assertRaises(UserError):
                make_plan(datasets[0], kind, field, metadata)

    def test_empty_selection_is_valid_empty_dataset(self):
        manifest, source, datasets, selections = setup(fixture())
        plan = FilterPlan.create('test', datasets[0], TimeFilter('partition', 'year,month,day', '2025-01-01', '2025-02-01'))
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'data'
            result = IngestDatasets(manifest, source, datasets, selections, output, plan=plan).execute()
            self.assertEqual(result.document['datasets'][0]['rows'], 0)
            self.assertFalse(source.reads)

    def test_cli_inspection_offline_and_plan_get_round_trip(self):
        manifest, source, datasets, selections = setup(fixture())
        with tempfile.TemporaryDirectory() as parent:
            schema = Path(parent) / 'schema.yml'
            schema.write_text(yaml.safe_dump(manifest.document))
            config = dict(action='plan', bucket='test', schema=str(schema))
            with patch('dredge.modes.s3.clients.open_source') as provider, patch('dredge.modules.ingest.presentation.present'):
                result = run_ingestion(config)
                self.assertIn('at', [f['name'] for f in result.document['filter_fields'][0]['fields']])
                provider.assert_not_called()
            plan_path = Path(parent) / 'plan.yml'
            config.update(dataset=[datasets[0]['id']], output=str(plan_path), partition_date='year,month,day',
                          time_from='2024-02-29', time_to='2024-03-01')
            with patch('dredge.modes.s3.clients.open_source', return_value=nullcontext(source)), \
                    patch('dredge.modules.ingest.presentation.present'):
                run_ingestion(config)
                result = run_ingestion(dict(action='get', bucket='test', schema=str(schema), plan=str(plan_path),
                                            output=str(Path(parent) / 'out'), dry_run=True))
                self.assertEqual(result.document['estimate']['selected_objects'], 1)
            self.assertFalse((Path(parent) / 'out').exists())
            self.assertFalse(source.reads)

    def test_cli_rejects_conflicting_options_before_provider_access(self):
        base = ['--module', 'ingest', '--mode', 's3', '--bucket', 'test', '--schema', 'schema.yml']
        for args in [ ['--action', 'get', '--output', 'out', '--plan', 'p', '--prefix', 'json/'],
                      ['--action', 'get', '--output', 'out', '--time-field', 'at'],
                      ['--action', 'plan', '--output', 'out', '--from', '2024-01-01'],
                      ['--action', 'plan', '--dry-run'],
                      ['--action', 'plan', '--time-field', 'at', '--from', '2024-01-01', '--to', '2025-01-01', '--output', 'out'] ]:
            with self.subTest(args=args), redirect_stderr(io.StringIO()), \
                    patch('dredge.cli.load_environment') as config, self.assertRaises(SystemExit) as error:
                main(base + args)
            self.assertEqual(error.exception.code, 2)
            config.assert_not_called()


class ParquetPlanTests(unittest.TestCase):
    def make_data(self, statistics=True):
        buffer = io.BytesIO()
        # Large column chunks ensure range logs can distinguish skipped groups from footer reads.
        table = pa.table({'at': [datetime(2024, 2, 28)] * 100 + [datetime(2024, 2, 29)] * 100 + [datetime(2024, 3, 1)] * 100,
                          'id': list(range(300)), 'payload': ['x' * 16384] * 300})
        pq.write_table(table, buffer, row_group_size=100, compression=None, use_dictionary=False,
                       write_statistics=statistics)
        return {'events/data.parquet': buffer.getvalue()}

    def test_metadata_estimate_and_download_skip_excluded_column_chunks(self):
        data = self.make_data()
        manifest, source, datasets, selections = setup(data)
        plan = make_plan(datasets[0], 'record', 'at', True)
        preview = PlanIngestion(source, datasets[0], selections[0], plan).execute()
        estimate = preview.document['estimate']
        self.assertEqual(estimate['selected_row_groups'], 1)
        self.assertEqual(estimate['row_groups'], 3)
        self.assertLess(estimate['metadata_bytes'], 100000)
        self.assertLess(estimate['estimated_data_bytes'], len(next(iter(data.values()))) / 2)
        source.reads.clear()
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'data'
            result = IngestDatasets(manifest, source, datasets, selections, output, plan=plan).execute()
            table = pq.read_table(output / result.document['datasets'][0]['data_path'])
            self.assertEqual(table['id'].to_pylist(), list(range(100, 200)))
            self.assertLess(sum(end - start + 1 for _, start, end in source.reads), len(next(iter(data.values()))) / 2)
            metadata = pq.ParquetFile(io.BytesIO(next(iter(data.values())))).metadata
            for group_index in (0, 2):
                column = metadata.row_group(group_index).column(2)
                # Mid-chunk bytes cannot be fetched by the normal footer request.
                midpoint = column.data_page_offset + column.total_compressed_size // 2
                self.assertFalse(any(start <= midpoint <= end for _, start, end in source.reads))

    def test_missing_statistics_keeps_all_groups_and_filters_rows(self):
        data = self.make_data(False)
        manifest, source, datasets, selections = setup(data)
        plan = make_plan(datasets[0], 'record', 'at', True)
        preview = PlanIngestion(source, datasets[0], selections[0], plan).execute()
        self.assertEqual(preview.document['estimate']['selected_row_groups'], 3)
        self.assertEqual(preview.document['estimate']['uncertain_row_groups'], 3)
        with tempfile.TemporaryDirectory() as parent:
            result = IngestDatasets(manifest, source, datasets, selections, Path(parent) / 'out', plan=plan).execute()
            self.assertEqual(result.document['datasets'][0]['rows'], 100)

    def test_parquet_listings_only_does_not_fetch_footers(self):
        _, source, datasets, selections = setup(self.make_data())
        plan = make_plan(datasets[0], 'record', 'at')
        result = PlanIngestion(source, datasets[0], selections[0], plan).execute()
        self.assertFalse(source.reads)
        self.assertEqual(result.document['estimate']['metadata_bytes'], 0)

    def test_null_statistics_and_half_open_end_exclude_groups(self):
        buffer = io.BytesIO()
        pq.write_table(pa.table({'at': pa.array([None, None, datetime(2024, 3, 1), datetime(2024, 3, 1)],
                                              type=pa.timestamp('us'))}), buffer, row_group_size=2)
        manifest, source, datasets, selections = setup({'a.parquet': buffer.getvalue()})
        plan = make_plan(datasets[0], 'record', 'at', True)
        preview = PlanIngestion(source, datasets[0], selections[0], plan).execute()
        self.assertEqual(preview.document['estimate']['selected_row_groups'], 0)
        self.assertEqual(preview.document['estimate']['selected_objects'], 0)
        with tempfile.TemporaryDirectory() as parent:
            result = IngestDatasets(manifest, source, datasets, selections, Path(parent) / 'out', plan=plan).execute()
            self.assertEqual(result.document['datasets'][0]['rows'], 0)

    def test_invalid_metadata_and_budget_fail_without_publishing(self):
        from dredge.formats.parquet_filter import inspect_parquet
        from dredge.core.sources import ObjectInfo
        data = self.make_data()
        _, source, datasets, selections = setup(data)
        plan = make_plan(datasets[0], 'record', 'at', True)
        key, content = next(iter(data.items()))
        with self.assertRaises(UserError):
            inspect_parquet(source, ObjectInfo(key, len(content)), plan.predicate, byte_limit=8)
        source.data[key] = b'not a parquet file'
        with tempfile.TemporaryDirectory() as parent:
            output = Path(parent) / 'plan.yml'
            with self.assertRaisesRegex(UserError, 'metadata'):
                PlanIngestion(source, datasets[0], selections[0], plan, output).execute()
            self.assertFalse(output.exists())

    def test_metadata_presentation_does_not_claim_no_downloads(self):
        from rich.console import Console
        _, source, datasets, selections = setup(self.make_data())
        plan = make_plan(datasets[0], 'record', 'at', True)
        result = PlanIngestion(source, datasets[0], selections[0], plan).execute()
        stream = io.StringIO()
        present(result, Console(file=stream, width=120))
        self.assertIn('explicitly enabled', stream.getvalue())
        self.assertNotIn('No object contents downloaded', stream.getvalue())
