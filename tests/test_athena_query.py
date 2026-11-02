"""Athena export behavior with SDK stubs and injected query repositories."""
from contextlib import contextmanager, nullcontext, redirect_stderr
import csv
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import boto3
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber

from dredge.cli import main
from dredge.core.exceptions import UserAbort, UserError
from dredge.modes.athena.adapters import AthenaQueries
from dredge.modules.ingest.query.commands import ExportAthenaQuery
from dredge.modules.ingest.query.requests import QueryRequest, validate_sql


class Clock:
    def __init__(self):
        self.now = 0
    def time(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds


class Queries:
    def __init__(self, managed=False, states=('QUEUED', 'RUNNING', 'SUCCEEDED'), content=b'"id"\n"1"\n'):
        self.managed, self.states, self.content = managed, iter(states), content
        self.stopped, self.started, self.closed = False, False, False
        self.columns = [{'name': 'id', 'type': 'bigint', 'nullable': 'UNKNOWN', 'precision': None, 'scale': None}]
        self.pages = [{'columns': self.columns, 'rows': [['id'], ['1']], 'next_token': None}]
        self.page_calls = []
    def prepare(self, workgroup, location):
        return {} if self.managed else {'OutputLocation': 's3://results/'}
    def start(self, *args):
        self.started = True
        self.start_arguments = args
        return 'query-1'
    def execution(self, query_id):
        state = 'CANCELLED' if self.stopped else next(self.states, 'RUNNING')
        return {'query_id': query_id, 'state': state, 'reason': 'test reason',
                'result_location': None if self.managed else 's3://results/query-1.csv',
                'managed_results': self.managed, 'engine_version': 'Athena engine version 3',
                'statistics': {'data_scanned_bytes': 12345}}
    def stop(self, query_id):
        self.stopped = True
    def result_page(self, query_id, token=None, size=1000):
        self.page_calls.append((token, size))
        return self.pages[int(token or 0)]
    @contextmanager
    def result_stream(self, location):
        body = io.BytesIO(self.content)
        try:
            yield body, len(self.content)
        finally:
            body.close()
            self.closed = body.closed


def execute(repository, output, **kwargs):
    clock = Clock()
    return ExportAthenaQuery(repository, QueryRequest('SELECT * FROM events', 'db', str(output), **kwargs),
                             clock=clock.time, sleep=clock.sleep)


class QueryExportTests(unittest.TestCase):
    def test_sql_classification_and_file_inputs(self):
        for sql in ('SELECT 1', 'WITH x AS (SELECT 1) SELECT * FROM x', "SELECT ';delete' AS value; --comment"):
            validate_sql(sql)
        for sql in ('', 'SELECT 1; SELECT 2', 'DROP TABLE t', 'INSERT INTO t SELECT 1',
                    'WITH x AS (DELETE FROM t) SELECT * FROM x', '-- only a comment'):
            with self.subTest(sql=sql), self.assertRaises(UserError):
                validate_sql(sql)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'query.sql'
            path.write_text('SELECT 1', encoding='utf-8')
            request = QueryRequest.from_config({'query_file': str(path), 'database': 'db', 'output': 'out'})
            self.assertEqual(request.sql, 'SELECT 1')
        with self.assertRaises(UserError):
            QueryRequest.from_config({'query_file': 'missing.sql', 'database': 'db', 'output': 'out'})

    def test_s3_export_is_atomic_and_notebook_preview_is_bounded(self):
        repository = Queries(content=b'"id"\n"1"\n"2"\n')
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'dataset'
            result = execute(repository, output).execute()
            self.assertEqual((output / 'data.csv').read_bytes(), repository.content)
            self.assertTrue(repository.closed)
            self.assertEqual(result.document['statistics']['data_scanned_bytes'], 12345)
            self.assertEqual(result.document['query_id'], 'query-1')
            self.assertIsNone(result.document['rows'])
            self.assertEqual(len(repository.start_arguments[-1]), 32)
            notebook = json.loads((output / 'preview.ipynb').read_text())
            self.assertEqual([c['cell_type'] for c in notebook['cells']], ['markdown', 'code'])
            self.assertIn('nrows=5', notebook['cells'][1]['source'])
            self.assertIn('bigint', notebook['cells'][0]['source'])
            self.assertEqual(list(Path(directory).iterdir()), [output])

    def test_managed_results_pagination_quotes_nulls_and_header(self):
        repository = Queries(managed=True)
        repository.columns[0]['type'] = 'varchar'
        repository.pages = [dict(columns=repository.columns, rows=[['id'], ['id'], ['a,"b"\nc']], next_token='1'),
                            dict(columns=repository.columns, rows=[[None], [''], ['last']], next_token=None)]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'dataset'
            result = execute(repository, output).execute()
            with (output / 'data.csv').open(newline='') as stream:
                self.assertEqual(list(csv.reader(stream)), [['id'], ['id'], ['a,"b"\nc'], [''], [''], ['last']])
            self.assertEqual(result.document['rows'], 5)
            self.assertEqual(repository.page_calls, [(None, 1000), ('1', 1000)])

    def test_empty_results_have_header_and_no_rows(self):
        repository = Queries(managed=True)
        repository.pages[0]['rows'] = [['id']]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'out'
            result = execute(repository, output).execute()
            self.assertEqual(result.document['rows'], 0)
            self.assertEqual((output / 'data.csv').read_text().strip(), 'id')

    def test_invalid_sql_and_existing_output_never_start_queries(self):
        repository = Queries()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'out'
            output.mkdir()
            with self.assertRaises(UserError):
                execute(repository, output).execute()
            with self.assertRaises(UserError):
                ExportAthenaQuery(repository, QueryRequest('DELETE FROM t', 'db', str(output))).execute()
            self.assertFalse(repository.started)

    def test_failed_and_cancelled_queries_publish_nothing(self):
        for state in ('FAILED', 'CANCELLED'):
            repository = Queries(states=(state,))
            with self.subTest(state=state), tempfile.TemporaryDirectory() as directory:
                with self.assertRaisesRegex(UserError, state.lower()):
                    execute(repository, Path(directory) / 'out').execute()
                self.assertEqual(list(Path(directory).iterdir()), [])
                self.assertFalse(repository.stopped)

    def test_timeout_requests_remote_stop_and_confirms_status(self):
        repository, events = Queries(states=('RUNNING',)), []
        with tempfile.TemporaryDirectory() as directory:
            command = execute(repository, Path(directory) / 'out', timeout=3)
            command.on_progress = events.append
            with self.assertRaisesRegex(UserError, 'timeout'):
                command.execute()
            self.assertTrue(repository.stopped)
            self.assertTrue(any(event.state == 'STOPPED' and 'CANCELLED' in event.detail for event in events))
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_interrupt_stops_remote_and_preserves_cancellation_on_stop_failure(self):
        for fail_stop in (False, True):
            repository, events = Queries(states=('RUNNING',)), []
            with tempfile.TemporaryDirectory() as directory:
                command = execute(repository, Path(directory) / 'out')
                command.on_progress = events.append
                command.sleep = lambda seconds: (_ for _ in ()).throw(KeyboardInterrupt())
                if fail_stop:
                    repository.stop = lambda query_id: (_ for _ in ()).throw(UserError('stop denied'))
                with self.assertRaises(KeyboardInterrupt):
                    command.execute()
                self.assertEqual(list(Path(directory).iterdir()), [])
                self.assertTrue(any(e.state == ('STOP_UNCONFIRMED' if fail_stop else 'STOPPED') for e in events))

    def test_submission_interruption_reports_unknown_remote_status(self):
        repository, events = Queries(), []
        repository.start = lambda *args: (_ for _ in ()).throw(KeyboardInterrupt())
        with tempfile.TemporaryDirectory() as directory:
            command = execute(repository, Path(directory) / 'out')
            command.on_progress = events.append
            with self.assertRaises(KeyboardInterrupt):
                command.execute()
            self.assertFalse(repository.stopped)
            self.assertTrue(any(e.state == 'SUBMISSION_UNCONFIRMED' for e in events))
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_export_failure_closes_stream_and_removes_output(self):
        repository = Queries()
        with tempfile.TemporaryDirectory() as directory:
            with patch('dredge.modules.ingest.query.commands.write_artifacts', side_effect=OSError('full')):
                with self.assertRaisesRegex(UserError, 'disk space'):
                    execute(repository, Path(directory) / 'out').execute()
            self.assertTrue(repository.closed)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_incomplete_download_and_bad_managed_header_roll_back(self):
        for managed in (False, True):
            repository = Queries(managed=managed)
            if managed:
                repository.pages[0]['rows'][0] = ['wrong header']
            else:
                @contextmanager
                def truncated(location):
                    yield io.BytesIO(b'id\n'), 100
                repository.result_stream = truncated
            with self.subTest(managed=managed), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(UserError):
                    execute(repository, Path(directory) / 'out').execute()
                self.assertEqual(list(Path(directory).iterdir()), [])

    def test_download_cancellation_does_not_stop_completed_query(self):
        repository = Queries()
        with tempfile.TemporaryDirectory() as directory:
            command = execute(repository, Path(directory) / 'out')
            def check():
                if command.terminal:
                    raise UserAbort()
            command.check_cancel = check
            with self.assertRaises(UserAbort):
                command.execute()
            self.assertFalse(repository.stopped)
            self.assertEqual(list(Path(directory).iterdir()), [])


class AthenaAdapterTests(unittest.TestCase):
    def setUp(self):
        options = dict(region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')
        self.client, self.s3 = boto3.client('athena', **options), boto3.client('s3', **options)
        self.addCleanup(self.client.close)
        self.addCleanup(self.s3.close)
        self.repository = AthenaQueries(self.client, self.s3)

    def test_workgroup_result_configuration_and_overrides(self):
        cases = [({'ManagedQueryResultsConfiguration': {'Enabled': True}}, None, {}),
                 ({'ResultConfiguration': {'OutputLocation': 's3://results/'}}, None, {'OutputLocation': 's3://results/'}),
                 ({'ResultConfiguration': {'OutputLocation': 's3://results/'}}, 's3://other/', {'OutputLocation': 's3://other/'}),
                 ({'ManagedQueryResultsConfiguration': {'Enabled': True}}, 's3://other/', None),
                 ({'EnforceWorkGroupConfiguration': True, 'ResultConfiguration': {'OutputLocation': 's3://results/'}}, 's3://other/', None),
                 ({}, None, None)]
        with Stubber(self.client) as stub:
            for configuration, location, expected in cases:
                stub.add_response('get_work_group', {'WorkGroup': {'Name': 'primary', 'State': 'ENABLED', 'Configuration': configuration}},
                                  {'WorkGroup': 'primary'})
                if expected is None:
                    with self.assertRaises(UserError):
                        self.repository.prepare('primary', location)
                else:
                    self.assertEqual(self.repository.prepare('primary', location), expected)

    def test_start_context_execution_metadata_and_result_pagination(self):
        with Stubber(self.client) as stub:
            stub.add_response('start_query_execution', {'QueryExecutionId': 'q'},
                              {'QueryString': 'SELECT 1', 'QueryExecutionContext': {'Catalog': 'AwsDataCatalog', 'Database': 'db'},
                               'WorkGroup': 'primary', 'ClientRequestToken': 'a' * 32})
            self.assertEqual(self.repository.start('SELECT 1', 'AwsDataCatalog', 'db', 'primary', {}, 'a' * 32), 'q')
            stub.add_response('get_query_execution', {'QueryExecution': {'QueryExecutionId': 'q', 'Status': {'State': 'SUCCEEDED'},
                              'ManagedQueryResultsConfiguration': {'Enabled': True}, 'Statistics': {'DataScannedInBytes': 123}}},
                              {'QueryExecutionId': 'q'})
            self.assertTrue(self.repository.execution('q')['managed_results'])
            stub.add_response('get_query_results', {'ResultSet': {'ResultSetMetadata': {'ColumnInfo': [{'Name': 'n', 'Type': 'integer'}]},
                              'Rows': [{'Data': [{'VarCharValue': '1'}]}, {'Data': [{}]}]}},
                              {'QueryExecutionId': 'q', 'MaxResults': 1000, 'QueryResultType': 'DATA_ROWS', 'NextToken': 'next'})
            self.assertEqual(self.repository.result_page('q', 'next')['rows'], [['1'], [None]])

    def test_s3_result_stream_closes_body_and_preserves_literal_key(self):
        raw = io.BytesIO(b'id\n1\n')
        body = StreamingBody(raw, 5)
        with Stubber(self.s3) as stub:
            stub.add_response('get_object', {'Body': body, 'ContentLength': 5}, {'Bucket': 'results', 'Key': 'a+b%20.csv'})
            with self.repository.result_stream('s3://results/a+b%20.csv') as (stream, size):
                self.assertEqual(stream.read(), b'id\n1\n')
            self.assertTrue(raw.closed)

    def test_managed_export_through_cli_and_sdk_adapters(self):
        with tempfile.TemporaryDirectory() as directory, Stubber(self.client) as stub, \
                patch('dredge.cli.load_environment', return_value=None), \
                patch('dredge.modes.aws.clients.open_clients', return_value=nullcontext({'athena': self.client, 's3': self.s3})), \
                patch('dredge.modules.ingest.query.presentation.present') as present:
            stub.add_response('get_work_group', {'WorkGroup': {'Name': 'primary', 'State': 'ENABLED',
                              'Configuration': {'ManagedQueryResultsConfiguration': {'Enabled': True}}}}, {'WorkGroup': 'primary'})
            stub.add_response('start_query_execution', {'QueryExecutionId': 'q'},
                              {'QueryString': 'SELECT 1 AS n', 'QueryExecutionContext': {'Catalog': 'AwsDataCatalog', 'Database': 'db'},
                               'WorkGroup': 'primary', 'ClientRequestToken': ANY})
            stub.add_response('get_query_execution', {'QueryExecution': {'QueryExecutionId': 'q', 'Status': {'State': 'SUCCEEDED'},
                              'ManagedQueryResultsConfiguration': {'Enabled': True}, 'Statistics': {'DataScannedInBytes': 0}}},
                              {'QueryExecutionId': 'q'})
            stub.add_response('get_query_results', {'ResultSet': {'ResultSetMetadata': {'ColumnInfo': [{'Name': 'n', 'Type': 'integer'}]},
                              'Rows': [{'Data': [{'VarCharValue': 'n'}]}, {'Data': [{'VarCharValue': '1'}]}]}},
                              {'QueryExecutionId': 'q', 'MaxResults': 1000, 'QueryResultType': 'DATA_ROWS'})
            output = Path(directory) / 'out'
            self.assertEqual(main(['--module', 'ingest', '--mode', 'athena', '--action', 'query', '--database', 'db',
                                   '--query', 'SELECT 1 AS n', '--output', str(output)]), 0)
            self.assertEqual((output / 'data.csv').read_text(), 'n\n1\n')
            self.assertEqual(present.call_args.args[0].document['region'], 'us-east-1')
            stub.assert_no_pending_responses()

    def test_permission_errors_are_actionable(self):
        with Stubber(self.client) as stub:
            stub.add_client_error('get_work_group', 'AccessDeniedException', expected_params={'WorkGroup': 'primary'})
            with self.assertRaisesRegex(UserError, 'permissions'):
                self.repository.prepare('primary', None)

    def test_shared_client_lifecycle_closes_clients_when_construction_fails(self):
        from botocore.exceptions import BotoCoreError
        from dredge.modes.aws.clients import open_clients
        from dredge.modes.aws.configuration import AWSSettings
        first = Mock()
        with patch('boto3.Session') as session:
            session.return_value.client.side_effect = [first, BotoCoreError()]
            with self.assertRaises(UserError):
                with open_clients(AWSSettings(), 'athena', 's3'):
                    self.fail('Second client construction should have failed')
        first.close.assert_called_once()

    def test_query_cli_validation_before_client_creation(self):
        base = ['--module', 'ingest', '--mode', 'athena', '--action', 'query']
        for arguments in ([], ['--query', 'SELECT 1'], ['--database', 'db', '--output', 'out', '--query', 'SELECT 1', '--query-file', 'q.sql'],
                          ['--database', 'db', '--output', 'out', '--query', 'SELECT 1', '--bucket', 'test']):
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main(base + arguments)
            self.assertEqual(error.exception.code, 2)
        with tempfile.TemporaryDirectory() as directory, patch('dredge.cli.load_environment', return_value=None), \
                patch('dredge.modes.aws.clients.open_clients') as factory, redirect_stderr(io.StringIO()):
            self.assertEqual(main(base + ['--database', 'db', '--output', str(Path(directory) / 'out'), '--query', 'DROP TABLE t']), 1)
            factory.assert_not_called()
