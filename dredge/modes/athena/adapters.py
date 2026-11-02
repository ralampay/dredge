"""Athena execution and result access behind an injected SDK client."""
from contextlib import contextmanager
from urllib.parse import urlsplit

from botocore.exceptions import BotoCoreError, ClientError
from ...core.exceptions import UserError


class AthenaQueries:
    def __init__(self, client, s3_client):
        self.client, self.s3_client = client, s3_client

    def _call(self, operation, **kwargs):
        try:
            return getattr(self.client, operation)(**kwargs)
        except (BotoCoreError, ClientError) as exc:
            code = exc.response.get('Error', {}).get('Code', '') if isinstance(exc, ClientError) else type(exc).__name__
            raise UserError(f'Athena {operation} failed ({code}). Check query context, credentials and workgroup permissions.') from exc

    def prepare(self, workgroup, results_location):
        group = self._call('get_work_group', WorkGroup=workgroup)['WorkGroup']
        if group.get('State') != 'ENABLED':
            raise UserError('The Athena workgroup is not enabled.')
        config = group.get('Configuration', {})
        if config.get('ManagedQueryResultsConfiguration', {}).get('Enabled'):
            if results_location:
                raise UserError('This workgroup uses Athena-managed results; omit --results-location.')
            return {}
        result = dict(config.get('ResultConfiguration') or {})
        if results_location:
            if config.get('EnforceWorkGroupConfiguration') and result.get('OutputLocation') != results_location:
                raise UserError('The workgroup enforces its result location; omit --results-location or use that location.')
            result['OutputLocation'] = results_location
        if not result.get('OutputLocation'):
            raise UserError('Configure an Athena workgroup result location or supply --results-location.')
        return result

    def start(self, sql, catalog, database, workgroup, configuration, token):
        arguments = dict(QueryString=sql, QueryExecutionContext={'Catalog': catalog, 'Database': database},
                         WorkGroup=workgroup, ClientRequestToken=token)
        if configuration:
            arguments['ResultConfiguration'] = configuration
        return self._call('start_query_execution', **arguments)['QueryExecutionId']

    def execution(self, query_id):
        execution = self._call('get_query_execution', QueryExecutionId=query_id)['QueryExecution']
        status, stats = execution['Status'], execution.get('Statistics', {})
        return {'query_id': query_id, 'region': self.client.meta.region_name, 'state': status['State'], 'reason': status.get('StateChangeReason'),
                'result_location': execution.get('ResultConfiguration', {}).get('OutputLocation'),
                'managed_results': execution.get('ManagedQueryResultsConfiguration', {}).get('Enabled', False),
                'engine_version': execution.get('EngineVersion', {}).get('EffectiveEngineVersion'),
                'statistics': {'data_scanned_bytes': stats.get('DataScannedInBytes', 0),
                               'engine_execution_ms': stats.get('EngineExecutionTimeInMillis'),
                               'total_execution_ms': stats.get('TotalExecutionTimeInMillis'),
                               'reused_result': stats.get('ResultReuseInformation', {}).get('ReusedPreviousResult', False)}}

    def stop(self, query_id):
        self._call('stop_query_execution', QueryExecutionId=query_id)

    def result_page(self, query_id, token=None, size=1000):
        arguments = {'QueryExecutionId': query_id, 'MaxResults': size, 'QueryResultType': 'DATA_ROWS'}
        if token:
            arguments['NextToken'] = token
        response = self._call('get_query_results', **arguments)
        result = response.get('ResultSet', {})
        columns = [{'name': c.get('Label') or c['Name'], 'type': c['Type'], 'nullable': c.get('Nullable', 'UNKNOWN'),
                    'precision': c.get('Precision'), 'scale': c.get('Scale')}
                   for c in result.get('ResultSetMetadata', {}).get('ColumnInfo', [])]
        rows = [[cell.get('VarCharValue') for cell in row.get('Data', [])] for row in result.get('Rows', [])]
        return {'columns': columns, 'rows': rows, 'next_token': response.get('NextToken')}

    @contextmanager
    def result_stream(self, location):
        uri = urlsplit(location or '')
        if uri.scheme != 's3' or not uri.netloc or not uri.path.lstrip('/'):
            raise UserError('Athena did not return a valid S3 CSV result location.')
        try:
            response = self.s3_client.get_object(Bucket=uri.netloc, Key=uri.path.lstrip('/'))
        except (BotoCoreError, ClientError) as exc:
            raise UserError('Cannot read Athena CSV results. Check S3/KMS permissions and result retention.') from exc
        body = response['Body']
        try:
            yield body, response.get('ContentLength')
        except (BotoCoreError, ClientError) as exc:
            raise UserError('Athena CSV result transfer failed; retry the export.') from exc
        finally:
            body.close()
