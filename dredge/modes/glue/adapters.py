"""Paginated Glue catalog metadata, independent of discovery presentation."""
from botocore.exceptions import BotoCoreError, ClientError
from ...core.exceptions import UserError


class GlueCatalog:
    def __init__(self, client):
        self.client = client

    def _call(self, operation, **kwargs):
        try:
            return getattr(self.client, operation)(**kwargs)
        except (BotoCoreError, ClientError) as exc:
            raise UserError(f'Glue {operation} failed. Check catalog/database/table names, region and Glue/Lake Formation permissions.') from exc

    def _pages(self, operation, result_key, **kwargs):
        while True:
            response = self._call(operation, **kwargs)
            yield from response.get(result_key, [])
            token = response.get('NextToken')
            if not token:
                return
            kwargs['NextToken'] = token

    def catalogs(self, catalog_id=None):
        if not hasattr(self.client, 'get_catalogs'):
            raise UserError('This boto3 version does not support Glue catalogs; upgrade boto3.')
        arguments = {'Recursive': True}
        if catalog_id is not None:
            arguments['ParentCatalogId'] = catalog_id
        if catalog_id is None or (catalog_id.isdigit() and len(catalog_id) == 12):
            arguments['IncludeRoot'] = True
        for item in self._pages('get_catalogs', 'CatalogList', **arguments):
            yield {key: item.get(source) for key, source in
                   [('id', 'CatalogId'), ('name', 'Name'), ('description', 'Description'), ('resource_arn', 'ResourceArn')]}

    def databases(self, catalog_id=None):
        arguments = {'CatalogId': catalog_id} if catalog_id else {}
        for item in self._pages('get_databases', 'DatabaseList', ResourceShareType='ALL', **arguments):
            yield {'name': item['Name'], 'description': item.get('Description'),
                   'catalog_id': item.get('CatalogId', catalog_id), 'location': item.get('LocationUri')}

    def tables(self, catalog_id, database):
        arguments = {'CatalogId': catalog_id} if catalog_id else {}
        for item in self._pages('get_tables', 'TableList', DatabaseName=database, **arguments):
            yield {'name': item['Name'], 'type': item.get('TableType'),
                   'description': item.get('Description'), 'location': item.get('StorageDescriptor', {}).get('Location')}

    def describe(self, catalog_id, database, table):
        arguments = {'CatalogId': catalog_id} if catalog_id else {}
        item = self._call('get_table', DatabaseName=database, Name=table, **arguments)['Table']
        storage = item.get('StorageDescriptor') or {}
        columns = []
        for partition, entries in ((False, storage.get('Columns', [])), (True, item.get('PartitionKeys', []))):
            columns.extend({'name': column['Name'], 'type': column.get('Type', 'unknown'),
                            'comment': column.get('Comment'), 'partition': partition} for column in entries)
        return {'name': item['Name'], 'database': item.get('DatabaseName', database),
                'catalog_id': item.get('CatalogId', catalog_id), 'type': item.get('TableType'),
                'description': item.get('Description'), 'location': storage.get('Location'),
                'input_format': storage.get('InputFormat'), 'output_format': storage.get('OutputFormat'),
                'columns': columns}
