"""Glue browsing through stubbed APIs and portable CLI composition."""
from contextlib import nullcontext, redirect_stderr
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import boto3
from botocore.stub import Stubber
import yaml

from dredge.cli import main
from dredge.core.exceptions import UserError
from dredge.modes.glue.adapters import GlueCatalog
from dredge.modules.discover.catalog.commands import BrowseCatalog
from dredge.modules.discover.catalog.requests import CatalogRequest


def client():
    return boto3.client('glue', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test')


class GlueDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.client = client()
        self.addCleanup(self.client.close)
        self.catalog = GlueCatalog(self.client)

    def test_catalogs_include_root_nested_and_all_pages(self):
        with Stubber(self.client) as stub:
            stub.add_response('get_catalogs', {'CatalogList': [{'CatalogId': '123456789012', 'Name': 'root'}], 'NextToken': 'next'},
                              {'Recursive': True, 'IncludeRoot': True})
            stub.add_response('get_catalogs', {'CatalogList': [{'CatalogId': '123456789012/child', 'Name': 'child'}]},
                              {'Recursive': True, 'IncludeRoot': True, 'NextToken': 'next'})
            result = BrowseCatalog(self.catalog, CatalogRequest('catalogs')).execute()
            self.assertEqual([r['id'] for r in result.document['items']], ['123456789012', '123456789012/child'])
            stub.assert_no_pending_responses()

    def test_nested_parent_omits_include_root(self):
        with Stubber(self.client) as stub:
            stub.add_response('get_catalogs', {'CatalogList': []}, {'Recursive': True, 'ParentCatalogId': '123456789012/child'})
            self.assertEqual(list(self.catalog.catalogs('123456789012/child')), [])

    def test_database_and_table_pagination(self):
        with Stubber(self.client) as stub:
            stub.add_response('get_databases', {'DatabaseList': [{'Name': 'one'}], 'NextToken': 'two'}, {'ResourceShareType': 'ALL'})
            stub.add_response('get_databases', {'DatabaseList': [{'Name': 'two'}]}, {'ResourceShareType': 'ALL', 'NextToken': 'two'})
            self.assertEqual([d['name'] for d in self.catalog.databases()], ['one', 'two'])
            args = {'CatalogId': '123456789012', 'DatabaseName': 'one'}
            stub.add_response('get_tables', {'TableList': [{'Name': 'events'}], 'NextToken': 'next'}, args)
            stub.add_response('get_tables', {'TableList': [{'Name': 'logs'}]}, {**args, 'NextToken': 'next'})
            self.assertEqual([t['name'] for t in self.catalog.tables('123456789012', 'one')], ['events', 'logs'])

    def test_describe_preserves_complex_types_partitions_and_yaml(self):
        table = {'Name': 'events', 'DatabaseName': 'db', 'TableType': 'EXTERNAL_TABLE',
                 'StorageDescriptor': {'Location': 's3://test/data/', 'Columns': [
                     {'Name': 'id', 'Type': 'bigint', 'Comment': '[red]literal[/red]'},
                     {'Name': 'details', 'Type': 'struct<name:string,values:array<int>>'}]},
                 'PartitionKeys': [{'Name': 'year', 'Type': 'string'}]}
        with tempfile.TemporaryDirectory() as directory, Stubber(self.client) as stub:
            output = Path(directory) / 'report.yml'
            stub.add_response('get_table', {'Table': table}, {'DatabaseName': 'db', 'Name': 'events'})
            result = BrowseCatalog(self.catalog, CatalogRequest('describe', database='db', table='events', output=str(output))).execute()
            columns = result.document['items'][0]['columns']
            self.assertEqual([c['partition'] for c in columns], [False, False, True])
            self.assertEqual(columns[1]['type'], table['StorageDescriptor']['Columns'][1]['Type'])
            self.assertEqual(yaml.safe_load(output.read_text()), result.document)
            from rich.console import Console
            from dredge.modules.discover.catalog.presentation import present
            stream = io.StringIO()
            present(result, Console(file=stream, width=160))
            self.assertIn('[red]literal[/red]', stream.getvalue())

    def test_view_and_permission_failure(self):
        with Stubber(self.client) as stub:
            stub.add_response('get_table', {'Table': {'Name': 'view', 'TableType': 'VIRTUAL_VIEW',
                              'StorageDescriptor': {'Columns': [{'Name': 'n', 'Type': 'int'}]}}},
                              {'DatabaseName': 'db', 'Name': 'view'})
            result = self.catalog.describe(None, 'db', 'view')
            self.assertIsNone(result['location'])
            stub.add_client_error('get_databases', 'AccessDeniedException', expected_params={'ResourceShareType': 'ALL'})
            with self.assertRaisesRegex(UserError, 'permissions'):
                list(self.catalog.databases())

    def test_cli_dispatch_and_invalid_options(self):
        base = ['--module', 'discover', '--mode', 'glue']
        with Stubber(self.client) as stub, patch('dredge.cli.load_environment', return_value=None), \
                patch('dredge.modes.aws.clients.open_clients', return_value=nullcontext({'glue': self.client})), \
                patch('dredge.modules.discover.catalog.presentation.present') as present:
            stub.add_response('get_catalogs', {'CatalogList': []}, {'Recursive': True, 'IncludeRoot': True})
            self.assertEqual(main(base + ['--action', 'catalogs']), 0)
            self.assertEqual(present.call_args.args[0].document['action'], 'catalogs')
        for args in (['--action', 'tables'], ['--action', 'describe', '--database', 'db'],
                     ['--action', 'catalogs', '--bucket', 'test'], ['--action', 'databases', '--table', 't']):
            with self.subTest(args=args), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main(base + args)
            self.assertEqual(error.exception.code, 2)
