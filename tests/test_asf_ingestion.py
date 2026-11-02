"""Synthetic ASF catalog and publication tests; no credentials or network."""
from contextlib import redirect_stderr
from dataclasses import replace
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch
import zipfile

from dredge.cli import build_parser
from dredge.cli_routing import validate_route
from dredge.core.exceptions import UserError
from dredge.core.imagery import AuthenticationError, Scene, SceneQuery
from dredge.modes.asf.adapters import AsfCatalog, AsfDownloader, map_scene, retry
from dredge.modes.asf.configuration import EarthdataSettings
from dredge.modules.ingest.imagery.commands import IngestScenes
from dredge.modules.ingest.imagery.preview import PreviewScenes
from dredge.modules.ingest.imagery.publication import ManagedOutput, extract_archive
from dredge.modules.ingest.imagery.requests import ImageryRequest, validate_processing


def polygon(w, s, e, n):
    return {'type': 'Polygon', 'coordinates': [[[w,s], [e,s], [e,n], [w,n], [w,s]]]}


def scene(identifier='synthetic-1', footprint=None, size=3, acquired='2025-01-01T00:00:00+00:00'):
    return Scene(identifier, identifier, 'Sentinel-1A', 'GRD_HD', acquired, 'VV+VH',
                 {'orbit': 123, 'pathNumber': 10, 'flightDirection': 'ASCENDING'},
                 footprint or polygon(0, 0, 2, 2), 'https://example.invalid/product.zip', size, 'IW')


def request(**kwargs):
    return ImageryRequest(SceneQuery((0, 0, 1, 1), '2025-01-01', '2025-12-31'), **kwargs)


class Catalog:
    def __init__(self, scenes):
        self.scenes = scenes
        self.calls = 0
    def search(self, query):
        self.calls += 1
        return self.scenes


class Downloader:
    def __init__(self):
        self.calls = 0
    def authenticate(self):
        pass
    def download(self, scene, path):
        self.calls += 1
        path.write_bytes(b'abc')


class AsfTests(unittest.TestCase):
    def test_cli_registration_and_isolation(self):
        parser = build_parser()
        args = parser.parse_args(['--module','ingest','--mode','asf','--action','get', '--bbox=0,0,1,1',
                                  '--start-date','2025-01-01','--end-date','2025-02-01','--dry-run'])
        self.assertEqual(validate_route(parser, vars(args)).mode, 'asf')
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            args.schema = 'unexpected.yaml'
            validate_route(parser, vars(args))
        self.assertEqual(error.exception.code, 2)

    def test_query_validation_and_date_bounds(self):
        for bounds in ((-1,-1,1,1), (-111,30,-110,31), (40,-40,41,-39), (179,-2,-179,2), (0,85,1,86)):
            with self.subTest(bounds=bounds):
                q = SceneQuery(bounds, '2025-01-01', '2025-01-01')
                self.assertTrue(q.end.endswith('23:59:59.999999+00:00'))
        for bounds in ((0,0,0,1), (181,0,182,1), (0,-91,1,0), (0,2,1,1), (float('nan'),0,1,1)):
            with self.subTest(bounds=bounds), self.assertRaises(UserError):
                SceneQuery(bounds, '2025-01-01', '2025-01-02')
        for start, end in (('bad','2025-01-01'), ('2026-01-01','2025-01-01'), ('2025-01-01T00:00:00','2025-01-02')):
            with self.assertRaises(UserError):
                SceneQuery((0,0,1,1), start, end)

    def test_pagination_filters_dateline_and_metadata(self):
        p = SimpleNamespace(properties={'fileID':'synthetic', 'sceneName':'synthetic', 'startTime':'2025-01-01T00:00:00Z',
                            'url':'https://example.invalid/a.zip', 'bytes':1234, 'processingLevel':'GRD_HD',
                            'platform':'Sentinel-1A', 'beamModeType':'IW', 'polarization':'VV', 'pathNumber':20},
                            geometry=polygon(179,0,-179,1))
        sdk = SimpleNamespace(PLATFORM=SimpleNamespace(SENTINEL1='SENTINEL-1'), search_generator=Mock(return_value=[[p],[p]]), search_count=Mock(return_value=1))
        q = SceneQuery((179,0,-179,1), '2025-01-01','2025-01-02', 'VV','ASCENDING',20,'IW')
        result = AsfCatalog(sdk).search(q)
        self.assertEqual(len(result),1)
        self.assertEqual(result[0].size_bytes,1234)
        self.assertEqual(sdk.search_generator.call_count,2)
        options = sdk.search_generator.call_args.kwargs
        self.assertEqual(options['relativeOrbit'],20)
        self.assertEqual(options['polarization'],'VV')
        self.assertEqual(options['beamMode'],'IW')
        self.assertEqual(options['flightDirection'],'ASCENDING')
        self.assertEqual(options['start'],q.start)
        self.assertNotIn('maxResults',options)

    def test_preview_counts_coverage_dates_and_no_side_effects(self):
        catalog = Catalog([scene(), scene('b', polygon(.5,.5,2,2),None,'2025-02-01T00:00:00+00:00'),
                           scene('c',polygon(3,3,4,4))])
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'unused'
            downloader, processor = Mock(), Mock()
            result = IngestScenes(catalog, downloader, request(output=str(output),dry_run=True,max_results=2), processor).execute()
            d = result.document
            self.assertEqual(catalog.calls,1)
            self.assertEqual((d['catalog_count'],d['selected_count']),(3,2))
            self.assertTrue(d['limited'])
            self.assertEqual(d['months'],{'2025-01':2,'2025-02':1})
            self.assertEqual(d['coverage_counts'],{'full':1,'partial':1})
            self.assertEqual(d['known_bytes'],3)
            self.assertEqual(d['unknown_size_count'],1)
            self.assertIsNone(d['tiff_count'])
            self.assertFalse(output.exists())
            self.assertEqual(downloader.mock_calls,[])
            self.assertEqual(processor.mock_calls,[])

    def test_idempotent_download_and_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = str(Path(tmp)/'run')
            catalog, downloader = Catalog([scene()]), Downloader()
            for _ in range(2):
                self.assertFalse(IngestScenes(catalog,downloader,request(output=output)).execute().partial)
            self.assertEqual(downloader.calls,1)
            manifest = json.loads((Path(output)/'manifest.json').read_text())
            raw = Path(output)/manifest['scenes']['synthetic-1']['raw']['path']
            raw.write_bytes(b'bad')
            with self.assertRaisesRegex(UserError,'overwrite'):
                IngestScenes(catalog,downloader,request(output=output)).execute()
            IngestScenes(catalog,downloader,request(output=output,overwrite=True)).execute()
            self.assertEqual(raw.read_bytes(),b'abc')
            self.assertEqual(downloader.calls,2)

    def test_continue_and_authentication_are_distinct(self):
        with tempfile.TemporaryDirectory() as tmp:
            downloader = Downloader()
            downloader.download = Mock(side_effect=UserError('network exhausted'))
            result = IngestScenes(Catalog([scene()]),downloader,request(output=str(Path(tmp)/'run'),on_error='continue')).execute()
            self.assertTrue(result.partial)
            downloader.download.side_effect = AuthenticationError('invalid credentials')
            with self.assertRaises(AuthenticationError):
                IngestScenes(Catalog([scene()]),downloader,request(output=str(Path(tmp)/'other'),on_error='continue')).execute()

    def test_authentication_settings_never_exposed(self):
        self.assertNotIn('private',repr(EarthdataSettings('private','private')))
        downloader = AsfDownloader(Mock(),EarthdataSettings())
        with self.assertRaises(AuthenticationError):
            downloader.authenticate()

    def test_retry_is_bounded(self):
        from requests.exceptions import ConnectionError, HTTPError
        operation = Mock(side_effect=ConnectionError('secret URL'))
        sleep = Mock()
        with self.assertRaises(UserError) as error:
            retry(operation,sleep)
        self.assertNotIn('secret',str(error.exception))
        self.assertEqual(operation.call_count,3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list],[1,2])
        operation = Mock(side_effect=HTTPError(response=SimpleNamespace(status_code=401)))
        with self.assertRaises(AuthenticationError):
            retry(operation,sleep)
        self.assertEqual(operation.call_count,1)

    def test_archives_reject_traversal_and_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name in ('../escape','/escape','a\\b'):
                archive=root/'bad.zip'
                with zipfile.ZipFile(archive,'w') as z:
                    z.writestr(name,b'x')
                with self.assertRaises(UserError):
                    extract_archive(archive,root/'out')
            with zipfile.ZipFile(root/'ok.zip','w') as z:
                z.writestr('data/a.txt',b'abc')
            self.assertEqual(extract_archive(root/'ok.zip',root/'out')[0].read_bytes(),b'abc')
            with self.assertRaises(UserError):
                extract_archive(root/'ok.zip',root/'small',max_bytes=1)

    def test_managed_output_rejects_unrelated_and_changed_area(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'unrelated';root.mkdir()
            with self.assertRaises(UserError), ManagedOutput(root,(0,0,1,1)).open():
                pass
            root=Path(tmp)/'managed'
            with ManagedOutput(root,(0,0,1,1)).open():
                pass
            with self.assertRaises(UserError), ManagedOutput(root,(1,1,2,2)).open():
                pass

    def test_processing_validation(self):
        for doc in ({}, {'version':1,'unknown':{}}, {'version':1,'reference_grid':{'resolution_m':10}},
                    {'version':1,'tiling':{'enabled':True}}, {'version':1,'rtc':{'enabled':'yes'}}):
            with self.assertRaises(UserError):
                validate_processing(doc)
        self.assertEqual(validate_processing({'version':1,'rtc':{'enabled':True}})['version'],1)

    def test_authenticated_stream_download_and_response_cleanup(self):
        sdk = MagicMock()
        session = sdk.ASFSession.return_value
        response = session.get.return_value.__enter__.return_value
        response.iter_content.return_value = [b'a',b'bc']
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp)/'download.part'
            downloader=AsfDownloader(sdk,EarthdataSettings('user','private'))
            downloader.download(scene(),destination)
            self.assertEqual(destination.read_bytes(),b'abc')
            session.auth_with_creds.assert_called_once_with('user','private')
            session.get.return_value.__exit__.assert_called_once()
            downloader.close()
            session.close.assert_called_once()

    def test_cancellation_does_not_publish_partial_download(self):
        from dredge.core.exceptions import UserAbort
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)/'run'
            class Interrupted(Downloader):
                def download(self,scene,path):
                    path.write_bytes(b'partial')
                    raise UserAbort()
            with self.assertRaises(UserAbort):
                IngestScenes(Catalog([scene()]),Interrupted(),request(output=str(output))).execute()
            self.assertFalse((output/'raw').exists())
            self.assertFalse(list(output.glob('.imagery-stage-*')))
            self.assertFalse((output.parent/('.'+output.name+'.imagery.lock')).exists())

    def test_retry_exhausted_catalog_is_not_a_partial_preview(self):
        from requests.exceptions import ConnectionError
        sdk=SimpleNamespace(PLATFORM=SimpleNamespace(SENTINEL1='SENTINEL-1'),
                            search_generator=Mock(side_effect=ConnectionError('unavailable')), search_count=Mock(return_value=1))
        with self.assertRaises(UserError):
            PreviewScenes(AsfCatalog(sdk,sleep=lambda _:None),request(dry_run=True)).execute()
        self.assertEqual(sdk.search_generator.call_count,3)

    def test_publication_integrity_error_is_fatal_under_continue(self):
        from dredge.modules.ingest.imagery.publication import PublicationError
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'run'
            IngestScenes(Catalog([scene()]),Downloader(),request(output=str(root))).execute()
            manifest=json.loads((root/'manifest.json').read_text())
            manifest['scenes']['synthetic-1']['raw']['path']='../escape'
            (root/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaises(PublicationError):
                IngestScenes(Catalog([scene()]),Downloader(),request(output=str(root),on_error='continue')).execute()

    def test_catalog_truncation_is_reported_as_incomplete(self):
        sdk=SimpleNamespace(PLATFORM=SimpleNamespace(SENTINEL1='SENTINEL-1'),
                            search_count=Mock(return_value=10),search_generator=Mock(return_value=[[]]))
        with self.assertRaisesRegex(UserError,'incomplete'):
            PreviewScenes(AsfCatalog(sdk),request(dry_run=True)).execute()

    def test_cli_dry_run_loads_configuration_once_and_never_authenticates(self):
        from contextlib import redirect_stdout
        from dredge.cli import main
        from dredge.core.configuration import EnvironmentConfig
        with tempfile.TemporaryDirectory() as tmp:
            environment=EnvironmentConfig(Path(tmp)/'.env',False,())
            with patch('dredge.cli.load_environment',return_value=environment) as load, \
                 patch('dredge.modes.asf.adapters.AsfCatalog.search',return_value=[scene()]) as search, \
                 patch('dredge.modes.asf.adapters.AsfDownloader.authenticate') as authenticate, \
                 redirect_stdout(io.StringIO()):
                code=main(['--module','ingest','--mode','asf','--action','get','--bbox=0,0,1,1',
                           '--start-date','2025-01-01','--end-date','2025-12-31','--dry-run',
                           '--allow-hyp3-submit','--output',str(Path(tmp)/'unused')])
                self.assertEqual(code,0)
                load.assert_called_once()
                search.assert_called_once()
                authenticate.assert_not_called()
                self.assertEqual(list(Path(tmp).iterdir()),[])
