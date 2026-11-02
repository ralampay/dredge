"""Mocked asynchronous RTC jobs with synthetic product archives."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
import zipfile

import numpy as np

from dredge.core.exceptions import UserError
from dredge.modules.ingest.imagery.process_scenes import ProcessScenes
from dredge.modules.ingest.imagery.publication import ManagedOutput
from test_asf_ingestion import request, scene
from test_raster_pipeline import write_raster


class FakeService:
    def __init__(self,status='SUCCEEDED'):
        self.status=status
        self.submissions=0
        self.matches=[]
        self.downloads=0
    def find(self,name):
        return self.matches
    def submit(self,scene,name,options):
        self.submissions+=1
        return {'job_id':'synthetic-job'}
    def get(self,identifier):
        return {'job_id':identifier,'status_code':self.status,'files':[{'url':'https://example.invalid/rtc.zip'}]}
    def download(self,file,destination):
        self.downloads+=1
        raster=destination.parent/'synthetic_VV.tif'
        write_raster(raster,np.ones((10,10)))
        with zipfile.ZipFile(destination,'w') as archive:
            archive.write(raster,'product/synthetic_VV.tif')
            archive.writestr('product/metadata.txt','synthetic provenance')


class HyP3Tests(unittest.TestCase):
    def test_requires_explicit_authorization(self):
        with tempfile.TemporaryDirectory() as tmp:
            service=FakeService()
            req=request(output=str(Path(tmp)/'run'),processing={'version':1,'rtc':{'enabled':True}})
            with ManagedOutput(req.output,req.query.bbox).open() as store:
                saved=store.document['scenes'].setdefault('synthetic-1',{})
                with self.assertRaisesRegex(UserError,'allow-hyp3-submit'):
                    ProcessScenes(service,req).process(scene(),saved,store)
            self.assertEqual(service.submissions,0)

    def test_completed_processing_and_idempotent_rerun(self):
        with tempfile.TemporaryDirectory() as tmp:
            service=FakeService()
            req=request(output=str(Path(tmp)/'run'),allow_hyp3_submit=True,
                        processing={'version':1,'rtc':{'enabled':True},
                                    'reference_grid':{'crs':'+proj=eqc +datum=WGS84 +units=m','resolution_m':20000},
                                    'tiling':{'enabled':True,'tile_size':4}})
            with ManagedOutput(req.output,req.query.bbox).open() as store:
                saved=store.document['scenes'].setdefault('synthetic-1',{})
                processor=ProcessScenes(service,req)
                processor.prepare(store)
                processor.process(scene(),saved,store)
                self.assertEqual(saved['status'],'complete')
                self.assertTrue((store.root/'reference-grid.json').exists())
                self.assertTrue(any(a['path'].startswith('tiles/') for a in saved['artifacts']))
                self.assertTrue(all(store.verified(a) for a in saved['artifacts']))
                processor.process(scene(),saved,store)
            self.assertEqual(service.downloads,1)
            self.assertEqual(service.submissions,1)

    def test_timeout_then_resume_without_authorization(self):
        with tempfile.TemporaryDirectory() as tmp:
            service=FakeService('RUNNING')
            req=request(output=str(Path(tmp)/'run'),allow_hyp3_submit=True,
                        processing={'version':1,'rtc':{'enabled':True,'timeout_seconds':1}})
            clock=iter([0,2])
            with ManagedOutput(req.output,req.query.bbox).open() as store:
                saved=store.document['scenes'].setdefault('synthetic-1',{})
                ProcessScenes(service,req,clock=lambda:next(clock)).process(scene(),saved,store)
                self.assertEqual(saved['status'],'pending')
                self.assertEqual(next(iter(saved['jobs'].values()))['job_id'],'synthetic-job')
                service.status='SUCCEEDED'
                from dataclasses import replace
                ProcessScenes(service,replace(req,allow_hyp3_submit=False)).process(scene(),saved,store)
                self.assertEqual(saved['status'],'complete')
                self.assertEqual(service.submissions,1)

    def test_ambiguous_submission_is_never_retried(self):
        with tempfile.TemporaryDirectory() as tmp:
            service=FakeService()
            service.submit=Mock(side_effect=UserError('ambiguous network response'))
            req=request(output=str(Path(tmp)/'run'),allow_hyp3_submit=True,processing={'version':1,'rtc':{'enabled':True}})
            with ManagedOutput(req.output,req.query.bbox).open() as store:
                saved=store.document['scenes'].setdefault('synthetic-1',{})
                for _ in range(2):
                    with self.assertRaises(UserError):
                        ProcessScenes(service,req).process(scene(),saved,store)
                self.assertTrue(next(iter(saved['jobs'].values()))['submission_intent'])
                self.assertEqual(service.submit.call_count,1)

    def test_failed_job_and_changed_grid(self):
        with tempfile.TemporaryDirectory() as tmp:
            service=FakeService('FAILED')
            req=request(output=str(Path(tmp)/'run'),allow_hyp3_submit=True,
                        processing={'version':1,'rtc':{'enabled':True},
                                    'reference_grid':{'crs':'+proj=eqc +datum=WGS84 +units=m','resolution_m':10000}})
            with ManagedOutput(req.output,req.query.bbox).open() as store:
                saved=store.document['scenes'].setdefault('synthetic-1',{})
                processor=ProcessScenes(service,req);processor.prepare(store)
                with self.assertRaisesRegex(UserError,'job failed'):
                    processor.process(scene(),saved,store)
                req.processing['reference_grid']['resolution_m']=20000
                with self.assertRaisesRegex(UserError,'grid differs'):
                    ProcessScenes(service,req).prepare(store)
