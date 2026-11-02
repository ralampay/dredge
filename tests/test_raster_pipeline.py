"""Generated rasters and synthetic AOIs, independent of named locations."""
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import rasterio
from affine import Affine

from dredge.core.exceptions import UserError
from dredge.formats.raster.geometry import coverage, union_coverage
from dredge.formats.raster.grid import ReferenceGrid, create_grid
from dredge.formats.raster.operations import align_raster, normalize_values
from dredge.formats.raster.tiling import TileRaster
from test_asf_ingestion import polygon


def write_raster(path, values, crs='EPSG:4326', transform=Affine(.1,0,0,0,-.1,1), nodata=-9999):
    with rasterio.open(path,'w',driver='GTiff',height=values.shape[0],width=values.shape[1],count=1,
                       dtype='float32',crs=crs,transform=transform,nodata=nodata) as dst:
        dst.write(values.astype('float32'),1)


class RasterTests(unittest.TestCase):
    def test_geographic_grid_cases_and_persistence(self):
        cases = [((10,10,10.02,10.02),'EPSG:32632'), ((-110,40,-109.98,40.02),'EPSG:32612'),
                 ((20,-20,20.02,-19.98),'EPSG:32734'), ((-40,-20,-39.98,-19.98),'EPSG:32724'),
                 ((0,-.01,.02,.01),'+proj=eqc +datum=WGS84 +units=m'),
                 ((20,85,20.1,85.1),'EPSG:3413'), ((20,-85,20.1,-84.9),'EPSG:3031'),
                 ((179.99,0,-179.99,.02),'+proj=eqc +lon_0=180 +datum=WGS84 +units=m')]
        for bounds, crs in cases:
            with self.subTest(bounds=bounds):
                grid = create_grid(bounds,{'crs':crs,'resolution_m':100})
                self.assertGreater(grid.width,0)
                self.assertGreater(grid.height,0)
                self.assertEqual(ReferenceGrid.from_document(json.loads(json.dumps(grid.document()))),grid)
                self.assertEqual(create_grid(bounds,{'crs':crs,'resolution_m':100}),grid)

    def test_invalid_crs_zone_crossing_and_pixel_budget(self):
        for settings in ({'crs':'EPSG:4326','resolution_m':10}, {'crs':'invalid','resolution_m':10},
                         {'crs':'EPSG:32631','resolution_m':10},
                         {'crs':'+proj=eqc +datum=WGS84 +units=m','resolution_m':.01,'max_pixels':10}):
            with self.subTest(settings=settings), self.assertRaises(UserError):
                create_grid((5.9,10,6.1,10.1),settings)
        self.assertGreater(create_grid((5.9,10,6.1,10.1),{'crs':'+proj=eqc +datum=WGS84 +units=m',
                                                       'resolution_m':100}).width,0)

    def test_alignment_clipping_nodata_and_determinism(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            source=root/'source.tif'
            values=np.full((10,10),10,dtype='float32');values[:2,:]=-9999
            write_raster(source,values)
            grid=create_grid((-.1,-.1,1.1,1.1),{'crs':'+proj=eqc +datum=WGS84 +units=m','resolution_m':10000})
            for i in range(2):
                align_raster(source,root/f'out{i}.tif',grid,{'input_units':'power','output_units':'db'})
            with rasterio.open(root/'out0.tif') as first,rasterio.open(root/'out1.tif') as second:
                self.assertEqual(first.transform,Affine(*grid.transform))
                self.assertEqual(first.shape,(grid.height,grid.width))
                self.assertEqual(first.transform,second.transform)
                np.testing.assert_equal(first.read(),second.read())
                data=first.read(1,masked=True)
                self.assertTrue(data.mask.any())
                np.testing.assert_allclose(data.compressed(),10,atol=1e-5)

    def test_partial_dateline_raster_warp(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            write_raster(root/'east.tif',np.ones((10,10)), transform=Affine(.001,0,179.99,0,-.001,.01))
            grid=create_grid((179.99,0,-179.99,.01),{'crs':'+proj=eqc +lon_0=180 +datum=WGS84 +units=m','resolution_m':100})
            align_raster(root/'east.tif',root/'out.tif',grid)
            with rasterio.open(root/'out.tif') as src:
                data=src.read(1,masked=True)
                self.assertGreater(data.count(),0)
                self.assertTrue(data.mask.any())

    def test_coverage_and_dateline(self):
        bounds=(0,0,1,1)
        for geometry, expected in ((polygon(-1,-1,2,2),'full'),(polygon(.5,.5,2,2),'partial'),
                                   (polygon(2,2,3,3),'zero'),(None,'unknown')):
            self.assertEqual(coverage(geometry,bounds),expected)
        self.assertEqual(coverage(polygon(179,-1,-179,2),(179.5,0,-179.5,1)),'full')
        self.assertEqual(union_coverage([polygon(0,0,.5,1),polygon(.5,0,1,1)],bounds),'full')

    def test_backscatter_units(self):
        values=np.array([1,10,100,0,-1,-9999,np.nan],dtype='float32')
        result=normalize_values(values,'power','db')
        np.testing.assert_allclose(result[:3],[0,10,20],atol=3e-6)
        np.testing.assert_equal(result[3:],[-9999]*4)
        np.testing.assert_allclose(normalize_values(result[:3],'db','power'),[1,10,100],rtol=1e-6)

    def test_tiling_overlap_padding_and_empty_tiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            values=np.ones((5,5));values[:2,:2]=-9999
            transform=Affine(10,0,100,0,-10,200)
            write_raster(root/'source.tif',values,'EPSG:32631',transform)
            tiles=TileRaster(root/'source.tif',root/'tiles',tile_size=4,overlap=.5,
                             classes={'a':'generic'},source_fingerprint='abc').execute()
            self.assertEqual(len(tiles),9)
            self.assertEqual(tiles[1]['window'],[2,0,4,4])
            self.assertEqual(tiles[-1]['valid_window'],[0,0,1,1])
            with rasterio.open(root/'tiles'/tiles[1]['path']) as src:
                self.assertEqual(src.transform,Affine(10,0,120,0,-10,200))
                self.assertEqual(src.shape,(4,4))
                self.assertEqual(src.read(1)[0,-1],-9999)
            self.assertEqual(tiles[0]['source_fingerprint'],'abc')
            write_raster(root/'empty.tif',np.full((2,2),-9999))
            self.assertEqual(TileRaster(root/'empty.tif',root/'none',tile_size=4,include_empty_tiles=False).execute(),[])

    def test_nearest_and_bilinear_resampling_differ(self):
        from dataclasses import replace
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            values=np.indices((10,10)).sum(axis=0)%2
            write_raster(root/'source.tif',values)
            grid=create_grid((0,0,1,1),{'crs':'+proj=eqc +datum=WGS84 +units=m','resolution_m':7000})
            align_raster(root/'source.tif',root/'near.tif',replace(grid,resampling='nearest'))
            align_raster(root/'source.tif',root/'linear.tif',grid)
            with rasterio.open(root/'near.tif') as n,rasterio.open(root/'linear.tif') as b:
                self.assertTrue(set(n.read(1,masked=True).compressed()).issubset({0,1}))
                values=b.read(1,masked=True).compressed()
                self.assertTrue(((values>0)&(values<1)).any())
