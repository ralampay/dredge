"""Generic deterministic georeferenced raster tiles and source provenance."""
import math
from pathlib import Path

from ...core.exceptions import UserError


class TileRaster:
    def __init__(self, source, output, tile_size=640, overlap=.20, preserve_georeferencing=True,
                 include_empty_tiles=True, classes=None, source_fingerprint=None, check_cancel=lambda: None):
        self.source, self.output = Path(source), Path(output)
        self.size, self.overlap = tile_size, overlap
        self.georeferencing, self.empty = preserve_georeferencing, include_empty_tiles
        self.classes, self.fingerprint, self.check_cancel = classes or {}, source_fingerprint, check_cancel

    def execute(self):
        import numpy as np
        import rasterio
        from rasterio.windows import Window, bounds
        from rasterio.warp import transform_bounds
        if type(self.size) is not int or self.size < 1 or not 0 <= self.overlap < 1:
            raise UserError('Invalid tile size or overlap.')
        stride = max(1, math.floor(self.size * (1 - self.overlap)))
        self.output.mkdir(parents=True, exist_ok=False)
        tiles = []
        with rasterio.open(self.source) as src:
            if not src.crs:
                raise UserError('Tiling requires a georeferenced raster.')
            nodata = src.nodata
            if nodata is None:
                raise UserError('Tiling requires an explicit NoData value for edge padding.')
            for row in range(0, src.height, stride):
                for col in range(0, src.width, stride):
                    self.check_cancel()
                    window = Window(col, row, self.size, self.size)
                    values = src.read(window=window, boundless=True, masked=True, fill_value=nodata)
                    if not self.empty and np.ma.getmaskarray(values).all():
                        continue
                    transform = src.window_transform(window)
                    profile = src.profile.copy()
                    profile.update(width=self.size, height=self.size, transform=transform, nodata=nodata)
                    if not self.georeferencing:
                        profile.pop('crs', None)
                        profile.pop('transform', None)
                    filename = f'tile-{row:08d}-{col:08d}.tif'
                    with rasterio.open(self.output / filename, 'w', **profile) as dst:
                        dst.write(values.filled(nodata))
                        dst.update_tags(**src.tags())
                    extent = bounds(window, src.transform)
                    tiles.append({'path': filename, 'source_fingerprint': self.fingerprint,
                                  'window': [col, row, self.size, self.size],
                                  'valid_window': [0, 0, min(self.size, src.width-col), min(self.size, src.height-row)],
                                  'crs': src.crs.to_wkt(), 'transform': list(transform)[:6],
                                  'bounds': list(extent), 'geographic_bounds': list(transform_bounds(src.crs, 'EPSG:4326', *extent)),
                                  'classes': self.classes})
        return tiles
