"""Windowed grid alignment, geographic clipping, and explicit backscatter units."""
from pathlib import Path

from ...core.exceptions import UserError


def normalize_values(values, input_units, output_units, nodata=-9999):
    import numpy as np
    if input_units not in ('power', 'db') or output_units not in ('power', 'db'):
        raise UserError('Backscatter units must be power or db.')
    result = np.ma.array(values, copy=True, dtype='float32')
    result.mask = np.ma.getmaskarray(result) | ~np.isfinite(result.data) | (result.data == nodata)
    if input_units != output_units:
        with np.errstate(over='ignore', divide='ignore', invalid='ignore'):
            if input_units == 'power':
                result.mask |= result.data <= 0
                result = 10 * np.ma.log10(result)
            else:
                result = np.ma.power(10.0, result / 10)
    result.mask = np.ma.getmaskarray(result) | ~np.isfinite(result.data)
    return result.filled(nodata)


def align_raster(source, destination, grid, normalization=None, check_cancel=lambda: None):
    import numpy as np
    import rasterio
    from affine import Affine
    from rasterio.enums import Resampling
    from rasterio.features import geometry_mask
    from rasterio.vrt import WarpedVRT
    from pyproj import Transformer
    from shapely.geometry import Polygon, mapping

    w, s, e, n = grid.bbox
    e = e if e > w else e + 360
    boundary = []
    for start, end in (((w, s), (e, s)), ((e, s), (e, n)), ((e, n), (w, n)), ((w, n), (w, s))):
        boundary.extend((start[0] + (end[0] - start[0]) * i / 256,
                         start[1] + (end[1] - start[1]) * i / 256) for i in range(256))
    project = Transformer.from_crs('EPSG:4326', grid.crs, always_xy=True, force_over=True)
    polygon = Polygon([project.transform(x, y, errcheck=True) for x, y in boundary])
    if not polygon.is_valid:
        raise UserError('AOI projection is discontinuous; select a suitable projected CRS.')
    try:
        with rasterio.open(source) as src:
            if not src.crs or src.transform.is_identity:
                raise UserError('Raster must be geocoded before alignment; raw GRD requires SAR processing.')
            profile = dict(driver='GTiff', width=grid.width, height=grid.height, count=src.count,
                           dtype='float32', crs=grid.crs, transform=Affine(*grid.transform),
                           nodata=grid.nodata, tiled=True, blockxsize=256, blockysize=256, compress='deflate')
            with WarpedVRT(src, crs=grid.crs, transform=Affine(*grid.transform), width=grid.width,
                           height=grid.height, nodata=grid.nodata, dtype='float32',
                           resampling=Resampling[grid.resampling]) as vrt, rasterio.open(destination, 'w', **profile) as dst:
                dst.update_tags(**src.tags(), grid_alignment='geometric; not image-to-image co-registration')
                if normalization:
                    dst.update_tags(backscatter_units=normalization['output_units'])
                for _, window in dst.block_windows(1):
                    check_cancel()
                    values = vrt.read(window=window, masked=True)
                    outside = geometry_mask([mapping(polygon)], out_shape=(int(window.height), int(window.width)),
                                            transform=dst.window_transform(window))
                    values.mask = np.ma.getmaskarray(values) | outside[None, :, :]
                    data = (normalize_values(values, **normalization, nodata=grid.nodata) if normalization
                            else values.filled(grid.nodata))
                    dst.write(data, window=window)
        return Path(destination)
    except rasterio.errors.RasterioError as exc:
        raise UserError('Cannot align geocoded raster; check its CRS, bounds, and pixel data.') from exc
