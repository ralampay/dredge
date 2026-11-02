"""Persistent exact projected grids, independent of providers and imagery type."""
from dataclasses import asdict, dataclass
import math

from ...core.exceptions import UserError
from ...core.imagery import parse_bbox


@dataclass(frozen=True)
class ReferenceGrid:
    crs: str
    transform: tuple
    width: int
    height: int
    extent: tuple
    bbox: tuple
    resolution_m: float
    resampling: str = 'bilinear'
    nodata: float = -9999

    def document(self):
        return {'grid_version': 1, **asdict(self)}

    @classmethod
    def from_document(cls, document):
        from pyproj.exceptions import CRSError
        if not isinstance(document, dict):
            raise UserError('Reference grid must be a JSON object.')
        if document.get('grid_version') != 1:
            raise UserError('Unsupported reference grid version.')
        try:
            values = {k: document[k] for k in cls.__dataclass_fields__}
            for name in ('transform', 'extent', 'bbox'):
                values[name] = tuple(values[name])
            grid = cls(**values)
            from pyproj import CRS
            from affine import Affine
            crs = CRS.from_user_input(grid.crs)
            parse_bbox(grid.bbox)
            transform = Affine(*grid.transform)
            if (not crs.is_projected or any(abs(a.unit_conversion_factor - 1) > 1e-12 for a in crs.axis_info[:2]) or
                    type(grid.width) is not int or type(grid.height) is not int or min(grid.width, grid.height) < 1 or
                    not math.isfinite(grid.resolution_m) or grid.resolution_m <= 0 or
                    not all(math.isfinite(v) for v in (*grid.transform, *grid.extent, grid.nodata)) or
                    transform.a != grid.resolution_m or transform.e != -grid.resolution_m or
                    transform.b != 0 or transform.d != 0 or grid.resampling not in ('bilinear', 'nearest', 'cubic')):
                raise UserError('Reference grid definition is inconsistent.')
            left, top = transform.c, transform.f
            right, bottom = left + transform.a * grid.width, top + transform.e * grid.height
            if not all(math.isclose(a, b, abs_tol=1e-7) for a, b in zip(grid.extent, (left, bottom, right, top))):
                raise UserError('Reference grid extent is inconsistent.')
            return grid
        except (KeyError, TypeError, ValueError, CRSError) as exc:
            raise UserError('Invalid reference grid definition.') from exc


def create_grid(bounds, settings):
    from pyproj import CRS, Transformer
    from pyproj.exceptions import CRSError, ProjError
    bounds = parse_bbox(bounds)
    try:
        crs = CRS.from_user_input(settings['crs'])
        resolution = float(settings['resolution_m'])
        if not crs.is_projected or any(abs(a.unit_conversion_factor - 1) > 1e-12 for a in crs.axis_info[:2]):
            raise UserError('Reference CRS must be projected with metre units.')
        if not math.isfinite(resolution) or resolution <= 0:
            raise UserError('Grid resolution must be positive and finite.')
        w, s, e, n = bounds
        east = e if e > w else e + 360
        # Densify all four edges; preserve continuous longitude across the dateline.
        points = []
        for index in range(257):
            t = index / 256
            points.extend(((w + (east - w) * t, s), (w + (east - w) * t, n),
                           (w, s + (n - s) * t), (east, s + (n - s) * t)))
        domain = crs.area_of_use
        if domain:
            for lon, lat in points:
                lon = (lon + 180) % 360 - 180
                inside_lon = (domain.west <= lon <= domain.east if domain.west <= domain.east
                              else lon >= domain.west or lon <= domain.east)
                if not inside_lon or not domain.south <= lat <= domain.north:
                    raise UserError('AOI exceeds this CRS area of use; select a suitable projected CRS.')
        transformer = Transformer.from_crs('EPSG:4326', crs, always_xy=True, force_over=True)
        projected = [transformer.transform(x, y, errcheck=True) for x, y in points]
        if not all(math.isfinite(v) for p in projected for v in p):
            raise UserError('AOI cannot be represented by the configured CRS.')
        xs, ys = zip(*projected)
        left, bottom = math.floor(min(xs) / resolution) * resolution, math.floor(min(ys) / resolution) * resolution
        right, top = math.ceil(max(xs) / resolution) * resolution, math.ceil(max(ys) / resolution) * resolution
        width, height = round((right - left) / resolution), round((top - bottom) / resolution)
        if width <= 0 or height <= 0 or width * height > settings.get('max_pixels', 100_000_000):
            raise UserError('Reference grid is empty or exceeds max_pixels; use a coarser resolution or smaller area.')
        return ReferenceGrid(crs.to_wkt(), (resolution, 0, left, 0, -resolution, top), width, height,
                             (left, bottom, right, top), bounds, resolution,
                             settings.get('resampling', 'bilinear'), settings.get('nodata', -9999))
    except (CRSError, ProjError, KeyError, TypeError, ValueError) as exc:
        raise UserError('Invalid projected CRS or grid settings.') from exc
