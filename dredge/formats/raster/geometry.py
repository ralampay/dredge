"""Longitude-normalized geometry for generic geographic areas."""


def area_geometry(bounds):
    from shapely.geometry import box
    w, s, e, n = bounds
    return box(w, s, e if e > w else e + 360, n)


def footprint_geometry(geometry, bounds):
    from shapely.geometry import Polygon, shape
    from shapely.errors import GEOSException
    from shapely.affinity import translate
    from shapely.ops import unary_union
    if not geometry:
        return None
    try:
        original = shape(geometry)
        if original.geom_type not in ('Polygon', 'MultiPolygon'):
            return None
        center = area_geometry(bounds).centroid.x
        polygons = []
        def unwrap(ring):
            points = []
            for coordinate in ring.coords:
                x, y = coordinate[:2]
                if not (-180 <= x <= 180 and -90 <= y <= 90):
                    raise ValueError
                if points:
                    while x - points[-1][0] > 180:
                        x -= 360
                    while x - points[-1][0] < -180:
                        x += 360
                points.append((x, y))
            return points
        for polygon in (original.geoms if original.geom_type == 'MultiPolygon' else (original,)):
            p = Polygon(unwrap(polygon.exterior), [unwrap(r) for r in polygon.interiors])
            if not p.is_valid or p.is_empty:
                return None
            p = translate(p, xoff=360 * round((center - p.centroid.x) / 360))
            polygons.extend([translate(p, xoff=offset) for offset in (-360, 0, 360)])
        return unary_union(polygons)
    except (TypeError, ValueError, KeyError, GEOSException):
        return None


def coverage(geometry, bounds):
    area = area_geometry(bounds)
    footprint = footprint_geometry(geometry, bounds)
    if footprint is None:
        return 'unknown'
    if footprint.covers(area):
        return 'full'
    return 'partial' if footprint.intersection(area).area > 0 else 'zero'


def union_coverage(geometries, bounds):
    from shapely.ops import unary_union
    area = area_geometry(bounds)
    shapes = [footprint_geometry(g, bounds) for g in geometries]
    known = [g for g in shapes if g is not None]
    if not known:
        return 'unknown' if shapes else 'zero'
    union = unary_union(known)
    if union.covers(area):
        return 'full'
    if any(g is None for g in shapes):
        return 'unknown'
    return 'partial' if union.intersection(area).area > 0 else 'zero'
