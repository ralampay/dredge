"""Catalog-only discovery and coverage summaries."""
from collections import Counter
from dataclasses import asdict

from ....formats.raster.geometry import coverage, union_coverage
from .models import ImageryResult


class PreviewScenes:
    def __init__(self, catalog, request):
        self.catalog, self.request = catalog, request

    def execute(self):
        scenes = self.catalog.search(self.request.query)
        selected = scenes[:self.request.max_results]
        dates = sorted({s.acquired for s in scenes})
        entries = [{**asdict(s), 'coverage': coverage(s.footprint, self.request.query.bbox),
                    'local_path': None, 'status': 'catalog'} for s in selected]
        return ImageryResult({
            'query': asdict(self.request.query), 'catalog_count': len(scenes), 'selected_count': len(selected),
            'limited': len(selected) < len(scenes), 'catalog_complete': True,
            'dates': dates, 'date_range': [dates[0], dates[-1]] if dates else None,
            'months': dict(sorted(Counter(s.acquired[:7] for s in scenes).items())),
            'product_types': dict(Counter(s.product_type for s in scenes)),
            'polarizations': dict(Counter(s.polarization for s in scenes)),
            'orbits': [dict(orbit) for orbit in sorted({tuple(sorted(s.orbit.items())) for s in scenes}, key=str)],
            'known_bytes': sum(s.size_bytes or 0 for s in selected),
            'unknown_size_count': sum(s.size_bytes is None for s in selected),
            'coverage_counts': dict(Counter(e['coverage'] for e in entries)),
            'union_coverage': union_coverage([s.footprint for s in selected], self.request.query.bbox),
            'scenes': entries, 'tiff_count': None, 'status': 'preview',
        })
