"""Literal Rich rendering for catalog previews and imagery runs."""
import json
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ....core.ui import print_info


def progress(message):
    print_info(message)


def present(result, console=None):
    console = console or Console()
    d = result.document
    table = Table('ASF Sentinel-1', 'Value')
    fields = [('Status', d['status']), ('Catalog matches', d['catalog_count']),
              ('Selected scenes', d['selected_count']), ('Selection limited', d['limited']),
              ('Acquisition range', d['date_range']), ('Acquisition dates (catalog)', d['dates']),
              ('Scenes by month (catalog)', d['months']), ('Product types (catalog)', d['product_types']),
              ('Polarizations (catalog)', d['polarizations']), ('Orbits (catalog)', d['orbits']),
              ('WGS84 bounds', d['query']['bbox']), ('Scene coverage (selected)', d['coverage_counts']),
              ('Union footprint coverage (selected)', d['union_coverage']),
              ('Known download bytes (selected)', d['known_bytes']),
              ('Scenes with unknown size (selected)', d['unknown_size_count']),
              ('Published TIFF count', 'unknown until processing' if d['tiff_count'] is None else d['tiff_count'])]
    for name, value in fields:
        table.add_row(Text(name), Text(json.dumps(value) if isinstance(value, (dict, list, tuple)) else str(value)))
    console.print(table)
    console.print(Text('Catalog footprints describe scene coverage; valid raster coverage may differ. Known bytes exclude unknown sizes.'))
