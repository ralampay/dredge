"""Typed imagery requests and strict optional pipeline configuration."""
from dataclasses import dataclass, field
from pathlib import Path
import math

from ....core.exceptions import UserError
from ....core.imagery import SceneQuery


@dataclass(frozen=True)
class ImageryRequest:
    query: SceneQuery
    output: str | None = None
    dry_run: bool = False
    max_results: int | None = None
    overwrite: bool = False
    on_error: str = 'fail'
    allow_hyp3_submit: bool = False
    processing: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.max_results is not None and (type(self.max_results) is not int or self.max_results < 1):
            raise UserError('max_results must be a positive integer.')
        if self.on_error not in ('fail', 'continue'):
            raise UserError('on_error must be fail or continue.')
        if self.processing:
            validate_processing(self.processing)
        if not self.dry_run and not self.output:
            raise UserError('Downloads require an output directory.')

    @classmethod
    def from_config(cls, config):
        return cls(SceneQuery.from_config(config), **{k: config[k] for k in
                   ('output', 'dry_run', 'max_results', 'overwrite', 'on_error', 'allow_hyp3_submit') if k in config},
                   processing=load_processing(config.get('processing_config')))


def load_processing(path):
    if not path:
        return {}
    import yaml
    try:
        document = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise UserError('Cannot read processing YAML; check its path and syntax.') from exc
    return validate_processing(document)


def validate_processing(document):
    sections = {
        'rtc': {'enabled', 'resolution', 'radiometry', 'timeout_seconds', 'poll_seconds', 'max_extract_bytes'},
        'reference_grid': {'crs', 'resolution_m', 'resampling', 'nodata', 'max_pixels'},
        'normalization': {'input_units', 'output_units'},
        'tiling': {'enabled', 'tile_size', 'overlap', 'preserve_georeferencing', 'include_empty_tiles'},
    }
    if not isinstance(document, dict) or type(document.get('version')) is not int or document.get('version') != 1:
        raise UserError('Processing configuration requires version: 1.')
    if set(document) - {'version', 'classes', *sections}:
        raise UserError('Unknown processing configuration section.')
    for name, keys in sections.items():
        value = document.get(name, {})
        if not isinstance(value, dict) or set(value) - keys:
            raise UserError(f'Invalid or unknown {name} settings.')
    def positive(value):
        return isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value) and value > 0
    rtc = document.get('rtc', {})
    tile = document.get('tiling', {})
    grid = document.get('reference_grid', {})
    for values, names in ((rtc, ('enabled',)), (tile, ('enabled', 'preserve_georeferencing', 'include_empty_tiles'))):
        if any(type(values[k]) is not bool for k in names if k in values):
            raise UserError('Processing switches must be YAML booleans.')
    if rtc.get('resolution', 30) not in (10, 20, 30) or rtc.get('radiometry', 'gamma0') not in ('gamma0', 'sigma0'):
        raise UserError('RTC requires resolution 10, 20, or 30 and radiometry gamma0 or sigma0.')
    if any(not positive(rtc.get(k, default)) for k, default in
           (('timeout_seconds', 1800), ('poll_seconds', 30), ('max_extract_bytes', 50 * 1024**3))):
        raise UserError('RTC time and extraction limits must be positive finite numbers.')
    if grid:
        if not grid.get('crs') or not positive(grid.get('resolution_m')):
            raise UserError('Reference grid requires an explicit projected CRS and positive resolution_m.')
        if grid.get('resampling', 'bilinear') not in ('bilinear', 'nearest', 'cubic'):
            raise UserError('Unsupported grid resampling method.')
        if not isinstance(grid.get('nodata', -9999), (int, float)) or not math.isfinite(grid.get('nodata', -9999)):
            raise UserError('NoData must be finite.')
        if not positive(grid.get('max_pixels', 100_000_000)):
            raise UserError('max_pixels must be positive.')
    if tile.get('enabled') and not grid:
        raise UserError('Pipeline tiling requires reference_grid settings.')
    if type(tile.get('tile_size', 640)) is not int or tile.get('tile_size', 640) < 1:
        raise UserError('tile_size must be a positive integer.')
    overlap = tile.get('overlap', .20)
    if not isinstance(overlap, (int, float)) or isinstance(overlap, bool) or not 0 <= overlap < 1:
        raise UserError('Tile overlap must be between zero (inclusive) and one (exclusive).')
    normalization = document.get('normalization', {})
    if normalization and (set(normalization) != {'input_units', 'output_units'} or
                          any(v not in ('power', 'db') for v in normalization.values())):
        raise UserError('Normalization requires input_units and output_units: power or db.')
    if normalization and not grid:
        raise UserError('Pipeline normalization requires reference_grid settings.')
    if rtc.get('enabled') and normalization and normalization['input_units'] != 'power':
        raise UserError('HyP3 RTC pipeline normalization input must be power.')
    if (grid or normalization or tile.get('enabled')) and not rtc.get('enabled'):
        raise UserError('ASF raster processing requires rtc.enabled; raw GRD is not geocoded. Use raster Python components for other geocoded inputs.')
    classes = document.get('classes', {})
    if not isinstance(classes, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in classes.items()):
        raise UserError('classes must map string identifiers to names.')
    return document
