# Ingest ASF Sentinel-1 imagery

[Documentation home](../../README.md) · [Configuration](../configuration.md)

Run CLI examples from the repository root.

Install `requirements.txt` for the official `asf_search` client, Shapely geometry,
PyProj CRS transformations, NumPy, and Rasterio/GDAL-compatible GeoTIFF operations.
The route is part of the existing CLI; imagery does not require a tabular schema
and is never converted to Parquet.

```bash
python -m dredge --module ingest --mode asf --action get \
  --bbox=10,10,10.2,10.2 --start-date 2025-01-01 --end-date 2025-12-31 \
  --dry-run
```

Bounds are WGS84 `west,south,east,north`; use `--bbox=-...` when the first
coordinate is negative. West greater than east explicitly crosses the antimeridian.
Latitude must increase, coordinates must be finite, and zero-area boxes are rejected.
Dates are inclusive: a calendar end date includes the entire UTC day; timestamps
must include a timezone. There are no geographic defaults or named-area behaviors.

Optional filters are `--polarization VV|VH|HH|HV|VV+VH|HH+HV`,
`--flight-direction ASCENDING|DESCENDING`, `--relative-orbit 1..175`, and
`--acquisition-mode IW|EW|SM|WV`. Polarization is the catalog's product polarization
selection (for dual-polarization products use `VV+VH` or `HH+HV`). All GRD product
variants are searched; actual availability depends on the archive.

## Catalog-only previews

`--dry-run` performs a real public ASF search without download credentials. It
never downloads imagery, initializes HyP3, submits jobs, creates directories, or
publishes artifacts, even if `--output` or `--allow-hyp3-submit` is supplied.

The Rich report separates total unique catalog matches from selected scenes and
final TIFF counts. It includes acquisition dates/range, monthly counts, product
types, polarizations, orbit metadata, geographic bounds, selected-scene full,
partial, zero or unknown coverage, and union footprint coverage. Footprints do not
guarantee valid pixels, terrain correction, or temporal coverage. Missing/invalid
footprints are unknown, never presumed full coverage.

`--max-results N` selects the first N scenes in acquisition-time/identifier order.
All catalog pages are still enumerated for exact unique counts and summaries;
large searches may take time and memory. Antimeridian searches are deduplicated.
Date/product/orbit summaries cover the entire catalog result; volume and coverage
summaries cover selected scenes. Download sizes use readable decimal units such as
MB and GB; result data retains exact byte counts. Known sizes exclude scenes with missing size
metadata and do not estimate uncompressed or derived size. Incomplete catalog
searches fail explicitly instead of reporting misleading totals.

## Earthdata authentication and downloads

Register at [NASA Earthdata Login](https://urs.earthdata.nasa.gov/), complete the
account profile, and authorize ASF access as required by Earthdata. Set these
values in your shell or repository `.env` (never commit populated credentials):

```dotenv
EARTHDATA_USERNAME=
EARTHDATA_PASSWORD=
```

Shell values retain precedence. Doctor masks the password and reports only keys
declared in `.env`. Credentials are excluded from manifests and error messages.
Remove `--dry-run` and supply `--output` to download original GRD ZIP products:

```bash
python -m dredge --module ingest --mode asf --action get \
  --bbox=10,10,10.2,10.2 --start-date 2025-01-01 --end-date 2025-01-31 \
  --acquisition-mode IW --max-results 2 --output artifacts/radar-area
```

The output parent must exist. A fresh directory receives an imagery manifest;
a previously managed directory with the same bounds can receive later acquisitions.
Unrelated directories, different bounds, and symlink artifacts are rejected.
Matching downloads are reused only after size and SHA-256 verification. Missing
files can be regenerated. `--overwrite` permits replacing conflicting managed
artifacts; it does not replace a whole output directory or change the saved grid.
S3 ingestion's fresh-directory policy is unchanged.

Transient requests have bounded retries and backoff. `--on-error fail` is the
default; `continue` records per-scene errors and continues, returning exit code 1
for partial output. Authentication and publication-integrity failures always stop.
Cancellation preserves already published artifacts for reruns. A process killed
without cleanup may leave a sibling `.imagery.lock`; verify that no process owns
the destination before removing that lock.

## Example: Subic Bay preview and download

These example bounds cover a study area around Subic Bay. They are supplied as
inputs, not application defaults; replace them to target another area.

First, preview matching scenes for January 2026 without credentials or downloads:

```bash
python -m dredge --module ingest --mode asf --action get \
  --bbox=120.14,14.72,120.33,14.90 \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --acquisition-mode IW \
  --max-results 10 \
  --dry-run
```

To fetch up to ten scenes using the same filters, set `EARTHDATA_USERNAME` and
`EARTHDATA_PASSWORD` in your shell or `.env`, then run:

```bash
mkdir -p artifacts

python -m dredge --module ingest --mode asf --action get \
  --bbox=120.14,14.72,120.33,14.90 \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --acquisition-mode IW \
  --max-results 10 \
  --output artifacts/subic-bay-2026-01
```

The preview enumerates all matching catalog metadata and reports the ten-scene
selection limit. The download command fetches raw GRD ZIP products and writes a
manifest; it does not submit HyP3 jobs or generate processed TIFFs. A rerun reuses
verified downloads in the same managed output directory.

## Optional HyP3 RTC and raster pipeline

HyP3 dependencies are included in `requirements.txt`; RTC processing remains opt-in. Copy
[`examples/asf/processing.yaml`](../../examples/asf/processing.yaml), replace its CRS
placeholder with a suitable metre-based projected CRS, and choose resolution and
processing settings. The version-1 file accepts only `rtc`, `reference_grid`,
`normalization`, `tiling`, and optional `classes` sections; catalog filters remain
CLI arguments. Processing is disabled unless explicitly configured.

```bash
python -m dredge --module ingest --mode asf --action get \
  --bbox=10,10,10.2,10.2 --start-date 2025-01-01 --end-date 2025-01-31 \
  --acquisition-mode IW --max-results 1 --output artifacts/radar-area \
  --processing-config processing.yaml --allow-hyp3-submit
```

For this illustrative box, EPSG:32632 is a suitable projected CRS. It is not an
application default. HyP3 RTC accepts supported IW GRD-H products; unsupported
products produce an explicit error. RTC uses the official HyP3 SDK and Earthdata
credentials. Service availability, eligibility, quotas and credits are controlled
by ASF. Job names and generated service products may be publicly visible; Dredge
uses non-sensitive hashed job names. No processing job is submitted by default.

New jobs require `--allow-hyp3-submit` on the current invocation. Job intent and IDs
are persisted immediately. Dredge waits up to `rtc.timeout_seconds` (default 1800),
polling at most every 30 seconds. Pending jobs return incomplete status/exit code 1;
rerun the same command without authorization to collect existing jobs. An ambiguous
submission is reconciled by job name; unresolved intent blocks automatic duplicate
submission. Failed jobs remain recorded and are not silently resubmitted.

RTC defaults to 30 m linear power, gamma0, with no speckle filter. Its resolution is
separate from target-grid spacing; resampling to finer pixels does not improve the
sensor's spatial resolution. Dredge does not implement a local GRD calibration or
terrain-correction engine. Reprojection of raw GRD is not SAR geocoding.

## Example: Subic Bay 640×640 tiles for January 2026

To turn the downloaded SAR products into georeferenced tiles, save the following
as `processing.yaml` in the repository root. EPSG:32651 is chosen for this example
area only; choose a suitable projected CRS when using different bounds.

```yaml
version: 1

rtc:
  enabled: true
  resolution: 30

reference_grid:
  crs: EPSG:32651
  resolution_m: 30
  resampling: bilinear
  nodata: -9999

tiling:
  enabled: true
  tile_size: 640
  overlap: 0.20
  preserve_georeferencing: true
  include_empty_tiles: true
```

With `EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD` configured, run:

```bash
mkdir -p artifacts

python -m dredge --module ingest --mode asf --action get \
  --bbox=120.14,14.72,120.33,14.90 \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --acquisition-mode IW \
  --max-results 10 \
  --output artifacts/subic-bay-2026-01 \
  --processing-config processing.yaml \
  --allow-hyp3-submit
```

This explicitly authorizes HyP3 RTC jobs for up to ten selected scenes, aligns the
processed rasters to the reference grid, and writes 640×640-pixel GeoTIFFs under
`artifacts/subic-bay-2026-01/tiles/`. Verified raw products from the preceding
download example are reused. Processing requires HyP3 access and available service
quota; the catalog-only `--dry-run` does not submit jobs or generate tiles.

Tiles overlap by 20%, preserve georeferencing, and include empty tiles. Edge tiles
are padded with NoData. At 30 m pixel spacing, a full tile spans 19.2 × 19.2 km;
the valid-data footprint may be smaller. Outputs retain linear-power backscatter
because this example does not request unit conversion.

If the command times out while jobs are pending, rerun it without
`--allow-hyp3-submit` to collect existing jobs. Use a new output directory if you
change the area or the persisted reference-grid settings.

## Exact reference grids and generic tiling

Alignment requires an explicitly configured projected CRS with metre units and
positive `resolution_m`. Grid extents derive from densified AOI edges and snap
outward to resolution multiples. `reference-grid.json` persists CRS WKT, affine
transform, width/height, extent, AOI, resampling, and NoData. Later acquisitions
must use this exact grid. Changed area/grid settings require a new destination,
even with `--overwrite`.

Choose a suitable projection for zone-crossing, equatorial, or polar areas. No UTM
zone is assumed. CRS area-of-use violations, invalid projections and excessive
pixel counts fail explicitly. Antimeridian discovery works independently of raster
processing; raster processing requires a projection continuous across that AOI,
such as an appropriate dateline-centered projection. A single projected grid is
not appropriate for every global extent. The default limit is 100 million pixels.

Raster alignment uses windowed Rasterio warping, clipping and explicit NoData.
Bilinear is the default; nearest and cubic are also supported. Alignment establishes
identical grid geometry, **not true image-to-image co-registration**. Linear
power ↔ dB conversion requires explicit units and masks nonpositive power before
logarithms; there is no scene-wise min/max normalization.

Tiling is optional, with defaults of 640 pixels, 20% overlap, preserved
georeferencing and empty tiles included. Stride is `floor(size * (1 - overlap))`,
at least one pixel. Edge tiles are padded with source NoData. Tile metadata retains
source checksums, pixel/valid windows, CRS, transforms, projected/geographic bounds,
and consumer-supplied class names. No object classes are hardcoded.

The generic Python components `create_grid`, `align_raster`, `normalize_values`,
and `TileRaster(...).execute()` in `dredge.formats.raster` also accept unrelated
geocoded rasters. The ASF CLI's raster stages consume HyP3 RTC outputs. Object
detection and model training are outside this workflow.

## Imagery manifests and provenance

```text
radar-area/
  manifest.json
  reference-grid.json        # when alignment is configured
  raw/                      # original GRD products
  derived/                  # complete RTC packages and aligned rasters
  tiles/                    # optional georeferenced tiles
```

`imagery_version: 1` is separate from tabular dataset manifests. Scene entries
retain identifier, platform/product, acquisition, polarization, orbit, footprint,
download URL, optional byte size, local path and status. Managed artifacts record
relative paths, sizes and checksums. Derived entries add job IDs, processing
configuration and grid provenance; complete RTC archives retain provider metadata.
Temporary signed HyP3 download URLs and credentials are not persisted.

Files and manifest updates are published atomically under a destination lock.
This is resumable per-artifact publication, not an all-or-nothing directory
transaction. Old processing versions may remain on disk; the manifest identifies
the current artifacts. No automatic deletion or garbage collection is performed.
Archive extraction rejects traversal, links, duplicate paths, and excessive sizes.

[`examples/asf/study-areas.yaml`](../../examples/asf/study-areas.yaml) contains optional
Subic Bay and Scarborough Shoal examples only. Neither application code nor tests
loads that file. Any valid supplied coordinates follow the same workflow.

Tests use mocked services and generated geometry/GeoTIFFs, never live downloads,
Earthdata credentials or processing credits. Live-service behavior requires a
separately authorized smoke test; ordinary tests are entirely offline.
