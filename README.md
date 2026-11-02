# Dredge

A modular Python 3.10+ CLI for data discovery and ingestion. Discover schemas and
catalogs, resolve schema conflicts, ingest S3 datasets, export Athena queries, or
download and process Sentinel-1 imagery through the same entry point.

## Setup

Run commands from the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m dredge --help
python -m dredge --doctor
```

On Windows, activate the virtual environment with `.venv\Scripts\Activate.ps1` in
PowerShell. If local configuration is needed, copy [`.env.dist`](.env.dist) to
`.env` and edit it. Shell variables take precedence; missing or empty `.env` files
are valid. See [configuration and doctor](docs/configuration.md) for details.

## Modules

Choose a workflow with `--module`, a source with `--mode`, and an explicit
`--action`. Each guide covers its options, credentials, examples, output contract,
and limitations.

| Module | Mode | Actions | Guide |
| --- | --- | --- | --- |
| `discover` | `s3` | `crawl` | [S3 schema discovery](docs/discover/s3.md): sampling, YAML contracts, JSON fallback, and combined discovery/resolution. |
| `discover` | `glue` | `catalogs`, `databases`, `tables`, `describe` | [Glue catalog discovery](docs/discover/glue.md): registered databases, tables, fields, and metadata reports. |
| `schema` | `s3` | `resolve` | [Schema resolution](docs/schema/resolve.md): type assertions, numeric normalization, and bounded validation. |
| `ingest` | `s3` | `get`, `plan` | [S3 ingestion](docs/ingest/s3.md): previews, partition/time plans, Parquet output, cancellation, and Python record consumption. |
| `ingest` | `athena` | `query` | [Athena exports](docs/ingest/athena.md): SELECT queries, CSV output, manifests, and notebooks. |
| `ingest` | `asf` | `get` | [ASF Sentinel-1 ingestion](docs/ingest/asf.md): catalog previews, downloads, optional HyP3 RTC, reference grids, and raster tiling. |

For S3 tabular data, start with [discovery](docs/discover/s3.md),
[resolve](docs/schema/resolve.md) any blocked fields, then
[preview and ingest](docs/ingest/s3.md). Glue catalogs, Athena exports, and ASF
imagery have their own workflows and artifact contracts.

ASF imagery is geographically independent. Its [processing template](examples/asf/processing.yaml)
and [optional study-area examples](examples/asf/study-areas.yaml) are explained in
the [ASF guide](docs/ingest/asf.md); examples do not define application defaults.

## Development

See [development and extension](docs/development.md) for the repository structure,
Python command interfaces, and instructions for adding modules, modes, and formats.
[ARCHITECTURE.md](ARCHITECTURE.md) defines canonical ownership and dependency
boundaries; [AGENTS.md](AGENTS.md) documents development conventions.

Run the offline test suite:

```bash
python -m unittest discover -s tests -v
```

Tests use temporary configuration, synthetic data, and injected collaborators;
they do not require local credentials, live downloads, or processing credits.
