# Development and extension

[Documentation home](../README.md)

- [Structure and extension](#structure-and-extension)
- [Run tests](#run-tests)

Run CLI examples from the repository root.

## Structure and extension

Dredge follows MLX's separation of declarative CLI options, lazy routing, typed
requests, thin runners, commands, and presentation. Modules own application
workflows; modes own source integrations reusable across modules.

```text
dredge/
  __main__.py             # Module entry point
  cli.py                  # Parse, validate, load environment, dispatch, exit codes
  cli_options.py          # Global option inventory
  cli_config.py           # Normalize parsed inputs
  cli_routing.py          # Immutable route descriptors and lazy lookup
  core/
    cli_options.py        # Lightweight immutable CliOption metadata
    sources.py            # ObjectInfo, ObjectSource, SourceAccessError
    schema_manifest.py    # Shared versioned contract validation
    partitioning.py       # Shared directory partition semantics
    ...                   # Configuration, errors, doctor, shared UI
  modules/
    discover/
      cli_options.py      # Discovery flags and validation
      requests.py         # CrawlOptions and CrawlRequest
      models.py           # DiscoveryReport
      runner.py           # Compose dependencies, execute, publish, present
      commands.py         # CrawlSchemas.execute()
      crawl_and_resolve.py # Compose crawl, rules, validation, and publication
      presentation.py     # Literal CLI summary
      catalog/            # Glue catalog discovery commands and presentation
      grouping.py         # Format detection and partition patterns
      readers.py          # Bounded schema inspection
      manifest.py         # Ingestion contract construction
      serialization.py    # Atomic YAML publication
    ingest/
      imagery/            # ASF ingestion, manifests, and optional RTC pipeline
      query/              # Athena query export workflow
      commands.py         # ReadDatasetRecords
      validation.py       # ValidateDatasetRecords (held-out checks)
      ingest_datasets.py  # IngestDatasets.execute(): coordinate materialization
      preview.py          # PreviewIngestion.execute(): metadata-only sizing
      writer.py           # Bounded Parquet batches and shard rotation
      notebook.py         # Two-cell pandas notebook generation
      publication.py      # Staged directory publication and cleanup
      serialization.py    # Versioned output manifests and safe dataset paths
      runner.py           # Compose source, selectors, and selected command
      presentation.py     # Rich progress and result tables
    schema/
      commands.py         # BuildResolvedSchema and standalone ResolveSchema
      rules.py            # Reusable rules against fresh discovery
      validation.py       # Bounded source validation and rule coverage
      requests.py         # ResolveRequest and ValidationLimits
      runner.py           # Lazy composition with the selected mode
      presentation.py     # Rich validation progress and summary
  modes/
    aws/                  # Shared AWS configuration and client lifecycle
    glue/                 # Registered catalog metadata
    athena/               # Query execution and result retrieval
    asf/                  # Sentinel-1 catalog and authenticated downloads
    hyp3/                 # Optional RTC service integration
    s3/
      cli_options.py      # S3 flags and validation
      configuration.py    # S3Settings
      clients.py          # AWS client creation and cleanup
      adapters.py         # S3Source
      datasets.py         # Execute manifest selectors
  formats/                # Shared parsers, format detection, schema/type helpers
    raster/               # Generic geometry, reference grids, alignment, and tiling
  features/discover/       # Compatibility re-exports only
tests/                    # Offline behavior and architecture tests
```

`module` identifies a use case, `mode` selects a source adapter, and `format`
selects a content reader. Commands accept injected collaborators, return structured
results, and work without CLI setup:

```python
from dredge.modules.discover.commands import CrawlSchemas
from dredge.modules.discover.requests import CrawlOptions

# source implements the core.sources.ObjectSource contract.
report = CrawlSchemas(source, CrawlOptions(bucket="example")).execute()
```

Existing `dredge.features.discover` imports remain compatibility re-exports of
the same objects. New code should use `modules`, `modes`, and `core.sources`.

To add a module, create its commands, typed requests/results, option inventory,
runner, and presentation as needed. Register a `RouteDescriptor` in
`cli_routing.py` for each supported module/mode combination. It declares actions,
a lazy runner reference, option inventories, and validation hooks. Parser choices,
help, and dispatch derive from descriptors; no feature branches are needed in
`cli.py`. Runners return structured results with a `partial` status for CLI exit
handling. Keep explicit action selection; there are no implicit default actions.

To add a source mode, implement its integration and settings under `modes/`, then
wire it into the owning module's runner and route descriptor. Reuse the same mode
from other modules. An API mode can define a capability-specific interface
rather than pretending to support S3 byte ranges. Neutral contracts belong in
`core` only when shared by modules and integrations. To add a file format, extend
shared format classification/parser and discovery inspection, returning the same recursive schema and
sampling coverage. Keep rendering in presentation modules and use `core.ui` for
literal output.

S3 ingestion reuses `modes.s3` and shared format contracts through injected
collaborators. Athena exports and ASF imagery have separate commands and output
contracts. ASF supports resumable downloads and optional raster processing; S3
Parquet ingestion and Athena exports require fresh output directories. See the
[module guides](../README.md#modules) for workflow-specific behavior.

See [ARCHITECTURE.md](../ARCHITECTURE.md) for dependency boundaries and extension
contracts, and [AGENTS.md](../AGENTS.md) for development conventions. Dependencies
are Rich for presentation, python-dotenv for configuration, Boto3 for AWS, PyYAML
for reports, ijson for streaming JSON arrays, and PyArrow for Parquet metadata, streaming reads, and dataset writing.
ASF uses asf_search, Shapely, PyProj, NumPy, and Rasterio; optional HyP3 integration
uses hyp3-sdk. Athena statement classification uses sqlparse.
Python 3.10+ is supported; pip selects compatible dependency releases.

## Run tests

```bash
python -m unittest discover -s tests -v
```

Tests use temporary configuration files and do not depend on your local `.env`.
