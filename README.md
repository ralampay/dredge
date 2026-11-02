# Dredge

A toolkit for data ingestion and pipeline tasks. Run it from the repository root with `python -m dredge`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m dredge --help
python -m dredge --doctor
```

Edit `.env` to configure your application. On Windows, activate the virtual
environment with `.venv\Scripts\Activate.ps1` in PowerShell.
Initialize a Git repository with `git init` when ready.

## Discover S3 schemas

Install the requirements, configure AWS credentials, then run:

```bash
python -m dredge --module discover --mode s3 --action crawl \
  --profile some_profile --bucket some_bucket --output schema.yml
```

`--profile` refers to an AWS named profile, not a Dredge configuration profile.
Omit it to use Boto3's credential chain, including environment credentials and
workload roles. `--region` overrides the SDK's configured region. Credentials
are resolved only during execution, after loading `.env`; secrets are never
written to the report. AWS requests require `s3:ListBucket` and `s3:GetObject`,
plus `kms:Decrypt` when applicable. Archived objects must already be restored.

The crawler recursively paginates the selected bucket/prefix, samples objects,
and writes local YAML. It does not call AWS Glue or modify S3. It supports CSV,
JSON objects and arrays of objects, JSON Lines (`.jsonl` / `.ndjson`), gzip text,
Parquet, and S3 server-access logs. Parquet inspection reads the header and exact schema footer rather
than data pages. Externally gzipped and encrypted Parquet are not supported.

```bash
python -m dredge --module discover --mode s3 --action crawl \
  --bucket some_bucket --prefix events/ --exclude '*/temporary/*' \
  --sample-files 20 --sample-records 2000 --output schema.yml

python -m dredge --module discover --mode s3 --help
```

Quote globs so your shell does not expand them. Filters match full object keys,
are case-sensitive, and `*` can span `/`. Includes are ORed; excludes always win.
An S3 prefix is literal: use `events/` to avoid also matching `events-backup/`.
Listing still visits every object under the prefix; sampling limits reads, not
listing cost. Reads are sequential, with SDK retries and bounded network timeouts.

| Option | Default / meaning |
| --- | --- |
| `--bucket`, `--output` | Required bucket name and local YAML path; output parent must exist. |
| `--profile`, `--region` | Optional AWS overrides. |
| `--prefix` | Empty: recursively enumerate the whole bucket. |
| `--include`, `--exclude` | Repeatable full-key glob filters. |
| `--format` | `auto` by extension/native S3 log name; override with `csv`, `json`, `jsonl`, `parquet`, `s3-access-log`. |
| `--partition-style` | `auto`; alternatives `hive`, `none`. |
| `--sample-files` | 10 files per dataset, selected deterministically by key hash. |
| `--sample-records` | 1,000 complete text records per sampled file. |
| `--sample-bytes` | 8,388,608 transferred bytes per file, and independently the same decompressed-text budget. |
| `--csv-delimiter` | Comma; override with one character. |
| `--csv-header` | `present`; `absent` generates `column_1`, `column_2`, etc. |
| `--encoding` | `utf-8-sig`; accepts UTF-8 with or without a BOM. |
| `--on-error` | `fail`; `continue` records object failures and publishes a partial report. |
| `--overwrite` | Permit replacing an existing regular report file. |

Files are grouped by parent directory pattern, partition layout, and format.
For example, `events/year=2026/month=11/a.json` and
`events/year=2025/month=12/b.json` share a dataset. Automatic grouping also
recognizes valid `YYYY/MM/DD` and `YYYY-MM-DD` date directories. Other folders
remain separate. Use `--partition-style hive` to recognize only `key=value`
folders, or `none` to retain literal folders. Partition definitions include their
segment positions and layouts and remain separate from record fields. Compression
can vary within a dataset; gzip and uncompressed files of the same format merge.

Folder markers, zero-byte objects, and unsupported extensions are counted and
skipped. Native S3 access-log names ending in `YYYY-MM-DD-hh-mm-ss-<16 hex digits>`
are recognized automatically (including gzip); sampled content must parse successfully.
For logs with custom names, use `--format s3-access-log`. Use an explicit format
for other extensionless data. CSV uses explicit delimiter/header
settings, rejects duplicate or empty headers and uneven row widths, and preserves
leading-zero identifiers as strings. Empty cells infer null; date-looking text
stays string. JSON records must be objects. Schema inference unions fields,
tracks observed missing/null values, widens integers to floats when needed, and
records incompatible types as explicit alternatives rather than dropping them.

### YAML contract

The report contains `schema_version: 2`, `source`, effective `settings`, `status`,
`counts`, `diagnostics`, and sorted `datasets`. Each dataset includes:

- Stable ID, human-readable name, schema origin/fingerprint, normalized `location_pattern`, format, compression, and partitions.
- Recursive `schema`: `type`, `nullable`, `fields` with names, and list `items`.
  Conflicting types use `type: union` with `alternatives`. Unknown/null-only data
  remains `unknown`. Parquet preserves decimal precision/scale, timestamp units
  and timezone, and physical integer/float storage types where compatible.
- `matched_objects`, bounded `samples` with keys, ETags, modification timestamps,
  record/byte counts, inspection method, and reached limits.
- `sample_files_limited`, `conflicts` with JSON-pointer-style paths, and failures.

Each dataset's `ingestion` block contains a versioned source selector and parser,
`ready`, `blockers`, and coverage warnings. Parser settings are self-contained per
dataset, so consumers need not reconstruct them from CLI defaults. Selectors bind
to a bucket, prefix, include/exclude filters, format policy, and partition layout.
`modes.s3.datasets.S3DatasetAdapter` executes those selectors exactly; it does not
interpret the display path as a glob. `schema_fingerprint` binds the field schema
to its parser settings. Version-1 reports must be regenerated before consumption.
Paths are relative to the bucket, not to `--prefix`. Partition placeholders describe
locations; they are not S3 prefixes to submit directly to the API. No record values,
AWS profile, credentials, or local output path are serialized.

`complete` means the requested sampling run finished without object failures, not
that all records were inspected. Limits are conservative: a limit reached exactly
at EOF may still be reported. Unsampled objects and records can contain additional
fields or errors; observed nullability is not a constraint guarantee. A record
larger than the byte budget cannot be inferred; increase `--sample-bytes`.
For Parquet, the byte budget must fit the metadata. Data schema conflicts are
reported but do not change a successful exit code.

Default failures leave existing output untouched. `--on-error continue` skips
individual unreadable/malformed objects, writes `status: partial`, and exits 1.
Credential and listing failures always stop. Empty buckets produce a valid empty
report. Report publication uses a temporary sibling file and atomic publication;
existing files require `--overwrite`. Exit codes remain 0 for success/help, 1 for
runtime failures/partial reports, 2 for invalid arguments, and 130 for cancellation.

## Consume and validate a schema

S3 access logs use a declared format contract, including fields absent from older
records. Missing `-` values become null, numeric counters become integers, and
request timestamps become UTC-aware Python datetimes. URL-encoded keys/request URIs
remain unchanged. Optional trailing fields become null; fields beyond the known
layout are retained in `extra_fields`. The log object name's delivery date is not
misrepresented as a record partition or event timestamp. See the
[AWS field specification](https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html).

Discovery and consumption share parsers under `dredge.formats`. The ingestion
Python command reads records with source key/ETag and separate partition values:

```python
from dredge.core.schema_manifest import SchemaManifest
from dredge.modes.s3.clients import open_source
from dredge.modes.s3.configuration import S3Settings
from dredge.modes.s3.datasets import S3DatasetAdapter
from dredge.modules.ingest.commands import ReadDatasetRecords

manifest = SchemaManifest.load("schema.yml")
dataset = manifest.dataset(manifest.document["datasets"][0]["id"])
source_info = manifest.document["source"]
with open_source(S3Settings(source_info["bucket"], profile="some_profile",
                            region=source_info.get("region"))) as source:
    selector = S3DatasetAdapter(source, dataset)
    records = ReadDatasetRecords(dataset, source, selector).execute()
    try:
        for record in records:
            consume(record.values)  # Your downstream destination.
    finally:
        records.close()
```

`SchemaManifest.load` validates versions, parser policy, selectors, field structure,
readiness, and fingerprints. Unknown/union/unsupported types and sample failures
block ingestion. Each consumed record is checked for unexpected fields, missing
required fields, and incompatible types; consumers do not silently drop or coerce
drift. Typed Parquet date/decimal/timestamp/binary values are retained. Source
objects are checked against their listed ETag during range reads. This is not a
snapshot or an incremental-ingestion checkpoint: newly matching objects are read
on subsequent runs, and deleted/changed objects can fail a run.

For bounded, read-only validation against **held-out** objects:

```bash
python scripts/validate_s3_manifest.py --schema schema.yml \
  --profile some_profile --sample-files 50 --sample-records 1000 \
  --output validation.json
```

The script uses the manifest's bucket and region (`--region` can override), lists
matching objects, excludes all discovery sample keys, and deterministically selects
up to 50 others. It applies the manifest parsers and schema to up to 1,000 records
per object, with an 8 MiB byte budget. It writes counts, object references, limits,
and sanitized failures, never record values or credentials. No held-out objects
means `not_validated`, not success. Exit 0 requires all datasets to pass; failures
or insufficient evidence return 1. Explicit byte exhaustion fails validation.
Live runs are opt-in; ordinary unit tests use only synthetic fixtures and fakes.

Generated reports belong in the ignored `artifacts/` directory. The `ingest/get`
CLI and downstream sinks remain unimplemented; these reusable consumer commands
and adapters provide their tested foundation.

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
      presentation.py     # Literal CLI summary
      grouping.py         # Format detection and partition patterns
      readers.py          # Bounded schema inspection
      manifest.py         # Ingestion contract construction
      serialization.py    # Atomic YAML publication
    ingest/
      commands.py         # ReadDatasetRecords
      validation.py       # ValidateDatasetRecords (held-out checks)
  modes/
    s3/
      cli_options.py      # S3 flags and validation
      configuration.py    # S3Settings
      clients.py          # AWS client creation and cleanup
      adapters.py         # S3Source
      datasets.py         # Execute manifest selectors
  formats/                # Shared parsers, format detection, schema/type helpers
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
from other modules. A future API mode can define a capability-specific interface
rather than pretending to support S3 byte ranges. Neutral contracts belong in
`core` only when shared by modules and integrations. To add a file format, extend
shared format classification/parser and discovery inspection, returning the same recursive schema and
sampling coverage. Keep rendering in presentation modules and use `core.ui` for
literal output.

The planned future ingestion interface is:

```bash
python -m dredge --module ingest --mode s3 --action get \
  --profile some_profile --bucket some_bucket --output some_dir
```

The ingestion CLI is not implemented yet; it rejects this combination. The Python
record consumer and validation commands above are available now. When added,
its runner can reuse `modes.s3` and `core.sources` directly. The discovery report
can supply dataset selection and parser settings, but is not an object snapshot
or a guarantee of schema validity. Transformations, pipeline orchestration, resume,
concurrency, and API consumption remain future capabilities.

See [ARCHITECTURE.md](ARCHITECTURE.md) for dependency boundaries and extension
contracts, and [AGENTS.md](AGENTS.md) for development conventions. Dependencies
are Rich for presentation, python-dotenv for configuration, Boto3 for AWS, PyYAML
for reports, ijson for streaming JSON arrays, and PyArrow for Parquet metadata.
Python 3.10+ is supported; pip selects compatible dependency releases.

## Configuration and doctor

The application reads `.env` beside the `dredge/` directory, independently of
the current working directory. Python still needs to find the package: normally
run commands from the repository root, or add that root to `PYTHONPATH`.

Configuration is loaded explicitly during command execution, never when importing
a module. Existing shell variables take precedence, including empty strings.
Quoted values and comments use python-dotenv syntax; `${VARIABLE}` interpolation
is disabled, so such strings remain literal. No settings are required.

`--doctor` reports the resolved path, whether the file exists, and each file key's
effective value and source. It does not list unrelated shell variables. Missing
or empty files are valid. `KEY=` appears as `<empty>`; a bare `KEY` appears as
`<unset>` unless supplied by the shell. Bare keys are not added to the environment.

Values are masked when the key contains `SECRET`, `TOKEN`, `PASSWORD`, `PASSWD`,
`API_KEY`, `PRIVATE_KEY`, or `CREDENTIAL` (case-insensitive). This is name-based
masking, not automatic secret detection; use recognizable names for credentials.
Configuration text is displayed literally, including Rich markup characters.
Keep local values in the ignored `.env`; commit only harmless examples in `.env.dist`.

No arguments or `--help` prints usage and exits with status 0. Doctor also returns
0 for missing configuration, 1 for an unreadable file, and 130 for cancellation.
Invalid CLI arguments return 2.

## Run tests

```bash
python -m unittest discover -s tests -v
```

Tests use temporary configuration files and do not depend on your local `.env`.
