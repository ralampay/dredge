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
| `--csv-header` | `auto`; override with `present` or `absent`. Headerless data uses `column_1`, etc. |
| `--temporal-types` | `auto` infers strict ISO dates and timezone-aware timestamps; `off` keeps strings. |
| `--encoding` | `utf-8-sig`; accepts UTF-8 with or without a BOM. |
| `--on-error` | `fail`; `continue` records object failures and publishes a partial report. |
| `--overwrite` | Permit replacing an existing regular report file. |
| `--report-fields` | Show 20 field entries per dataset; use a positive count or `all`. Affects the terminal report only. |

Rich progress separates listing from schema inspection. Listing shows objects found
so far because S3 does not provide a total up front. Once listing completes, the
inspection bar shows completed/selected files, files remaining, transferred bytes,
and an estimated time remaining. Sampling limits apply per dataset; this is not a
full scan of every record. Redirected output contains phase summaries without an
animated display.

The compact report shows field names, assumed types, nullability, header/source
origins, and ingestion blockers. Unknown or conflicting fields appear first when
the report is truncated. Nested fields use dotted paths. The YAML always contains
the complete contract regardless of `--report-fields`.

For the Canvas bucket (create the output directory first):

```bash
mkdir -p artifacts
python -m dredge --module discover --mode s3 --action crawl \
  --profile ateneo_research --region ap-northeast-2 \
  --bucket admu-canvas-data --sample-files 10 --sample-records 1000 \
  --report-fields 20 --output artifacts/admu-canvas-data.schema.yml --overwrite
```

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
leading-zero identifiers as strings. Empty cells infer null. Valid ISO dates and ISO timestamps with explicit offsets
can be assumed temporal types; ambiguous date text stays string. JSON records must be objects. Schema inference unions fields,
tracks observed missing/null values, widens integers to floats when needed, and
records incompatible types as explicit alternatives rather than dropping them.

### YAML contract

The report contains `schema_version: 3`, `source`, effective `settings`, `status`,
`counts`, `diagnostics`, and sorted `datasets`. Dataset contracts precede crawl settings, counts, and diagnostics. Each dataset includes:

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
interpret the display path as a glob. `schema_fingerprint` binds the executable field schema
to its parser settings; version 3 excludes source annotations and observational
counters so increased sample evidence alone does not change it. Version-2 reports remain readable with their existing semantics; version-1 reports
must be regenerated before consumption.
Paths are relative to the bucket, not to `--prefix`. Partition placeholders describe
locations; they are not S3 prefixes to submit directly to the API. No record values,
AWS profile, credentials, or local output path are serialized.

Fields appear in source order: CSV/Parquet columns, known log positions, or
first-observed JSON key order, with newly observed fields appended deterministically.
`schema.fields` is the single authoritative field list. For example, a field in
the assumed extraction contract can look like:

```yaml
name: created_at
type: timestamp
nullable: true
unit: us
timezone: UTC
input_format: iso-timestamp-offset
source:
  kind: csv-header
  path: /created_at
  positions: [2]
inference:
  origin: sample-inference
  present_count: 100
  null_count: 2
  missing_count: 0
  observed_types: {'null': 2, string: 98}
  assumption: all_observed_non_null_strings_match_iso-timestamp-offset
```

Source positions are zero-based. CSV sources also record `assumed: true` when
header presence was automatically inferred, or `false` for an explicit override. `source.kind` distinguishes actual CSV headers,
generated positions, JSON keys, Parquet fields, and known-format fields. Text
inference counts describe only processed sample records: `present_count` includes
nulls, `null_count` records explicit null values, and `missing_count` counts absent
fields relative to observed parent objects. Array-item counts refer to sampled
items. Parquet metadata has no sampled record counts. S3 log types remain
format-defined even when a field was null throughout the sample.

CSV `auto` inspects up to 20 complete initial rows (bounded by the record budget).
It requires unique label-like first-row values followed by stable typed columns
to identify a header; consistent typed initial rows indicate headerless data.
All-string and single-row samples are ambiguous: the first row is retained as data,
columns get generated names, and ingestion is blocked until an explicit
`--csv-header present|absent` override resolves the assumption. Header modes must
agree across sampled objects; header order may differ when names identify columns.
An explicit header-only CSV still reports its column names with unknown types.

Temporal auto-inference requires every observed non-null string in a field to
match a valid `YYYY-MM-DD` date or an ISO timestamp with `T`, seconds, an explicit
`Z`/offset, and at most six fractional digits. Mixed temporal-looking and other
strings remain strings with a diagnostic. Timezone-free timestamps, ambiguous
regional dates, leading-zero identifiers, and numeric JSON strings stay strings.
Use `--temporal-types off` for date-shaped identifiers. Ingestion applies only
recorded `input_format` conversions, normalizes offset timestamps to UTC, and
rejects later incompatible values as drift.

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

## Discover with automatic JSON fallback

Use `--unresolved-as json` to prepare a usable schema in one command without a
rules file or manual type guesses:

```bash
python -m dredge --module discover --mode s3 --action crawl \
  --profile ateneo_research --region ap-northeast-2 \
  --bucket admu-canvas-data \
  --sample-files 10 --sample-records 1000 --report-fields all \
  --unresolved-as json \
  --output artifacts/admu-canvas-data.schema.yml --overwrite
```

Fields with clear types keep their native types. Unknown or conflicting fields
become JSON-encoded string columns. Empty structs also use this fallback because
Parquet cannot represent a struct with no fields. If an array contains an unresolved element
schema anywhere inside it, the **entire containing array** is encoded; sibling
fields outside that array stay native. Nested structs retain their structure
when only individual child fields need fallback. The Rich field report labels
converted fields `JSON-encoded`.

For the saved Canvas schema this converts `body_attachment_ids`, `body_score`,
and empty structs such as `body` and `body_integration_data`:

| Source value | Stored Parquet value |
| --- | --- |
| `[]` | String containing `[]` |
| `{}` | String containing `{}` |
| `[123,456]` | String containing `[123,456]` |
| `42.5` | String containing `42.5` |
| `{"double":42.5,"int":null}` | String containing that JSON object |
| `"text"` | String containing `"text"`, including JSON quotes |
| `null` | Native null, not the string `"null"` |

The version-4 schema records explicit `encode_json` operations, the original
discovery baseline, and bounded validation results. Ingestion applies the
operations automatically before validating output records; no ingestion flag is
needed. Clear fields still enforce their inferred types and nullability. JSON
fallback fields intentionally accept future JSON-compatible shapes, including
populated arrays after empty-array discovery. New fields outside fallback values,
malformed records, parser ambiguities, source failures, and unrelated discovery
blockers still fail normally.

Encoding is deterministic, with sorted object keys. Decimal values supplied by
the parser are written without additional float rounding; existing JSONL numeric
parsing semantics remain unchanged. Binary data, native
Python/Parquet dates and timestamps, non-string object keys, and non-finite
numbers inside a fallback value are rejected rather than silently stringified.
Native dates outside fallback fields remain typed normally. JSON fallback preserves
JSON values, not original whitespace or Python-specific types.

Validation remains mandatory and bounded, using `--resolve-sample-files` (50),
`--resolve-sample-records` (1,000), and `--resolve-sample-bytes` (8 MiB per file).
These limits are separate from discovery's `--sample-*` limits. Empty arrays,
nulls, and absent nullable fields can validate successfully because JSON encoding
does not assert an unknown source type. Other assertion/normalization operations
still require meaningful non-null evidence. Failed runs preserve existing final
output even with `--overwrite`. Optional `--discovery-output` saves a separate raw
baseline before validation.

This policy is opt-in. Without it, unresolved fields remain blocked as before.
`--unresolved-as json` cannot be combined with `--resolve-with`; choose automatic
JSON fallback or explicit rules for a run. Numeric operations and nested-field
filtering on encoded columns require decoding first. Output manifests and the
two-cell notebooks document the encoding. For a top-level JSON column:

```python
import json
import pandas as pd

# df is the DataFrame loaded from an ingested dataset directory.
df["body_score"] = df["body_score"].map(
    lambda value: json.loads(value) if pd.notna(value) else None
)
```

Use `json.loads(value, parse_float=Decimal)` with `from decimal import Decimal`
when downstream processing needs exact JSON decimal numbers.

### Unexpected fields during validation

`--unresolved-as json` handles fields already present in discovery whose types are
unknown or conflicting. It does not absorb additional field names outside the
encoded values. Validation reports those names and their structural path without
printing record values. Increase `--sample-files` to cover more objects, or
`--sample-records` when the extra fields occur later within files. Rerunning the
same settings selects the same objects when the source is unchanged.

For the observed Canvas error involving `metadata_real_user_id`, the failing JSON
object ranked 155th in discovery's deterministic ordering among 310 matching JSON
objects. A crawl with `--sample-files 310 --sample-records 1000` covers those JSON
objects at the current inventory size; the file limit applies per dataset, so it
also samples up to 310 Parquet files. Broader sampling does not guarantee that
later records or future objects conform. Full ingestion continues to validate
records and never silently drops additional fields. A failed combined crawl
preserves the existing final schema.

## Discover and resolve in one command

Add `--resolve-with` to crawl with reusable, explicit rules. The command discovers
a fresh baseline, applies the rules, validates bounded source samples, and writes
an ingestion-ready version-4 schema only when every dataset is ready.

```bash
python -m dredge --module discover --mode s3 --action crawl \
  --profile ateneo_research --region ap-northeast-2 \
  --bucket admu-canvas-data \
  --sample-files 10 --sample-records 1000 --report-fields all \
  --resolve-with resolutions.yml \
  --discovery-output artifacts/admu-canvas-data.discovery.yml \
  --output artifacts/admu-canvas-data.schema.yml --overwrite
```

Create `resolutions.yml` using IDs from discovery. Here is an example rule set;
**the attachment element type below is illustrative and must be confirmed**:

```yaml
rules_version: 1
datasets:
  - id: "<JSON dataset ID>"
    rules:
      - op: assert_type
        path:
          - field: body_attachment_ids
          - items: true
        schema:
          type: integer
  - id: "<Parquet dataset ID>"
    rules:
      - op: extract_number
        path:
          - field: body_score
        members: [double, int]
```

`assert_type.schema` supplies the replacement type and any required type parameters.
Do not include the target's `name` or `nullable`: those are preserved. Newly
introduced nested fields and list items require their own complete schemas,
including names where applicable and nullability. `extract_number` uses the same
strict numeric rules described below. The rules file contains no parser settings,
selectors, fingerprints, or discovery evidence, so it can be reused after crawling
again. It is a different format from the edited discovery YAML accepted by the
standalone `schema resolve` command.

A rule whose target already has a compatible resolved type is reported as
`satisfied`; it adds no transformation. Explicitly supplied type parameters must
match. Changed dataset IDs, missing paths, incompatible types, overlapping rules,
and unsupported attributes fail rather than silently being ignored. Datasets
without rules must already be ingestion-ready. An empty rules file is rejected.

| Combined-crawl option | Meaning |
| --- | --- |
| `--resolve-with` | Rules YAML enabling discovery plus resolution. |
| `--discovery-output` | Optional raw discovery YAML, published before resolution. Requires `--resolve-with` or `--unresolved-as json`. |
| `--resolve-sample-files` | Validation files per rule-targeted dataset; default 50. |
| `--resolve-sample-records` | Validation records per file; default 1,000. |
| `--resolve-sample-bytes` | Validation transferred/decompressed byte budget per file; default 8,388,608. |

The existing `--sample-*` flags control discovery; `--resolve-sample-*` control
subsequent validation and require `--resolve-with` or `--unresolved-as json`. Validation prefers held-out
objects and fills remaining slots with discovery-sampled objects. Insufficient
non-null coverage for an applied resolution is still an error. Increasing
discovery sampling does not remove this validation requirement.

`--overwrite` allows replacing either output, but the final output is replaced
**only after all resolution and validation succeed**. On failure, cancellation,
partial discovery, or remaining blockers, the previous final output stays intact
and the command exits unsuccessfully. The optional raw discovery copy remains
available for diagnosis if discovery completed and published it. Both output
paths must differ from each other and from the rules file. Without
`--discovery-output`, the fresh baseline is preserved inside the successful
version-4 artifact only. Plain crawl without `--resolve-with` is unchanged.

## Resolve unknown or conflicting types

Use an **unedited discovery baseline** and an edited copy. The resolver accepts
original discovery versions 2 and 3 and publishes a validated version-4 contract.
It never guesses a type, clears unrelated discovery failures, or modifies the
baseline. It does not accept a previously resolved contract as a new baseline.

```bash
cp artifacts/admu-canvas-data.schema.yml artifacts/admu-canvas-data.edited.yml
# Edit unresolved field schemas in the copy, as described below.
python -m dredge --module schema --mode s3 --action resolve \
  --profile ateneo_research --region ap-northeast-2 \
  --bucket admu-canvas-data \
  --baseline artifacts/admu-canvas-data.schema.yml \
  --schema artifacts/admu-canvas-data.edited.yml \
  --output artifacts/admu-canvas-data.resolved.yml
```

The output path must be new. To resolve only one dataset, add `--dataset ID`
(repeatable) and edit only those datasets. Unselected datasets retain their source
schemas and blockers. Use the same selection when ingesting if others remain
blocked. No `--overwrite`, `--prefix`, `--dry-run`, or validation-bypass flag is
accepted for resolution.

### Type assertions

For an empty-array unknown such as `body_attachment_ids`, change its `items.type`
from `unknown` to an explicit supported type. For example, `integer` is appropriate
**only after confirming the elements really are integers**. Keep names, field
order, nullability, and discovery evidence unchanged. Do not manually edit
`ready`, `blockers`, `schema_fingerprint`, parser settings, selectors, or samples.
The resolver generates an `assert_type` operation from the edited unknown node;
it does not convert source values. Explicitly declaring the operation is also
supported, using the typed path syntax below.

At an edited unresolved node, remove attributes that do not belong to the new
type, such as `alternatives` when replacing a union. Existing `inference` and
`source` annotations may remain unchanged in the draft. The resolver removes old
inference annotations from changed output nodes and retains the full original
evidence in the embedded baseline. Assertions may use nested supported schemas,
but every node must declare its type and nullability; implicit conversions such
as newly added `input_format` rules are not accepted.

### Numeric normalization

For the mixed numeric/struct `body_score` field, set its type to `float`, remove
its `alternatives`, preserve its existing nullability, and add this **dataset-level**
entry alongside `schema` and `ingestion`:

```yaml
resolutions:
  - op: extract_number
    path:
      - field: body_score
    members: [double, int]
```

The transformation accepts a numeric scalar or a wrapper containing exactly one
non-null configured member. It rejects multiple populated members even when their
values are equal, all-null wrappers, unconfigured members, strings, booleans,
non-finite values, and conversions that lose numeric precision. An actual null
remains null only where the field was already nullable. Output is float64.

Paths use literal segments, so field names containing dots, slashes, or asterisks
are unambiguous. For an array element, the optional explicit assertion looks like:

```yaml
resolutions:
  - op: assert_type
    path:
      - field: body_attachment_ids
      - items: true
```

Only edits at original `unknown` or `union` nodes are accepted. Duplicate,
overlapping, unused, and arbitrary-code rules are rejected. Numeric extraction
can resolve numeric scalars and numeric-wrapper structs, including nullable
unknown members representing observed nulls; it cannot resolve arbitrary unions.

### Mandatory bounded validation

Before publication, the resolver lists matching objects, prefers deterministic
held-out samples, and fills unused sample slots with discovery-sampled objects.
Reuse is recorded in the validation report. Defaults are 50 files per selected
dataset, 1,000 records per file, and an 8 MiB transferred/decompressed byte budget
per file. Override with `--sample-files`, `--sample-records`, and `--sample-bytes`.
These limits bound content reads, not listing requests.

Every selected dataset must validate successfully, and every resolution must
encounter a meaningful non-null value. Empty arrays or all-null values alone do
not establish the asserted type. Insufficient coverage, schema drift, ambiguous
numeric wrappers, unsafe Parquet conversions, and byte exhaustion prevent
publication. Increase sample limits or provide representative data when evidence
is insufficient. Errors identify source objects and field paths where available
without printing record values.

The version-4 artifact contains the original baseline, its SHA-256 digest,
compiled operations, regenerated executable fingerprints, and per-dataset
validation limits, object references, reuse flags, record counts, and rule
coverage. Fingerprints include transformations as well as schema/parser settings;
the loader verifies validation bindings and permitted changes against the baseline.
These are consistency checks, not cryptographic proof that a third-party artifact
was validated. Use contracts from a trusted source.

Then ingest with `--schema artifacts/admu-canvas-data.resolved.yml`. Transformations
run before record validation and are documented in output manifests and notebook
Markdown cells. Ingestion still checks every record; sample validation cannot
guarantee that all unsampled data conforms. Ingestion's metadata-only `--dry-run`
remains available and downloads no records.

## Ingest a dataset

Discovery YAML is the input contract for ingestion. Preview the source volume first:

```bash
python -m dredge --module ingest --mode s3 --action get \
  --profile some_profile --bucket some_bucket \
  --schema schema.yml --output datasets/run-001 --dry-run
```

Remove `--dry-run` to create the dataset. The output parent must already exist;
the output directory itself must be new. Ingestion never overwrites a completed
run. `--region` overrides the schema's region, with AWS SDK defaults used when
neither supplies one. Credentials are resolved through the selected profile.

| Option | Default / meaning |
| --- | --- |
| `--schema` | Required discovery YAML (version 2/3) or resolved YAML (version 4). |
| `--output` | Required new local dataset collection directory. |
| `--dataset` | Repeatable dataset ID; otherwise ingest all datasets in the schema. |
| `--dry-run` | List matching object counts, stored bytes, largest objects, and known blockers. |
| `--batch-rows` | 10,000 records per Arrow write batch. |
| `--batch-bytes` | 16,777,216 estimated Python record-payload bytes per batch. |
| `--file-bytes` | 134,217,728 uncompressed Arrow bytes per Parquet shard, approximately. |

Dry-run only lists object metadata; it makes no object-content requests and writes
no files, including notebooks. It can estimate volume for a blocked schema and
returns 1 when selected datasets are blocked. Malformed schemas and bucket
mismatches fail before listing. Stored source size includes compression and is
not a prediction of peak memory, Parquet output size, or exact network transfer.
Listings make AWS requests, and a later run may see different objects.

Actual ingestion reads **all matching objects and records**, not just discovery
samples. Source filters, grouping, and parser settings come from the contract;
`--prefix` and discovery sampling flags cannot override them. Blocked schemas,
unknown fields, invalid nulls/types, conversion overflow, and changed or unreadable
objects fail the run. Select specific ready datasets with `--dataset` if other
datasets remain blocked. Output is staged beside the destination, published only
when complete, and removed on handled failure or cancellation.

### Output contract

```text
run-001/
  manifest.json
  schema.yaml
  datasets/
    <dataset-id>/
      preview.ipynb
      data/
        part-000000.parquet
        part-000001.parquet
```

`manifest.json` has `dataset_version: 1`, completion status, UTC creation time,
source provenance, effective sizing settings, and a digest of the validated input
schema. Its `schema_digest_encoding` specifies the digest serialization. Each
dataset entry records its ID/name, `path`, `data_path`, `notebook`, schema
fingerprint, output `fields`, partition-column mappings, row/byte totals, and file
inventory. Dataset paths are relative to the output root; file inventory paths
are relative to that dataset's `path`. Consumers should use these recorded paths.
`schema.yaml` retains the validated discovery contract. This output is a completed
run, not an incremental checkpoint or a transactional snapshot of the S3 bucket.

Parquet files use Snappy compression and explicit schemas with preserved field
order, nested structs/lists, decimal precision, and timestamp units/timezones.
Source partitions become string columns named `__dredge_partition_<segment>`;
collisions with source fields fail preflight. Shards use a flat directory to avoid
creating many small partition files. Small datasets use one shard; empty datasets
still have an empty schema-bearing file. File size targets apply between batches,
so a single batch can exceed the target. Source records are never deduplicated.

Each dataset's notebook has exactly one Markdown cell documenting its headers,
types, nullability, nested paths, and partition columns, plus one code cell:

```python
import pandas as pd

df = pd.read_parquet("data", engine="pyarrow")
df.head()
```

Run the notebook from its containing directory. It reads all shards, including
records from multiple source partitions, and displays the first five rows. The
notebook is generated unexecuted. Install `pandas` and Jupyter separately for
notebook use; ingestion itself does not depend on either.

### Memory-efficient processing

Ingestion streams records and flushes batches when either row or estimated-byte
limits are reached. Only one output dataset writer is active. Memory does not
scale with total row count, but the limits are not hard RSS caps: a large individual
record, parser buffers, and Arrow conversion buffers can exceed them. Ingestion
uses a 1 MiB source read buffer to amortize S3 range requests.

The notebook's simple pandas call loads the full dataset. For large datasets,
read Arrow batches and convert each batch independently:

```python
import json
from pathlib import Path
import pyarrow.dataset as ds

root = Path("datasets/run-001")
manifest = json.loads((root / "manifest.json").read_text())
entry = manifest["datasets"][0]  # Select the desired dataset by ID in production.
dataset = ds.dataset(root / entry["data_path"], format="parquet")
for batch in dataset.to_batches(batch_size=10_000):
    frame = batch.to_pandas()
    # Process or persist this frame before reading the next batch.
    print(frame.head())
```

Select needed columns and filters with Arrow's scanner for less downstream work.
Avoid collecting or concatenating all batches when the dataset exceeds memory.

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
CLI materializes these validated records as local Parquet datasets; other
downstream sinks can reuse the record-consumption command.

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
      grouping.py         # Format detection and partition patterns
      readers.py          # Bounded schema inspection
      manifest.py         # Ingestion contract construction
      serialization.py    # Atomic YAML publication
    ingest/
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

Ingestion reuses `modes.s3` and shared format contracts through injected
collaborators. Transformations, pipeline orchestration, incremental ingestion,
resume, concurrency, and API consumption remain future capabilities.

See [ARCHITECTURE.md](ARCHITECTURE.md) for dependency boundaries and extension
contracts, and [AGENTS.md](AGENTS.md) for development conventions. Dependencies
are Rich for presentation, python-dotenv for configuration, Boto3 for AWS, PyYAML
for reports, ijson for streaming JSON arrays, and PyArrow for Parquet metadata, streaming reads, and dataset writing.
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
