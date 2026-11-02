# Resolve S3 schemas

[Documentation home](../../README.md) · [Configuration](../configuration.md)

Run CLI examples from the repository root.

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

## Type assertions

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

## Numeric normalization

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

## Mandatory bounded validation

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

## Related guides

- [Create a discovery baseline](../discover/s3.md).
- [Discover and resolve in one command](../discover/s3.md#discover-and-resolve-in-one-command).
- [Ingest a resolved schema](../ingest/s3.md).
