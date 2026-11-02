# Export Athena queries

[Documentation home](../../README.md) · [Configuration](../configuration.md)

Run CLI examples from the repository root.

Install the updated dependencies with `python -m pip install -r requirements.txt`.
The additional `sqlparse` dependency classifies query statements before submission.
The wrapper accepts one `SELECT` or `WITH ... SELECT` statement, supplied through
exactly one of `--query` or `--query-file` (UTF-8). Athena performs SQL execution
validation. DDL, INSERT, CTAS, UNLOAD, and multiple statements are outside this
export action.

```bash
python -m dredge --module ingest --mode athena --action query \
  --profile ateneo_research --region ap-northeast-2 \
  --catalog AwsDataCatalog --database DATABASE --workgroup primary \
  --query-file queries/export.sql \
  --output artifacts/athena-export
```

For an inline query, replace `--query-file` with, for example:

```bash
--query 'SELECT * FROM "your_table" LIMIT 100'
```

Choose literals and types matching the table's Glue definition. A SQL row limit
limits returned rows; it is not a source-byte scan estimate.

`--catalog` is an **Athena catalog name**, defaulting to `AwsDataCatalog`; Glue's
`--catalog-id` is a different identifier. Use the Athena registration name when
querying another catalog. `--database` is required. `--workgroup` defaults to
`primary`, and `--query-timeout` defaults to 1800 seconds. No workgroups, buckets,
catalogs, or permissions are provisioned by this command.

The exporter reads the existing workgroup configuration. It supports either:

- Customer-owned S3 results: use the configured location, or supply
  `--results-location s3://YOUR-RESULTS-BUCKET/prefix/` when the workgroup permits
  an override. Enforced conflicting overrides are rejected. The actual location
  returned by Athena is used to stream the completed CSV in bounded chunks.
- Athena-managed results: omit `--results-location`. Results are fetched using
  paginated `GetQueryResults` and written incrementally as CSV, with one header.

The terminal shows the query ID, state, elapsed time, reported bytes scanned, and
exported bytes. This action executes a query; it does not support `--dry-run`.
Source-data access, workgroup, result retrieval, S3 and KMS permissions must be
available as applicable. Athena query costs depend on the workgroup's billing
configuration; the report exposes AWS's actual scan statistics rather than a cost
prediction.

The output directory must be new, and its parent must exist:

```text
athena-export/
  data.csv
  manifest.json
  preview.ipynb
```

Publication is staged: unsuccessful exports remove unpublished local output.
`manifest.json` uses `query_dataset_version: 1` and records SQL, execution ID,
catalog/database/workgroup, returned column metadata, execution statistics, result
location when available, CSV size and relative file paths. A streamed S3 export
leaves its row count null instead of rereading the entire CSV just to count rows.
Managed exports record the rows actually written. CSV empty values can represent
SQL NULL or empty strings; CSV does not preserve SQL types. Duplicate headers
are retained in the file and pandas may rename them.

The notebook contains exactly one Markdown cell documenting columns and one code
cell with a bounded preview:

```python
import pandas as pd

df = pd.read_csv("data.csv", nrows=5)
df
```

For larger processing, iterate `pd.read_csv("data.csv", chunksize=10000)`.
The notebook is generated, not executed automatically.

Ctrl+C and query-wait timeouts request `StopQueryExecution` for an active query
and briefly poll for a terminal state before local cleanup. Cancellation exits
with code 130; timeout/failure exits with code 1. If stopping cannot be confirmed,
the terminal reports the query ID and that it may still be running. If submission
was interrupted before a query ID arrived, check the workgroup's query history.
A query already completed is not stopped when cancelling its local download.
SDK request durations can extend the polling deadlines. Remote result files are
left to Athena/workgroup or S3 lifecycle retention, including after local failures.

See [Glue discovery](../discover/glue.md) to inspect registered databases, tables, and fields before querying.
