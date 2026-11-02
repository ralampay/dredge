# Discover AWS Glue catalogs

[Documentation home](../../README.md) · [Configuration](../configuration.md)

Run CLI examples from the repository root.

Glue discovery reads registered metadata; it does not crawl object contents.
Use the account/profile and region containing your catalog:

```bash
python -m dredge --module discover --mode glue --action catalogs \
  --profile ateneo_research --region ap-northeast-2

python -m dredge --module discover --mode glue --action databases \
  --profile ateneo_research --region ap-northeast-2 --catalog-id CATALOG_ID

python -m dredge --module discover --mode glue --action tables \
  --profile ateneo_research --region ap-northeast-2 \
  --catalog-id CATALOG_ID --database DATABASE

python -m dredge --module discover --mode glue --action describe \
  --profile ateneo_research --region ap-northeast-2 \
  --catalog-id CATALOG_ID --database DATABASE --table TABLE \
  --output artifacts/table.glue.yml
```

`catalogs` includes the default catalog and nested catalogs visible in the chosen
account/region. With `--catalog-id`, it enumerates that parent's hierarchy.
For the other actions, omitting `--catalog-id` uses the default account catalog.
All listing actions paginate. Fields belong to **tables** within databases;
`describe` shows Glue column types, comments, partition keys, and storage location.
Nested Glue types are retained as declared type strings. Views may have no S3
location. Access depends on Glue and Lake Formation permissions.

Every action renders a Rich report. Optional `--output` saves a new YAML file with
`catalog_report_version: 1`, action, source context, timestamp, and metadata items.
This report describes registered Glue metadata; it is not a sampled/validated S3
schema manifest and is not accepted as `--schema` by S3 ingestion. Athena export
uses the catalog directly, without requiring this file. Older boto3 installations
without Glue `GetCatalogs` need an upgrade.

## Related guides

- [Export a query through Athena](../ingest/athena.md).
- [Discover schemas from S3 objects](s3.md).
