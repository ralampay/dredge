"""Compose ingestion workflows without provider operations or rendering logic."""


def run_ingestion(config):
    from ...core.schema_manifest import SchemaManifest
    from ...modes.s3.clients import open_source
    from ...modes.s3.configuration import S3Settings
    from ...modes.s3.datasets import S3DatasetAdapter
    from .ingest_datasets import IngestDatasets
    from .planning import select_datasets, require_ready
    from .preview import PreviewIngestion
    from .publication import validate_output
    from .requests import IngestRequest
    from .presentation import IngestionProgress, present

    request = IngestRequest.from_config(config)
    request.options.validate()
    manifest = SchemaManifest.load(request.schema)
    datasets = select_datasets(manifest, config['bucket'], request.datasets)
    if not request.dry_run:
        require_ready(datasets)
        validate_output(request.output)
    settings = S3Settings.from_config({**config, 'region': config.get('region') or manifest.document['source'].get('region')})
    with IngestionProgress(request.dry_run) as progress:
        with open_source(settings) as source:
            selections = [S3DatasetAdapter(source, dataset) for dataset in datasets]
            command = (PreviewIngestion(source, datasets, selections, progress) if request.dry_run else
                       IngestDatasets(manifest, source, datasets, selections, request.output, request.options, progress))
            result = command.execute()
    present(result)
    return result
