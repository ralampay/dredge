"""Compose schema resolution without importing providers at import time."""


def run_resolution(config):
    from ...core.schema_manifest import SchemaManifest
    from ...core.resolution_contract import compile_draft
    from ...core.schema_io import validate_output
    from ...modes.s3.clients import open_source
    from ...modes.s3.configuration import S3Settings
    from ...modes.s3.datasets import S3DatasetAdapter
    from .commands import ResolveSchema, check_bucket
    from .requests import ResolveRequest
    from .serialization import load_draft
    from .presentation import ResolutionProgressDisplay, present

    request = ResolveRequest.from_config(config)
    request.limits.validate()
    baseline = SchemaManifest.load(request.baseline)
    check_bucket(baseline, config['bucket'])
    draft = load_draft(request.schema)
    compile_draft(baseline.document, draft, request.datasets)
    validate_output(request.output)
    settings = S3Settings(config['bucket'], config.get('profile'), config.get('region') or baseline.document['source'].get('region'))
    with ResolutionProgressDisplay() as progress:
        with open_source(settings) as source:
            result = ResolveSchema(baseline, draft, source, S3DatasetAdapter, request.output,
                                   dataset_ids=request.datasets, limits=request.limits, on_progress=progress).execute()
    present(result, request.output)
    return result
