"""Compose ingestion workflows without provider operations or rendering logic."""


def run_ingestion(config):
    from ...core.cancellation import handle_interrupts
    from ...core.ui import announce_cancellation

    with handle_interrupts(announce_cancellation) as cancellation:
        result = dispatch_ingestion(config, cancellation.check)
        cancellation.check()
        return result


def dispatch_ingestion(config, check_cancel=None):
    from .plan_io import load_plan
    from .source_plan import SourcePlan
    from .requests import source_selection_requested

    plan = load_plan(config['plan']) if config.get('plan') else None
    if isinstance(plan, SourcePlan) or source_selection_requested(config):
        return run_source_selection(config, plan, check_cancel)
    if config.get('action') == 'plan':
        return run_plan(config)
    return run_schema_ingestion(config, plan, check_cancel)


def run_schema_ingestion(config, plan=None, check_cancel=None):
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

    from ...core.exceptions import UserError
    from .plan_ingestion import PlanIngestion

    request = IngestRequest.from_config(config)
    request.options.validate()
    if not request.schema:
        raise UserError('Version-1 time plans require --schema; use a version-2 source plan for schema-free previews.')
    manifest = SchemaManifest.load(request.schema)
    if plan is not None and request.datasets:
        raise UserError('A version-1 time plan selects its own dataset; remove --dataset.')
    datasets = select_datasets(manifest, config['bucket'], (plan.dataset_id,) if plan else request.datasets)
    if plan is not None:
        plan.validate(config['bucket'], datasets[0])
    if not request.dry_run:
        require_ready(datasets)
        validate_output(request.output)
    settings = S3Settings.from_config({**config, 'region': config.get('region') or manifest.document['source'].get('region')})
    with IngestionProgress(request.dry_run, metadata=bool(plan and plan.inspect_metadata)) as progress:
        with open_source(settings) as source:
            selections = [S3DatasetAdapter(source, dataset) for dataset in datasets]
            if request.dry_run:
                command = (PlanIngestion(source, datasets[0], selections[0], plan, on_progress=progress)
                           if plan else PreviewIngestion(source, datasets, selections, progress))
            else:
                command = IngestDatasets(manifest, source, datasets, selections, request.output,
                                         request.options, progress, plan=plan, check_cancel=check_cancel)
            result = command.execute()
    present(result)
    return result


def run_plan(config):
    from ...core.exceptions import UserError
    from ...core.schema_manifest import SchemaManifest
    from ...core.schema_io import validate_output
    from ...modes.s3.clients import open_source
    from ...modes.s3.configuration import S3Settings
    from ...modes.s3.datasets import S3DatasetAdapter
    from .filter_plan import FilterPlan
    from .plan_ingestion import InspectIngestionFields, PlanIngestion
    from .planning import select_datasets
    from .requests import PlanRequest
    from .presentation import IngestionProgress, present

    request = PlanRequest.from_config(config)
    manifest = SchemaManifest.load(request.schema)
    datasets = select_datasets(manifest, config['bucket'], request.datasets)
    if request.predicate is None:
        result = InspectIngestionFields(datasets).execute()
    else:
        if len(datasets) != 1 or request.output is None:
            raise UserError('Saving a plan requires one dataset and an output path.')
        plan = FilterPlan.create(config['bucket'], datasets[0], request.predicate, request.inspect_metadata)
        validate_output(request.output)
        settings = S3Settings.from_config({**config, 'region': config.get('region') or manifest.document['source'].get('region')})
        with IngestionProgress(True, metadata=plan.inspect_metadata) as progress:
            with open_source(settings) as source:
                result = PlanIngestion(source, datasets[0], S3DatasetAdapter(source, datasets[0]),
                                       plan, request.output, progress).execute()
    present(result)
    return result


def run_source_selection(config, plan=None, check_cancel=None):
    from ...core.exceptions import UserError
    from ...core.schema_manifest import SchemaManifest
    from ...core.schema_io import validate_output as validate_plan_output
    from ...modes.s3.clients import open_source
    from ...modes.s3.configuration import S3Settings
    from ...modes.s3.datasets import S3DatasetAdapter, S3ObjectSelection
    from .planning import select_datasets
    from .publication import validate_output
    from .requests import IngestRequest
    from .source_plan import SourcePlan
    from .source_selection import PreviewSourceSelection, IngestSelectedObjects
    from .presentation import IngestionProgress, present

    request = IngestRequest.from_config(config)
    request.options.validate()
    plan = plan or SourcePlan.from_config(config)
    plan.validate(config['bucket'])
    manifest = SchemaManifest.load(request.schema) if request.schema else None
    datasets = select_datasets(manifest, config['bucket'], request.datasets) if manifest else None
    preview = config.get('action') == 'plan' or request.dry_run
    save_output = request.output if config.get('action') == 'plan' else None
    if save_output is not None:
        validate_plan_output(save_output)
    if not preview:
        if manifest is None or request.output is None:
            raise UserError('Downloading requires --schema and --output.')
        validate_output(request.output)
    region = config.get('region') or (manifest.document['source'].get('region') if manifest else None)
    settings = S3Settings.from_config({**config, 'region': region})
    with IngestionProgress(preview) as progress:
        with open_source(settings) as source:
            selection = S3ObjectSelection(source, plan.prefix, plan.partitions)
            selections = [S3DatasetAdapter(source, d) for d in datasets] if datasets is not None else []
            command = (PreviewSourceSelection(selection, plan, datasets, selections, save_output, progress)
                       if preview else IngestSelectedObjects(manifest, source, datasets, selections, selection,
                                                            plan, request.output, request.options, progress, check_cancel=check_cancel))
            result = command.execute()
    present(result)
    return result
