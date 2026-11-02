"""Preflight validation shared by previews and materialization."""
from collections.abc import Sequence

from ...core.exceptions import UserError
from ...core.sources import DatasetSelection
from ...formats.arrow_contract import output_schema


def select_datasets(manifest, bucket, ids=()):
    if manifest.document['source']['bucket'] != bucket:
        raise UserError('Schema source bucket does not match --bucket.')
    selected = [manifest.dataset(i) for i in dict.fromkeys(ids)] if ids else manifest.document['datasets']
    if not selected:
        raise UserError('Schema contains no datasets to ingest.')
    return selected


def blockers(dataset):
    issues = list(dataset['ingestion']['blockers'])
    if not issues:
        try:
            output_schema(dataset)
        except UserError as exc:
            issues.append(str(exc))
    return issues


def require_ready(datasets):
    problems = [(d['id'], blockers(d)) for d in datasets]
    messages = [f'{identity}: {", ".join(issues)}' for identity, issues in problems if issues]
    if messages:
        raise UserError('Datasets are not ingestion-ready: ' + '; '.join(messages))


def pair_selections(datasets: Sequence[dict], selections: Sequence[DatasetSelection]):
    """Validate parallel inputs before either workflow touches the source."""
    if not datasets or len(datasets) != len(selections):
        raise UserError('Each selected dataset requires exactly one source selector.')
    if len({dataset['id'] for dataset in datasets}) != len(datasets):
        raise UserError('Selected dataset IDs must be unique.')
    return tuple(zip(datasets, selections))
