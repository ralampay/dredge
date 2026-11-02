"""Typed ingestion configuration, independent of the CLI."""
from dataclasses import dataclass

from .time_filters import TimeFilter

from ...core.exceptions import UserError


@dataclass(frozen=True)
class IngestOptions:
    batch_rows: int = 10_000
    batch_bytes: int = 16 * 1024 * 1024
    file_bytes: int = 128 * 1024 * 1024

    def validate(self):
        if any(type(v) is not int or v <= 0 for v in (self.batch_rows, self.batch_bytes, self.file_bytes)):
            raise UserError('Batch and file limits must be positive integers.')


@dataclass(frozen=True)
class IngestRequest:
    schema: str | None
    output: str | None
    datasets: tuple[str, ...] = ()
    dry_run: bool = False
    options: IngestOptions = IngestOptions()
    plan: str | None = None

    @classmethod
    def from_config(cls, config):
        return cls(config.get('schema'), config.get('output'), tuple(config.get('dataset', ())),
                   config.get('dry_run', False), IngestOptions(**{
                       key: config[key] for key in ('batch_rows', 'batch_bytes', 'file_bytes') if key in config}), config.get('plan'))


@dataclass(frozen=True)
class PlanRequest:
    schema: str
    datasets: tuple[str, ...]
    output: str | None
    predicate: TimeFilter | None
    inspect_metadata: bool = False

    @classmethod
    def from_config(cls, config):
        field = config.get('partition_date') or config.get('time_field')
        predicate = (TimeFilter('partition' if config.get('partition_date') else 'record',
                                field, config['time_from'], config['time_to'], config.get('timezone', 'UTC'))
                     if field else None)
        return cls(config['schema'], tuple(config.get('dataset', ())), config.get('output'),
                   predicate, config.get('inspect_parquet_metadata', False))


def source_selection_requested(config):
    """Choose the schema-independent workflow without reading files or providers."""
    if config.get('plan'):
        return False  # The saved plan version determines the workflow.
    if config.get('prefix') is not None or config.get('partition'):
        return True
    if config.get('time_field') or config.get('inspect_parquet_metadata'):
        return False
    if config.get('partition_date'):
        return config.get('action') == 'get' or not config.get('schema') or not config.get('dataset')
    if config.get('action') == 'plan':
        return not config.get('schema') or bool(config.get('output'))
    return not config.get('schema')
