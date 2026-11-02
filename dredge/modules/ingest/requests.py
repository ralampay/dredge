"""Typed ingestion configuration, independent of the CLI."""
from dataclasses import dataclass

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
    schema: str
    output: str
    datasets: tuple[str, ...] = ()
    dry_run: bool = False
    options: IngestOptions = IngestOptions()

    @classmethod
    def from_config(cls, config):
        return cls(config['schema'], config['output'], tuple(config.get('dataset', ())),
                   config.get('dry_run', False), IngestOptions(**{
                       key: config[key] for key in ('batch_rows', 'batch_bytes', 'file_bytes') if key in config}))
