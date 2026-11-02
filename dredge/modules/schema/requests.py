"""Explicit resolution inputs and bounded validation settings."""
from dataclasses import dataclass

from ...core.exceptions import UserError


@dataclass(frozen=True)
class ValidationLimits:
    sample_files: int = 50
    sample_records: int = 1000
    sample_bytes: int = 8 * 1024 * 1024

    def validate(self):
        if any(type(v) is not int or v <= 0 for v in (self.sample_files, self.sample_records, self.sample_bytes)):
            raise UserError('Resolution validation limits must be positive integers.')


@dataclass(frozen=True)
class ResolveRequest:
    baseline: str
    schema: str
    output: str
    datasets: tuple[str, ...] = ()
    limits: ValidationLimits = ValidationLimits()

    @classmethod
    def from_config(cls, config):
        return cls(config['baseline'], config['schema'], config['output'], tuple(config.get('dataset', ())),
                   ValidationLimits(**{k: config[k] for k in ('sample_files', 'sample_records', 'sample_bytes') if k in config}))
