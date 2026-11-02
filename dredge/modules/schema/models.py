"""Resolution results and terminal-independent validation progress."""
from dataclasses import dataclass


@dataclass(frozen=True)
class ResolutionProgress:
    dataset_id: str
    objects: int
    records: int


@dataclass(frozen=True)
class ResolutionResult:
    document: dict
    partial: bool = False
