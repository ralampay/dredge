"""Terminal-independent workflow results and progress."""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class IngestProgress:
    objects: int = 0
    records: int = 0
    bytes_read: int = 0
    files: int = 0


@dataclass(frozen=True)
class IngestResult:
    document: dict
    output: str | None = None
    partial: bool = False


@dataclass
class PreviewDataset:
    id: str
    name: str
    blockers: list[str] = field(default_factory=list)
    objects: int = 0
    bytes: int = 0
    largest: int = 0


@dataclass(frozen=True)
class DatasetRecord:
    key: str
    etag: str
    partitions: list[dict]
    values: dict
