"""Provider-neutral object-source contracts shared by modules and modes."""
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Protocol

from .exceptions import UserError


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    etag: str = ""
    modified: str = ""


class SourceAccessError(UserError):
    """Source-wide authentication failure that cannot be skipped per object."""


class ObjectSource(Protocol):
    def objects(self, prefix: str) -> Iterator[ObjectInfo]: ...
    def read_range(self, obj: ObjectInfo, start: int, end: int) -> bytes: ...



class DatasetSelection(Protocol):
    """Object selection needed by preview and record-consumption commands."""
    @property
    def prefix(self) -> str: ...

    def matches(self, obj: ObjectInfo) -> bool: ...
    def objects(self) -> Iterator[ObjectInfo]: ...
    def partition_values(self, obj: ObjectInfo) -> list[dict]: ...


class CountedSource:
    """Decorate source reads with terminal-independent transfer accounting."""
    def __init__(self, source: ObjectSource, on_read: Callable[[int], None]):
        self.source, self.on_read = source, on_read

    def objects(self, prefix: str) -> Iterator[ObjectInfo]:
        return self.source.objects(prefix)

    def read_range(self, obj: ObjectInfo, start: int, end: int) -> bytes:
        data = self.source.read_range(obj, start, end)
        self.on_read(len(data))
        return data
