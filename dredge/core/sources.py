"""Provider-neutral object-source contracts shared by modules and modes."""
from collections.abc import Iterator
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


