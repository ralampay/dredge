"""Terminal-independent query progress and repository boundary."""
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class QueryProgress:
    state: str
    query_id: str | None = None
    elapsed: float = 0
    bytes_scanned: int = 0
    bytes_written: int = 0
    detail: str = ''


class QueryRepository(Protocol):
    def prepare(self, workgroup, results_location): ...
    def start(self, sql, catalog, database, workgroup, configuration, token): ...
    def execution(self, query_id): ...
    def stop(self, query_id): ...
    def result_page(self, query_id, token=None, size=1000): ...
    def result_stream(self, location): ...
