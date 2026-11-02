"""Browse Glue metadata through an injected catalog repository."""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from ....core.schema_io import validate_output, write_document


class CatalogRepository(Protocol):
    def catalogs(self, catalog_id=None): ...
    def databases(self, catalog_id=None): ...
    def tables(self, catalog_id, database): ...
    def describe(self, catalog_id, database, table): ...


@dataclass(frozen=True)
class CatalogReport:
    document: dict
    output: str | None = None
    partial: bool = False


class BrowseCatalog:
    def __init__(self, repository: CatalogRepository, request):
        self.repository, self.request = repository, request

    def execute(self):
        request = self.request
        request.validate()
        if request.output:
            validate_output(request.output)
        if request.action == 'catalogs':
            items = list(self.repository.catalogs(request.catalog_id))
        elif request.action == 'databases':
            items = list(self.repository.databases(request.catalog_id))
        elif request.action == 'tables':
            items = list(self.repository.tables(request.catalog_id, request.database))
        else:
            items = [self.repository.describe(request.catalog_id, request.database, request.table)]
        document = {'catalog_report_version': 1, 'action': request.action,
                    'created_at': datetime.now(timezone.utc).isoformat(),
                    'source': {'mode': 'glue', 'region': request.region, 'catalog_id': request.catalog_id,
                               'database': request.database, 'table': request.table}, 'items': items}
        if request.output:
            write_document(document, request.output)
        return CatalogReport(document, request.output)
