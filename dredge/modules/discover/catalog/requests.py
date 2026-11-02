"""Catalog browsing inputs, independent of providers and CLI parsing."""
from dataclasses import dataclass
from ....core.exceptions import UserError


@dataclass(frozen=True)
class CatalogRequest:
    action: str
    catalog_id: str | None = None
    database: str | None = None
    table: str | None = None
    output: str | None = None
    region: str | None = None

    @classmethod
    def from_config(cls, config):
        return cls(**{k: config[k] for k in cls.__dataclass_fields__ if k in config})

    def validate(self):
        if self.action not in ('catalogs', 'databases', 'tables', 'describe'):
            raise UserError('Unsupported Glue discovery action.')
        if self.action in ('tables', 'describe') and not self.database:
            raise UserError('Glue table discovery requires a database.')
        if self.action != 'describe' and self.table:
            raise UserError('A table name applies only to Glue describe.')
        if self.action in ('catalogs', 'databases') and self.database:
            raise UserError('A database name applies only to Glue tables/describe.')
        if self.action == 'describe' and not self.table:
            raise UserError('Glue describe requires a table.')
