"""Validated SQL export requests; SQL is passed unchanged to Athena."""
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from ....core.exceptions import UserError


def validate_sql(sql):
    import sqlparse
    from sqlparse import tokens as T
    if not isinstance(sql, str) or not sql.strip() or len(sql) > 262144:
        raise UserError('SQL must contain one SELECT statement, at most 262144 characters.')
    try:
        statements = [s for s in sqlparse.parse(sql) if s.get_type() != 'UNKNOWN' or
                      any(not t.is_whitespace and t.ttype not in T.Comment and t.value != ';' for t in s.flatten())]
    except sqlparse.exceptions.SQLParseError as exc:
        raise UserError('Cannot parse SQL; supply one SELECT statement.') from exc
    if len(statements) != 1 or statements[0].get_type() != 'SELECT':
        raise UserError('CSV export accepts one SELECT or WITH ... SELECT statement only.')
    # Reject modifying statements inside CTEs too; this is classification, not an SQL security boundary.
    if any(t.ttype in T.Keyword.DML and t.normalized != 'SELECT' or
           t.ttype in T.Keyword.DDL for t in statements[0].flatten()):
        raise UserError('CSV export does not accept data or catalog modification statements.')


@dataclass(frozen=True)
class QueryRequest:
    sql: str
    database: str
    output: str
    catalog: str = 'AwsDataCatalog'
    workgroup: str = 'primary'
    results_location: str | None = None
    timeout: int = 1800

    @classmethod
    def from_config(cls, config):
        if bool(config.get('query')) == bool(config.get('query_file')):
            raise UserError('Provide exactly one of --query and --query-file.')
        sql = config.get('query')
        if config.get('query_file'):
            try:
                sql = Path(config['query_file']).read_text(encoding='utf-8-sig')
            except (OSError, UnicodeError) as exc:
                raise UserError('Cannot read SQL file; provide a readable UTF-8 file.') from exc
        return cls(sql, config['database'], config['output'], config.get('catalog', 'AwsDataCatalog'),
                   config.get('workgroup', 'primary'), config.get('results_location'), config.get('query_timeout', 1800))

    def validate(self):
        validate_sql(self.sql)
        if not all(isinstance(value, str) and value.strip() for value in (self.database, self.catalog, self.workgroup, self.output)):
            raise UserError('Database, catalog, workgroup, and output must be nonempty.')
        if type(self.timeout) is not int or self.timeout <= 0:
            raise UserError('Query timeout must be a positive number of seconds.')
        if self.results_location:
            location = urlsplit(self.results_location)
            if location.scheme != 's3' or not location.netloc or location.query or location.fragment or '@' in location.netloc:
                raise UserError('Results location must be an s3://bucket/prefix/ URI.')
