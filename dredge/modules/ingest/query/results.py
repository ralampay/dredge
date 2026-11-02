"""Bounded CSV transfer and managed-result serialization."""
import csv

from ....core.exceptions import UserError


class ExportQueryResults:
    def __init__(self, repository, execution, output, check_cancel, on_bytes=None):
        self.repository, self.execution, self.output = repository, execution, output
        self.check_cancel, self.on_bytes = check_cancel, on_bytes

    def execute(self):
        query_id = self.execution['query_id']
        managed = self.execution['managed_results']
        self.check_cancel()
        first = self.repository.result_page(query_id, size=1000 if managed else 1)
        columns = first['columns']
        if not columns:
            raise UserError('Athena returned no column metadata for the SELECT result.')
        if managed:
            rows = self._write_pages(first, columns)
        else:
            self._download()
            rows = None  # Do not scan the downloaded CSV again just to count records.
        return {'columns': columns, 'rows': rows, 'bytes': self.output.stat().st_size}

    def _download(self):
        total = 0
        with self.repository.result_stream(self.execution['result_location']) as (body, expected), self.output.open('wb') as stream:
            while True:
                self.check_cancel()
                chunk = body.read(1024 * 1024)
                self.check_cancel()
                if not chunk:
                    break
                stream.write(chunk)
                total += len(chunk)
                if self.on_bytes:
                    self.on_bytes(total)
        if expected is not None and total != expected:
            raise UserError('Athena CSV result download was incomplete; retry the export.')

    def _write_pages(self, first, columns):
        count, page, first_row = 0, first, True
        names = [column['name'] for column in columns]
        with self.output.open('w', encoding='utf-8', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(names)
            while True:
                self.check_cancel()
                if page['columns'] and page['columns'] != columns:
                    raise UserError('Athena result column metadata changed between pages.')
                for row in page['rows']:
                    self.check_cancel()
                    if first_row:
                        first_row = False
                        if row != names:
                            raise UserError('Unexpected Athena result header; cannot safely export this response.')
                        continue
                    if len(row) > len(columns):
                        raise UserError('Athena result row has more values than columns.')
                    writer.writerow(row + [None] * (len(columns) - len(row)))
                    count += 1
                stream.flush()
                if self.on_bytes:
                    self.on_bytes(self.output.stat().st_size)
                if not page['next_token']:
                    return count
                page = self.repository.result_page(self.execution['query_id'], page['next_token'])
