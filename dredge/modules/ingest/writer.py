"""Incremental Parquet output with bounded batches and one active file."""
import sys

from ...core.exceptions import UserError
from ...formats.arrow_contract import normalize_value


def payload_size(value):
    """Conservative Python payload estimate; not a process RSS guarantee."""
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(sys.getsizeof(k) + payload_size(v) for k, v in value.items())
    elif isinstance(value, (list, tuple)):
        size += sum(payload_size(v) for v in value)
    return size


class ParquetDatasetWriter:
    def __init__(self, directory, dataset, schema, options, on_file=None):
        self.directory, self.dataset, self.schema, self.options = directory, dataset, schema, options
        self.on_file = on_file
        self.batch, self.batch_bytes = [], 0
        self.writer = None
        self.file_rows = self.file_bytes = self.rows = 0
        self.files = []

    def __enter__(self):
        self.directory.mkdir(parents=True)
        return self

    def append(self, record):
        values = normalize_value(record.values, self.dataset['schema'])
        for partition in record.partitions:
            values['__dredge_partition_' + str(partition['segment'])] = partition['value']
        size = payload_size(values)
        if self.batch and self.batch_bytes + size > self.options.batch_bytes:
            self.flush()
        self.batch.append(values)
        self.batch_bytes += size
        if len(self.batch) >= self.options.batch_rows or self.batch_bytes >= self.options.batch_bytes:
            self.flush()

    def _open(self):
        import pyarrow.parquet as pq
        self.path = self.directory / f'part-{len(self.files):06d}.parquet'
        self.writer = pq.ParquetWriter(self.path, self.schema, compression='snappy')
        self.file_rows = self.file_bytes = 0

    def _close(self, *, record_success=True):
        if self.writer is None:
            return
        import pyarrow as pa
        writer, self.writer = self.writer, None
        try:
            writer.close()
        except pa.ArrowException as exc:
            raise UserError('Cannot finalize the Parquet output file.') from exc
        if not record_success:
            return
        self.files.append({'path': 'data/' + self.path.name, 'rows': self.file_rows,
                           'bytes': self.path.stat().st_size, 'arrow_bytes': self.file_bytes})
        if self.on_file:
            self.on_file()

    def flush(self):
        if not self.batch:
            return
        import pyarrow as pa
        try:
            table = pa.Table.from_pylist(self.batch, schema=self.schema)
        except (pa.ArrowException, ValueError, OverflowError) as exc:
            raise UserError('Cannot safely encode records as the declared Parquet schema.') from exc
        if self.writer is not None and self.file_bytes + table.nbytes > self.options.file_bytes:
            self._close()
        if self.writer is None:
            self._open()
        try:
            self.writer.write_table(table, row_group_size=self.options.batch_rows)
        except pa.ArrowException as exc:
            raise UserError('Cannot write the Parquet output batch.') from exc
        self.file_rows += table.num_rows
        self.file_bytes += table.nbytes
        self.rows += table.num_rows
        self.batch.clear()
        self.batch_bytes = 0

    def __exit__(self, exc_type, exc, traceback):
        completed = False
        try:
            if exc_type is None:
                self.flush()
                if self.writer is None and not self.files:
                    self._open()
                completed = True
        finally:
            failure = (sys.exc_info()[1] or exc) if not completed else None
            try:
                self._close(record_success=completed)
            except (OSError, UserError) as close_error:
                if failure is not None:
                    # A failed close must not hide schema drift or cancellation.
                    raise failure from close_error
                raise
        return False
