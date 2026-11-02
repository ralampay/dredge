"""Bounded text sampling and metadata-only Parquet inspection."""
import codecs
import csv
import gzip
import io
import json
from itertools import chain, islice
import zlib
from contextlib import ExitStack, contextmanager

import ijson

from ..core.exceptions import UserError
from .schema import csv_value


class SampleLimit(Exception):
    pass


class RangeReader(io.RawIOBase):
    def __init__(self, source, obj, budget):
        self.source, self.obj, self.budget = source, obj, budget
        self.position = 0
        self.transferred = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        position = offset + (self.position if whence == 1 else self.obj.size if whence == 2 else 0)
        if whence not in (0, 1, 2) or position < 0:
            raise ValueError("Invalid seek")
        self.position = position
        return position

    def read(self, size=-1):
        remaining = self.obj.size - self.position
        if remaining <= 0 or size == 0:
            return b""
        available = self.budget - self.transferred
        if available <= 0:
            raise SampleLimit()
        count = min(remaining, available, remaining if size < 0 else size)
        data = self.source.read_range(self.obj, self.position, self.position + count - 1)
        self.position += len(data)
        self.transferred += len(data)
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)


class ExpandedReader(io.RawIOBase):
    def __init__(self, stream, budget):
        self.stream, self.budget, self.count = stream, budget, 0

    def readable(self):
        return True

    def readinto(self, buffer):
        if self.count >= self.budget:
            raise SampleLimit()
        data = self.stream.read(min(len(buffer), self.budget - self.count))
        buffer[:len(data)] = data
        self.count += len(data)
        return len(data)


class UTF8Reader:
    """Incrementally transcode bounded binary chunks for ijson."""
    def __init__(self, binary, encoding):
        self.binary = binary
        self.decoder = codecs.getincrementaldecoder(encoding)()

    def read(self, size=-1):
        if size == 0:
            return b""
        while True:
            data = self.binary.read1(4096 if size < 0 else min(size, 4096))
            result = self.decoder.decode(data, final=not data).encode("utf-8")
            if result or not data:
                return result


def json_records(stream):
    events = iter(ijson.parse(stream, buf_size=4096))
    first = next(events, None)
    if first is None:
        return
    if first[1] not in ("start_map", "start_array"):
        raise ValueError("JSON records must be objects")
    array = first[1] == "start_array"
    builder = None if array else ijson.ObjectBuilder()
    depth = 0 if array else 1
    if builder is not None:
        builder.event("start_map", None)
    for prefix, event, value in events:
        if builder is None:
            if event == "end_array" and prefix == "":
                # Consume the parser to validate trailing data.
                if next(events, None) is not None:
                    raise ValueError("Trailing JSON data")
                return
            if event != "start_map":
                raise ValueError("JSON records must be objects")
            builder, depth = ijson.ObjectBuilder(), 0
        builder.event(event, value)
        if event in ("start_map", "start_array"):
            depth += 1
        elif event in ("end_map", "end_array"):
            depth -= 1
        if depth == 0:
            yield builder.value
            builder = None
            if not array:
                if next(events, None) is not None:
                    raise ValueError("Trailing JSON data")
                return


def text_records(stream, fmt, options, metadata=None):
    if fmt == "s3-access-log":
        from .s3_access_log import parse_record
        for line in stream:
            if line.strip():
                yield parse_record(line)
    elif fmt == "jsonl":
        for line in stream:
            if line.strip():
                value = json.loads(line, parse_constant=lambda _: invalid_constant())
                if not isinstance(value, dict):
                    raise ValueError("JSON Lines records must be objects")
                yield value
    else:
        from .csv_headers import detect_header
        metadata = metadata if metadata is not None else {}
        rows = (row for row in csv.reader(stream, delimiter=options.csv_delimiter, strict=True) if row)
        policy = options.csv_header
        if policy == 'auto':
            lookahead, limited = [], False
            try:
                for row in islice(rows, min(20, getattr(options, 'sample_records', 1000) + 1)):
                    lookahead.append(row)
            except SampleLimit:
                limited = True
            policy, ambiguous = detect_header(lookahead)
            metadata.update(csv_header=policy, header_ambiguous=ambiguous)
            def remainder(original=rows):
                if limited:
                    raise SampleLimit()
                yield from original
            rows = chain(lookahead, remainder())
        else:
            metadata.update(csv_header=policy, header_ambiguous=False)
        header = None
        for row in rows:
            if header is None:
                header = row if policy == 'present' else [f"column_{i + 1}" for i in range(len(row))]
                if len(set(header)) != len(header) or any(not name for name in header):
                    raise ValueError("CSV headers must be nonempty and unique")
                metadata['columns'] = list(header)
                if policy == 'present':
                    continue
            if len(row) != len(header):
                raise ValueError("CSV row width differs from header")
            yield {name: csv_value(value) for name, value in zip(header, row)}


def invalid_constant():
    raise ValueError("Non-finite JSON number")



@contextmanager
def _open_records(raw, fmt, compression, options, metadata=None):
    """Yield records with identical parser semantics for discovery and ingestion."""
    with ExitStack() as stack:
        if fmt == 'parquet':
            import pyarrow.parquet as pq
            file = stack.enter_context(pq.ParquetFile(stack.enter_context(raw)))
            predicate = getattr(options, 'parquet_predicate', None)
            groups = None
            if predicate is not None:
                from .parquet_filter import select_row_groups
                groups = list(select_row_groups(file, predicate).indices)
            yield (record for batch in file.iter_batches(batch_size=1000, row_groups=groups)
                   for record in batch.to_pylist())
            return
        buffered = stack.enter_context(io.BufferedReader(raw, buffer_size=getattr(options, 'read_buffer_bytes', 65536)))
        binary = stack.enter_context(gzip.GzipFile(fileobj=buffered)) if compression == 'gzip' else buffered
        expanded = ExpandedReader(binary, options.sample_bytes)
        decoded = stack.enter_context(io.BufferedReader(expanded))
        if fmt == 'json':
            yield json_records(UTF8Reader(decoded, options.encoding))
        else:
            text = stack.enter_context(io.TextIOWrapper(decoded, encoding=options.encoding, newline=''))
            yield text_records(text, fmt, options, metadata)


@contextmanager
def open_records(raw, fmt, compression, options, metadata=None):
    """Translate format failures at the shared content-adapter boundary."""
    try:
        with _open_records(raw, fmt, compression, options, metadata) as records:
            yield records
    except (UnicodeError, ValueError, csv.Error, ijson.JSONError, EOFError, OSError, zlib.error) as exc:
        raise UserError(f"Cannot parse {raw.obj.key!r} as {fmt}; check its format, encoding, and parser settings.") from exc
