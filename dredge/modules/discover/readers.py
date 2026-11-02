"""Bounded schema inspection, using shared content adapters."""
import io
import csv
import zlib
import ijson
import pyarrow as pa
import pyarrow.parquet as pq

from ...core.exceptions import UserError
from ...formats.records import RangeReader, SampleLimit, open_records
from ...formats.s3_access_log import access_log_schema
from .schema import arrow_schema, infer, merge


class InspectObject:
    def __init__(self, source, options):
        self.source, self.options = source, options

    def execute(self, obj, fmt, compression):
        raw = RangeReader(self.source, obj, self.options.sample_bytes)
        if fmt == "parquet":
            try:
                if obj.size < 12:
                    raise ValueError("Invalid Parquet size")
                if raw.read(4) != b"PAR1":
                    raise ValueError("Invalid Parquet header")
                raw.seek(-8, 2)
                trailer = raw.read(8)
                if len(trailer) != 8 or trailer[4:] != b"PAR1":
                    raise ValueError("Invalid Parquet footer")
                length = int.from_bytes(trailer[:4], "little")
                if length > obj.size - 12:
                    raise ValueError("Invalid Parquet metadata length")
                if length > raw.budget - raw.transferred:
                    raise SampleLimit()
                raw.seek(obj.size - 8 - length)
                metadata = raw.read(length)
                # A synthetic metadata-only file avoids PyArrow's normal tail read,
                # which can otherwise include data pages preceding the footer.
                schema = arrow_schema(pq.read_schema(io.BytesIO(b"PAR1" + metadata + trailer)))
                return schema, {"records": 0, "bytes": raw.transferred, "limits": [], "method": "metadata"}
            except SampleLimit as exc:
                raise UserError(f"Parquet metadata for {obj.key!r} exceeds --sample-bytes; increase the budget.") from exc
            except (pa.ArrowException, OSError, ValueError) as exc:
                raise UserError(f"Cannot inspect Parquet metadata for {obj.key!r}; check the file and --sample-bytes budget.") from exc
            finally:
                raw.close()
        schema = {"type": "unknown", "nullable": False}
        count, limits = 0, []
        try:
            with open_records(raw, fmt, compression, self.options) as records:
                for record in records:
                    if fmt == "s3-access-log":
                        schema = access_log_schema()
                    else:
                        schema = merge(schema, infer(record))
                    count += 1
                    if count >= self.options.sample_records:
                        limits.append("records")
                        break
        except SampleLimit:
            limits.append("bytes")
        except (UnicodeError, ValueError, csv.Error, ijson.JSONError, EOFError, OSError, zlib.error) as exc:
            raise UserError(f"Cannot parse {obj.key!r} as {fmt}; check its format, encoding, and parser settings.") from exc
        if count == 0 and limits:
            raise UserError(f"No complete record fits the sample budget for {obj.key!r}; increase --sample-bytes.")
        return schema, {"records": count, "bytes": raw.transferred, "limits": limits, "method": "records"}
