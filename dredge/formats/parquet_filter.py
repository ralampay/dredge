"""Conservative row-group pruning shared by estimation and streaming reads."""
from dataclasses import dataclass

from ..core.exceptions import UserError


@dataclass(frozen=True)
class RowGroupSelection:
    indices: tuple[int, ...]
    compressed_bytes: int
    uncertain_groups: int


def select_row_groups(file, predicate):
    selected, size, uncertain = [], 0, 0
    metadata = file.metadata
    # Only scalar, top-level temporal columns have safe predicate semantics here.
    eligible = False
    if predicate.field in file.schema_arrow.names:
        import pyarrow as pa
        dtype = file.schema_arrow.field(predicate.field).type
        eligible = pa.types.is_timestamp(dtype) or pa.types.is_date(dtype)
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        columns = [group.column(i) for i in range(group.num_columns)]
        candidates = [c for c in columns if c.path_in_schema == predicate.field]
        stats = candidates[0].statistics if eligible and len(candidates) == 1 else None
        keep, known = True, False
        if stats is not None:
            if stats.has_null_count and stats.null_count == group.num_rows:
                keep, known = False, True
            elif stats.has_min_max:
                try:
                    lower, upper = predicate.instant(stats.min), predicate.instant(stats.max)
                    start, end = predicate.bounds
                    if lower <= upper:
                        keep, known = upper >= start and lower < end, True
                except (ValueError, TypeError, OverflowError, UserError):
                    pass
        if keep:
            selected.append(index)
            size += sum(max(0, column.total_compressed_size) for column in columns)
            uncertain += not known
    return RowGroupSelection(tuple(selected), size, uncertain)


@dataclass(frozen=True)
class MetadataInspection:
    groups: RowGroupSelection
    bytes_read: int
    total_groups: int


def inspect_parquet(source, obj, predicate, byte_limit=16 * 1024 * 1024):
    """Read bounded file metadata; small objects may fit in the footer request."""
    import pyarrow.parquet as pq
    from .records import RangeReader, SampleLimit

    raw = RangeReader(source, obj, byte_limit)
    try:
        with raw, pq.ParquetFile(raw) as file:
            return MetadataInspection(select_row_groups(file, predicate), raw.transferred,
                                      file.metadata.num_row_groups)
    except SampleLimit as exc:
        raise UserError(f'Parquet metadata for {obj.key!r} exceeds the {byte_limit:,}-byte planning limit.') from exc
    except (OSError, ValueError) as exc:
        raise UserError(f'Cannot inspect Parquet metadata for {obj.key!r}.') from exc
