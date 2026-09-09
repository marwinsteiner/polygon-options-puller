"""Row filters applied to each record batch while streaming."""

from __future__ import annotations

from typing import Callable, Iterable

import pyarrow as pa
import pyarrow.compute as pc

#: A callable that takes a record batch and returns a (possibly smaller) batch.
BatchFilter = Callable[[pa.RecordBatch], pa.RecordBatch]


def build_filter(
    column: str = "ticker",
    values: Iterable[str] = (),
    prefixes: Iterable[str] = (),
) -> BatchFilter | None:
    """Build a filter keeping rows whose *column* equals one of *values* or starts with a prefix.

    Returns ``None`` when there is nothing to filter on, so callers can skip
    the work entirely.
    """
    values = [str(v) for v in values]
    prefixes = [str(p) for p in prefixes]
    if not values and not prefixes:
        return None

    value_set = pa.array(values, type=pa.string()) if values else None

    def _apply(batch: pa.RecordBatch) -> pa.RecordBatch:
        if column not in batch.schema.names:
            raise ValueError(
                f"Filter column {column!r} not found in file columns {batch.schema.names}"
            )
        col = batch.column(column)
        if not pa.types.is_string(col.type) and not pa.types.is_large_string(col.type):
            col = col.cast(pa.string())
        mask = None
        if value_set is not None:
            mask = pc.is_in(col, value_set=value_set)
        for prefix in prefixes:
            m = pc.starts_with(col, pattern=prefix)
            mask = m if mask is None else pc.or_(mask, m)
        return batch.filter(mask)

    return _apply


def filter_signature(
    column: str = "ticker",
    values: Iterable[str] = (),
    prefixes: Iterable[str] = (),
) -> str:
    """Stable string describing a filter, used for idempotency bookkeeping."""
    values = sorted(str(v) for v in values)
    prefixes = sorted(str(p) for p in prefixes)
    if not values and not prefixes:
        return ""
    return f"{column}:in={','.join(values)}:prefix={','.join(prefixes)}"
