"""Convert Polygon flat-file data to Parquet with optimal compression.

Provides Arrow schema definitions, DataFrame→Arrow Table conversion with
proper typing (timestamps, dictionary encoding), and Parquet validation
for content-aware idempotency.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import TIMESTAMP_COLUMNS

# --------------------------------------------------------------------------- #
# Option-ticker parsing
# --------------------------------------------------------------------------- #

# Polygon option tickers look like  O:AAPL230120C00150000
#   O:<underlying><YYMMDD><C|P><strike * 1000, zero-padded to 8 digits>
_OPTION_TICKER_RE = re.compile(r"^O:([A-Z]+)\d{6}[CP]\d{8}$")


def extract_underlying(option_ticker: str) -> str:
    """Return the underlying symbol from a Polygon option ticker.

    >>> extract_underlying("O:AAPL230120C00150000")
    'AAPL'
    """
    m = _OPTION_TICKER_RE.match(option_ticker)
    if m:
        return m.group(1)
    # Fallback: strip the ``O:`` prefix and grab leading alpha chars.
    stripped = option_ticker.removeprefix("O:")
    underlying = ""
    for ch in stripped:
        if ch.isalpha():
            underlying += ch
        else:
            break
    return underlying or "UNKNOWN"


# --------------------------------------------------------------------------- #
# Arrow schemas  (one per data type)
# --------------------------------------------------------------------------- #
# We define explicit Arrow schemas so that types are tight and dictionary
# encoding kicks in where it matters.

_TRADES_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.dictionary(pa.int16(), pa.string())),
        pa.field("conditions", pa.string()),  # stored as original CSV string
        pa.field("correction", pa.uint8()),
        pa.field("exchange", pa.dictionary(pa.int8(), pa.int16())),
        pa.field("participant_timestamp", pa.int64()),
        pa.field("price", pa.float64()),
        pa.field("sequence_number", pa.int64()),
        pa.field("sip_timestamp", pa.int64()),
        pa.field("size", pa.uint32()),
    ]
)

_QUOTES_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.dictionary(pa.int16(), pa.string())),
        pa.field("ask_exchange", pa.dictionary(pa.int8(), pa.int16())),
        pa.field("ask_price", pa.float64()),
        pa.field("ask_size", pa.uint32()),
        pa.field("bid_exchange", pa.dictionary(pa.int8(), pa.int16())),
        pa.field("bid_price", pa.float64()),
        pa.field("bid_size", pa.uint32()),
        pa.field("sequence_number", pa.int64()),
        pa.field("sip_timestamp", pa.int64()),
    ]
)

_DAY_AGGS_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.dictionary(pa.int16(), pa.string())),
        pa.field("close", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("transactions", pa.uint32()),
        pa.field("volume", pa.uint64()),
        pa.field("vwap", pa.float64()),
        pa.field("otc", pa.bool_()),
    ]
)

_MINUTE_AGGS_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.dictionary(pa.int16(), pa.string())),
        pa.field("close", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("open", pa.float64()),
        pa.field("timestamp", pa.int64()),
        pa.field("transactions", pa.uint32()),
        pa.field("volume", pa.uint64()),
        pa.field("vwap", pa.float64()),
        pa.field("otc", pa.bool_()),
    ]
)

SCHEMAS: dict[str, pa.Schema] = {
    "trades": _TRADES_SCHEMA,
    "quotes": _QUOTES_SCHEMA,
    "day_aggs": _DAY_AGGS_SCHEMA,
    "minute_aggs": _MINUTE_AGGS_SCHEMA,
}

# --------------------------------------------------------------------------- #
# Output schema (timestamps converted from int64 → timestamp)
# --------------------------------------------------------------------------- #

_TS_TYPE = pa.timestamp("ns", tz="UTC")


@lru_cache(maxsize=4)
def get_output_schema(data_type: str) -> pa.Schema:
    """Return the output Parquet schema with timestamp columns converted."""
    base = SCHEMAS[data_type]
    ts_cols = set(TIMESTAMP_COLUMNS.get(data_type, []))
    fields = []
    for field in base:
        if field.name in ts_cols:
            fields.append(pa.field(field.name, _TS_TYPE))
        else:
            fields.append(field)
    return pa.schema(fields)


# --------------------------------------------------------------------------- #
# DataFrame → Arrow Table
# --------------------------------------------------------------------------- #


def dataframe_to_table(df, data_type: str) -> pa.Table:
    """Convert a pandas DataFrame chunk to an Arrow Table with the output schema.

    Handles timestamp conversion, dictionary encoding, and type coercion.
    """
    import pandas as pd

    output_schema = get_output_schema(data_type)
    ts_cols = set(TIMESTAMP_COLUMNS.get(data_type, []))

    arrays = []
    for field in output_schema:
        col_name = field.name
        if col_name not in df.columns:
            arrays.append(pa.nulls(len(df), type=field.type))
            continue

        values = df[col_name]

        if col_name in ts_cols:
            ts_series = pd.to_datetime(values, unit="ns", utc=True)
            arrays.append(pa.array(ts_series, type=_TS_TYPE))
        elif pa.types.is_dictionary(field.type):
            value_type = field.type.value_type
            arr = pa.array(values, type=value_type)
            arrays.append(arr.dictionary_encode())
        else:
            arrays.append(pa.array(values, type=field.type))

    return pa.table(arrays, schema=output_schema)


# --------------------------------------------------------------------------- #
# Parquet validation for idempotency
# --------------------------------------------------------------------------- #


def validate_parquet(path: Path, symbol_prefix: str, min_rows: int = 1000) -> bool:
    """Check that an existing Parquet file is valid for the given symbol prefix.

    Returns False if the file is missing, unreadable, has fewer than *min_rows*,
    or contains tickers that don't start with ``O:{symbol_prefix}``.
    """
    if not path.exists():
        return False
    try:
        pf = pq.ParquetFile(path)
        if pf.metadata.num_rows < min_rows:
            return False
        # Read only the ticker column for efficiency.
        ticker_table = pf.read(columns=["ticker"])
        tickers = ticker_table.column("ticker")
        # Combine chunks and handle dictionary encoding.
        combined = tickers.combine_chunks()
        if pa.types.is_dictionary(combined.type):
            unique_tickers = combined.dictionary_decode().unique().to_pylist()
        else:
            unique_tickers = combined.unique().to_pylist()
        expected_prefix = f"O:{symbol_prefix}"
        return all(t.startswith(expected_prefix) for t in unique_tickers if t is not None)
    except Exception:
        return False
