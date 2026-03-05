"""Convert Polygon CSV.GZ flat files to Parquet with optimal compression.

Partitioning strategy
---------------------
Parquet files are written per (date, underlying_ticker) pair.  This is the
single biggest lever for disk savings:

* **Columnar storage** – Parquet stores each column contiguously, enabling
  very efficient dictionary / RLE encoding for the many low-cardinality
  columns (exchange ids, correction flags, conditions, etc.).
* **Snappy compression** on top of dictionary-encoded pages typically
  achieves 80-90 % size reduction vs gzipped CSV for this kind of data.
* **Underlying-level partitioning** means every file contains only option
  tickers that share the same root, so the ``ticker`` column compresses
  even further (common prefix) and users can read only the underlyings
  they care about without touching the rest.

With this scheme the ~100 TB full options history should compress to well
under 50 TB (realistic expectation: 15-30 TB).
"""

from __future__ import annotations

import csv
import gzip
import io
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

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
# CSV → grouped rows
# --------------------------------------------------------------------------- #

# How many CSV rows to accumulate before flushing a per-underlying batch to
# Parquet.  Larger values use more RAM but produce fewer, bigger row-groups.
_BATCH_FLUSH_ROWS = 500_000


def _parse_value(value: str, field: pa.Field):
    """Coerce a single CSV string *value* according to the Arrow *field* type."""
    if value == "" or value == "null":
        return None

    base_type = field.type
    # Unwrap dictionary types to their value type for parsing.
    if pa.types.is_dictionary(base_type):
        base_type = base_type.value_type

    if pa.types.is_boolean(base_type):
        return value.lower() in ("true", "1", "t")
    if pa.types.is_floating(base_type):
        return float(value)
    if pa.types.is_integer(base_type):
        # Handle potential floats like "3.0" in integer columns.
        return int(float(value))
    # Default: keep as string (covers conditions, ticker, etc.)
    return value


def _rows_to_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Convert a list of row-dicts into an Arrow Table with *schema*."""
    arrays = {}
    for field in schema:
        raw = [_parse_value(row.get(field.name, ""), field) for row in rows]
        base_type = field.type
        if pa.types.is_dictionary(base_type):
            arr = pa.array(raw, type=base_type.value_type)
            arrays[field.name] = arr.dictionary_encode()
        else:
            arrays[field.name] = pa.array(raw, type=base_type)
    return pa.table(arrays, schema=schema)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def csv_gz_to_parquet(
    csv_gz_path: str | Path,
    output_dir: str | Path,
    data_type: str,
    date_str: str,
    *,
    underlying_filter: str | None = None,
) -> list[Path]:
    """Read a gzipped CSV flat file and write per-underlying Parquet files.

    Parameters
    ----------
    csv_gz_path:
        Path to the downloaded ``.csv.gz`` file.
    output_dir:
        Root directory for Parquet output.  Files are written as
        ``{output_dir}/{data_type}/{date}/{underlying}.parquet``.
    data_type:
        One of ``trades``, ``quotes``, ``day_aggs``, ``minute_aggs``.
    date_str:
        ISO date string (``YYYY-MM-DD``) used in the output path.
    underlying_filter:
        If set, only rows whose underlying matches this ticker are kept.

    Returns
    -------
    List of Parquet file paths that were written.
    """
    csv_gz_path = Path(csv_gz_path)
    output_dir = Path(output_dir)
    schema = SCHEMAS[data_type]

    written: list[Path] = []

    # Accumulate rows keyed by underlying.
    buckets: dict[str, list[dict]] = {}
    total_buffered = 0

    def _flush(force_all: bool = False):
        nonlocal total_buffered
        for underlying, rows in list(buckets.items()):
            if not force_all and len(rows) < _BATCH_FLUSH_ROWS:
                continue
            _write_batch(rows, underlying, schema, data_type, date_str, output_dir, written)
            total_buffered -= len(rows)
            del buckets[underlying]

    with gzip.open(csv_gz_path, "rt", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ticker = row.get("ticker", "")
            underlying = extract_underlying(ticker)

            if underlying_filter and underlying != underlying_filter:
                continue

            buckets.setdefault(underlying, []).append(row)
            total_buffered += 1

            if total_buffered >= _BATCH_FLUSH_ROWS * 4:
                _flush()

    # Flush remaining rows.
    _flush(force_all=True)

    return written


def _write_batch(
    rows: list[dict],
    underlying: str,
    schema: pa.Schema,
    data_type: str,
    date_str: str,
    output_dir: Path,
    written: list[Path],
) -> None:
    table = _rows_to_table(rows, schema)

    dest_dir = output_dir / data_type / f"date={date_str}" / f"underlying={underlying}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "data.parquet"

    if dest_path.exists():
        existing = pq.read_table(dest_path)
        table = pa.concat_tables([existing, table])

    pq.write_table(
        table,
        dest_path,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )
    if dest_path not in written:
        written.append(dest_path)
