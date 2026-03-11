"""Orchestrates streaming download from S3, symbol filtering, and Parquet writing."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
from tqdm import tqdm

from .config import DATA_TYPES
from .converter import dataframe_to_table, get_output_schema, validate_parquet
from .s3 import get_s3_client, get_streaming_body


def _trading_dates(start: date, end: date) -> list[date]:
    """Return NYSE trading dates between *start* and *end* inclusive."""
    import pandas_market_calendars as mcal

    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=start, end_date=end)
    return [d.date() for d in schedule.index]


def _cleanup_stale_tmp(output_dir: Path) -> None:
    """Delete any leftover ``.parquet.tmp`` files from interrupted runs."""
    for tmp in output_dir.rglob("*.parquet.tmp"):
        try:
            tmp.unlink()
        except OSError:
            pass


def _output_path(output_dir: Path, data_type: str, d: date) -> Path:
    """Return the canonical output path for a (data_type, date) pair."""
    return output_dir / data_type / f"{d.isoformat()}.parquet"


def _s3_key_for_date(data_type: str, d: date) -> str:
    """Build the S3 object key for a given data type and date."""
    prefix = DATA_TYPES[data_type]["prefix"]
    return f"{prefix}/{d.year}/{d.month:02d}/{d.strftime('%Y-%m-%d')}.csv.gz"


def _process_day(
    s3_client,
    data_type: str,
    d: date,
    symbol_prefix: str,
    output_dir: Path,
    min_rows: int,
) -> Path | None:
    """Stream one day's flat file from S3, filter, and write Parquet.

    Returns the output path if rows were written, or None if skipped/empty.
    """
    import pandas as pd

    dest = _output_path(output_dir, data_type, d)

    # Idempotency: skip if a valid file already exists.
    if validate_parquet(dest, symbol_prefix, min_rows):
        tqdm.write(f"  Skipping {data_type} {d.isoformat()} (already valid)")
        return None

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest.with_suffix(".parquet.tmp")

    key = _s3_key_for_date(data_type, d)
    body = get_streaming_body(s3_client, key)

    output_schema = get_output_schema(data_type)
    writer = None
    rows_written = 0
    ticker_prefix = f"O:{symbol_prefix}"

    try:
        reader = pd.read_csv(body, compression="gzip", chunksize=500_000, low_memory=False)
        for chunk in reader:
            if "ticker" not in chunk.columns:
                continue
            filtered = chunk[chunk["ticker"].str.startswith(ticker_prefix, na=False)]
            if filtered.empty:
                continue
            table = dataframe_to_table(filtered, data_type)
            if writer is None:
                writer = pq.ParquetWriter(
                    str(tmp_path),
                    output_schema,
                    compression="snappy",
                    use_dictionary=True,
                )
            writer.write_table(table)
            rows_written += len(filtered)
    finally:
        if writer is not None:
            writer.close()

    if rows_written == 0:
        tmp_path.unlink(missing_ok=True)
        return None

    # Atomic rename (works on both Windows and POSIX).
    os.replace(str(tmp_path), str(dest))
    return dest


def pull(
    access_key: str,
    secret_key: str,
    output_dir: str | Path,
    data_types: list[str],
    symbol_prefix: str,
    start_date: date,
    end_date: date,
    workers: int = 8,
    min_rows: int = 1000,
) -> list[Path]:
    """Download flat files from S3, stream-filter by symbol, write Parquet.

    Parameters
    ----------
    access_key, secret_key:
        Polygon / Massive S3 credentials.
    output_dir:
        Root directory for Parquet output.
    data_types:
        List of data types, e.g. ``["quotes"]`` or ``["trades", "quotes"]``.
    symbol_prefix:
        Underlying ticker prefix to filter on (e.g. ``"SPXW"``).
        The code prepends ``O:`` automatically.
    start_date, end_date:
        Date range to download (inclusive).
    workers:
        Number of parallel download threads.
    min_rows:
        Minimum row count for a Parquet file to be considered valid
        (used by idempotency check).

    Returns
    -------
    List of Parquet file paths that were written.
    """
    for dt in data_types:
        if dt not in DATA_TYPES:
            raise ValueError(f"Unknown data_type {dt!r}. Choose from: {', '.join(DATA_TYPES)}")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    _cleanup_stale_tmp(output_dir)

    dates = _trading_dates(start_date, end_date)
    if not dates:
        tqdm.write("No NYSE trading dates in the given range.")
        return []

    # Build work items: all (data_type, date) combinations.
    work_items = [(dt, d) for d in dates for dt in data_types]

    s3 = get_s3_client(access_key, secret_key)
    written: list[Path] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _process_day, s3, dt, d, symbol_prefix, output_dir, min_rows
            ): (dt, d)
            for dt, d in work_items
        }
        with tqdm(total=len(futures), desc=f"Pulling {symbol_prefix}", unit="file") as pbar:
            for future in as_completed(futures):
                dt, d = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        written.append(result)
                        tqdm.write(f"  {dt} {d.isoformat()}: wrote {result.name}")
                except Exception as exc:
                    tqdm.write(f"  {dt} {d.isoformat()}: {exc}")
                pbar.update(1)

    return written
