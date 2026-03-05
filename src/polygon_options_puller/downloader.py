"""Orchestrates downloading flat files from S3 and converting to Parquet."""

from __future__ import annotations

import tempfile
from datetime import date, timedelta
from pathlib import Path

from tqdm import tqdm

from .config import DATA_TYPES
from .converter import csv_gz_to_parquet
from .s3 import download_file, get_s3_client, list_keys


def _s3_key_for_date(data_type: str, d: date) -> str:
    """Build the S3 object key for a given data type and date."""
    prefix = DATA_TYPES[data_type]["prefix"]
    return f"{prefix}/{d.year}/{d.month:02d}/{d.strftime('%Y-%m-%d')}.csv.gz"


def _date_range(start: date, end: date):
    """Yield dates from *start* to *end* inclusive, skipping weekends."""
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon–Fri
            yield d
        d += timedelta(days=1)


def pull(
    access_key: str,
    secret_key: str,
    output_dir: str | Path,
    data_type: str = "quotes",
    underlying: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[Path]:
    """Download flat files from S3 and convert to partitioned Parquet.

    Parameters
    ----------
    access_key, secret_key:
        Polygon / Massive S3 credentials.
    output_dir:
        Root directory for Parquet output.
    data_type:
        ``trades``, ``quotes``, ``day_aggs``, or ``minute_aggs``.
    underlying:
        If provided, only keep rows for this underlying (e.g. ``AAPL``).
    start_date, end_date:
        Date range to download.  Both default to yesterday.

    Returns
    -------
    List of Parquet file paths that were written.
    """
    if data_type not in DATA_TYPES:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Choose from: {', '.join(DATA_TYPES)}"
        )

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    yesterday = date.today() - timedelta(days=1)
    start = start_date or yesterday
    end = end_date or yesterday

    s3 = get_s3_client(access_key, secret_key)
    dates = list(_date_range(start, end))
    all_written: list[Path] = []

    for d in tqdm(dates, desc=f"Pulling {data_type}", unit="day"):
        key = _s3_key_for_date(data_type, d)
        date_str = d.isoformat()

        with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            download_file(s3, key, tmp_path)
        except Exception as exc:
            tqdm.write(f"  skip {date_str}: {exc}")
            Path(tmp_path).unlink(missing_ok=True)
            continue

        written = csv_gz_to_parquet(
            tmp_path,
            output_dir,
            data_type,
            date_str,
            underlying_filter=underlying,
        )
        all_written.extend(written)
        Path(tmp_path).unlink(missing_ok=True)
        tqdm.write(f"  {date_str}: wrote {len(written)} parquet file(s)")

    return all_written
