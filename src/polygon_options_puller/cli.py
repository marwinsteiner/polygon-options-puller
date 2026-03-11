"""Command-line interface."""

from __future__ import annotations

import click

from .config import DATA_TYPES
from .downloader import pull


@click.group()
def cli():
    """Polygon / Massive options flat-file downloader.

    Downloads gzipped CSV flat files from the Polygon (Massive) S3 bucket,
    streams and filters them by symbol prefix, and writes the matching rows
    as Snappy-compressed, dictionary-encoded Parquet files.
    """


@cli.command()
@click.option(
    "--access-key",
    envvar="POLYGON_S3_ACCESS_KEY",
    required=True,
    help="S3 access key (or set POLYGON_S3_ACCESS_KEY).",
)
@click.option(
    "--secret-key",
    envvar="POLYGON_S3_SECRET_KEY",
    required=True,
    help="S3 secret key (or set POLYGON_S3_SECRET_KEY).",
)
@click.option(
    "-o",
    "--output-dir",
    required=True,
    type=click.Path(),
    help="Root directory for Parquet output.",
)
@click.option(
    "-t",
    "--data-type",
    type=click.Choice(list(DATA_TYPES) + ["both"]),
    default="quotes",
    show_default=True,
    help="Which flat-file dataset to pull. 'both' pulls trades and quotes.",
)
@click.option(
    "--symbol-prefix",
    required=True,
    help="Underlying symbol prefix to filter on (e.g. SPXW, AAPL, SPY).",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="End date (YYYY-MM-DD).",
)
@click.option(
    "--workers",
    type=int,
    default=8,
    show_default=True,
    help="Number of parallel download threads.",
)
@click.option(
    "--min-rows",
    type=int,
    default=1000,
    show_default=True,
    help="Minimum rows for a Parquet file to be considered valid (idempotency check).",
)
def download(
    access_key: str,
    secret_key: str,
    output_dir: str,
    data_type: str,
    symbol_prefix: str,
    start_date,
    end_date,
    workers: int,
    min_rows: int,
):
    """Download flat files, stream-filter by symbol, and write Parquet."""
    start = start_date.date()
    end = end_date.date()

    if data_type == "both":
        data_types = ["trades", "quotes"]
    else:
        data_types = [data_type]

    written = pull(
        access_key=access_key,
        secret_key=secret_key,
        output_dir=output_dir,
        data_types=data_types,
        symbol_prefix=symbol_prefix,
        start_date=start,
        end_date=end,
        workers=workers,
        min_rows=min_rows,
    )
    click.echo(f"\nDone. Wrote {len(written)} parquet file(s) to {output_dir}/")


@cli.command("list-dates")
@click.option(
    "--access-key",
    envvar="POLYGON_S3_ACCESS_KEY",
    required=True,
    help="S3 access key (or set POLYGON_S3_ACCESS_KEY).",
)
@click.option(
    "--secret-key",
    envvar="POLYGON_S3_SECRET_KEY",
    required=True,
    help="S3 secret key (or set POLYGON_S3_SECRET_KEY).",
)
@click.option(
    "-t",
    "--data-type",
    type=click.Choice(list(DATA_TYPES)),
    default="quotes",
    show_default=True,
    help="Which flat-file dataset to list.",
)
@click.option(
    "--year",
    type=int,
    default=None,
    help="Only list dates for this year.",
)
@click.option(
    "--month",
    type=int,
    default=None,
    help="Only list dates for this month (requires --year).",
)
def list_dates(
    access_key: str, secret_key: str, data_type: str, year: int | None, month: int | None
):
    """List available dates for a flat-file dataset."""
    from .s3 import get_s3_client, list_keys

    prefix = DATA_TYPES[data_type]["prefix"]
    if year:
        prefix = f"{prefix}/{year}"
        if month:
            prefix = f"{prefix}/{month:02d}"

    s3 = get_s3_client(access_key, secret_key)
    keys = list_keys(s3, prefix)
    for key in keys:
        click.echo(key)
