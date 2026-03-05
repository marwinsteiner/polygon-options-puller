"""Command-line interface."""

from __future__ import annotations

import os
from datetime import date

import click

from .config import DATA_TYPES
from .downloader import pull


@click.group()
def cli():
    """Polygon / Massive options flat-file downloader.

    Downloads gzipped CSV flat files from the Polygon (Massive) S3 bucket and
    converts them to Snappy-compressed, dictionary-encoded Parquet files
    partitioned by date and underlying ticker.
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
    default="data",
    show_default=True,
    type=click.Path(),
    help="Root directory for Parquet output.",
)
@click.option(
    "-t",
    "--data-type",
    type=click.Choice(list(DATA_TYPES)),
    default="quotes",
    show_default=True,
    help="Which flat-file dataset to pull.",
)
@click.option(
    "-u",
    "--underlying",
    default=None,
    help="Only download data for this underlying ticker (e.g. AAPL, SPY).",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Start date (YYYY-MM-DD). Defaults to yesterday.",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="End date (YYYY-MM-DD). Defaults to yesterday.",
)
def download(
    access_key: str,
    secret_key: str,
    output_dir: str,
    data_type: str,
    underlying: str | None,
    start_date,
    end_date,
):
    """Download flat files and convert to Parquet."""
    start = start_date.date() if start_date else None
    end = end_date.date() if end_date else None

    written = pull(
        access_key=access_key,
        secret_key=secret_key,
        output_dir=output_dir,
        data_type=data_type,
        underlying=underlying,
        start_date=start,
        end_date=end,
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
def list_dates(access_key: str, secret_key: str, data_type: str, year: int | None, month: int | None):
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
