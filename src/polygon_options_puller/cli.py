"""Command-line interface."""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime

import click
from tqdm import tqdm

from . import __version__
from .config import DATA_TYPES, KNOWN_DATASETS
from .datasets import discover_latest, list_datasets, resolve_dataset
from .downloader import DEFAULT_PATH_TEMPLATE, LEGACY_PATH_TEMPLATE, PullResult, pull
from .formats import FORMATS, format_names
from .reader import parse_column_types
from .s3 import get_s3_client, list_keys


class _TqdmHandler(logging.Handler):
    """Log handler that plays nicely with an active tqdm progress bar."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            tqdm.write(self.format(record), file=sys.stderr)
        except Exception:  # pragma: no cover - logging must never crash the run
            self.handleError(record)


def _configure_logging(verbose: bool, quiet: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING if quiet else logging.INFO
    logger = logging.getLogger("polygon_options_puller")
    logger.setLevel(level)
    if not any(isinstance(h, _TqdmHandler) for h in logger.handlers):
        handler = _TqdmHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
        logger.addHandler(handler)


def _credential_options(fn):
    fn = click.option(
        "--secret-key",
        envvar="POLYGON_S3_SECRET_KEY",
        required=True,
        help="S3 secret key (or set POLYGON_S3_SECRET_KEY).",
    )(fn)
    fn = click.option(
        "--access-key",
        envvar="POLYGON_S3_ACCESS_KEY",
        required=True,
        help="S3 access key (or set POLYGON_S3_ACCESS_KEY).",
    )(fn)
    return fn


def _to_date(value: datetime | None) -> date | None:
    return value.date() if value is not None else None


def _report(result: PullResult, dry_run: bool) -> None:
    if dry_run:
        click.echo(
            f"Dry run: {len(result.planned)} file(s) would be pulled, "
            f"{len(result.skipped)} already up to date."
        )
        for path in result.planned:
            click.echo(f"  {path}")
        return
    click.echo(
        f"Done. Wrote {len(result.written)} file(s), skipped {len(result.skipped)} "
        f"up-to-date, {len(result.empty)} with no matching rows, {len(result.failed)} failed."
    )
    for key, error in result.failed.items():
        click.echo(f"  FAILED {key}: {error}", err=True)


@click.group()
@click.version_option(__version__, prog_name="polygon-options-puller")
def cli():
    """Pull Polygon / Massive flat files from S3 and store them locally in any format.

    Works with every dataset in the flat-files bucket (stocks, options,
    indices, forex, crypto ...), streams each file so nothing is fully held
    in memory, and records what it has already pulled so it can run as a cron
    job that keeps a local cold-storage copy up to date.
    """


@cli.command("pull")
@_credential_options
@click.option(
    "-d",
    "--dataset",
    "datasets",
    multiple=True,
    required=True,
    help=(
        "Dataset alias (e.g. stocks/trades, options/quotes) or bucket prefix "
        "(e.g. us_stocks_sip/trades_v1). Repeatable."
    ),
)
@click.option(
    "-o",
    "--output-dir",
    required=True,
    type=click.Path(file_okay=False),
    help="Root directory for output files.",
)
@click.option(
    "-f",
    "--format",
    "output_format",
    type=click.Choice(format_names()),
    default="parquet",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--compression",
    default=None,
    help="Codec for the output format (e.g. zstd, snappy, gzip, lz4, none). Defaults per format.",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="First date to pull (YYYY-MM-DD).",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Last date to pull (YYYY-MM-DD). Defaults to today.",
)
@click.option(
    "--latest",
    is_flag=True,
    help="Pull only the most recent available file of each dataset.",
)
@click.option(
    "--last",
    type=click.IntRange(min=1),
    default=None,
    metavar="N",
    help="Pull the N most recent available files of each dataset.",
)
@click.option(
    "--ticker",
    "tickers",
    multiple=True,
    help="Keep only rows whose ticker equals this value. Repeatable.",
)
@click.option(
    "--ticker-prefix",
    "ticker_prefixes",
    multiple=True,
    help="Keep only rows whose ticker starts with this value (e.g. O:SPXW). Repeatable.",
)
@click.option(
    "--filter-column",
    default="ticker",
    show_default=True,
    help="Column the --ticker / --ticker-prefix filters apply to.",
)
@click.option(
    "--columns",
    default=None,
    help="Comma-separated subset of columns to keep.",
)
@click.option(
    "--column-type",
    "column_types",
    multiple=True,
    metavar="NAME=TYPE",
    help="Force an Arrow type for a column (e.g. otc=bool, trf_id=int64). Repeatable.",
)
@click.option(
    "--timestamp-columns",
    default="auto",
    show_default=True,
    help=(
        "'auto' converts Polygon's integer epoch columns to timestamps, 'none' keeps "
        "them as integers, or give comma-separated column names."
    ),
)
@click.option(
    "--timestamp-unit",
    type=click.Choice(["s", "ms", "us", "ns"]),
    default="ns",
    show_default=True,
    help="Unit of the integer epoch values.",
)
@click.option(
    "--path-template",
    default=DEFAULT_PATH_TEMPLATE,
    show_default=True,
    help=(
        "Output path relative to --output-dir. Placeholders: {dataset} {asset_class} "
        "{data_type} {name} {year} {month} {day} {date} {ext} {filename}."
    ),
)
@click.option(
    "--workers",
    type=click.IntRange(min=1),
    default=8,
    show_default=True,
    help="Number of files downloaded concurrently.",
)
@click.option("--force", is_flag=True, help="Re-download files even if already up to date.")
@click.option("--dry-run", is_flag=True, help="Show what would be pulled without downloading.")
@click.option("-q", "--quiet", is_flag=True, help="Only log warnings and errors.")
@click.option("-v", "--verbose", is_flag=True, help="Log debug output.")
def pull_cmd(
    access_key: str,
    secret_key: str,
    datasets: tuple[str, ...],
    output_dir: str,
    output_format: str,
    compression: str | None,
    start_date: datetime | None,
    end_date: datetime | None,
    latest: bool,
    last: int | None,
    tickers: tuple[str, ...],
    ticker_prefixes: tuple[str, ...],
    filter_column: str,
    columns: str | None,
    column_types: tuple[str, ...],
    timestamp_columns: str,
    timestamp_unit: str,
    path_template: str,
    workers: int,
    force: bool,
    dry_run: bool,
    quiet: bool,
    verbose: bool,
):
    """Pull flat files and store them locally.

    Select dates with --start-date/--end-date, or use --latest / --last N to
    grab the newest files (ideal for a cron job). Re-running is safe: files
    that are already up to date are skipped.
    """
    _configure_logging(verbose, quiet)

    if latest and last:
        raise click.UsageError("Use either --latest or --last N, not both.")
    n_latest = 1 if latest else last
    if n_latest is None and start_date is None:
        raise click.UsageError("Specify --start-date (and optionally --end-date), --latest, or --last N.")
    if n_latest is not None and start_date is not None:
        raise click.UsageError("--latest / --last cannot be combined with --start-date.")

    try:
        result = pull(
            access_key,
            secret_key,
            output_dir,
            list(datasets),
            start_date=_to_date(start_date),
            end_date=_to_date(end_date),
            latest=n_latest,
            output_format=output_format,
            compression="default" if compression is None else compression,
            tickers=list(tickers),
            ticker_prefixes=list(ticker_prefixes),
            filter_column=filter_column,
            columns=[c.strip() for c in columns.split(",") if c.strip()] if columns else None,
            column_types=parse_column_types(column_types),
            timestamp_columns=timestamp_columns,
            timestamp_unit=timestamp_unit,
            path_template=path_template,
            workers=workers,
            force=force,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from None

    _report(result, dry_run)
    if result.failed:
        sys.exit(1)


@cli.command("download", deprecated=True)
@_credential_options
@click.option(
    "-o",
    "--output-dir",
    required=True,
    type=click.Path(file_okay=False),
    help="Root directory for Parquet output.",
)
@click.option(
    "-t",
    "--data-type",
    type=click.Choice(list(DATA_TYPES) + ["both"]),
    default="quotes",
    show_default=True,
    help="Which options dataset to pull. 'both' pulls trades and quotes.",
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
    type=click.IntRange(min=1),
    default=8,
    show_default=True,
    help="Number of parallel download threads.",
)
@click.option(
    "--min-rows",
    type=int,
    default=None,
    hidden=True,
    help="Ignored; kept for backwards compatibility.",
)
def download(
    access_key: str,
    secret_key: str,
    output_dir: str,
    data_type: str,
    symbol_prefix: str,
    start_date: datetime,
    end_date: datetime,
    workers: int,
    min_rows: int | None,
):
    """Download options flat files filtered by symbol prefix (pre-0.3 behaviour).

    Equivalent to: pull -d options/<type> --ticker-prefix O:<SYMBOL> -f parquet
    --path-template "{name}/{date}.{ext}".
    """
    _configure_logging(verbose=False, quiet=False)
    click.echo(
        "Note: 'download' is deprecated. Use "
        f"'pull -d options/{data_type} --ticker-prefix O:{symbol_prefix} ...' instead.",
        err=True,
    )
    data_types = ["trades", "quotes"] if data_type == "both" else [data_type]
    try:
        result = pull(
            access_key,
            secret_key,
            output_dir,
            [f"options/{dt}" for dt in data_types],
            start_date=start_date.date(),
            end_date=end_date.date(),
            ticker_prefixes=[f"O:{symbol_prefix}"],
            output_format="parquet",
            path_template=LEGACY_PATH_TEMPLATE,
            workers=workers,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(f"\nDone. Wrote {len(result.written)} parquet file(s) to {output_dir}/")
    if result.failed:
        for key, error in result.failed.items():
            click.echo(f"  FAILED {key}: {error}", err=True)
        sys.exit(1)


@cli.command("list-datasets")
@_credential_options
def list_datasets_cmd(access_key: str, secret_key: str):
    """List every dataset prefix available in the bucket (and its alias, if any)."""
    s3 = get_s3_client(access_key, secret_key)
    aliases = {prefix: alias for alias, prefix in KNOWN_DATASETS.items()}
    for prefix in list_datasets(s3):
        alias = aliases.get(prefix)
        click.echo(f"{prefix:40s} {alias or ''}".rstrip())


@cli.command("list-dates")
@_credential_options
@click.option(
    "-d",
    "--dataset",
    required=True,
    help="Dataset alias or bucket prefix.",
)
@click.option("--year", type=int, default=None, help="Only list files for this year.")
@click.option("--month", type=int, default=None, help="Only list files for this month (needs --year).")
@click.option(
    "--last",
    type=click.IntRange(min=1),
    default=None,
    metavar="N",
    help="Only list the N most recent files.",
)
def list_dates(
    access_key: str,
    secret_key: str,
    dataset: str,
    year: int | None,
    month: int | None,
    last: int | None,
):
    """List the flat-file keys available for a dataset."""
    try:
        ds = resolve_dataset(dataset)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from None
    s3 = get_s3_client(access_key, secret_key)
    if last:
        for f in discover_latest(s3, ds, n=last):
            click.echo(f.key)
        return
    prefix = ds.prefix
    if year:
        prefix = f"{prefix}/{year:04d}"
        if month:
            prefix = f"{prefix}/{month:02d}"
    for key in list_keys(s3, prefix):
        click.echo(key)


@cli.command("list-formats")
def list_formats():
    """List the available output formats."""
    for name in format_names():
        cls = FORMATS[name]
        doc = (cls.__doc__ or "").strip().splitlines()[0] if cls.__doc__ else ""
        ext = cls.extension or "(source extension)"
        comp = cls.default_compression or "none"
        click.echo(f"{name:10s} .{ext:9s} default compression: {comp:6s} {doc}")
