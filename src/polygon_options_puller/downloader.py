"""Orchestrates discovery, streaming download, filtering and writing of flat files."""

from __future__ import annotations

import logging
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable, Sequence

import pyarrow as pa
from tqdm import tqdm

from .config import DATA_TYPES
from .datasets import Dataset, FlatFile, discover_files, discover_latest, resolve_dataset
from .filters import BatchFilter, build_filter, filter_signature
from .formats import Writer, get_format, normalize_compression
from .manifest import Manifest
from .reader import ReadOptions, compression_for_name, open_csv_stream
from .s3 import get_s3_client, open_object

log = logging.getLogger(__name__)

#: Mirrors the bucket layout: ``us_stocks_sip/trades_v1/2025/03/2025-03-17.parquet``
DEFAULT_PATH_TEMPLATE = "{dataset}/{year}/{month}/{date}.{ext}"
#: Layout written by versions before 0.3: ``quotes/2025-03-17.parquet``
LEGACY_PATH_TEMPLATE = "{name}/{date}.{ext}"

_TEMPLATE_PLACEHOLDERS = (
    "dataset",
    "asset_class",
    "data_type",
    "name",
    "year",
    "month",
    "day",
    "date",
    "ext",
    "filename",
)


@dataclass
class PullResult:
    """Outcome of a :func:`pull` call."""

    #: Output files written (or overwritten) during this run.
    written: list[Path] = field(default_factory=list)
    #: Output files that were already up to date.
    skipped: list[Path] = field(default_factory=list)
    #: S3 keys that were scanned but matched no rows (no file written).
    empty: list[str] = field(default_factory=list)
    #: S3 keys that failed, mapped to the error message.
    failed: dict[str, str] = field(default_factory=dict)
    #: Output files that *would* be written (``dry_run=True`` only).
    planned: list[Path] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed


def render_path(template: str, file: FlatFile, extension: str) -> Path:
    """Render an output path (relative to the output directory) for *file*.

    ``extension`` is the format's extension; when empty (raw format) the
    source file's own extension is used.
    """
    d = file.date
    ds = file.dataset
    values = {
        "dataset": ds.prefix,
        "asset_class": ds.asset_class,
        "data_type": ds.data_type,
        "name": ds.name,
        "year": f"{d.year:04d}",
        "month": f"{d.month:02d}",
        "day": f"{d.day:02d}",
        "date": d.isoformat(),
        "ext": extension or file.extension,
        "filename": file.filename,
    }
    try:
        rendered = template.format(**values)
    except (KeyError, IndexError, ValueError) as exc:
        raise ValueError(
            f"Bad path template {template!r} ({exc}). Placeholders: "
            + ", ".join("{" + p + "}" for p in _TEMPLATE_PLACEHOLDERS)
        ) from None
    path = Path(rendered)
    if path.is_absolute() or ".." in path.parts or not rendered.strip():
        raise ValueError(f"Path template must produce a relative path, got {rendered!r}")
    return path


@dataclass(frozen=True)
class _Job:
    file: FlatFile
    dest: Path
    rel: str


def _cleanup_stale_tmp(output_dir: Path) -> None:
    """Delete leftover ``*.tmp`` files from interrupted runs."""
    for tmp in output_dir.rglob("*.tmp"):
        try:
            tmp.unlink()
        except OSError:
            pass


def _select_files(
    s3_client,
    datasets: Sequence[Dataset],
    start_date: date | None,
    end_date: date | None,
    latest: int | None,
) -> list[FlatFile]:
    if latest is not None and start_date is not None:
        raise ValueError("Use either latest=N or start_date/end_date, not both.")
    if latest is None and start_date is None:
        raise ValueError("Specify start_date (and optionally end_date) or latest=N.")
    files: list[FlatFile] = []
    for ds in datasets:
        if latest is not None:
            found = discover_latest(s3_client, ds, n=latest)
        else:
            found = discover_files(s3_client, ds, start_date, end_date or date.today())
        if not found:
            log.warning("%s: no files found in the requested range", ds)
        files.extend(found)
    return files


def _skip_reason(job: _Job, manifest: Manifest, signature: str, force: bool) -> str | None:
    """Return why *job* can be skipped, or ``None`` if it must be processed."""
    if force:
        return None
    entry = manifest.get(job.rel)
    if entry is not None:
        if not manifest.is_current(job.rel, job.file.etag, signature):
            return None  # upstream object or settings changed
        if not entry.get("written") or job.dest.exists():
            return "up to date"
        return None  # output was deleted; rebuild it
    if job.dest.exists():
        return "exists"  # written by an older version, or by hand; left alone
    return None


def _process_file(
    s3_client,
    job: _Job,
    fmt_cls: type[Writer],
    fmt_options: dict,
    read_options: ReadOptions,
    batch_filter: BatchFilter | None,
    row_filter: BatchFilter | None,
    manifest: Manifest,
    signature: str,
) -> tuple[str, int | None]:
    """Stream one object from S3 into its output file.  Returns ``(status, rows)``."""
    file = job.file
    dest = job.dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")

    rows: int | None
    try:
        fileobj = open_object(s3_client, file.key)
        writer = fmt_cls(tmp, **fmt_options)
        try:
            if fmt_cls.raw:
                writer.write_stream(fileobj)
                rows = None
            else:
                stream = open_csv_stream(
                    fileobj,
                    compression=compression_for_name(file.key),
                    options=read_options,
                )
                for batch in stream:
                    if batch_filter is not None:
                        batch = batch_filter(batch)
                    if row_filter is not None:
                        batch = row_filter(batch)
                    if batch is None or batch.num_rows == 0:
                        continue
                    writer.write_batch(batch)
                rows = writer.rows
        finally:
            try:
                writer.close()
            finally:
                fileobj.close()
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    if rows == 0:
        tmp.unlink(missing_ok=True)
        manifest.record(
            job.rel,
            key=file.key,
            etag=file.etag,
            size=file.size,
            rows=0,
            signature=signature,
            format=fmt_cls.name,
            written=False,
        )
        return "empty", 0

    os.replace(tmp, dest)
    manifest.record(
        job.rel,
        key=file.key,
        etag=file.etag,
        size=file.size,
        rows=rows,
        signature=signature,
        format=fmt_cls.name,
        written=True,
    )
    return "written", rows


def pull(
    access_key: str | None = None,
    secret_key: str | None = None,
    output_dir: str | Path = ".",
    datasets: Sequence[str | Dataset] | str | None = None,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    latest: int | None = None,
    output_format: str = "parquet",
    compression: str | None = "default",
    format_options: dict | None = None,
    tickers: Iterable[str] = (),
    ticker_prefixes: Iterable[str] = (),
    filter_column: str = "ticker",
    columns: Sequence[str] | None = None,
    column_types: dict[str, pa.DataType] | None = None,
    timestamp_columns: Sequence[str] | str = "auto",
    timestamp_unit: str = "ns",
    path_template: str = DEFAULT_PATH_TEMPLATE,
    workers: int = 8,
    force: bool = False,
    dry_run: bool = False,
    row_filter: BatchFilter | None = None,
    progress: bool = True,
    s3_client=None,
    # Deprecated (pre-0.3) parameters, kept so old scripts keep working.
    data_types: Sequence[str] | None = None,
    symbol_prefix: str | None = None,
    min_rows: int | None = None,
) -> PullResult:
    """Pull flat files from the Polygon/Massive bucket and store them locally.

    Parameters
    ----------
    access_key, secret_key:
        Polygon / Massive S3 credentials (not needed when *s3_client* is given).
    output_dir:
        Root directory for output files and the idempotency manifest.
    datasets:
        One or more dataset aliases (``"stocks/trades"``) or bucket prefixes
        (``"us_options_opra/quotes_v1"``).
    start_date, end_date:
        Inclusive date range.  *end_date* defaults to today.
    latest:
        Instead of a date range, pull the N most recent files of each dataset.
    output_format:
        Any registered format: ``parquet``, ``feather``, ``csv``, ``csv.gz``,
        ``jsonl``, ``jsonl.gz`` or ``raw`` (byte-for-byte copy).
    compression:
        Codec for the output format; ``"default"`` picks the format's default.
    format_options:
        Extra keyword arguments passed to the format's writer.
    tickers, ticker_prefixes, filter_column:
        Keep only rows whose *filter_column* equals one of *tickers* or starts
        with one of *ticker_prefixes*.  No filter keeps every row.
    columns:
        Subset of columns to keep.
    column_types:
        Explicit Arrow types by column name (``{"otc": pa.bool_()}``).
    timestamp_columns, timestamp_unit:
        ``"auto"`` converts Polygon's integer epoch columns to Arrow
        timestamps, ``"none"`` leaves them as integers, or pass column names.
    path_template:
        Output path relative to *output_dir*; see :data:`DEFAULT_PATH_TEMPLATE`.
    workers:
        Number of files downloaded concurrently.
    force:
        Re-download even when the manifest says a file is up to date.
    dry_run:
        Only report what would be pulled.
    row_filter:
        Optional callable applied to every record batch after the ticker
        filter; return a (possibly smaller) batch or ``None`` to drop it.
    progress:
        Show a progress bar (auto-disabled when stderr is not a terminal).
    s3_client:
        Pre-built boto3 client, mainly for testing.

    Returns
    -------
    :class:`PullResult` with written, skipped, empty and failed files.
    """
    # -- legacy argument mapping ----------------------------------------- #
    if data_types is not None or symbol_prefix is not None or min_rows is not None:
        warnings.warn(
            "pull(data_types=..., symbol_prefix=..., min_rows=...) is deprecated; "
            "use datasets=['options/quotes'], ticker_prefixes=['O:AAPL'] instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if data_types:
            unknown = [d for d in data_types if d not in DATA_TYPES]
            if unknown:
                raise ValueError(
                    f"Unknown data_type(s) {unknown}. Choose from: {', '.join(DATA_TYPES)}"
                )
            datasets = [f"options/{d}" for d in data_types]
            if path_template == DEFAULT_PATH_TEMPLATE:
                path_template = LEGACY_PATH_TEMPLATE
        if symbol_prefix:
            ticker_prefixes = [*ticker_prefixes, f"O:{symbol_prefix}"]

    # -- validate inputs ------------------------------------------------- #
    if isinstance(datasets, (str, Dataset)):
        datasets = [datasets]
    if not datasets:
        raise ValueError("At least one dataset is required.")
    resolved = [ds if isinstance(ds, Dataset) else resolve_dataset(ds) for ds in datasets]

    fmt_cls = get_format(output_format)
    if compression == "default":
        resolved_compression = fmt_cls.default_compression
    else:
        resolved_compression = normalize_compression(compression)
    fmt_options = dict(format_options or {})
    fmt_options["compression"] = resolved_compression

    tickers = list(tickers)
    ticker_prefixes = list(ticker_prefixes)
    batch_filter = build_filter(filter_column, tickers, ticker_prefixes)
    if fmt_cls.raw and (batch_filter or row_filter or columns or column_types):
        raise ValueError(
            "The raw format copies files byte-for-byte and cannot filter rows or "
            "select columns; choose a parsed format such as parquet or csv.gz."
        )

    read_options = ReadOptions(
        columns=list(columns) if columns else None,
        column_types=dict(column_types or {}),
        timestamp_columns=(
            timestamp_columns if isinstance(timestamp_columns, str) else list(timestamp_columns)
        ),
        timestamp_unit=timestamp_unit,
    )
    signature = "|".join(
        [
            fmt_cls.name,
            str(resolved_compression),
            filter_signature(filter_column, tickers, ticker_prefixes),
            read_options.signature(),
            "row_filter" if row_filter is not None else "",
        ]
    )

    if s3_client is None:
        if not access_key or not secret_key:
            raise ValueError("access_key and secret_key are required (or pass s3_client).")
        s3_client = get_s3_client(access_key, secret_key)

    # -- discover and plan ------------------------------------------------ #
    files = _select_files(s3_client, resolved, start_date, end_date, latest)

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    _cleanup_stale_tmp(root)
    manifest = Manifest(root)

    jobs: list[_Job] = []
    for file in files:
        rel = render_path(path_template, file, fmt_cls.extension)
        jobs.append(_Job(file=file, dest=root / rel, rel=rel.as_posix()))

    result = PullResult()
    todo: list[_Job] = []
    for job in jobs:
        reason = _skip_reason(job, manifest, signature, force)
        if reason:
            log.info("skip %s (%s)", job.rel, reason)
            result.skipped.append(job.dest)
        else:
            todo.append(job)

    if dry_run:
        for job in todo:
            log.info("would pull %s -> %s", job.file.key, job.rel)
            result.planned.append(job.dest)
        return result
    if not todo:
        log.info("Nothing to do: %d file(s) already up to date.", len(result.skipped))
        return result

    # -- execute ----------------------------------------------------------- #
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(
                _process_file,
                s3_client,
                job,
                fmt_cls,
                fmt_options,
                read_options,
                batch_filter,
                row_filter,
                manifest,
                signature,
            ): job
            for job in todo
        }
        with tqdm(
            total=len(futures),
            desc="Pulling",
            unit="file",
            disable=None if progress else True,
        ) as bar:
            for future in as_completed(futures):
                job = futures[future]
                try:
                    status, rows = future.result()
                except Exception as exc:  # noqa: BLE001 - reported per file
                    result.failed[job.file.key] = f"{type(exc).__name__}: {exc}"
                    log.error("%s: %s: %s", job.file.key, type(exc).__name__, exc)
                else:
                    if status == "written":
                        result.written.append(job.dest)
                        log.info("wrote %s (%s rows)", job.rel, "?" if rows is None else rows)
                    else:
                        result.empty.append(job.file.key)
                        log.info("no matching rows in %s", job.file.key)
                bar.update(1)

    return result
