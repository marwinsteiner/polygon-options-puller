"""Pluggable output formats.

Every format is a :class:`Writer` subclass registered in :data:`FORMATS`.
Writers receive Arrow record batches one at a time and must never need the
whole file in memory.  Register your own with :func:`register_format`::

    from polygon_options_puller.formats import Writer, register_format

    @register_format
    class MyWriter(Writer):
        name = "myformat"
        extension = "myext"

        def _open(self, schema): ...
        def _write(self, batch): ...
        def _close(self): ...
"""

from __future__ import annotations

import gzip
import json
import shutil
from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

_NO_COMPRESSION = {None, "", "none", "None", "NONE", "uncompressed"}


def normalize_compression(value: str | None) -> str | None:
    """Map the various spellings of "no compression" to ``None``."""
    return None if value in _NO_COMPRESSION else value


class Writer(ABC):
    """Base class for output formats.

    Subclasses set :attr:`name` (the ``--format`` value), :attr:`extension`
    (appended to output paths) and implement ``_open``, ``_write`` and
    ``_close``.  The file is opened lazily on the first batch, so a filter that
    matches nothing produces no output file.
    """

    #: Name used on the command line and in :data:`FORMATS`.
    name: ClassVar[str]
    #: File extension without the leading dot (``parquet``, ``csv.gz``).
    extension: ClassVar[str]
    #: Raw writers copy the source bytes and never see record batches.
    raw: ClassVar[bool] = False
    #: Compression used when the caller does not specify one.
    default_compression: ClassVar[str | None] = None

    def __init__(self, path: str | Path, *, compression: str | None = "default", **options: Any):
        self.path = Path(path)
        if compression == "default":
            self.compression = self.default_compression
        else:
            self.compression = normalize_compression(compression)
        self.options = options
        self.rows = 0
        self._opened = False

    # -- public API ------------------------------------------------------- #

    def write_batch(self, batch: pa.RecordBatch) -> None:
        if self.raw:
            raise TypeError(f"{self.name!r} is a raw format and does not accept record batches")
        if not self._opened:
            self._open(batch.schema)
            self._opened = True
        self._write(batch)
        self.rows += batch.num_rows

    def write_stream(self, fileobj) -> None:
        """Copy raw bytes from *fileobj*; only implemented by raw formats."""
        raise TypeError(f"{self.name!r} does not support raw byte streaming")

    def close(self) -> None:
        if self._opened:
            try:
                self._close()
            finally:
                self._opened = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    # -- subclass hooks --------------------------------------------------- #

    @abstractmethod
    def _open(self, schema: pa.Schema) -> None: ...

    @abstractmethod
    def _write(self, batch: pa.RecordBatch) -> None: ...

    @abstractmethod
    def _close(self) -> None: ...


FORMATS: dict[str, type[Writer]] = {}


def register_format(cls: type[Writer]) -> type[Writer]:
    """Register a :class:`Writer` subclass under its ``name``.  Usable as a decorator."""
    FORMATS[cls.name] = cls
    return cls


def get_format(name: str) -> type[Writer]:
    try:
        return FORMATS[name]
    except KeyError:
        raise ValueError(
            f"Unknown output format {name!r}. Available: {', '.join(format_names())}"
        ) from None


def format_names() -> list[str]:
    return sorted(FORMATS)


# --------------------------------------------------------------------------- #
# Built-in formats
# --------------------------------------------------------------------------- #


@register_format
class ParquetWriter(Writer):
    """Apache Parquet, dictionary-encoded, zstd by default (snappy, gzip, lz4, brotli, none)."""

    name = "parquet"
    extension = "parquet"
    default_compression = "zstd"

    def _open(self, schema: pa.Schema) -> None:
        self._writer = pq.ParquetWriter(
            str(self.path),
            schema,
            compression=self.compression or "none",
            use_dictionary=self.options.get("use_dictionary", True),
        )

    def _write(self, batch: pa.RecordBatch) -> None:
        self._writer.write_batch(batch)

    def _close(self) -> None:
        self._writer.close()


@register_format
class FeatherWriter(Writer):
    """Arrow IPC file (Feather v2), zstd by default (lz4 or none)."""

    name = "feather"
    extension = "feather"
    default_compression = "zstd"

    def _open(self, schema: pa.Schema) -> None:
        opts = pa.ipc.IpcWriteOptions(compression=self.compression)
        # Own the sink so the file handle is released before the atomic rename.
        self._sink = pa.OSFile(str(self.path), "wb")
        self._writer = pa.ipc.new_file(self._sink, schema, options=opts)

    def _write(self, batch: pa.RecordBatch) -> None:
        self._writer.write_batch(batch)

    def _close(self) -> None:
        try:
            self._writer.close()
        finally:
            self._sink.close()


def _naive_utc_schema(schema: pa.Schema) -> pa.Schema:
    """Replace timezone-aware timestamp fields with naive ones (values stay UTC)."""
    fields = [
        pa.field(f.name, pa.timestamp(f.type.unit)) if pa.types.is_timestamp(f.type) and f.type.tz
        else f
        for f in schema
    ]
    return pa.schema(fields)


def _naive_utc_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    target = _naive_utc_schema(batch.schema)
    return batch if target.equals(batch.schema) else batch.cast(target)


def _csv_cell(value: str) -> str:
    if any(ch in value for ch in ',"\r\n'):
        return '"' + value.replace('"', '""') + '"'
    return value


@register_format
class CsvWriter(Writer):
    """Plain CSV with a header row; timestamps are written as ISO-8601 in UTC."""

    name = "csv"
    extension = "csv"
    default_compression = None

    def _open(self, schema: pa.Schema) -> None:
        self._sink = pa.output_stream(str(self.path), compression=self.compression)
        # Arrow always quotes header cells; write it ourselves so the output
        # has the same unquoted header Polygon ships.
        self._sink.write((",".join(_csv_cell(n) for n in schema.names) + "\n").encode("utf-8"))
        self._writer = pacsv.CSVWriter(
            self._sink,
            _naive_utc_schema(schema),
            write_options=pacsv.WriteOptions(include_header=False, quoting_style="needed"),
        )

    def _write(self, batch: pa.RecordBatch) -> None:
        # Arrow needs an IANA tz database to format zone-aware timestamps, which
        # Windows lacks by default; the values are already UTC so drop the zone.
        self._writer.write_batch(_naive_utc_batch(batch))

    def _close(self) -> None:
        try:
            self._writer.close()
        finally:
            self._sink.close()


@register_format
class CsvGzWriter(CsvWriter):
    """Gzipped CSV, the same shape Polygon ships."""

    name = "csv.gz"
    extension = "csv.gz"
    default_compression = "gzip"


def _stringify_timestamps(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Render timestamp columns as ISO-8601 strings at full precision.

    Python ``datetime`` cannot hold nanoseconds and zone-aware formatting needs
    a tz database, so timestamps are converted in Arrow: zone-aware values are
    written as UTC with a ``Z`` suffix, naive values without one.
    """
    columns = list(batch.columns)
    fields = list(batch.schema)
    changed = False
    for i, f in enumerate(fields):
        if not pa.types.is_timestamp(f.type):
            continue
        arr = batch.column(i)
        aware = bool(f.type.tz)
        if aware:
            arr = arr.cast(pa.timestamp(f.type.unit))
        text = pc.replace_substring(arr.cast(pa.string()), pattern=" ", replacement="T")
        if aware:
            text = pc.binary_join_element_wise(text, "Z", "")
        columns[i] = text
        fields[i] = pa.field(f.name, pa.string())
        changed = True
    if not changed:
        return batch
    return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


@register_format
class JsonlWriter(Writer):
    """Newline-delimited JSON, one object per row; timestamps as ISO-8601 strings."""

    name = "jsonl"
    extension = "jsonl"
    default_compression = None

    def _open(self, schema: pa.Schema) -> None:
        if self.compression is None:
            self._fh = open(self.path, "wb")
        elif self.compression == "gzip":
            self._fh = gzip.open(self.path, "wb")
        else:
            raise ValueError(f"jsonl supports gzip or no compression, not {self.compression!r}")

    def _write(self, batch: pa.RecordBatch) -> None:
        lines = [
            json.dumps(row, default=_json_default, separators=(",", ":"))
            for row in _stringify_timestamps(batch).to_pylist()
        ]
        if lines:
            self._fh.write(("\n".join(lines) + "\n").encode("utf-8"))

    def _close(self) -> None:
        self._fh.close()


@register_format
class JsonlGzWriter(JsonlWriter):
    """Gzipped newline-delimited JSON."""

    name = "jsonl.gz"
    extension = "jsonl.gz"
    default_compression = "gzip"


@register_format
class RawWriter(Writer):
    """Byte-for-byte copy of the source object (no parsing, no filtering)."""

    name = "raw"
    extension = ""  # keeps the source file's own extension
    raw = True

    def write_stream(self, fileobj) -> None:
        with open(self.path, "wb") as fh:
            shutil.copyfileobj(fileobj, fh, 1 << 20)

    def _open(self, schema: pa.Schema) -> None:  # pragma: no cover - never called
        raise TypeError("raw format does not accept record batches")

    def _write(self, batch: pa.RecordBatch) -> None:  # pragma: no cover - never called
        raise TypeError("raw format does not accept record batches")

    def _close(self) -> None:  # pragma: no cover - never called
        pass
