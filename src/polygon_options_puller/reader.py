"""Streaming CSV reader with head-sampled schema inference.

Polygon flat files are gzipped CSVs, often tens of gigabytes each.  We never
materialise a whole file: the object is decompressed and parsed in blocks, and
each block is handed to the caller as an Arrow ``RecordBatch``.

Arrow's streaming CSV reader fixes the schema from the first block it sees,
which breaks on columns that are empty early in the file and populated later
(``trf_timestamp``, ``indicators`` ...).  To avoid that we read a sample from
the head of the file, infer a schema from it, promote empty columns to a known
hint type (or string), and then stream the entire file with that fixed schema.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.csv as pacsv

from .config import COLUMN_TYPE_HINTS, TIMESTAMP_COLUMN_NAMES

DEFAULT_PEEK_BYTES = 16 * 1024 * 1024
DEFAULT_BLOCK_SIZE = 32 * 1024 * 1024

_COMPRESSION_SUFFIXES = {
    ".gz": "gzip",
    ".gzip": "gzip",
    ".zst": "zstd",
    ".bz2": "bz2",
    ".lz4": "lz4",
}


def compression_for_name(name: str) -> str | None:
    """Guess the compression codec of a file from its name (``None`` if uncompressed)."""
    lower = name.lower()
    for suffix, codec in _COMPRESSION_SUFFIXES.items():
        if lower.endswith(suffix):
            return codec
    return None


def parse_type(alias: str) -> pa.DataType:
    """Turn an Arrow type alias such as ``int64`` or ``timestamp[ms]`` into a DataType."""
    try:
        return pa.type_for_alias(alias.strip())
    except (ValueError, KeyError) as exc:
        raise ValueError(f"Unknown Arrow type {alias!r}") from exc


def parse_column_types(items: Iterable[str]) -> dict[str, pa.DataType]:
    """Parse ``NAME=TYPE`` strings (as given on the command line) into a type mapping."""
    types: dict[str, pa.DataType] = {}
    for item in items:
        name, sep, alias = item.partition("=")
        if not sep or not name.strip() or not alias.strip():
            raise ValueError(f"Column type must look like NAME=TYPE, got {item!r}")
        types[name.strip()] = parse_type(alias)
    return types


@dataclass
class ReadOptions:
    """How to parse a flat file into Arrow batches."""

    #: Subset of columns to keep (``None`` keeps every column).
    columns: list[str] | None = None
    #: Explicit Arrow types by column name; overrides inference and hints.
    column_types: dict[str, pa.DataType] = field(default_factory=dict)
    #: ``"auto"`` converts known epoch columns, ``"none"`` converts nothing,
    #: or pass an explicit list of column names.
    timestamp_columns: list[str] | str = "auto"
    #: Unit of the integer epoch values (``s``, ``ms``, ``us`` or ``ns``).
    timestamp_unit: str = "ns"
    #: Bytes of decompressed data sampled to infer the schema.
    peek_bytes: int = DEFAULT_PEEK_BYTES
    #: Bytes of decompressed data parsed per batch.
    block_size: int = DEFAULT_BLOCK_SIZE

    def signature(self) -> str:
        """Stable string describing the options that affect output content."""
        cols = ",".join(self.columns) if self.columns else "*"
        types = ",".join(f"{k}={v}" for k, v in sorted(self.column_types.items()))
        ts = self.timestamp_columns
        ts_str = ts if isinstance(ts, str) else ",".join(ts)
        return f"columns={cols};types={types};ts={ts_str};unit={self.timestamp_unit}"


class _ChainedReader(io.RawIOBase):
    """Serve the already-consumed *head* bytes first, then the rest of *stream*."""

    def __init__(self, head: bytes, stream):
        super().__init__()
        self._head = memoryview(head)
        self._pos = 0
        self._stream = stream

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        want = len(buffer)
        if self._pos < len(self._head):
            chunk = self._head[self._pos : self._pos + want]
            n = len(chunk)
            buffer[:n] = chunk
            self._pos += n
            return n
        data = self._stream.read(want)
        n = len(data)
        buffer[:n] = data
        return n


def _read_exact(stream, n: int) -> bytes:
    """Read up to *n* bytes, looping until *n* bytes are collected or EOF."""
    parts: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = stream.read(remaining)
        if not chunk:
            break
        parts.append(chunk)
        remaining -= len(chunk)
    return b"".join(parts)


def infer_schema(sample: bytes, options: ReadOptions) -> pa.Schema:
    """Infer a fixed schema from a CSV *sample* (header plus complete rows)."""
    convert = pacsv.ConvertOptions(
        include_columns=options.columns,
        column_types=options.column_types,
    )
    table = pacsv.read_csv(pa.BufferReader(sample), convert_options=convert)
    fields = []
    for f in table.schema:
        dtype = f.type
        if f.name in options.column_types:
            dtype = options.column_types[f.name]
        elif pa.types.is_null(dtype):
            hint = COLUMN_TYPE_HINTS.get(f.name)
            dtype = parse_type(hint) if hint else pa.string()
        fields.append(pa.field(f.name, dtype))
    return pa.schema(fields)


def resolve_timestamp_columns(schema: pa.Schema, spec: list[str] | str | None) -> list[str]:
    """Decide which integer columns should be converted to timestamps."""
    if spec is None or spec == "none" or spec == []:
        return []
    if spec == "auto":
        return [
            f.name
            for f in schema
            if (f.name in TIMESTAMP_COLUMN_NAMES or f.name.endswith("_timestamp"))
            and pa.types.is_integer(f.type)
        ]
    if isinstance(spec, str):
        spec = [s.strip() for s in spec.split(",") if s.strip()]
    missing = [c for c in spec if c not in schema.names]
    if missing:
        raise ValueError(f"Timestamp column(s) {missing} not found in file columns {schema.names}")
    bad = [c for c in spec if not pa.types.is_integer(schema.field(c).type)]
    if bad:
        raise ValueError(f"Timestamp column(s) {bad} are not integer columns and cannot be converted")
    return list(spec)


def timestamp_type(unit: str) -> pa.DataType:
    return pa.timestamp(unit, tz="UTC")


def convert_timestamps(batch: pa.RecordBatch, columns: list[str], unit: str) -> pa.RecordBatch:
    """Cast the named integer columns of *batch* to UTC timestamps of *unit*."""
    if not columns:
        return batch
    ts_type = timestamp_type(unit)
    for name in columns:
        idx = batch.schema.get_field_index(name)
        batch = batch.set_column(idx, pa.field(name, ts_type), batch.column(idx).cast(ts_type))
    return batch


def output_schema(schema: pa.Schema, columns: list[str], unit: str) -> pa.Schema:
    """Return *schema* with the named columns replaced by UTC timestamps of *unit*."""
    if not columns:
        return schema
    ts_type = timestamp_type(unit)
    fields = [pa.field(f.name, ts_type) if f.name in columns else f for f in schema]
    return pa.schema(fields)


class CsvStream:
    """Iterable of Arrow record batches parsed from one flat file."""

    def __init__(self, reader, schema: pa.Schema | None, timestamp_columns: list[str], unit: str):
        self._reader = reader
        self.schema = schema
        self.timestamp_columns = timestamp_columns
        self.unit = unit

    @property
    def empty(self) -> bool:
        return self._reader is None

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        if self._reader is None:
            return
        for batch in self._reader:
            yield convert_timestamps(batch, self.timestamp_columns, self.unit)


def open_csv_stream(
    fileobj,
    compression: str | None = None,
    options: ReadOptions | None = None,
) -> CsvStream:
    """Open a (possibly compressed) CSV file object as a stream of record batches."""
    options = options or ReadOptions()
    source = pa.input_stream(fileobj, compression=compression)

    head = _read_exact(source, options.peek_bytes)
    at_eof = len(head) < options.peek_bytes
    if at_eof:
        sample = head
    else:
        cut = head.rfind(b"\n")
        sample = head[: cut + 1] if cut >= 0 else head

    if not sample.strip():
        return CsvStream(None, None, [], options.timestamp_unit)

    schema = infer_schema(sample, options)
    ts_columns = resolve_timestamp_columns(schema, options.timestamp_columns)

    chained = io.BufferedReader(_ChainedReader(head, source), buffer_size=1 << 20)
    reader = pacsv.open_csv(
        chained,
        read_options=pacsv.ReadOptions(block_size=options.block_size),
        convert_options=pacsv.ConvertOptions(
            include_columns=options.columns,
            column_types={f.name: f.type for f in schema},
        ),
    )
    return CsvStream(
        reader,
        output_schema(schema, ts_columns, options.timestamp_unit),
        ts_columns,
        options.timestamp_unit,
    )
