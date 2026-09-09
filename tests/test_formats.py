import gzip
import io
import json

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.feather as pf
import pyarrow.parquet as pq
import pytest

from polygon_options_puller.formats import (
    FORMATS,
    Writer,
    format_names,
    get_format,
    normalize_compression,
    register_format,
)

SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("px", pa.float64()),
        ("ts", pa.timestamp("ns", tz="UTC")),
    ]
)


def _batch(n: int) -> pa.RecordBatch:
    return pa.record_batch(
        [
            pa.array([f"T{i}" for i in range(n)]),
            pa.array([float(i) for i in range(n)]),
            pa.array([1_700_000_000_000_000_000 + i for i in range(n)]).cast(SCHEMA.field("ts").type),
        ],
        schema=SCHEMA,
    )


def _read_back(name: str, path) -> int:
    if name == "parquet":
        return pq.read_table(path).num_rows
    if name == "feather":
        return pf.read_table(path).num_rows
    if name in ("csv", "csv.gz"):
        raw = gzip.open(path, "rb").read() if name == "csv.gz" else open(path, "rb").read()
        return pacsv.read_csv(io.BytesIO(raw)).num_rows
    if name in ("jsonl", "jsonl.gz"):
        raw = gzip.open(path, "rb").read() if name == "jsonl.gz" else open(path, "rb").read()
        rows = [json.loads(line) for line in raw.decode().splitlines()]
        assert rows[0]["ticker"] == "T0"
        assert rows[1]["ts"] == "2023-11-14T22:13:20.000000001Z"  # nanoseconds preserved
        return len(rows)
    raise AssertionError(name)


@pytest.mark.parametrize("name", [n for n in format_names() if n != "raw"])
def test_write_two_batches_and_read_back(tmp_path, name):
    cls = get_format(name)
    path = tmp_path / f"out.{cls.extension}"
    with cls(path) as writer:
        writer.write_batch(_batch(3))
        writer.write_batch(_batch(4))
    assert writer.rows == 7
    assert path.exists()
    assert _read_back(name, path) == 7


def test_parquet_default_compression_is_zstd(tmp_path):
    path = tmp_path / "a.parquet"
    with FORMATS["parquet"](path) as w:
        w.write_batch(_batch(2))
    assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == "ZSTD"


def test_parquet_explicit_compression(tmp_path):
    path = tmp_path / "a.parquet"
    with FORMATS["parquet"](path, compression="snappy") as w:
        w.write_batch(_batch(2))
    assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == "SNAPPY"
    with FORMATS["parquet"](tmp_path / "b.parquet", compression="none") as w:
        w.write_batch(_batch(2))
    assert pq.ParquetFile(tmp_path / "b.parquet").metadata.row_group(0).column(0).compression == "UNCOMPRESSED"


def test_csv_with_gzip_compression_option(tmp_path):
    path = tmp_path / "a.csv"
    with FORMATS["csv"](path, compression="gzip") as w:
        w.write_batch(_batch(2))
    assert gzip.open(path).read().startswith(b"ticker,px,ts")


def test_lazy_open_writes_nothing_without_batches(tmp_path):
    path = tmp_path / "never.parquet"
    w = FORMATS["parquet"](path)
    w.close()
    assert not path.exists()


def test_raw_copies_bytes(tmp_path):
    src = b"\x1f\x8b some bytes"
    path = tmp_path / "x.csv.gz"
    w = FORMATS["raw"](path)
    w.write_stream(io.BytesIO(src))
    assert path.read_bytes() == src
    with pytest.raises(TypeError):
        w.write_batch(_batch(1))
    with pytest.raises(TypeError):
        FORMATS["parquet"](tmp_path / "y").write_stream(io.BytesIO(b""))


def test_jsonl_rejects_unknown_compression(tmp_path):
    with pytest.raises(ValueError):
        with FORMATS["jsonl"](tmp_path / "a.jsonl", compression="zstd") as w:
            w.write_batch(_batch(1))


def test_register_custom_format(tmp_path):
    @register_format
    class LineCount(Writer):
        name = "linecount-test"
        extension = "txt"

        def _open(self, schema):
            self._fh = open(self.path, "w")

        def _write(self, batch):
            self._fh.write(f"{batch.num_rows}\n")

        def _close(self):
            self._fh.close()

    try:
        assert get_format("linecount-test") is LineCount
        with LineCount(tmp_path / "c.txt") as w:
            w.write_batch(_batch(5))
        assert (tmp_path / "c.txt").read_text() == "5\n"
    finally:
        FORMATS.pop("linecount-test", None)


def test_unknown_format():
    with pytest.raises(ValueError, match="Unknown output format"):
        get_format("xlsx")


def test_normalize_compression():
    assert normalize_compression("none") is None
    assert normalize_compression("") is None
    assert normalize_compression("zstd") == "zstd"
