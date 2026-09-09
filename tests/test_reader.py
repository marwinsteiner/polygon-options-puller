import gzip
import io

import pyarrow as pa
import pytest

from conftest import NS, csv_bytes, quotes_csv
from polygon_options_puller.reader import (
    ReadOptions,
    compression_for_name,
    open_csv_stream,
    parse_column_types,
    parse_type,
)


def _read_all(data: bytes, compression="gzip", **kwargs):
    stream = open_csv_stream(io.BytesIO(data), compression=compression, options=ReadOptions(**kwargs))
    batches = list(stream)
    table = pa.Table.from_batches(batches, schema=stream.schema) if batches else None
    return stream, table


def test_basic_gzip_with_auto_timestamps():
    stream, table = _read_all(quotes_csv(["AAPL", "MSFT"], per_ticker=2))
    assert table.num_rows == 4
    assert table.schema.field("sip_timestamp").type == pa.timestamp("ns", tz="UTC")
    assert stream.schema == table.schema
    assert table.column("ticker").to_pylist()[:2] == ["AAPL", "AAPL"]
    assert table.column("ask_price").type == pa.float64()


def test_timestamp_unit_and_explicit_columns():
    data = csv_bytes(["ticker", "t", "sip_timestamp"], [["A", 1_700_000_000_000, 5]])
    _, table = _read_all(data, timestamp_columns=["t"], timestamp_unit="ms")
    assert table.schema.field("t").type == pa.timestamp("ms", tz="UTC")
    assert table.schema.field("sip_timestamp").type == pa.int64()


def test_timestamp_none_keeps_integers():
    _, table = _read_all(quotes_csv(["AAPL"]), timestamp_columns="none")
    assert table.schema.field("sip_timestamp").type == pa.int64()


def test_timestamp_missing_or_non_integer_column_errors():
    with pytest.raises(ValueError, match="not found"):
        _read_all(quotes_csv(["AAPL"]), timestamp_columns=["nope"])
    with pytest.raises(ValueError, match="not integer"):
        _read_all(quotes_csv(["AAPL"]), timestamp_columns=["ticker"])


def test_sparse_column_promoted_to_string_across_blocks():
    # Column is empty in the sampled head and populated much later in the file.
    rows = [[f"T{i}", i, None] for i in range(3000)] + [[f"T{i}", i, 1] for i in range(3000)]
    data = csv_bytes(["ticker", "val", "corr_x"], rows, compress=False)
    stream, table = _read_all(data, compression=None, peek_bytes=2048, block_size=2048)
    assert table.num_rows == 6000
    assert table.schema.field("corr_x").type == pa.string()
    assert table.column("corr_x").to_pylist()[-1] == "1"


def test_sparse_known_column_uses_hint():
    rows = [["AAPL", None]] * 10 + [["AAPL", "true"]]
    data = csv_bytes(["ticker", "otc"], rows, compress=False)
    _, table = _read_all(data, compression=None, peek_bytes=32, block_size=32)
    assert table.schema.field("otc").type == pa.bool_()
    assert table.column("otc").to_pylist()[-1] is True


def test_column_type_override_wins():
    rows = [["AAPL", 1]] * 5
    data = csv_bytes(["ticker", "val"], rows, compress=False)
    _, table = _read_all(data, compression=None, column_types={"val": pa.float32()})
    assert table.schema.field("val").type == pa.float32()


def test_column_subset():
    _, table = _read_all(quotes_csv(["AAPL"]), columns=["ticker", "sip_timestamp"])
    assert table.schema.names == ["ticker", "sip_timestamp"]


def test_peek_boundary_mid_line_loses_nothing():
    rows = [[f"TICKER{i:05d}", i * 1.5, NS + i] for i in range(5000)]
    data = csv_bytes(["ticker", "px", "sip_timestamp"], rows)
    for peek in (100, 1000, 4097, 1 << 20):
        _, table = _read_all(data, peek_bytes=peek, block_size=4096)
        assert table.num_rows == 5000
        assert table.column("ticker").to_pylist()[-1] == "TICKER04999"


def test_whole_file_smaller_than_peek():
    _, table = _read_all(quotes_csv(["AAPL"], per_ticker=1), peek_bytes=1 << 20)
    assert table.num_rows == 1


def test_empty_and_header_only_files():
    stream, table = _read_all(gzip.compress(b""))
    assert stream.empty and table is None
    stream, table = _read_all(gzip.compress(b"ticker,px\n"))
    assert not stream.empty
    assert table is None or table.num_rows == 0
    assert stream.schema.field("ticker").type == pa.string()


def test_compression_for_name():
    assert compression_for_name("a/b/2025-01-01.csv.gz") == "gzip"
    assert compression_for_name("x.CSV.ZST") == "zstd"
    assert compression_for_name("x.csv") is None


def test_parse_types():
    assert parse_type("int64") == pa.int64()
    assert parse_type("timestamp[ms]") == pa.timestamp("ms")
    with pytest.raises(ValueError):
        parse_type("not_a_type")
    assert parse_column_types(["a=int32", " b = string "]) == {"a": pa.int32(), "b": pa.string()}
    with pytest.raises(ValueError):
        parse_column_types(["novalue"])


def test_read_options_signature_changes_with_settings():
    a = ReadOptions().signature()
    b = ReadOptions(columns=["ticker"]).signature()
    c = ReadOptions(timestamp_columns="none").signature()
    assert len({a, b, c}) == 3
