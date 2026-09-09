import pyarrow as pa
import pytest

from polygon_options_puller.filters import build_filter, filter_signature

BATCH = pa.record_batch(
    {
        "ticker": ["O:AAPL230120C00150000", "O:SPY240315C00500000", "AAPL", "MSFT", None],
        "px": [1.0, 2.0, 3.0, 4.0, 5.0],
    }
)


def test_no_filter_returns_none():
    assert build_filter("ticker", [], []) is None
    assert filter_signature("ticker", [], []) == ""


def test_prefix_filter():
    f = build_filter("ticker", prefixes=["O:AAPL"])
    out = f(BATCH)
    assert out.column("ticker").to_pylist() == ["O:AAPL230120C00150000"]


def test_exact_and_prefix_combined():
    f = build_filter("ticker", values=["MSFT"], prefixes=["O:SPY"])
    assert f(BATCH).column("px").to_pylist() == [2.0, 4.0]


def test_multiple_prefixes():
    f = build_filter("ticker", prefixes=["O:AAPL", "O:SPY"])
    assert f(BATCH).num_rows == 2


def test_non_string_column_is_cast():
    batch = pa.record_batch({"code": [10, 11, 20]})
    f = build_filter("code", prefixes=["1"])
    assert f(batch).column("code").to_pylist() == [10, 11]


def test_missing_column_raises():
    f = build_filter("nope", values=["x"])
    with pytest.raises(ValueError, match="not found"):
        f(BATCH)


def test_signature_is_order_independent():
    a = filter_signature("ticker", ["B", "A"], ["O:Z", "O:Y"])
    b = filter_signature("ticker", ["A", "B"], ["O:Y", "O:Z"])
    assert a == b
    assert a != filter_signature("symbol", ["A", "B"], ["O:Y", "O:Z"])
