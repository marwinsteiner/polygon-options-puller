import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from polygon_options_puller.converter import (
    dataframe_to_table,
    extract_underlying,
    get_output_schema,
    validate_parquet,
)


# --------------------------------------------------------------------------- #
# extract_underlying (existing tests)
# --------------------------------------------------------------------------- #


def test_extract_underlying_standard():
    assert extract_underlying("O:AAPL230120C00150000") == "AAPL"
    assert extract_underlying("O:SPY240315P00500000") == "SPY"
    assert extract_underlying("O:TSLA250620C00200000") == "TSLA"


def test_extract_underlying_short_ticker():
    assert extract_underlying("O:X240315C00025000") == "X"


def test_extract_underlying_long_ticker():
    assert extract_underlying("O:BRKB230120C00300000") == "BRKB"


def test_extract_underlying_fallback():
    assert extract_underlying("O:AAPL") == "AAPL"
    assert extract_underlying("BADFORMAT") == "BADFORMAT"


# --------------------------------------------------------------------------- #
# dataframe_to_table
# --------------------------------------------------------------------------- #


def test_dataframe_to_table_quotes():
    df = pd.DataFrame(
        {
            "ticker": ["O:AAPL230120C00150000", "O:AAPL230120P00150000"],
            "ask_exchange": [1, 2],
            "ask_price": [3.50, 4.25],
            "ask_size": [10, 20],
            "bid_exchange": [1, 3],
            "bid_price": [3.40, 4.15],
            "bid_size": [5, 15],
            "sequence_number": [100, 200],
            "sip_timestamp": [1_700_000_000_000_000_000, 1_700_000_001_000_000_000],
        }
    )
    table = dataframe_to_table(df, "quotes")
    assert isinstance(table, pa.Table)
    assert len(table) == 2
    # sip_timestamp should be a proper timestamp type, not int64
    assert table.schema.field("sip_timestamp").type == pa.timestamp("ns", tz="UTC")
    # ticker should be dictionary-encoded
    assert pa.types.is_dictionary(table.schema.field("ticker").type)


def test_dataframe_to_table_trades():
    df = pd.DataFrame(
        {
            "ticker": ["O:SPY240315C00500000"],
            "conditions": ["[1]"],
            "correction": [0],
            "exchange": [4],
            "participant_timestamp": [1_700_000_000_000_000_000],
            "price": [5.00],
            "sequence_number": [999],
            "sip_timestamp": [1_700_000_000_000_000_000],
            "size": [50],
        }
    )
    table = dataframe_to_table(df, "trades")
    assert len(table) == 1
    assert table.schema.field("sip_timestamp").type == pa.timestamp("ns", tz="UTC")
    assert table.schema.field("participant_timestamp").type == pa.timestamp("ns", tz="UTC")


def test_dataframe_to_table_day_aggs_no_timestamps():
    df = pd.DataFrame(
        {
            "ticker": ["O:AAPL230120C00150000"],
            "close": [5.0],
            "high": [6.0],
            "low": [4.0],
            "open": [5.5],
            "transactions": [100],
            "volume": [1000],
            "vwap": [5.25],
            "otc": [False],
        }
    )
    table = dataframe_to_table(df, "day_aggs")
    assert len(table) == 1
    # No timestamp columns in day_aggs
    for field in table.schema:
        assert not pa.types.is_timestamp(field.type)


# --------------------------------------------------------------------------- #
# validate_parquet
# --------------------------------------------------------------------------- #


def _write_test_parquet(path, tickers, num_rows=None):
    """Write a minimal Parquet file with the given tickers."""
    if num_rows is None:
        num_rows = len(tickers)
    # Repeat tickers to fill num_rows
    repeated = (tickers * ((num_rows // len(tickers)) + 1))[:num_rows]
    table = pa.table(
        {"ticker": pa.array(repeated, type=pa.string()).dictionary_encode()},
        schema=pa.schema([pa.field("ticker", pa.dictionary(pa.int16(), pa.string()))]),
    )
    pq.write_table(table, str(path))


def test_validate_parquet_valid(tmp_path):
    path = tmp_path / "test.parquet"
    _write_test_parquet(path, ["O:AAPL230120C00150000", "O:AAPL230120P00150000"], num_rows=1500)
    assert validate_parquet(path, "AAPL", min_rows=1000) is True


def test_validate_parquet_missing_file(tmp_path):
    path = tmp_path / "nope.parquet"
    assert validate_parquet(path, "AAPL") is False


def test_validate_parquet_too_few_rows(tmp_path):
    path = tmp_path / "test.parquet"
    _write_test_parquet(path, ["O:AAPL230120C00150000"], num_rows=10)
    assert validate_parquet(path, "AAPL", min_rows=1000) is False


def test_validate_parquet_wrong_prefix(tmp_path):
    path = tmp_path / "test.parquet"
    _write_test_parquet(path, ["O:SPY240315C00500000"], num_rows=1500)
    assert validate_parquet(path, "AAPL", min_rows=1000) is False


def test_validate_parquet_mixed_prefix_fails(tmp_path):
    path = tmp_path / "test.parquet"
    _write_test_parquet(
        path, ["O:AAPL230120C00150000", "O:SPY240315C00500000"], num_rows=1500
    )
    assert validate_parquet(path, "AAPL", min_rows=1000) is False
