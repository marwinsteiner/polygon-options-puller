from polygon_options_puller.converter import extract_underlying


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
