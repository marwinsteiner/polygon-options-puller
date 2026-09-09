"""Helpers for working with Polygon tickers.

The schema/Parquet machinery that used to live here moved to
:mod:`polygon_options_puller.reader` (schema inference, timestamp conversion)
and :mod:`polygon_options_puller.formats` (output writers).
"""

from __future__ import annotations

import re

from .reader import convert_timestamps, output_schema  # noqa: F401  (re-exported)

# Polygon option tickers look like  O:AAPL230120C00150000
#   O:<underlying><YYMMDD><C|P><strike * 1000, zero-padded to 8 digits>
_OPTION_TICKER_RE = re.compile(r"^O:([A-Z]+)\d{6}[CP]\d{8}$")


def extract_underlying(option_ticker: str) -> str:
    """Return the underlying symbol from a Polygon option ticker.

    >>> extract_underlying("O:AAPL230120C00150000")
    'AAPL'
    """
    m = _OPTION_TICKER_RE.match(option_ticker)
    if m:
        return m.group(1)
    # Fallback: strip the ``O:`` prefix and grab leading alpha chars.
    stripped = option_ticker.removeprefix("O:")
    underlying = ""
    for ch in stripped:
        if ch.isalpha():
            underlying += ch
        else:
            break
    return underlying or "UNKNOWN"
