"""Shared fixtures: an in-memory fake S3 client and CSV builders."""

from __future__ import annotations

import gzip
import hashlib
import io
from datetime import date, datetime, timedelta, timezone

import pytest

NS = 1_700_000_000_000_000_000


class _FakePaginator:
    def __init__(self, s3: FakeS3):
        self.s3 = s3

    def paginate(self, Bucket, Prefix="", Delimiter=None):
        keys = sorted(k for k in self.s3.objects if k.startswith(Prefix))
        contents, common, seen = [], [], set()
        for key in keys:
            rest = key[len(Prefix) :]
            if Delimiter and Delimiter in rest:
                cp = Prefix + rest.split(Delimiter, 1)[0] + Delimiter
                if cp not in seen:
                    seen.add(cp)
                    common.append({"Prefix": cp})
            else:
                data = self.s3.objects[key]
                contents.append(
                    {
                        "Key": key,
                        "ETag": '"' + hashlib.md5(data).hexdigest() + '"',
                        "Size": len(data),
                        "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc),
                    }
                )
        yield {"Contents": contents, "CommonPrefixes": common}


class FakeS3:
    """Minimal stand-in for a boto3 S3 client backed by a dict of key -> bytes."""

    def __init__(self, objects: dict[str, bytes] | None = None):
        self.objects: dict[str, bytes] = dict(objects or {})
        self.get_calls: list[str] = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self)

    def get_object(self, Bucket, Key):
        self.get_calls.append(Key)
        if Key not in self.objects:
            raise RuntimeError(f"NoSuchKey: {Key}")
        return {"Body": io.BytesIO(self.objects[Key])}


def csv_bytes(header: list[str], rows: list[list], compress: bool = True) -> bytes:
    lines = [",".join(header)] + [",".join("" if v is None else str(v) for v in r) for r in rows]
    raw = ("\n".join(lines) + "\n").encode()
    return gzip.compress(raw) if compress else raw


def quotes_csv(tickers: list[str], per_ticker: int = 3) -> bytes:
    rows = []
    for i, t in enumerate(tickers):
        for j in range(per_ticker):
            rows.append([t, 1.0 + j, 0.5 + j, NS + i * 1000 + j])
    return csv_bytes(["ticker", "ask_price", "bid_price", "sip_timestamp"], rows)


def aggs_csv(tickers: list[str]) -> bytes:
    rows = [[t, 10 + i, 11 + i, 9 + i, 10.5 + i, 1000 * (i + 1), NS + i, None] for i, t in enumerate(tickers)]
    return csv_bytes(
        ["ticker", "open", "high", "low", "close", "volume", "window_start", "otc"], rows
    )


OPTION_TICKERS = ["O:AAPL230120C00150000", "O:AAPL230120P00150000", "O:SPY240315C00500000"]
STOCK_TICKERS = ["AAPL", "MSFT", "SPY"]


def recent_dates(n: int = 3) -> list[date]:
    """Dates in the recent past (spanning the previous month) for --latest tests."""
    today = date.today()
    first_of_month = today.replace(day=1)
    prev_month_mid = (first_of_month - timedelta(days=1)).replace(day=15)
    prev_month_first = prev_month_mid.replace(day=1)
    return [prev_month_first, prev_month_mid, first_of_month][-n:]


@pytest.fixture
def fake_s3() -> FakeS3:
    objects = {
        "us_options_opra/quotes_v1/2025/03/2025-03-17.csv.gz": quotes_csv(OPTION_TICKERS),
        "us_options_opra/quotes_v1/2025/03/2025-03-18.csv.gz": quotes_csv(OPTION_TICKERS),
        "us_options_opra/quotes_v1/2025/04/2025-04-01.csv.gz": quotes_csv(OPTION_TICKERS),
        "us_stocks_sip/day_aggs_v1/2025/03/2025-03-17.csv.gz": aggs_csv(STOCK_TICKERS),
        "us_stocks_sip/day_aggs_v1/2025/03/2025-03-18.csv.gz": aggs_csv(STOCK_TICKERS),
        "us_stocks_sip/day_aggs_v1/2025/04/2025-04-01.csv.gz": aggs_csv(STOCK_TICKERS),
    }
    for d in recent_dates():
        objects[f"us_stocks_sip/day_aggs_v1/{d.year}/{d.month:02d}/{d.isoformat()}.csv.gz"] = (
            aggs_csv(STOCK_TICKERS)
        )
    return FakeS3(objects)
