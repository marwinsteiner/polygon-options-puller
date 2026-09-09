from datetime import date

import pytest

from polygon_options_puller.datasets import (
    Dataset,
    date_from_key,
    discover_files,
    discover_latest,
    iter_months,
    list_datasets,
    resolve_dataset,
)


def test_resolve_alias():
    ds = resolve_dataset("stocks/trades")
    assert ds.prefix == "us_stocks_sip/trades_v1"
    assert ds.asset_class == "us_stocks_sip"
    assert ds.data_type == "trades_v1"
    assert ds.name == "trades"
    assert ds.alias == "stocks/trades"


def test_resolve_asset_class_with_raw_type():
    assert resolve_dataset("options/quotes_v2").prefix == "us_options_opra/quotes_v2"


def test_resolve_raw_prefix():
    ds = resolve_dataset("/us_indices/values_v1/")
    assert ds.prefix == "us_indices/values_v1"
    assert ds.alias is None
    assert ds.name == "values"


@pytest.mark.parametrize("bad", ["", "quotes", "stocks//", "/"])
def test_resolve_invalid(bad):
    with pytest.raises(ValueError):
        resolve_dataset(bad)


def test_key_for_date():
    ds = Dataset("us_stocks_sip/trades_v1")
    assert ds.key_for_date(date(2025, 3, 7)) == "us_stocks_sip/trades_v1/2025/03/2025-03-07.csv.gz"
    assert ds.month_prefix(2025, 3) == "us_stocks_sip/trades_v1/2025/03/"


def test_date_from_key():
    assert date_from_key("a/b/2025/03/2025-03-07.csv.gz") == date(2025, 3, 7)
    assert date_from_key("a/b/2025/03/notes.txt") is None
    assert date_from_key("a/b/2025-13-45.csv.gz") is None


def test_iter_months_across_year():
    months = list(iter_months(date(2024, 11, 5), date(2025, 2, 1)))
    assert months == [(2024, 11), (2024, 12), (2025, 1), (2025, 2)]


def test_discover_files_range(fake_s3):
    ds = resolve_dataset("options/quotes")
    files = discover_files(fake_s3, ds, date(2025, 3, 18), date(2025, 4, 30))
    assert [f.date for f in files] == [date(2025, 3, 18), date(2025, 4, 1)]
    assert files[0].key.endswith("2025-03-18.csv.gz")
    assert files[0].etag and files[0].size > 0
    assert files[0].extension == "csv.gz"
    assert files[0].filename == "2025-03-18.csv.gz"


def test_discover_files_bad_range(fake_s3):
    with pytest.raises(ValueError):
        discover_files(fake_s3, resolve_dataset("options/quotes"), date(2025, 4, 1), date(2025, 3, 1))


def test_discover_latest_walks_back_months(fake_s3):
    ds = resolve_dataset("options/quotes")
    latest = discover_latest(fake_s3, ds, n=1, today=date(2025, 6, 15))
    assert [f.date for f in latest] == [date(2025, 4, 1)]
    last3 = discover_latest(fake_s3, ds, n=3, today=date(2025, 6, 15))
    assert [f.date for f in last3] == [date(2025, 3, 17), date(2025, 3, 18), date(2025, 4, 1)]


def test_discover_latest_respects_max_months(fake_s3):
    ds = resolve_dataset("options/quotes")
    assert discover_latest(fake_s3, ds, n=1, today=date(2026, 1, 1), max_months=3) == []
    with pytest.raises(ValueError):
        discover_latest(fake_s3, ds, n=0)


def test_list_datasets(fake_s3):
    assert list_datasets(fake_s3) == ["us_options_opra/quotes_v1", "us_stocks_sip/day_aggs_v1"]
