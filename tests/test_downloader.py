import gzip
import warnings
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from conftest import OPTION_TICKERS, FakeS3, quotes_csv, recent_dates
from polygon_options_puller.datasets import FlatFile, resolve_dataset
from polygon_options_puller.downloader import LEGACY_PATH_TEMPLATE, pull, render_path
from polygon_options_puller.manifest import Manifest


def _pull(fake_s3, out, **kwargs):
    kwargs.setdefault("progress", False)
    kwargs.setdefault("workers", 2)
    return pull(output_dir=out, s3_client=fake_s3, **kwargs)


def test_pull_range_writes_mirrored_layout(fake_s3, tmp_path):
    result = _pull(
        fake_s3,
        tmp_path,
        datasets=["options/quotes", "stocks/day_aggs"],
        start_date=date(2025, 3, 1),
        end_date=date(2025, 3, 31),
    )
    assert result.ok
    expected = {
        tmp_path / "us_options_opra/quotes_v1/2025/03/2025-03-17.parquet",
        tmp_path / "us_options_opra/quotes_v1/2025/03/2025-03-18.parquet",
        tmp_path / "us_stocks_sip/day_aggs_v1/2025/03/2025-03-17.parquet",
        tmp_path / "us_stocks_sip/day_aggs_v1/2025/03/2025-03-18.parquet",
    }
    assert set(result.written) == expected
    assert all(p.exists() for p in expected)

    table = pq.read_table(tmp_path / "us_options_opra/quotes_v1/2025/03/2025-03-17.parquet")
    assert table.num_rows == 9
    assert table.schema.field("sip_timestamp").type == pa.timestamp("ns", tz="UTC")
    aggs = pq.read_table(tmp_path / "us_stocks_sip/day_aggs_v1/2025/03/2025-03-17.parquet")
    assert aggs.schema.field("window_start").type == pa.timestamp("ns", tz="UTC")
    assert aggs.schema.field("otc").type == pa.bool_()  # empty column, typed via hint

    manifest = Manifest(tmp_path)
    assert len(manifest) == 4
    entry = manifest.get("us_options_opra/quotes_v1/2025/03/2025-03-17.parquet")
    assert entry["rows"] == 9 and entry["written"] is True and entry["format"] == "parquet"
    assert not list(tmp_path.rglob("*.tmp"))


def test_rerun_is_idempotent(fake_s3, tmp_path):
    kwargs = dict(datasets=["options/quotes"], start_date=date(2025, 3, 17), end_date=date(2025, 3, 18))
    _pull(fake_s3, tmp_path, **kwargs)
    fake_s3.get_calls.clear()
    result = _pull(fake_s3, tmp_path, **kwargs)
    assert result.written == [] and len(result.skipped) == 2
    assert fake_s3.get_calls == []


def test_upstream_change_triggers_redownload(fake_s3, tmp_path):
    kwargs = dict(datasets=["options/quotes"], start_date=date(2025, 3, 17), end_date=date(2025, 3, 17))
    _pull(fake_s3, tmp_path, **kwargs)
    key = "us_options_opra/quotes_v1/2025/03/2025-03-17.csv.gz"
    fake_s3.objects[key] = quotes_csv(OPTION_TICKERS, per_ticker=5)  # new ETag
    result = _pull(fake_s3, tmp_path, **kwargs)
    assert len(result.written) == 1
    assert pq.read_table(result.written[0]).num_rows == 15


def test_settings_change_triggers_redownload(fake_s3, tmp_path):
    kwargs = dict(datasets=["options/quotes"], start_date=date(2025, 3, 17), end_date=date(2025, 3, 17))
    _pull(fake_s3, tmp_path, **kwargs)
    result = _pull(fake_s3, tmp_path, ticker_prefixes=["O:AAPL"], **kwargs)
    assert len(result.written) == 1
    assert pq.read_table(result.written[0]).num_rows == 6


def test_deleted_output_is_rebuilt(fake_s3, tmp_path):
    kwargs = dict(datasets=["options/quotes"], start_date=date(2025, 3, 17), end_date=date(2025, 3, 17))
    first = _pull(fake_s3, tmp_path, **kwargs)
    first.written[0].unlink()
    second = _pull(fake_s3, tmp_path, **kwargs)
    assert second.written == first.written


def test_force_redownloads(fake_s3, tmp_path):
    kwargs = dict(datasets=["options/quotes"], start_date=date(2025, 3, 17), end_date=date(2025, 3, 17))
    _pull(fake_s3, tmp_path, **kwargs)
    result = _pull(fake_s3, tmp_path, force=True, **kwargs)
    assert len(result.written) == 1


def test_preexisting_file_without_manifest_is_left_alone(fake_s3, tmp_path):
    dest = tmp_path / "us_options_opra/quotes_v1/2025/03/2025-03-17.parquet"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"old")
    result = _pull(
        fake_s3, tmp_path, datasets=["options/quotes"], start_date=date(2025, 3, 17), end_date=date(2025, 3, 17)
    )
    assert result.skipped == [dest] and dest.read_bytes() == b"old"


def test_filter_with_no_matches_records_empty(fake_s3, tmp_path):
    kwargs = dict(
        datasets=["options/quotes"],
        start_date=date(2025, 3, 17),
        end_date=date(2025, 3, 17),
        ticker_prefixes=["O:TSLA"],
    )
    result = _pull(fake_s3, tmp_path, **kwargs)
    assert result.written == [] and len(result.empty) == 1
    assert not (tmp_path / "us_options_opra").exists() or not list((tmp_path / "us_options_opra").rglob("*.parquet"))
    entry = Manifest(tmp_path).get("us_options_opra/quotes_v1/2025/03/2025-03-17.parquet")
    assert entry["rows"] == 0 and entry["written"] is False

    fake_s3.get_calls.clear()
    again = _pull(fake_s3, tmp_path, **kwargs)
    assert len(again.skipped) == 1 and fake_s3.get_calls == []


def test_exact_ticker_filter_and_column_subset(fake_s3, tmp_path):
    result = _pull(
        fake_s3,
        tmp_path,
        datasets=["stocks/day_aggs"],
        start_date=date(2025, 3, 17),
        end_date=date(2025, 3, 17),
        tickers=["AAPL", "SPY"],
        columns=["ticker", "close"],
    )
    table = pq.read_table(result.written[0])
    assert table.schema.names == ["ticker", "close"]
    assert table.column("ticker").to_pylist() == ["AAPL", "SPY"]


def test_row_filter_callable(fake_s3, tmp_path):
    import pyarrow.compute as pc

    result = _pull(
        fake_s3,
        tmp_path,
        datasets=["stocks/day_aggs"],
        start_date=date(2025, 3, 17),
        end_date=date(2025, 3, 17),
        row_filter=lambda b: b.filter(pc.greater(b.column("volume"), 1000)),
    )
    assert pq.read_table(result.written[0]).num_rows == 2


def test_latest_and_last(fake_s3, tmp_path):
    dates = recent_dates()
    result = _pull(fake_s3, tmp_path, datasets=["stocks/day_aggs"], latest=1)
    assert [p.stem for p in result.written] == [dates[-1].isoformat()]
    result = _pull(fake_s3, tmp_path / "two", datasets=["stocks/day_aggs"], latest=2)
    assert sorted(p.stem for p in result.written) == [d.isoformat() for d in dates[-2:]]


def test_latest_conflicts_with_dates(fake_s3, tmp_path):
    with pytest.raises(ValueError):
        _pull(fake_s3, tmp_path, datasets=["stocks/day_aggs"], latest=1, start_date=date(2025, 1, 1))
    with pytest.raises(ValueError):
        _pull(fake_s3, tmp_path, datasets=["stocks/day_aggs"])


@pytest.mark.parametrize("fmt", ["feather", "csv", "csv.gz", "jsonl", "jsonl.gz"])
def test_other_formats(fake_s3, tmp_path, fmt):
    result = _pull(
        fake_s3,
        tmp_path,
        datasets=["options/quotes"],
        start_date=date(2025, 3, 17),
        end_date=date(2025, 3, 17),
        output_format=fmt,
    )
    assert len(result.written) == 1
    assert result.written[0].name == f"2025-03-17.{fmt}"
    assert result.written[0].stat().st_size > 0


def test_raw_format_copies_source_bytes(fake_s3, tmp_path):
    key = "us_options_opra/quotes_v1/2025/03/2025-03-17.csv.gz"
    result = _pull(
        fake_s3,
        tmp_path,
        datasets=["options/quotes"],
        start_date=date(2025, 3, 17),
        end_date=date(2025, 3, 17),
        output_format="raw",
    )
    dest = tmp_path / key
    assert result.written == [dest]
    assert dest.read_bytes() == fake_s3.objects[key]
    assert gzip.decompress(dest.read_bytes()).startswith(b"ticker,")


def test_raw_format_rejects_filters(fake_s3, tmp_path):
    with pytest.raises(ValueError, match="raw"):
        _pull(
            fake_s3,
            tmp_path,
            datasets=["options/quotes"],
            start_date=date(2025, 3, 17),
            output_format="raw",
            ticker_prefixes=["O:AAPL"],
        )


def test_path_template(fake_s3, tmp_path):
    result = _pull(
        fake_s3,
        tmp_path,
        datasets=["options/quotes"],
        start_date=date(2025, 3, 17),
        end_date=date(2025, 3, 17),
        path_template="{asset_class}/{name}-{date}.{ext}",
    )
    assert result.written == [tmp_path / "us_options_opra" / "quotes-2025-03-17.parquet"]


def test_render_path_errors():
    f = FlatFile(resolve_dataset("stocks/trades"), date(2025, 1, 2), "us_stocks_sip/trades_v1/2025/01/2025-01-02.csv.gz")
    assert render_path("{name}/{date}.{ext}", f, "parquet").as_posix() == "trades/2025-01-02.parquet"
    assert render_path("{filename}", f, "").as_posix() == "2025-01-02.csv.gz"
    with pytest.raises(ValueError, match="Bad path template"):
        render_path("{bogus}/{date}", f, "parquet")
    with pytest.raises(ValueError, match="relative"):
        render_path("../{date}.{ext}", f, "parquet")


def test_dry_run(fake_s3, tmp_path):
    result = _pull(
        fake_s3,
        tmp_path,
        datasets=["options/quotes"],
        start_date=date(2025, 3, 1),
        end_date=date(2025, 4, 30),
        dry_run=True,
    )
    assert len(result.planned) == 3 and result.written == []
    assert fake_s3.get_calls == []
    assert not list(tmp_path.rglob("*.parquet"))


def test_failure_is_reported_not_raised(fake_s3, tmp_path):
    key = "us_options_opra/quotes_v1/2025/03/2025-03-18.csv.gz"
    fake_s3.objects[key] = b"definitely not gzip"
    result = _pull(fake_s3, tmp_path, datasets=["options/quotes"], start_date=date(2025, 3, 17), end_date=date(2025, 3, 18))
    assert not result.ok
    assert set(result.failed) == {key}
    assert len(result.written) == 1
    assert not list(tmp_path.rglob("*.tmp"))
    assert Manifest(tmp_path).get("us_options_opra/quotes_v1/2025/03/2025-03-18.parquet") is None


def test_stale_tmp_files_are_removed(fake_s3, tmp_path):
    stale = tmp_path / "us_options_opra/quotes_v1/2025/03/2025-03-17.parquet.tmp"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"junk")
    _pull(fake_s3, tmp_path, datasets=["options/quotes"], start_date=date(2025, 3, 17), end_date=date(2025, 3, 17))
    assert not stale.exists()


def test_legacy_keyword_arguments(fake_s3, tmp_path):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = pull(
            access_key="k",
            secret_key="s",
            output_dir=tmp_path,
            data_types=["quotes"],
            symbol_prefix="AAPL",
            start_date=date(2025, 3, 17),
            end_date=date(2025, 3, 18),
            workers=2,
            min_rows=1,
            s3_client=fake_s3,
            progress=False,
        )
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)
    assert set(result.written) == {tmp_path / "quotes/2025-03-17.parquet", tmp_path / "quotes/2025-03-18.parquet"}
    table = pq.read_table(tmp_path / "quotes/2025-03-17.parquet")
    assert all(t.startswith("O:AAPL") for t in table.column("ticker").to_pylist())
    assert LEGACY_PATH_TEMPLATE == "{name}/{date}.{ext}"


def test_legacy_unknown_data_type(fake_s3, tmp_path):
    with pytest.raises(ValueError, match="Unknown data_type"), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pull(output_dir=tmp_path, data_types=["bogus"], start_date=date(2025, 1, 1), s3_client=fake_s3)


def test_missing_credentials():
    with pytest.raises(ValueError, match="access_key"):
        pull(output_dir=".", datasets=["stocks/trades"], start_date=date(2025, 1, 1))


def test_no_files_in_range_warns(fake_s3, tmp_path, caplog):
    result = _pull(fake_s3, tmp_path, datasets=["options/quotes"], start_date=date(2020, 1, 1), end_date=date(2020, 1, 31))
    assert result.written == [] and result.ok
    assert "no files found" in caplog.text


def test_empty_object(tmp_path):
    s3 = FakeS3({"us_stocks_sip/trades_v1/2025/01/2025-01-02.csv.gz": gzip.compress(b"")})
    result = _pull(s3, tmp_path, datasets=["stocks/trades"], start_date=date(2025, 1, 1), end_date=date(2025, 1, 31))
    assert result.ok and result.empty == ["us_stocks_sip/trades_v1/2025/01/2025-01-02.csv.gz"]
