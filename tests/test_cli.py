import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from conftest import recent_dates
from polygon_options_puller import cli as cli_module
from polygon_options_puller import downloader
from polygon_options_puller.cli import cli

CREDS = ["--access-key", "k", "--secret-key", "s"]


@pytest.fixture
def runner(fake_s3, monkeypatch):
    monkeypatch.setattr(downloader, "get_s3_client", lambda *a, **k: fake_s3)
    monkeypatch.setattr(cli_module, "get_s3_client", lambda *a, **k: fake_s3)
    return CliRunner()


def test_pull_latest(runner, tmp_path):
    result = runner.invoke(
        cli,
        ["pull", *CREDS, "-d", "stocks/day_aggs", "-o", str(tmp_path), "--latest", "-f", "csv.gz"],
    )
    assert result.exit_code == 0, result.output
    newest = recent_dates()[-1]
    dest = tmp_path / f"us_stocks_sip/day_aggs_v1/{newest.year}/{newest.month:02d}/{newest.isoformat()}.csv.gz"
    assert dest.exists()
    assert "Wrote 1 file(s)" in result.output


def test_pull_range_with_filters_and_types(runner, tmp_path):
    result = runner.invoke(
        cli,
        [
            "pull",
            *CREDS,
            "-d",
            "options/quotes",
            "-o",
            str(tmp_path),
            "--start-date",
            "2025-03-17",
            "--end-date",
            "2025-03-17",
            "--ticker-prefix",
            "O:AAPL",
            "--columns",
            "ticker,ask_price,sip_timestamp",
            "--column-type",
            "ask_price=float32",
            "--timestamp-columns",
            "none",
            "--compression",
            "snappy",
            "--path-template",
            "{name}/{date}.{ext}",
        ],
    )
    assert result.exit_code == 0, result.output
    table = pq.read_table(tmp_path / "quotes/2025-03-17.parquet")
    assert table.num_rows == 6
    assert table.schema.names == ["ticker", "ask_price", "sip_timestamp"]
    assert str(table.schema.field("ask_price").type) == "float"
    assert str(table.schema.field("sip_timestamp").type) == "int64"


def test_pull_requires_a_date_selector(runner, tmp_path):
    result = runner.invoke(cli, ["pull", *CREDS, "-d", "stocks/day_aggs", "-o", str(tmp_path)])
    assert result.exit_code != 0
    assert "--start-date" in result.output
    result = runner.invoke(
        cli, ["pull", *CREDS, "-d", "stocks/day_aggs", "-o", str(tmp_path), "--latest", "--last", "2"]
    )
    assert result.exit_code != 0


def test_pull_dry_run(runner, tmp_path):
    result = runner.invoke(
        cli,
        ["pull", *CREDS, "-d", "options/quotes", "-o", str(tmp_path), "--start-date", "2025-03-01", "--end-date", "2025-03-31", "--dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert "2 file(s) would be pulled" in result.output
    assert not list(tmp_path.rglob("*.parquet"))


def test_pull_unknown_dataset_is_a_clean_error(runner, tmp_path):
    result = runner.invoke(cli, ["pull", *CREDS, "-d", "nonsense", "-o", str(tmp_path), "--latest"])
    assert result.exit_code != 0
    assert "Unknown dataset" in result.output


def test_pull_exit_code_on_failure(runner, fake_s3, tmp_path):
    fake_s3.objects["us_options_opra/quotes_v1/2025/03/2025-03-17.csv.gz"] = b"not gzip"
    result = runner.invoke(
        cli,
        ["pull", *CREDS, "-d", "options/quotes", "-o", str(tmp_path), "--start-date", "2025-03-17", "--end-date", "2025-03-17"],
    )
    assert result.exit_code == 1
    assert "FAILED" in result.output


def test_legacy_download_command(runner, tmp_path):
    result = runner.invoke(
        cli,
        [
            "download",
            *CREDS,
            "--symbol-prefix",
            "SPY",
            "-t",
            "quotes",
            "--start-date",
            "2025-03-17",
            "--end-date",
            "2025-03-18",
            "-o",
            str(tmp_path),
            "--min-rows",
            "5",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "deprecated" in result.output
    for day in ("2025-03-17", "2025-03-18"):
        table = pq.read_table(tmp_path / "quotes" / f"{day}.parquet")
        assert table.column("ticker").to_pylist() == ["O:SPY240315C00500000"] * 3


def test_list_datasets(runner):
    result = runner.invoke(cli, ["list-datasets", *CREDS])
    assert result.exit_code == 0, result.output
    assert "us_stocks_sip/day_aggs_v1" in result.output and "stocks/day_aggs" in result.output


def test_list_dates(runner):
    result = runner.invoke(cli, ["list-dates", *CREDS, "-d", "options/quotes", "--year", "2025", "--month", "3"])
    assert result.exit_code == 0, result.output
    assert result.output.strip().splitlines() == [
        "us_options_opra/quotes_v1/2025/03/2025-03-17.csv.gz",
        "us_options_opra/quotes_v1/2025/03/2025-03-18.csv.gz",
    ]
    result = runner.invoke(cli, ["list-dates", *CREDS, "-d", "stocks/day_aggs", "--last", "1"])
    assert result.exit_code == 0
    assert result.output.strip().endswith(f"{recent_dates()[-1].isoformat()}.csv.gz")


def test_list_formats(runner):
    result = runner.invoke(cli, ["list-formats"])
    assert result.exit_code == 0
    for name in ("parquet", "feather", "csv.gz", "jsonl", "raw"):
        assert name in result.output


def test_version(runner):
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0 and "0.3.0" in result.output
