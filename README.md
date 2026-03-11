# polygon-options-puller

Download [Polygon / Massive](https://massive.com) US options (OPRA) flat files
from their S3 bucket and store them locally as Snappy-compressed,
dictionary-encoded **Parquet** files, filtered by **symbol prefix**.

## How it works

Polygon ships daily `.csv.gz` files containing *all* option tickers for an
entire trading day.  The quote files alone are ~120 GB compressed each.
This tool **streams** each file directly from S3, filters to your symbol prefix
in-flight, and writes only matching rows to Parquet — no temp files, no
downloading 120 GB just to keep 500 MB.

Key features:
- **Streaming**: decompresses and filters in-flight, never writes the full CSV to disk
- **Parallel**: uses a thread pool to download multiple days concurrently
- **NYSE-aware**: uses `pandas_market_calendars` to skip holidays and weekends
- **Idempotent**: re-running skips days that already have valid Parquet files
- **Atomic writes**: uses temp files + `os.replace()` to prevent corrupt output

## Installation

```bash
pip install .
# or in editable mode for development:
pip install -e ".[dev]"
```

## Credentials

You need Polygon / Massive S3 credentials.  Get them from your
[Massive dashboard](https://massive.com/dashboard).

```bash
export POLYGON_S3_ACCESS_KEY="your-access-key"
export POLYGON_S3_SECRET_KEY="your-secret-key"
```

## Usage

### Download data

```bash
# Download AAPL option quotes for a date range
polygon-options-puller download \
    --symbol-prefix AAPL \
    -t quotes \
    --start-date 2025-03-17 \
    --end-date 2025-03-21 \
    -o ./data/aapl

# Download SPXW trades with 16 workers
polygon-options-puller download \
    --symbol-prefix SPXW \
    -t trades \
    --start-date 2025-04-01 \
    --end-date 2025-04-30 \
    -o ./data/spxw \
    --workers 16

# Download both trades and quotes
polygon-options-puller download \
    --symbol-prefix SPY \
    -t both \
    --start-date 2025-04-01 \
    --end-date 2025-04-02 \
    -o ./data/spy

# Download minute aggregates
polygon-options-puller download \
    --symbol-prefix AAPL \
    -t minute_aggs \
    --start-date 2025-01-02 \
    --end-date 2025-01-02 \
    -o ./data/aapl
```

### List available dates

```bash
# List all available quote files
polygon-options-puller list-dates

# List files for a specific year/month
polygon-options-puller list-dates --year 2024 --month 3
```

### Python API

```python
from datetime import date
from polygon_options_puller.downloader import pull

written = pull(
    access_key="your-key",
    secret_key="your-secret",
    output_dir="data/aapl",
    data_types=["quotes"],
    symbol_prefix="AAPL",
    start_date=date(2025, 3, 17),
    end_date=date(2025, 3, 21),
    workers=8,
)
```

## Output layout

```
data/aapl/
├── quotes/
│   ├── 2025-03-17.parquet
│   ├── 2025-03-18.parquet
│   ├── 2025-03-19.parquet
│   ├── 2025-03-20.parquet
│   └── 2025-03-21.parquet
└── trades/
    ├── 2025-03-17.parquet
    └── ...
```

Each Parquet file contains only rows matching the `--symbol-prefix` you
specified.  Namespace different underlyings by using different `--output-dir`
paths.

## Data types

| Type | S3 prefix | Description |
|---|---|---|
| `quotes` | `us_options_opra/quotes_v1` | Top-of-book quotes, nanosecond timestamps |
| `trades` | `us_options_opra/trades_v1` | Tick-level trades, nanosecond timestamps |
| `day_aggs` | `us_options_opra/day_aggs_v1` | Daily OHLCV candles |
| `minute_aggs` | `us_options_opra/minute_aggs_v1` | Minute OHLCV candles |

## License

MIT
