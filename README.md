# polygon-options-puller

Download [Polygon / Massive](https://massive.com) US options (OPRA) flat files
from their S3 bucket and store them locally as Snappy-compressed,
dictionary-encoded **Parquet** files, partitioned by **date** and **underlying
ticker**.

## Why Parquet?

Polygon ships daily `.csv.gz` files containing *all* option tickers for an
entire trading day.  The full quote history is roughly **100 TB** in this
format.  Converting to Parquet with the right settings cuts that dramatically:

| Lever | Savings |
|---|---|
| **Columnar layout** – values of the same type stored together compress far better than row-oriented CSV | ~3-4× |
| **Dictionary encoding** – exchange IDs, tickers, correction flags are tiny enum-like columns that dictionary-encode to a few bytes per value | ~2-3× on those columns |
| **Snappy compression** on top of the above | additional ~1.5-2× |
| **Underlying-based partitioning** – all option tickers for a given root (e.g. all AAPL options) land in the same file, concentrating similar data | better dictionary & RLE hits |

Realistic total: **5-10×** smaller than gzipped CSV → **~10-20 TB** for the
full history.

## Installation

```bash
pip install .
# or in editable mode for development:
pip install -e ".[dev]"
```

## Credentials

You need Polygon / Massive S3 credentials.  Get them from your
[Massive dashboard](https://massive.com/dashboard).

Set them as environment variables or pass them as CLI flags:

```bash
export POLYGON_S3_ACCESS_KEY="your-access-key"
export POLYGON_S3_SECRET_KEY="your-secret-key"
```

## Usage

### Download data

```bash
# Download yesterday's quotes for all underlyings
polygon-options-puller download

# Download AAPL option quotes for a specific date range
polygon-options-puller download \
    --underlying AAPL \
    --start-date 2024-03-01 \
    --end-date 2024-03-31

# Download trades instead of quotes
polygon-options-puller download -t trades --underlying SPY --start-date 2024-06-01

# Download minute aggregates
polygon-options-puller download -t minute_aggs --start-date 2024-01-02
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
    output_dir="data",
    data_type="quotes",
    underlying="AAPL",
    start_date=date(2024, 3, 1),
    end_date=date(2024, 3, 1),
)
```

## Output layout

```
data/
└── quotes/
    └── date=2024-03-01/
        ├── underlying=AAPL/
        │   └── data.parquet
        ├── underlying=SPY/
        │   └── data.parquet
        └── underlying=TSLA/
            └── data.parquet
```

Partitioning by `date` then `underlying` means you only touch the files you
need, and each file's columns are extremely homogeneous—ideal for Parquet's
dictionary and RLE encoders.

## Data types

| Type | S3 prefix | Description |
|---|---|---|
| `quotes` | `us_options_opra/quotes_v1` | Top-of-book quotes, nanosecond timestamps |
| `trades` | `us_options_opra/trades_v1` | Tick-level trades, nanosecond timestamps |
| `day_aggs` | `us_options_opra/day_aggs_v1` | Daily OHLCV candles |
| `minute_aggs` | `us_options_opra/minute_aggs_v1` | Minute OHLCV candles |

## License

MIT
