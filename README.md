# polygon-options-puller

Pull **any** [Polygon / Massive](https://massive.com) flat file from their S3
bucket and store it locally in **any** format: Parquet, Feather, CSV, JSON
Lines, or a byte-for-byte raw mirror.

It started life as an options-only Parquet downloader (the name stuck), and now
covers every dataset in the flat-files bucket: stocks, options, indices, forex
and crypto, plus any prefix that appears there in future. The intended use is
a cron job that pulls the newest file every morning into cold storage in
whatever format you prefer.

## How it works

Polygon ships one gzipped CSV per dataset per day; the options quote files
alone are ~120 GB compressed. This tool never holds a whole file in memory:

- **Streams** each object from S3, decompresses and parses it in blocks, and
  writes each block straight to the output file.
- **Filters in flight** (optional) by ticker or ticker prefix, so you can keep
  500 MB of SPXW quotes out of a 120 GB file without downloading it twice.
- **Discovers dates from the bucket**, so weekends, holidays and not-yet
  published days are skipped for every asset class without a market calendar.
- **Remembers what it pulled** in a small manifest, keyed by S3 ETag. Re-runs
  skip files that are up to date, re-pull files Polygon has republished, and
  do not re-scan files a filter matched nothing in.
- **Writes atomically** (temp file + rename), runs downloads in parallel, and
  exits non-zero if any file failed so your scheduler notices.

## Installation

```bash
pip install polygon-options-puller
```

Two commands are installed and do the same thing: `polygon-puller` and the
original `polygon-options-puller`.

## Credentials

Get S3 credentials from your [Massive dashboard](https://massive.com/dashboard).

```bash
export POLYGON_S3_ACCESS_KEY="your-access-key"
export POLYGON_S3_SECRET_KEY="your-secret-key"
```

## Quick start

```bash
# See what your subscription can access
polygon-puller list-datasets

# Pull yesterday's stock daily bars as Parquet
polygon-puller pull -d stocks/day_aggs --latest -o /data/polygon

# Mirror the raw .csv.gz files exactly as Polygon ships them
polygon-puller pull -d stocks/trades -d stocks/quotes --latest -f raw -o /data/polygon

# Backfill a date range as Feather
polygon-puller pull -d crypto/minute_aggs --start-date 2025-01-01 --end-date 2025-03-31 -f feather -o /data/polygon

# Keep only SPXW option quotes, as zstd Parquet
polygon-puller pull -d options/quotes --ticker-prefix O:SPXW --start-date 2025-04-01 -o /data/spxw
```

### As a cron job

Pull the three most recent files every weekday at 06:00. Using `--last 3`
instead of `--latest` means a missed night catches up automatically; files
that are already present are skipped.

```cron
0 6 * * 1-6  POLYGON_S3_ACCESS_KEY=... POLYGON_S3_SECRET_KEY=... \
  /usr/local/bin/polygon-puller pull -d stocks/day_aggs -d stocks/minute_aggs \
    --last 3 -f parquet -o /mnt/cold/polygon -q >> /var/log/polygon-puller.log 2>&1
```

The progress bar switches itself off when stderr is not a terminal, `-q` keeps
the log to warnings and errors, and the exit code is 1 if any file failed.

## Datasets

Pass `-d` as many times as you like. Each value is either a short alias or the
dataset's prefix in the bucket:

| Alias | Bucket prefix |
|---|---|
| `stocks/trades`, `stocks/quotes`, `stocks/minute_aggs`, `stocks/day_aggs` | `us_stocks_sip/<type>_v1` |
| `options/trades`, `options/quotes`, `options/minute_aggs`, `options/day_aggs` | `us_options_opra/<type>_v1` |
| `indices/minute_aggs`, `indices/day_aggs` | `us_indices/<type>_v1` |
| `forex/quotes`, `forex/minute_aggs`, `forex/day_aggs` | `global_forex/<type>_v1` |
| `crypto/trades`, `crypto/quotes`, `crypto/minute_aggs`, `crypto/day_aggs` | `global_crypto/<type>_v1` |

Anything else in the bucket works too: `-d us_stocks_sip/trades_v2` or
`-d stocks/trades_v2`. `polygon-puller list-datasets` walks the bucket and
prints every prefix your credentials can see, and
`polygon-puller list-dates -d stocks/trades --year 2025 --month 3` lists the
files under one.

## Selecting dates

| Option | Meaning |
|---|---|
| `--start-date YYYY-MM-DD [--end-date YYYY-MM-DD]` | Every available file in the range; `--end-date` defaults to today |
| `--latest` | The single most recent file of each dataset |
| `--last N` | The N most recent files of each dataset |
| `--dry-run` | Print what would be pulled and stop |

## Output formats

| `-f` | Extension | Default compression | Notes |
|---|---|---|---|
| `parquet` | `.parquet` | zstd | Dictionary-encoded; also snappy, gzip, lz4, brotli, none |
| `feather` | `.feather` | zstd | Arrow IPC file; also lz4, none |
| `csv` | `.csv` | none | Unquoted header, timestamps as ISO-8601 UTC |
| `csv.gz` | `.csv.gz` | gzip | Same as `csv`, gzipped |
| `jsonl` | `.jsonl` | none | One JSON object per row, timestamps as ISO-8601 UTC strings |
| `jsonl.gz` | `.jsonl.gz` | gzip | Same as `jsonl`, gzipped |
| `raw` | source's | as shipped | Exact copy of the S3 object; no parsing or filtering |

Override the codec with `--compression`, e.g. `-f parquet --compression snappy`
or `-f csv --compression gzip`. `polygon-puller list-formats` prints this list.

## Filtering and shaping rows

All of these are optional; with none of them every row is kept.

| Option | Effect |
|---|---|
| `--ticker AAPL` (repeatable) | Keep rows whose ticker equals the value |
| `--ticker-prefix O:SPXW` (repeatable) | Keep rows whose ticker starts with the value |
| `--filter-column NAME` | Apply the two filters above to a different column |
| `--columns a,b,c` | Keep only these columns |

Option tickers look like `O:AAPL230120C00150000`, so `--ticker-prefix O:AAPL`
keeps every AAPL contract. Note that `O:AAPL` also matches `O:AAPLW` style
tickers; use several `--ticker` values for exact matches.

## Schema and timestamps

Column types are inferred from a sample at the head of each file. Columns that
are empty in the sample fall back to a known type (`otc` is boolean,
`trf_timestamp` is an integer, and so on) or to string. Force a type with
`--column-type NAME=TYPE` using Arrow type names (`int64`, `float32`, `string`,
`bool`, `timestamp[ms]` ...).

Polygon stores timestamps as integer nanoseconds since the epoch. By default
(`--timestamp-columns auto`) the well-known columns (`sip_timestamp`,
`participant_timestamp`, `trf_timestamp`, `window_start`, and anything ending
in `_timestamp`) are converted to UTC timestamps. Use
`--timestamp-columns none` to keep the integers, or name the columns
explicitly. `--timestamp-unit ms` handles datasets that use milliseconds.

## Output layout

By default the local tree mirrors the bucket, with the extension swapped:

```
/data/polygon/
├── .pull-manifest.json
├── us_stocks_sip/
│   └── day_aggs_v1/
│       └── 2025/
│           └── 03/
│               ├── 2025-03-17.parquet
│               └── 2025-03-18.parquet
└── us_options_opra/
    └── quotes_v1/
        └── 2025/03/...
```

Change it with `--path-template`, a path relative to `--output-dir` using
these placeholders:

| Placeholder | Example |
|---|---|
| `{dataset}` | `us_stocks_sip/day_aggs_v1` |
| `{asset_class}` | `us_stocks_sip` |
| `{data_type}` | `day_aggs_v1` |
| `{name}` | `day_aggs` |
| `{year}` `{month}` `{day}` | `2025` `03` `17` |
| `{date}` | `2025-03-17` |
| `{ext}` | `parquet` |
| `{filename}` | `2025-03-17.csv.gz` |

For example `--path-template "{name}/{date}.{ext}"` gives the flat
`day_aggs/2025-03-17.parquet` layout that versions before 0.3 produced.

## Idempotency and the manifest

`.pull-manifest.json` in the output directory records, for each output path,
the S3 key and ETag it came from, the format and filter used, and how many
rows were written. On every run:

- a file is **skipped** if its manifest entry matches the object's current
  ETag and the same settings, and the output still exists;
- it is **re-pulled** if Polygon republished the object (new ETag), you
  changed the format, filter, columns or timestamp settings, or the output
  file was deleted;
- files that already exist but have no manifest entry (written by an older
  version, or by hand) are left alone.

`--force` re-pulls everything regardless. Deleting the manifest only forgets
the bookkeeping; existing files are still left in place.

## Python API

```python
from datetime import date
from polygon_options_puller import pull

result = pull(
    access_key="your-key",
    secret_key="your-secret",
    output_dir="/data/polygon",
    datasets=["stocks/day_aggs", "options/quotes"],
    latest=3,                       # or start_date=date(2025, 1, 1), end_date=...
    output_format="parquet",        # parquet | feather | csv | csv.gz | jsonl | jsonl.gz | raw
    compression="zstd",
    ticker_prefixes=["O:SPXW"],     # optional
    workers=8,
)
print(result.written, result.skipped, result.empty, result.failed)
```

`row_filter` accepts any callable that takes a `pyarrow.RecordBatch` and
returns a (smaller) batch, for filters the CLI cannot express:

```python
import pyarrow.compute as pc

pull(..., row_filter=lambda b: b.filter(pc.greater(b.column("volume"), 10_000)))
```

### Adding an output format

Formats are plain classes; register one and it becomes available to both the
API and the CLI:

```python
import pyarrow as pa
from polygon_options_puller.formats import Writer, register_format

@register_format
class ArrowStreamWriter(Writer):
    name = "arrows"
    extension = "arrows"

    def _open(self, schema: pa.Schema):
        self._sink = pa.OSFile(str(self.path), "wb")
        self._writer = pa.ipc.new_stream(self._sink, schema)

    def _write(self, batch: pa.RecordBatch):
        self._writer.write_batch(batch)

    def _close(self):
        self._writer.close()
        self._sink.close()
```

The writer is opened lazily on the first batch, so a filter that matches
nothing produces no file, and `self.compression` holds the resolved
`--compression` value.

## Migrating from 0.2

- The `download` command still works (with a deprecation notice) and writes
  the same `quotes/2025-03-17.parquet` layout as before. The equivalent new
  command is
  `pull -d options/quotes --ticker-prefix O:SPXW --path-template "{name}/{date}.{ext}"`.
- `pull(data_types=..., symbol_prefix=..., min_rows=...)` keeps working with a
  `DeprecationWarning`; use `datasets=` and `ticker_prefixes=` instead.
- Parquet now defaults to zstd rather than snappy; pass `--compression snappy`
  to keep the old codec. Every Parquet reader handles both.
- Column types are inferred from the data instead of a hard-coded schema, so
  integer columns are stored as `int64` and dictionary encoding happens at the
  Parquet level. Timestamps are still `timestamp[ns, UTC]`.
- `pandas` and `pandas_market_calendars` are no longer dependencies.

## License

MIT
