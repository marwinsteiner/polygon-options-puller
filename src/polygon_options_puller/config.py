"""Static configuration: S3 endpoint and the registry of known Polygon flat-file datasets.

Nothing in this module is required to pull a dataset.  Any prefix that exists in
the bucket can be pulled by passing it verbatim (``us_stocks_sip/trades_v1``);
the tables below only provide short aliases and a few typing hints.
"""

from __future__ import annotations

S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET = "flatfiles"

# Short names for the top-level asset-class prefixes in the bucket.
ASSET_CLASSES: dict[str, str] = {
    "stocks": "us_stocks_sip",
    "options": "us_options_opra",
    "indices": "us_indices",
    "forex": "global_forex",
    "crypto": "global_crypto",
}

# Convenience aliases for datasets Polygon is known to publish.  The bucket is
# the source of truth: run ``polygon-puller list-datasets`` to see what your
# subscription can access, and pass any prefix directly if it is not listed here.
KNOWN_DATASETS: dict[str, str] = {
    "stocks/trades": "us_stocks_sip/trades_v1",
    "stocks/quotes": "us_stocks_sip/quotes_v1",
    "stocks/minute_aggs": "us_stocks_sip/minute_aggs_v1",
    "stocks/day_aggs": "us_stocks_sip/day_aggs_v1",
    "options/trades": "us_options_opra/trades_v1",
    "options/quotes": "us_options_opra/quotes_v1",
    "options/minute_aggs": "us_options_opra/minute_aggs_v1",
    "options/day_aggs": "us_options_opra/day_aggs_v1",
    "indices/minute_aggs": "us_indices/minute_aggs_v1",
    "indices/day_aggs": "us_indices/day_aggs_v1",
    "forex/quotes": "global_forex/quotes_v1",
    "forex/minute_aggs": "global_forex/minute_aggs_v1",
    "forex/day_aggs": "global_forex/day_aggs_v1",
    "crypto/trades": "global_crypto/trades_v1",
    "crypto/quotes": "global_crypto/quotes_v1",
    "crypto/minute_aggs": "global_crypto/minute_aggs_v1",
    "crypto/day_aggs": "global_crypto/day_aggs_v1",
}

# Column names that hold integer epoch timestamps in Polygon flat files.  When
# timestamp conversion is set to ``auto`` these columns (plus any column whose
# name ends in ``_timestamp``) are converted from integers to Arrow timestamps.
TIMESTAMP_COLUMN_NAMES: frozenset[str] = frozenset(
    {
        "sip_timestamp",
        "participant_timestamp",
        "trf_timestamp",
        "window_start",
        "window_end",
        "timestamp",
    }
)

# Fallback Arrow types (by alias) for columns that are entirely empty in the
# sampled head of a file.  Without a hint such columns are stored as strings.
COLUMN_TYPE_HINTS: dict[str, str] = {
    "otc": "bool",
    "correction": "int64",
    "tape": "int64",
    "trf_id": "int64",
    "trf_timestamp": "int64",
    "sequence_number": "int64",
    "sip_timestamp": "int64",
    "participant_timestamp": "int64",
    "window_start": "int64",
    "conditions": "string",
    "indicators": "string",
}

# --------------------------------------------------------------------------- #
# Legacy (pre-0.3) options-only registry.  Kept so that existing imports and the
# deprecated ``download`` command keep working.  Prefer KNOWN_DATASETS.
# --------------------------------------------------------------------------- #

OPTIONS_PREFIX = ASSET_CLASSES["options"]

DATA_TYPES: dict[str, dict] = {
    name: {"prefix": f"{OPTIONS_PREFIX}/{name}_v1"}
    for name in ("trades", "quotes", "day_aggs", "minute_aggs")
}
