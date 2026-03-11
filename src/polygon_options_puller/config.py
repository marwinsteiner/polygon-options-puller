"""S3 connection and flat-file path configuration."""

from __future__ import annotations

S3_ENDPOINT = "https://files.massive.com"
S3_BUCKET = "flatfiles"

# Columns that contain nanosecond-epoch timestamps and should be stored as
# pa.timestamp("ns", tz="UTC") in output Parquet files.
TIMESTAMP_COLUMNS: dict[str, list[str]] = {
    "trades": ["sip_timestamp", "participant_timestamp"],
    "quotes": ["sip_timestamp"],
    "day_aggs": [],
    "minute_aggs": ["timestamp"],
}

# Top-level prefix for US options OPRA data inside the bucket.
OPTIONS_PREFIX = "us_options_opra"

# Available flat-file data types and their CSV column schemas.
# Column order matches the CSV headers Polygon ships.
DATA_TYPES: dict[str, dict] = {
    "trades": {
        "prefix": f"{OPTIONS_PREFIX}/trades_v1",
        "columns": [
            "ticker",
            "conditions",
            "correction",
            "exchange",
            "participant_timestamp",
            "price",
            "sequence_number",
            "sip_timestamp",
            "size",
        ],
    },
    "quotes": {
        "prefix": f"{OPTIONS_PREFIX}/quotes_v1",
        "columns": [
            "ticker",
            "ask_exchange",
            "ask_price",
            "ask_size",
            "bid_exchange",
            "bid_price",
            "bid_size",
            "sequence_number",
            "sip_timestamp",
        ],
    },
    "day_aggs": {
        "prefix": f"{OPTIONS_PREFIX}/day_aggs_v1",
        "columns": [
            "ticker",
            "close",
            "high",
            "low",
            "open",
            "transactions",
            "volume",
            "vwap",
            "otc",
        ],
    },
    "minute_aggs": {
        "prefix": f"{OPTIONS_PREFIX}/minute_aggs_v1",
        "columns": [
            "ticker",
            "close",
            "high",
            "low",
            "open",
            "timestamp",
            "transactions",
            "volume",
            "vwap",
            "otc",
        ],
    },
}
