"""Dataset naming, S3 key layout, and discovery of available flat files."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Iterator

from .config import ASSET_CLASSES, KNOWN_DATASETS
from .s3 import list_common_prefixes, list_objects

_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
_VERSION_SUFFIX_RE = re.compile(r"_v\d+$")


@dataclass(frozen=True)
class Dataset:
    """A flat-file dataset, identified by its S3 prefix (e.g. ``us_stocks_sip/trades_v1``)."""

    prefix: str

    @property
    def asset_class(self) -> str:
        return self.prefix.split("/")[0]

    @property
    def data_type(self) -> str:
        return self.prefix.split("/")[-1]

    @property
    def name(self) -> str:
        """Data type without its version suffix (``trades_v1`` -> ``trades``)."""
        return _VERSION_SUFFIX_RE.sub("", self.data_type)

    @property
    def alias(self) -> str | None:
        for alias, prefix in KNOWN_DATASETS.items():
            if prefix == self.prefix:
                return alias
        return None

    def month_prefix(self, year: int, month: int) -> str:
        return f"{self.prefix}/{year:04d}/{month:02d}/"

    def key_for_date(self, d: date, extension: str = "csv.gz") -> str:
        return f"{self.month_prefix(d.year, d.month)}{d.isoformat()}.{extension}"

    def __str__(self) -> str:
        return self.prefix


@dataclass(frozen=True)
class FlatFile:
    """One dated flat file in the bucket, as returned by discovery."""

    dataset: Dataset
    date: date
    key: str
    etag: str = ""
    size: int = 0

    @property
    def filename(self) -> str:
        return self.key.rsplit("/", 1)[-1]

    @property
    def extension(self) -> str:
        """Everything after the first dot of the filename (``csv.gz``)."""
        name = self.filename
        return name.split(".", 1)[1] if "." in name else ""


def resolve_dataset(name: str) -> Dataset:
    """Turn a user-supplied dataset name into a :class:`Dataset`.

    Accepts a known alias (``stocks/trades``), an asset-class alias combined
    with a raw data type (``stocks/trades_v2``), or a raw bucket prefix
    (``us_stocks_sip/trades_v1``).
    """
    cleaned = name.strip().strip("/")
    if not cleaned:
        raise ValueError("Dataset name must not be empty.")
    if cleaned in KNOWN_DATASETS:
        return Dataset(KNOWN_DATASETS[cleaned])
    parts = cleaned.split("/")
    if len(parts) == 2 and parts[0] in ASSET_CLASSES:
        return Dataset(f"{ASSET_CLASSES[parts[0]]}/{parts[1]}")
    if len(parts) >= 2 and all(parts):
        return Dataset(cleaned)
    known = ", ".join(sorted(KNOWN_DATASETS))
    raise ValueError(
        f"Unknown dataset {name!r}. Use an alias ({known}) or a bucket prefix "
        "such as 'us_stocks_sip/trades_v1'."
    )


def date_from_key(key: str) -> date | None:
    """Extract the ``YYYY-MM-DD`` date embedded in a flat-file key's basename."""
    m = _DATE_RE.search(key.rsplit("/", 1)[-1])
    if not m:
        return None
    try:
        return date.fromisoformat(m.group(1))
    except ValueError:
        return None


def iter_months(start: date, end: date) -> Iterator[tuple[int, int]]:
    """Yield ``(year, month)`` pairs from *start* to *end* inclusive."""
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def _files_under(s3_client, dataset: Dataset, year: int, month: int) -> list[FlatFile]:
    files = []
    for obj in list_objects(s3_client, dataset.month_prefix(year, month)):
        d = date_from_key(obj.key)
        if d is None:
            continue
        files.append(FlatFile(dataset=dataset, date=d, key=obj.key, etag=obj.etag, size=obj.size))
    return files


def discover_files(s3_client, dataset: Dataset, start: date, end: date) -> list[FlatFile]:
    """Return the flat files available for *dataset* between *start* and *end* inclusive.

    Availability is determined by listing the bucket, so weekends, holidays and
    not-yet-published days are naturally excluded for every asset class.
    """
    if start > end:
        raise ValueError(f"start date {start} is after end date {end}")
    files: list[FlatFile] = []
    for year, month in iter_months(start, end):
        files.extend(
            f for f in _files_under(s3_client, dataset, year, month) if start <= f.date <= end
        )
    files.sort(key=lambda f: f.date)
    return files


def discover_latest(
    s3_client,
    dataset: Dataset,
    n: int = 1,
    today: date | None = None,
    max_months: int = 24,
) -> list[FlatFile]:
    """Return the *n* most recent flat files for *dataset*, oldest first.

    Walks backwards month by month from *today* (default: the current date)
    until *n* files have been found or *max_months* have been inspected.
    """
    if n < 1:
        raise ValueError("n must be at least 1")
    today = today or date.today()
    year, month = today.year, today.month
    found: list[FlatFile] = []
    for _ in range(max_months):
        found.extend(_files_under(s3_client, dataset, year, month))
        if len(found) >= n:
            break
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    found.sort(key=lambda f: f.date, reverse=True)
    return sorted(found[:n], key=lambda f: f.date)


def list_datasets(s3_client) -> list[str]:
    """Walk the bucket two levels deep and return every ``asset_class/data_type`` prefix."""
    datasets: list[str] = []
    for asset_prefix in list_common_prefixes(s3_client, ""):
        for data_prefix in list_common_prefixes(s3_client, asset_prefix):
            datasets.append(data_prefix.strip("/"))
    return datasets
