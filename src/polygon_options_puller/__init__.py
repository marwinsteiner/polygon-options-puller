"""Pull any Polygon (Massive) flat file from S3 and store it locally in your preferred format."""

__version__ = "0.3.0"

from .datasets import Dataset, FlatFile, resolve_dataset  # noqa: E402
from .downloader import DEFAULT_PATH_TEMPLATE, PullResult, pull  # noqa: E402
from .formats import FORMATS, Writer, register_format  # noqa: E402

__all__ = [
    "DEFAULT_PATH_TEMPLATE",
    "Dataset",
    "FORMATS",
    "FlatFile",
    "PullResult",
    "Writer",
    "__version__",
    "pull",
    "register_format",
    "resolve_dataset",
]
