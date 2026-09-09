"""Idempotency bookkeeping.

A small JSON manifest at the root of the output directory records, for every
output path, which S3 object (key + ETag) it was produced from and with what
settings.  On the next run a file is skipped when the object is unchanged and
the settings match; if Polygon republishes a file (new ETag) or you change the
format/filter, it is pulled again.  Filters that matched zero rows are also
recorded so the same multi-gigabyte file is not re-scanned every night.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MANIFEST_NAME = ".pull-manifest.json"
MANIFEST_VERSION = 1


class Manifest:
    """Thread-safe, atomically saved map of output path -> provenance."""

    def __init__(self, root: str | Path, filename: str = MANIFEST_NAME):
        self.path = Path(root) / filename
        self._lock = threading.Lock()
        self._entries: dict[str, dict[str, Any]] = self._load()

    def _load(self) -> dict[str, dict[str, Any]]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        entries = data.get("entries") if isinstance(data, dict) else None
        return dict(entries) if isinstance(entries, dict) else {}

    @property
    def entries(self) -> dict[str, dict[str, Any]]:
        return dict(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, rel_path: str) -> dict[str, Any] | None:
        return self._entries.get(rel_path)

    def is_current(self, rel_path: str, etag: str, signature: str) -> bool:
        """True if *rel_path* was produced from an object with *etag* under *signature*."""
        entry = self._entries.get(rel_path)
        return bool(entry) and entry.get("etag") == etag and entry.get("signature") == signature

    def record(
        self,
        rel_path: str,
        *,
        key: str,
        etag: str,
        size: int,
        rows: int | None,
        signature: str,
        format: str,
        written: bool,
    ) -> None:
        entry = {
            "key": key,
            "etag": etag,
            "size": size,
            "rows": rows,
            "signature": signature,
            "format": format,
            "written": written,
            "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        with self._lock:
            self._entries[rel_path] = entry
            self._save()

    def forget(self, rel_path: str) -> None:
        with self._lock:
            if self._entries.pop(rel_path, None) is not None:
                self._save()

    def _save(self) -> None:
        payload = {"version": MANIFEST_VERSION, "entries": dict(sorted(self._entries.items()))}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)
