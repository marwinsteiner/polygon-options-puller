import json

from polygon_options_puller.manifest import MANIFEST_NAME, Manifest


def _record(m: Manifest, rel: str, etag: str = "e1", signature: str = "s1", written: bool = True):
    m.record(rel, key="k/" + rel, etag=etag, size=10, rows=5, signature=signature, format="parquet", written=written)


def test_record_and_query(tmp_path):
    m = Manifest(tmp_path)
    assert m.get("a.parquet") is None
    _record(m, "a.parquet")
    assert m.is_current("a.parquet", "e1", "s1")
    assert not m.is_current("a.parquet", "e2", "s1")
    assert not m.is_current("a.parquet", "e1", "other")
    assert m.get("a.parquet")["rows"] == 5
    assert len(m) == 1


def test_persists_atomically(tmp_path):
    m = Manifest(tmp_path)
    _record(m, "x/a.parquet")
    _record(m, "x/b.parquet", written=False)
    assert (tmp_path / MANIFEST_NAME).exists()
    assert not (tmp_path / (MANIFEST_NAME + ".tmp")).exists()
    payload = json.loads((tmp_path / MANIFEST_NAME).read_text())
    assert payload["version"] == 1
    assert set(payload["entries"]) == {"x/a.parquet", "x/b.parquet"}

    again = Manifest(tmp_path)
    assert again.is_current("x/a.parquet", "e1", "s1")
    assert again.get("x/b.parquet")["written"] is False


def test_forget(tmp_path):
    m = Manifest(tmp_path)
    _record(m, "a")
    m.forget("a")
    assert m.get("a") is None
    assert Manifest(tmp_path).get("a") is None


def test_corrupt_manifest_is_ignored(tmp_path):
    (tmp_path / MANIFEST_NAME).write_text("{not json")
    assert len(Manifest(tmp_path)) == 0
    (tmp_path / MANIFEST_NAME).write_text(json.dumps({"entries": [1, 2]}))
    assert len(Manifest(tmp_path)) == 0
