"""Tests for src/00f_extract_scopus_documents.py: the throttling backoff and cumulative output (no network)."""
import importlib
import sys

import pandas as pd
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

m = importlib.import_module("src.00f_extract_scopus_documents")


class E429(Exception):
    pass


def _fetcher(monkeypatch, tmp_path, fails):
    monkeypatch.setattr(m, "LOG", tmp_path / "run.log")
    monkeypatch.setattr(m, "BACKOFF", [0.01, 0.01, 0.01])
    f = m.Fetcher()
    f.e429 = E429
    calls = {"n": 0}

    def search(q, **kw):
        calls["n"] += 1
        if calls["n"] <= fails:
            raise E429("quota")
        return SimpleNamespace(results=[SimpleNamespace(eid="2-s2.0-1", doi="10.1/X", coverDate="2010-01-01",
                                                        subtype="ar", title="t", publicationName="J")])
    f.search = search
    return f, calls


def test_backoff_then_success(monkeypatch, tmp_path):
    f, calls = _fetcher(monkeypatch, tmp_path, fails=2)
    f.one("123")
    assert calls["n"] == 3 and f.done == 1 and f.waits == 2 and f.stop is None
    assert f.rows["123"][0][2] == "10.1/x"                       # DOI lower-cased
    assert "pause" in (tmp_path / "run.log").read_text()


def test_stops_after_last_wait(monkeypatch, tmp_path):
    f, calls = _fetcher(monkeypatch, tmp_path, fails=99)
    f.one("123")
    assert f.stop and f.stop.startswith("429") and f.done == 0
    f.one("456")                                                  # nothing more is fetched once stopped
    assert calls["n"] == 4


def test_output_keeps_earlier_runs(monkeypatch, tmp_path):
    cols = ["scopus_id", "eid", "doi", "year", "subtype", "title", "source"]
    prior = pd.DataFrame([("1", "e1", None, "2001", "ar", "a", "J"), ("2", "e2", None, "2002", "ar", "old", "J")], columns=cols)
    monkeypatch.setattr(m, "DOCS", tmp_path / "docs.parquet")
    monkeypatch.setattr(m, "_prior", prior)
    out = m.write_documents({"2": [("2", "e3", None, "2003", "ar", "new", "J")], "3": []})
    assert sorted(out.eid) == ["e1", "e3"]                       # profile 2 replaced, profile 1 kept
    assert len(pd.read_parquet(tmp_path / "docs.parquet")) == 2
