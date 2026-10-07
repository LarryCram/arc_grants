"""Tests for src/oeuvre/acif_works.py (hand-built frames and tiny parquet sources)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oeuvre.acif_works import accepted_links, build_acif_works, connect


def _links():
    return pd.DataFrame([
        {"cluster_id": "C1", "author_idx": 10, "status": "accept_name_key"},
        {"cluster_id": "C1", "author_idx": 11, "status": "accept_hand"},
        {"cluster_id": "C1", "author_idx": 12, "status": "reject_unrelated"},
        {"cluster_id": "C2", "author_idx": 20, "status": "accept_name_key"},
    ])


def _write(tmp_path):
    for d in ("authorships", "works", "topics"):
        (tmp_path / d).mkdir()
    au = lambda w, a, n, i, c: {"work_idx": w, "author_idx": a, "author_name": n, "institution_idx": i,
                                 "institution_name": None if i is None else f"U{i}", "ror": None, "country_code": c}
    pd.DataFrame([
        au(1, 10, "Jan Smith", 7, "AU"), au(1, 10, "Jan Smith", 7, "AU"),      # exact duplicate row
        au(1, 10, "Jan Smith", 8, "GB"),                                       # second institution
        au(1, 20, "Ann Lee", 7, "AU"),                                         # same work, other ACIF
        au(2, 11, "J. Smith", None, None),                                     # no institution
        au(3, 12, "Bob Other", 9, "US"),                                       # rejected link
    ]).to_parquet(tmp_path / "authorships" / "p.parquet")
    pd.DataFrame([{"work_idx": w, "doi": None, "title": "t", "authors_count": 2, "institutions_distinct_count": 1,
                   "publication_year": 2010, "referenced_works_count": 0, "cited_by_count": 1, "type": "article",
                   "is_retracted": False, "is_paratext": False, "volume": None, "issue": None, "first_page": None,
                   "last_page": None, "source_id": 5, "host": None} for w in (1, 2, 3)]).to_parquet(tmp_path / "works" / "p.parquet")
    t = lambda w, s, sf, f: {"work_idx": w, "topic_idx": 1, "score": s, "subfield_idx": 1, "subfield_name": sf,
                             "field_idx": 1, "field_name": f, "domain_idx": 1, "domain_name": "D"}
    pd.DataFrame([t(1, 1.0, "Physiology", "Medicine"), t(1, 1.0, "Cardiology", "Medicine"),
                  t(2, 1.0, "Math Phys", "Mathematics"), t(2, 1.0, "Mech", "Engineering"),
                  t(2, 1.0, "Theory", "Computer Science")]).to_parquet(tmp_path / "topics" / "p.parquet")


def test_accepted_links_only():
    assert list(accepted_links(_links()).author_idx) == [10, 11, 20]


def test_build_acif_works(tmp_path):
    _write(tmp_path)
    out = tmp_path / "aw.parquet"
    n = build_acif_works(_links(), out, connect(), authorships=tmp_path / "authorships",
                         works=tmp_path / "works", topics=tmp_path / "topics")
    w = pd.read_parquet(out).set_index(["cluster_id", "work_idx"])
    assert n == 3 and sorted(w.index) == [("C1", 1), ("C1", 2), ("C2", 1)]       # rejected author's work absent
    a = w.loc[("C1", 1), "authorships"]
    assert len(a) == 1 and a[0]["author_idx"] == 10 and a[0]["printed_name"] == "Jan Smith"
    assert sorted(i["country"] for i in a[0]["institutions"]) == ["AU", "GB"]      # duplicate dropped, both kept
    assert len(w.loc[("C1", 2), "authorships"][0]["institutions"]) == 0
    assert w.loc[("C1", 1), "dominant_field"] == "Medicine" and pd.isna(w.loc[("C1", 1), "dominant_subfield"])
    assert pd.isna(w.loc[("C1", 2), "dominant_field"])
    assert w.loc[("C1", 1), ["work_authors", "work_authors_with_institution"]].tolist() == [2, 2]
    assert w.loc[("C1", 2), ["work_authors", "work_authors_with_institution"]].tolist() == [1, 0]


def test_filter_works(tmp_path):
    from src.oeuvre.work_filter import filter_works
    row = dict(is_paratext=False, is_retracted=False, type="article", doi="10.1/x", work_authors=2,
               work_authors_with_institution=1, publication_year=2010)
    rows = [dict(row, work_idx=1),                                          # kept
            dict(row, work_idx=2, is_paratext=True, is_retracted=True),      # paratext comes first
            dict(row, work_idx=3, is_retracted=True),
            dict(row, work_idx=4, type="dataset"),
            dict(row, work_idx=5, type="dissertation"),                      # kept
            dict(row, work_idx=6, doi=None, work_authors_with_institution=0),
            dict(row, work_idx=7, doi=None),                                 # kept: institution present
            dict(row, work_idx=8, work_authors_with_institution=0)]          # kept: DOI present
    pd.DataFrame([dict(r, cluster_id="C1") for r in rows]).to_parquet(tmp_path / "in.parquet")
    kept, dropped = filter_works(connect(), tmp_path / "in.parquet", tmp_path / "k.parquet", tmp_path / "d.parquet")
    assert (kept, dropped) == (4, 4)
    assert sorted(pd.read_parquet(tmp_path / "k.parquet").work_idx) == [1, 5, 7, 8]
    d = pd.read_parquet(tmp_path / "d.parquet").set_index("work_idx").drop_reason.to_dict()
    assert d == {2: "paratext", 3: "retracted", 4: "type", 6: "no_inst_no_doi"}
