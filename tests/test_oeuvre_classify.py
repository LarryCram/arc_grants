"""Tests for src/oeuvre/classify.py rules and verdict mapping (tiny parquet inputs; no API calls)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oeuvre import classify as cls
from src.oeuvre.acif_works import connect


def test_rules_requests_and_verdicts(tmp_path):
    g, w = [], []
    def add(wi, comp, size, anchored, core, y, field):
        g.append({"cluster_id": "C1", "work_idx": wi, "component": comp, "component_size": size,
                  "component_anchored": anchored, "in_core": core})
        w.append({"cluster_id": "C1", "work_idx": wi, "publication_year": y, "fields": [{"name": field, "share": 1.0}],
                  "title": f"t{wi}", "type": "article", "source_id": None, "authors_count": 2, "cited_by_count": 0,
                  "authorships": [], "doi": None})
    for i, y in enumerate(range(2000, 2011, 2), 1):                  # core 1-6, field A, 2000-2010
        add(i, 1, 6, 3, True, y, "A")
    add(7, 7, 1, 1, False, 2005, "Z")                               # anchored component
    add(8, 8, 1, 0, False, 1960, "A")                               # before career
    for i, y in zip(range(9, 14), range(2002, 2007)):               # namesake component: same years, field B
        add(i, 9, 5, 0, False, y, "B")
    for i, y in zip(range(14, 19), range(2003, 2008)):              # similar-field component -> request
        add(i, 14, 5, 0, False, y, "A")
    add(19, 19, 1, 0, False, 2005, "A")                             # fits core
    add(20, 20, 1, 0, False, 2005, "C")                             # other field -> works request
    add(21, 21, 1, 0, False, 2030, "A")                             # outside the years -> works request
    for i in (30, 31, 32):                                          # encyclopedia entries, 120 authors each
        add(i, 1, 6, 3, True, 2010, "A")
    add(33, 1, 6, 3, True, 2010, "A")                               # 25-author chapter, only one of its book
    add(34, 34, 1, 0, False, 2010, "A"); add(35, 35, 1, 0, False, 2010, "A")   # another encyclopedia, none on ORCID
    add(40, 40, 1, 0, False, 2006, "A"); add(41, 41, 1, 0, False, 2006, "A")   # fits core, absent from Scopus
    for x in w:
        if x["work_idx"] in (19, 40, 41):
            x.update(doi=f"10.1/W{x['work_idx']}")
        if x["work_idx"] == 41:
            x.update(type="book-chapter")
        if x["work_idx"] in (30, 31, 32):
            x.update(type="book-chapter", authors_count=120, doi=f"10.1007/978-3-540-68706-1_{x['work_idx']}")
        if x["work_idx"] in (34, 35):
            x.update(type="book-chapter", authors_count=60, doi=f"10.1016/b978-0-12-111111-1.000{x['work_idx']}-x")
        if x["work_idx"] == 33:
            x.update(type="book-chapter", authors_count=25, doi="10.1007/978-1-11-111111-1_1")
    for x in w:
        x["versions"] = [{"doi": x["doi"]}] if x["doi"] else []
    pd.DataFrame(g).to_parquet(tmp_path / "g.parquet")
    pd.DataFrame(w).to_parquet(tmp_path / "w.parquet")
    con = connect()
    con.register("acif_in", pd.DataFrame([{"cluster_id": "C1", "first_year": 2004}]))
    con.register("orcid_dois", pd.DataFrame([{"cluster_id": "C1", "doi": "10.1007/978-3-540-68706-1_31"}]))
    con.register("scopus_dois", pd.DataFrame([{"cluster_id": "C1", "doi": "10.1/w19"}]))   # lower-cased
    con.register("scopus_acifs", pd.DataFrame([{"cluster_id": "C1"}]))
    con.register("saved_works", pd.DataFrame({"cluster_id": pd.Series(dtype=str), "work_idx": pd.Series(dtype="int64")}))
    cls._rules(con, tmp_path / "w.parquet", tmp_path / "g.parquet")
    r = dict(con.execute("SELECT work_idx, rule FROM cls").fetchall())
    assert [r[i] for i in (1, 7, 8, 9, 14, 19, 20, 21)] == ["core", "anchored component", "before career",
                                                            "namesake component", "component", "fits core", "works", "works"]
    assert [r[i] for i in (30, 31, 32, 33)] == ["reference work: collapsed", "reference work: on ORCID list",
                                                 "reference work: collapsed", "core"]
    assert [r[i] for i in (34, 35)] == ["reference work: one per book", "reference work: collapsed"]
    req = cls.requests_table(con).set_index("kind")
    assert list(req.loc["component", "work_idxs"]) == [14, 15, 16, 17, 18]
    assert list(req.loc["works", "work_idxs"]) == [20, 21]
    req = req.reset_index().assign(id_order=lambda d: [[], [20, 21]] if d.kind.iloc[0] == "component" else [[20, 21], []])
    v = {req.request_key[req.kind == "component"].iloc[0]: {"answer": {"verdict": "same", "confidence": "high"}},
         req.request_key[req.kind == "works"].iloc[0]: {"kind": "works", "cluster_id": "C1", "id_order": [20, 21], "answer": [
             {"id": 1, "verdict": "out", "confidence": "high", "reason": "x"},
             {"id": 2, "verdict": "out", "confidence": "medium", "reason": "y"}]}}
    cls.apply_verdicts(con, v, req, tmp_path / "k.parquet")
    k = pd.read_parquet(tmp_path / "k.parquet").set_index("work_idx")
    assert k.loc[14, "decision"] == "accept" and k.loc[14, "decided_by"] == "gemini"
    assert k.loc[20, "decision"] == "reject" and k.loc[21, "decision"] == "unsure"
    assert k.loc[9, "decision"] == "reject" and k.loc[1, "decided_by"] == "rule"
    # fits core, decided from the trusted Scopus profile
    assert k.loc[19, "decision"] == "accept" and k.loc[19, "decided_by"] == "scopus" and k.loc[19, "on_scopus_profile"]
    assert k.loc[40, "decision"] == "reject" and k.loc[41, "decision"] == "unsure"
    # without a trusted profile, 'fits core' works become unsure
    con.register("scopus_acifs", pd.DataFrame({"cluster_id": pd.Series(dtype=str)}))
    cls.apply_verdicts(con, v, req, tmp_path / "k2.parquet")
    k2 = pd.read_parquet(tmp_path / "k2.parquet").set_index("work_idx")
    assert k2.loc[19, "decision"] == "unsure" and k2.loc[19, "reason"] == "no trusted Scopus profile"
    assert k2.loc[1, "on_scopus_profile"] is None or pd.isna(k2.loc[1, "on_scopus_profile"])
