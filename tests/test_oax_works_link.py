"""Tests for src/oax/works_link.py (tiny parquet fixtures)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oax.works_link import coinvestigator_authors, decide, works_evidence
from src.oeuvre.acif_works import connect


def test_works_first_anchors_and_decisions(tmp_path):
    for d in ("au", "wk"):
        (tmp_path / d).mkdir()
    a = lambda w, au, inst=None: {"work_idx": w, "author_idx": au, "institution_idx": inst}
    # ACIF A: candidates 10 (2 works with co-investigator record 99) and 11 (1) -> the top one, 10
    # ACIF B: candidates 20 (2 years at I5), 21 (1 year at I5) -> accept_university 20
    # ACIF C: candidates 30, 31 both 2 years at I6 -> several_university
    pd.DataFrame([a(1, 10, 7), a(1, 99, 7), a(2, 11, 8), a(2, 99), a(3, 99), a(11, 10), a(11, 99),
                  a(4, 20, 5), a(5, 20, 5), a(6, 21, 5),
                  a(7, 30, 6), a(8, 30, 6), a(9, 31, 6), a(10, 31, 6)]).to_parquet(tmp_path / "au" / "p.parquet")
    pd.DataFrame([{"work_idx": w, "publication_year": y} for w, y in
                  [(1, 2005), (2, 2006), (3, 2007), (11, 2008), (4, 2010), (5, 2011), (6, 2012), (7, 2013), (8, 2014), (9, 2013), (10, 2015)]]
                 ).to_parquet(tmp_path / "wk" / "p.parquet")
    acifs = pd.DataFrame([{"cluster_id": "A", "coawardee_acif_ids": ["X"]}, {"cluster_id": "B", "coawardee_acif_ids": []},
                          {"cluster_id": "C", "coawardee_acif_ids": []}, {"cluster_id": "D", "coawardee_acif_ids": []}])
    coinv = coinvestigator_authors(acifs, pd.DataFrame([{"cluster_id": "X", "author_idx": 99}]))
    cand = pd.DataFrame([("A", 10), ("A", 11), ("B", 20), ("B", 21), ("C", 30), ("C", 31)], columns=["cluster_id", "author_idx"])
    win = pd.DataFrame([("B", "g1", "I5", 2009, 2013), ("C", "g2", "I6", 2012, 2016)],
                       columns=["cluster_id", "grant_code", "inst", "y0", "y1"])
    ev = works_evidence(connect(), cand, coinv, win, authorships=tmp_path / "au", works=tmp_path / "wk")
    e = ev.set_index(["cluster_id", "author_idx"])
    assert e.loc[("A", 10), "coinv_works"] == 2 and e.loc[("A", 11), "coinv_works"] == 1
    assert e.loc[("B", 20), "uni_years"] == 2 and e.loc[("B", 21), "uni_years"] == 1
    dec, links = decide(["A", "B", "C", "D"], ev)
    d = dec.set_index("cluster_id").status.to_dict()
    assert d == {"A": "accept_coinvestigator", "B": "accept_university", "C": "several_university", "D": "no_candidate"}
    assert sorted(map(tuple, links[["cluster_id", "author_idx"]].values.tolist())) == [("A", 10), ("B", 20)]
