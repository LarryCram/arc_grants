"""Tests for src/oeuvre/work_graph.py (tiny parquet sources)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oeuvre.acif_works import connect
from src.oeuvre.work_graph import build_work_graph


def _au(author, inst=None):
    return {"author_idx": author, "link_status": "accept_name_key", "printed_name": "X",
            "institutions": [] if inst is None else [{"institution_idx": inst, "name": "U", "country": "AU"}]}


def test_components_core_and_anchors(tmp_path):
    (tmp_path / "authorships").mkdir()
    # ACIF C1, own author 1. Works 10-11-12 chained by co-authors 7 and 8; 13 joins 12 by institution 500;
    # 20-21 share co-author 9 (a namesake pile); 30 shares only a mega venue with 10; 40 is a 60-author work
    # sharing co-author 7 (hyperauthored: no co-author link).
    w = lambda wi, src=None, n=3, au=None, y=2010: {"cluster_id": "C1", "work_idx": wi, "publication_year": y,
                                                     "authors_count": n, "source_id": src, "authorships": [au or _au(1)]}
    works = [w(10, src=1), w(11), w(12, au=_au(1, 500)), w(13, au=_au(1, 500)), w(20, y=1975), w(21, y=1976),
             w(30, src=1), w(40, n=60)]
    works[6]["source_id"] = 2
    works[0]["source_id"] = 2
    pd.DataFrame(works).to_parquet(tmp_path / "w.parquet")
    a = lambda wi, ai: {"work_idx": wi, "author_idx": ai}
    pd.DataFrame([a(10, 1), a(10, 7), a(11, 1), a(11, 7), a(11, 8), a(12, 1), a(12, 8), a(13, 1),
                  a(20, 1), a(20, 9), a(21, 1), a(21, 9), a(30, 1), a(40, 1), a(40, 7)]
                 ).to_parquet(tmp_path / "authorships" / "p.parquet")
    pd.DataFrame([{"source_idx": 2, "type": "journal", "works_count": 300_000}]).to_parquet(tmp_path / "s.parquet")
    acifs = pd.DataFrame([{"cluster_id": "C1", "first_year": 2008, "last_year": 2012, "grant_inst": [500]}])
    coinv = pd.DataFrame([{"cluster_id": "C1", "author_idx": 8}])
    n = build_work_graph(connect(), tmp_path / "w.parquet", tmp_path / "g.parquet", acifs, coinv,
                         authorships=tmp_path / "authorships", sources=tmp_path / "s.parquet")
    g = pd.read_parquet(tmp_path / "g.parquet").set_index("work_idx")
    assert g.loc[[10, 11, 12, 13], "component"].tolist() == [10] * 4
    assert g.loc[20, "component"] == g.loc[21, "component"] == 20
    assert g.loc[30, "component"] == 30 and g.loc[40, "component"] == 40      # mega venue, hyperauthored
    assert g.in_core.to_dict() == {10: True, 11: True, 12: True, 13: True, 20: False, 21: False, 30: False, 40: False}
    assert g.loc[11, "anchor_coinvestigator"] and g.loc[12, "anchor_grant_university"]
    assert g.loc[10, "core_by"] == "anchors" and g.loc[11, "coauthor_links"] == 2
    assert n["links_coauthor"] == 6 and n["links_institution"] == 2 and "links_venue" not in n
