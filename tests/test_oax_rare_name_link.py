"""Tests for src/oax/rare_name_link.py (the rare-family-name stage), hand-built frames only."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.oax import rare_name_link as rn


def _cands(rows):
    rows = [r[:5] + (any(len(k.split("_", 1)[0]) > 1 for k in r[3]),) + r[5:] for r in rows]
    return pd.DataFrame(rows, columns=["author_idx", "family", "author_name", "author_keys", "orcid", "has_full",
                                       "in_pool", "first_name", "first_initial", "works"])


def _main(rows):
    return pd.DataFrame(rows, columns=["cluster_id", "k"])


def test_compatible_forms():
    c = _cands([(1, "quispel", "Quispel, Reinout", ["reinout_quispel", "r_quispel"], None, True, "reinout", "r", 5),
                (2, "opat", "Opat, G. I.", ["g_opat", "i_opat", "gi_opat"], None, True, "g.", "g", 3),
                (3, "watling", "R. John Watling", ["r_watling", "john_watling", "j_watling"], None, True, "r.", "r", 93),
                (4, "quispel", "Chris Quispel", ["chris_quispel", "c_quispel"], None, True, "chris", "c", 18)])
    rows = list(c.itertuples())
    assert rn.compatible(["reinout_quispel", "r_quispel"], ["reinout_quispel"], rows[0])
    assert rn.compatible(["geoffrey_opat", "g_opat"], ["geoffrey_opat"], rows[1])         # initials only
    assert rn.compatible(["roger_watling", "r_watling"], ["roger_watling"], rows[2])        # R. John
    assert not rn.compatible(["reinout_quispel", "r_quispel"], ["reinout_quispel"], rows[3])
    bai = _cands([(5, "hosseini bai", "Shaoyuan Bai", ["s_hosseini bai"], None, False, None, None, 4)])
    bai["has_full"] = True                                                                  # shaoyuan_bai elsewhere
    assert not rn.compatible(["shahla_hosseini bai", "s_hosseini bai"], ["shahla_hosseini bai"], next(bai.itertuples()))


def test_decide_outcomes():
    main = _main([("A", "reinout_quispel"), ("B", "gay_ochiltree"), ("C", "eva_abal"), ("D", "john_smith"),
                  ("E", "sam_rare"), ("F", "sam_rare"), ("G", "ann_odd"), ("H", "tom_unique")])
    keys = {c: list(d.k) for c, d in main.groupby("cluster_id")}
    counts = {"quispel": 6, "ochiltree": 1, "abal": 1, "smith": 5000, "rare": 2, "odd": 1, "unique": 1}
    cands = _cands([(1, "quispel", "Quispel, Reinout", ["reinout_quispel"], None, True, "reinout", "r", 5),
                    (2, "ochiltree", "Gay Ochiltree", ["gay_ochiltree"], None, False, None, None, 12),
                    (3, "abal", "Eva Abal", ["eva_abal"], None, True, "eva", "e", 30),
                    (4, "abal", "E. G. Abal", ["e_abal", "g_abal", "eg_abal"], None, True, "e.", "e", 4),
                    (5, "odd", "Ann Odd", ["ann_odd"], "o-x", True, "ann", "a", 9),
                    (6, "unique", "Tom Unique", ["tom_unique"], None, True, "tom", "t", 9)])
    dec, links = rn.decide(list("ABCDEGH"), keys, main, counts, cands, {6: "Z"}, {"G": "o-g"}, {}, rare_max=6)
    st = dict(zip(dec.cluster_id, dec.status))
    assert st == {"A": "accept_rare_name", "B": "accept_rare_name", "C": "several_candidates", "D": "not_rare",
                  "E": "several_acifs", "G": "record_other_orcid", "H": "record_linked_elsewhere"}
    assert sorted(links.author_idx) == [1, 2]


def test_calibrate_bands():
    kdec = pd.DataFrame({"cluster_id": ["A", "B", "C"], "status": ["accept_rare_name"] * 3,
                         "author_idx": [1, 2, 3], "n_candidates": [1, 1, 1], "family_count": [1, 1, 8]})
    cal = rn.calibrate(kdec, pd.DataFrame({"cluster_id": ["A", "C"], "author_idx": [1, 9]}))
    assert cal.set_index("family_count").loc["1"].tolist() == [2, 1]
    assert cal.set_index("family_count").loc["6-10"].tolist() == [1, 0]
