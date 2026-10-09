"""Tests for src/oax/name_link.py (tiny parquet fixtures)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oax.name_link import name_links
from src.oeuvre.acif_works import connect


def test_name_links_tiers_and_decisions(tmp_path):
    (tmp_path / "authors").mkdir()
    A = lambda i, name, keys, orcid=None: {"author_idx": i, "orcid": orcid, "full_name": name, "full_name_keys": keys,
                                           "full_name_key": keys[0]}
    pd.DataFrame([
        A(1, "Jim Bloggs", ["jim_bloggs", "j_bloggs"]),
        A(2, "Joan Bloggs", ["joan_bloggs", "j_bloggs"]),              # incompatible with Jim
        A(3, "Patricia Yeadon", ["patricia_yeadon", "p_yeadon"]),
        A(4, "Wei Wang", ["wei_wang", "w_wang"]), A(5, "Wei Wang", ["wei_wang", "w_wang"]),
        A(6, "Ann Lee", ["ann_lee", "a_lee"]),                          # taken by stage 1
        A(7, "Bo Wu", ["bo_wu", "b_wu"], orcid="0000-9"),               # other ORCID than the ACIF's
        A(8, "Jillian R. Sewell", ["jillian_sewell", "j_sewell", "r_sewell"]),  # middle initial only
    ]).to_parquet(tmp_path / "prep.parquet")
    aff = lambda i, inst, ys: {"author_idx": i, "affiliations": [
        {"institution": {"id": f"https://openalex.org/{inst}", "lineage": [f"https://openalex.org/{inst}"]}, "years": ys}]}
    pd.DataFrame([aff(1, "I1", [2005, 2006, 2007]), aff(2, "I1", [2005, 2006]), aff(3, "I2", [2010, 2011]),
                  aff(4, "I3", [2012, 2013]), aff(5, "I3", [2013, 2014]), aff(6, "I4", [2001, 2002]),
                  aff(7, "I5", [2003, 2004]), aff(8, "I6", [2010, 2011])]).to_parquet(tmp_path / "authors" / "p.parquet")
    acifs = pd.DataFrame([
        {"cluster_id": "A", "orcids": [], "full_name_keys": ["jim_bloggs", "j_bloggs"]},
        {"cluster_id": "B", "orcids": [], "full_name_keys": ["p_yeadon"]},
        {"cluster_id": "C", "orcids": [], "full_name_keys": ["wei_wang", "w_wang"]},
        {"cluster_id": "D", "orcids": [], "full_name_keys": ["ann_lee", "a_lee"]},
        {"cluster_id": "E", "orcids": ["0000-1"], "full_name_keys": ["bo_wu", "b_wu"]},
        {"cluster_id": "F", "orcids": [], "full_name_keys": ["jim_bloggs"]},
        {"cluster_id": "G", "orcids": [], "full_name_keys": ["robert_sewell", "r_sewell"]},
    ])
    windows = pd.DataFrame([("A", "g1", "I1", 2005, 2009), ("B", "g2", "I2", 2009, 2013), ("C", "g3", "I3", 2012, 2016),
                            ("D", "g4", "I4", 2000, 2004), ("E", "g5", "I5", 2002, 2006), ("G", "g6", "I6", 2009, 2013)],
                           columns=["cluster_id", "grant_code", "inst", "y0", "y1"])
    main = pd.DataFrame([("A", "jim_bloggs"), ("B", "p_yeadon"), ("C", "wei_wang"), ("D", "ann_lee"), ("E", "bo_wu"),
                         ("F", "jim_bloggs"), ("G", "robert_sewell")], columns=["cluster_id", "k"])
    pairs, dec = name_links(connect(), acifs, taken={6}, windows=windows, arc_main=main,
                            prep=tmp_path / "prep.parquet", authors=tmp_path / "authors")
    d = dec.set_index("cluster_id")
    assert (d.loc["A", "status"], d.loc["A", "tier"], d.loc["A", "author_idx"]) == ("accept", "full", 1)
    assert 2 not in set(pairs[pairs.cluster_id == "A"].author_idx)                 # Joan is incompatible
    assert (d.loc["B", "status"], d.loc["B", "tier"], d.loc["B", "author_idx"]) == ("accept", "loose", 3)
    assert d.loc["C", "status"] == "several_pass" and d.loc["C", "n_passing"] == 2
    assert d.loc["D", "status"] == "no_candidate"                                  # only candidate taken
    assert d.loc["E", "status"] == "no_candidate"                                  # ORCID guard
    assert d.loc["F", "status"] == "no_single_institution_grant"
    assert d.loc["G", "status"] == "none_pass" or d.loc["G", "status"] == "no_candidate"      # Jillian != Robert
    assert 8 not in set(pairs[(pairs.cluster_id == "G")].author_idx)


def test_name_tier(tmp_path):
    import duckdb
    from src.oax.name_link import name_tier
    prep = pd.DataFrame({"author_idx": [1, 2, 3, 4], "full_name_key": ["shaowu_zhang", "senyuan_zhang", "s_zhang", "lyn_beazley"],
                         "full_name_keys": [["shaowu_zhang", "s_zhang"], ["senyuan_zhang", "s_zhang"], ["s_zhang"],
                                            ["lyn_beazley", "l_beazley"]]})
    prep.to_parquet(tmp_path / "prep.parquet")
    acifs = pd.DataFrame({"cluster_id": ["Z", "B"], "full_name_keys": [["shaowu_zhang", "s_zhang"], ["lynda_beazley", "l_beazley"]]})
    main = pd.DataFrame({"cluster_id": ["Z", "B"], "k": ["shaowu_zhang", "lynda_beazley"]})
    pairs = pd.DataFrame({"cluster_id": ["Z", "Z", "Z", "B"], "author_idx": [1, 2, 3, 4]})
    t = name_tier(duckdb.connect(), pairs, acifs, main, prep=tmp_path / "prep.parquet")
    got = {a: (None if pd.isna(x) else x) for a, x in zip(t.author_idx, t.tier)}
    assert got == {1: "full", 2: None, 3: "loose", 4: None}
