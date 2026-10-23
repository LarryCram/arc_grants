"""Tests for src/oax/shared_records.py (hand-built facts; no data files)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oax.shared_records import decide, name_fits_record


def _f(fields, grants, orcids=(), coinv=()):
    return {"orcids": set(orcids), "for": set(fields), "grants": grants, "coinv": set(coinv)}


def test_name_fits_record():
    assert name_fits_record({"jane"}, ["jane_mitchell", "margaret_mitchell"])
    assert not name_fits_record({"thomas"}, ["mark_gabbott", "m_gabbott"])
    assert name_fits_record({"hui"}, ["h_zhao", "c_zhao"])           # initials-only record: first initial
    assert name_fits_record(set(), ["anything_x"])                    # no full given name: nothing to test


def test_decide_sets_ties_same_grant_and_names():
    facts = {
        "A1": _f({"Civil engineering"}, [("LP1", 2012, frozenset())]),
        "A2": _f({"Civil engineering"}, [("DP2", 2014, frozenset())]),
        "A3": _f({"Gender studies"}, [("DP3", 2025, frozenset())]),
        "A4": _f({"Gender studies"}, [("DP4", 2025, frozenset())]),   # 2 + 2 on one record: a tie, nothing merged
        "B1": _f({"Genetics"}, [("DP5", 2005, frozenset())]),
        "B2": _f({"Genetics"}, [("DP6", 2008, frozenset())]),
        "B3": _f({"History"}, [("DP7", 2009, frozenset())]),            # no shared field or co-investigator
        "C1": _f({"Law"}, [("LP8", 2004, frozenset())]),
        "C2": _f({"Law"}, [("LP8", 2004, frozenset())]),                # same grant: two investigators
    }
    links = pd.DataFrame([("A1", 1), ("A2", 1), ("A3", 1), ("A4", 1), ("B1", 2), ("B2", 2), ("B3", 2),
                          ("C1", 3), ("C2", 3)], columns=["cluster_id", "author_idx"])
    givens = {c: {"james"} for c in "A1 A2 A3 A4".split()} | {c: {"ann"} for c in "B1 B2 B3".split()} | {"C1": {"mark"}, "C2": {"mark"}}
    keys = {1: ["james_smith"], 2: ["ann_lee"], 3: ["mark_gabbott"]}
    sets, left = decide(links, facts, givens, {1: "J Smith", 2: "Ann Lee", 3: "Gabbott, Mark"}, keys)
    assert [list(s) for s in sets.cluster_ids] == [["B1", "B2"]]
    r = dict(zip(left.cluster_id, left.reason))
    assert r["B3"] == "outside_the_merged_set" and r["A1"] == "no_compatible_set" and r["C1"] == "shares_a_grant"


def test_keep_apart_pair_never_proposed():
    facts = {"H1": _f({"Geography"}, [("DP1", 2004, frozenset())]),
             "H2": _f({"Geography"}, [("DP2", 2005, frozenset())])}
    links = pd.DataFrame([("H1", 9), ("H2", 9)], columns=["cluster_id", "author_idx"])
    args = ({"H1": {"gregory"}, "H2": {"gregory"}}, {9: "Greg R. Hancock"}, {9: ["gregory_hancock"]})
    sets, _ = decide(links, facts, *args)
    assert len(sets) == 1
    facts["H1"]["apart"], facts["H2"]["apart"] = {"H2"}, {"H1"}
    sets, left = decide(links, facts, *args)
    assert sets.empty and set(left.cluster_id) == {"H1", "H2"}
