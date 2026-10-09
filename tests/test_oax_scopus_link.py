"""Tests for src/oax/scopus_link.py (the Scopus DOI bridge), hand-built frames only."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.oax import scopus_link as sl


def _ev(rows):
    return pd.DataFrame(rows, columns=["cluster_id", "scopus_id", "author_idx", "author_name", "author_orcid",
                                       "profile_dois", "profile_dois_in_openalex", "shared_dois"])


def test_usable_profiles_drops_other_and_refused_orcids():
    p = pd.DataFrame({"cluster_id": ["A", "A", "B", "C"], "scopus_id": ["1", "2", "3", "4"],
                      "profile_orcid": ["o-a", "o-x", None, "o-c"]})
    refused = pd.DataFrame({"cluster_id": ["C"], "orcid": ["o-c"]})
    out = sl.usable_profiles(p, {"A": "o-a"}, refused)
    assert list(out.scopus_id) == ["1", "3"]       # A's other-ORCID profile and C's refused one go


def test_decide_outcomes():
    ids = list("ABCDEFGHI")
    prof = pd.DataFrame({"cluster_id": ["A", "B", "C", "C", "E", "F", "G", "H", "I"],
                         "scopus_id": ["1", "2", "3", "4", "6", "7", "8", "9", "10"]})
    ev = _ev([("A", "1", 10, "a", None, 20, 18, 15), ("A", "1", 11, "a", None, 20, 18, 2),   # A: top record 10
              ("B", "2", 20, "b", None, 9, 9, 3), ("B", "2", 21, "b", None, 9, 9, 3),        # B: tie
              ("E", "6", 30, "e", None, 5, 5, 1),                                           # E: below MIN_SHARED
              ("F", "7", 40, "f", None, 100, 100, 5),                                       # F: below MIN_SHARE
              ("G", "8", 50, "g", None, 10, 10, 8),                                         # G: taken elsewhere
              ("H", "9", 60, "h", "o-x", 10, 10, 8)])                                       # H: other ORCID
    tiers = {("A", 10): "full", ("G", 50): "full", ("H", 60): "full"}
    dec, links = sl.decide(ids, prof, {"1", "2", "3", "4", "6", "7", "8", "9", "10"}, ev, {"H": "o-h"}, {50: "Z"}, tiers)
    st = dict(zip(dec.cluster_id, dec.status))
    assert st == {"A": "accept_scopus", "B": "several_records", "C": "several_profiles", "D": "no_profile",
                  "E": "weak_shared_record", "F": "weak_shared_record", "G": "record_linked_elsewhere",
                  "H": "record_other_orcid", "I": "no_candidate"}
    assert links.author_idx.tolist() == [10]
    assert dec.set_index("cluster_id").loc["G", "linked_to_acif"] == "Z"


def test_calibrate_counts_correct_top_record():
    ev = _ev([("A", "1", 10, "", None, 5, 5, 4), ("A", "1", 11, "", None, 5, 5, 1), ("B", "2", 20, "", None, 5, 5, 3),
              ("C", "3", 30, "", None, 5, 5, 2), ("C", "3", 31, "", None, 5, 5, 2)])
    cal = sl.calibrate(ev, pd.DataFrame({"cluster_id": ["A", "B"], "author_idx": [10, 21]}))
    assert cal == {"acifs": 3, "linked": 2, "correct": 1, "ties": 1}


def test_name_differs_when_no_tier():
    prof = pd.DataFrame({"cluster_id": ["A"], "scopus_id": ["1"]})
    ev = _ev([("A", "1", 10, "Senyuan Zhang", None, 9, 9, 8)])
    dec, links = sl.decide(["A"], prof, {"1"}, ev, {}, {}, {})
    assert dec.status.tolist() == ["record_name_differs"] and links.empty


def test_nickname_accepted_namesake_refused():
    assert sl.nickname_related(["lynda_beazley", "l_beazley"], ["lyn_beazley", "l_beazley"])
    assert sl.nickname_related(["james_hagan"], ["jim_hagan"])
    assert not sl.nickname_related(["shao-wu_zhang", "s_zhang"], ["senyuan_zhang", "s_zhang"])
    assert not sl.nickname_related(["stuart_taylor"], ["seamus_taylor"])
    prof = pd.DataFrame({"cluster_id": ["A"], "scopus_id": ["1"]})
    ev = _ev([("A", "1", 10, "Lyn D. Beazley", None, 9, 9, 8)])
    dec, links = sl.decide(["A"], prof, {"1"}, ev, {}, {}, {}, {("A", 10)})
    assert dec.status.tolist() == ["accept_scopus_nickname"] and links.author_idx.tolist() == [10]
