"""Tests for src/oax/orcid_hint_link.py (hand-built frames) and stage 1's accept_orcid_names widening."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oax.orcid_hint_link import COMMON, decide, evidence, names_compatible


def test_names_compatible():
    assert names_compatible(["susan_dockett"], ["sue_dockett"])                                        # nickname list
    assert names_compatible(["bradley_wilson sherman"], ["brad_sherman"])                              # compound surname
    assert not names_compatible(["robin_hyndman", "r_hyndman"], ["rob_hyndman", "r_hyndman"])        # no clipped forms
    assert not names_compatible(["hongjie_an"], ["hong_an"])
    assert names_compatible(["killugudi_swaminatha-iyer"], ["killugudi_iyer", "swaminatha_iyer"])
    assert names_compatible(["charles_lineweaver"], ["charles_lineweaver", "c_lineweaver"])            # shared key
    assert not names_compatible(["andrew_killcross", "a_killcross"], ["simon_killcross", "s_killcross"])
    assert not names_compatible(["robert_white"], ["roberta_whitehead"])
    assert not names_compatible(["j_smith"], ["j_smith"])                                              # initials never count


def _cands(rows):
    return pd.DataFrame(rows, columns=["cluster_id", "author_idx", "oax_name", "oax_orcid", "author_keys", "route", "n_candidates"])


def _facts(rows):
    return pd.DataFrame(rows, columns=["orcid", "orcid_source", "orcid_keys", "orcid_name", "orcid_au", "orcid_affiliations"])


def test_evidence_checks_and_decide():
    keys = {"C1": ["robert_white"], "C2": ["wei_liu"], "C3": ["lisa_spagnolo"], "C4": ["jan_ross"]}
    fields = {"C1": {"Arts and Humanities"}, "C2": {"Social Sciences"}, "C3": {"Social Sciences"}, "C4": {"Medicine"}}
    cands = _cands([
        ("C1", 1, "Robert B. White", "O1", ["robert_white"], "name", 3),     # biochemist: works outside the fields
        ("C1", 2, "Robert S. White", "O2", ["robert_white"], "name", 3),     # passes
        ("C1", 3, "Robert White", None, ["robert_white"], "name", 3),        # no ORCID: counted, not tested
        ("C2", 4, "Liu, Wei", "O4", ["wei_liu"], "name", COMMON + 5),        # common name, ORCID shows no Australia
        ("C3", 5, "Spagnolo, Lisa", "O5", ["lisa_spagnolo"], "name", 1),     # ACIF holds another ORCID
        ("C4", 6, "Jan Ross", "O6", ["jan_ross"], "name", 2),                # two records, one ORCID: both linked
        ("C4", 7, "J. Ross", "O6", ["jan_ross", "j_ross"], "name", 2),
    ])
    facts = _facts([("O1", "live", ["robert_white"], "Robert White", True, ""),
                    ("O2", "live", ["robert_white"], "Robert White", False, ""),
                    ("O4", "live", ["wei_liu"], "Wei Liu", False, ""),
                    ("O5", "live", ["lisa_spagnolo"], "Lisa Spagnolo", True, ""),
                    ("O6", "cache", ["jan_ross"], "Jan Ross", False, "")])
    rec = ({1: {"Biochemistry, Genetics and Molecular Biology": 30, "Arts and Humanities": 1},
            2: {"Arts and Humanities": 500, "Social Sciences": 40}, 4: {"Social Sciences": 40},
            5: {"Social Sciences": 50}, 6: {"Medicine": 10}, 7: {"Medicine": 2}},
           {1: 31, 2: 540, 4: 40, 5: 50, 6: 10, 7: 2}, {})
    ev = evidence(cands, facts, rec, keys, fields, {"C3": "O-other"}, {}, {})
    got = dict(zip(ev.author_idx, ev.check))
    assert got == {1: "works_outside_acif_fields", 2: "pass", 4: "common_name_no_australian_evidence", 5: "pass", 6: "pass", 7: "pass"}
    d, links = decide(["C1", "C2", "C3", "C4", "C5"], cands, ev)
    assert dict(zip(d.cluster_id, d.status)) == {"C1": "accept_orcid_hint", "C2": "none_pass", "C3": "pass_other_orcid",
                                                  "C4": "accept_orcid_hint", "C5": "no_candidate"}
    assert sorted(links.author_idx) == [2, 6, 7]


def test_evidence_held_refused_and_names():
    cands = _cands([("C1", 1, "Ann Lee", "O1", ["ann_lee"], "name", 2), ("C1", 2, "Ann Lee", "O2", ["ann_lee"], "name", 2),
                    ("C1", 3, "Ann Lee", "O3", ["ann_lee"], "name", 2), ("C1", 4, "Ann Lee", "O4", ["ann_lee"], "name", 2)])
    facts = _facts([("O1", "live", ["ann_lee"], "Ann Lee", True, ""), ("O2", "live", ["ann_lee"], "Ann Lee", True, ""),
                    ("O3", "live", ["mary_jones"], "Mary Jones", True, ""), ("O4", "none", [], None, False, "")])
    rec = ({i: {"Medicine": 5} for i in range(1, 5)}, {i: 5 for i in range(1, 5)}, {})
    ev = evidence(cands, facts, rec, {"C1": ["ann_lee"]}, {"C1": {"Medicine"}}, {}, {"O1": "C9"}, {"C1": {"O2"}})
    assert dict(zip(ev.author_idx, ev.check)) == {1: "orcid_held_by_another_acif", 2: "orcid_refused_by_hand",
                                                   3: "orcid_names_differ_from_acif", 4: "orcid_record_missing"}


def test_stage1_orcid_names_bridge_an_unrelated_name():
    from src.oax.orcid_link import decide as decide1
    links = pd.DataFrame([{"cluster_id": "C1", "orcid": "O1", "author_idx": 1, "full_name_keys": ["debra_bernhardt"],
                           "author_keys": ["debra_searles", "d_searles"], "author_name": "Searles, Debra J.", "works_share": 1.0}])
    assert decide1(links).status.tolist() == ["review_unrelated"]
    assert decide1(links, orcid_names={"O1": {"debra_bernhardt", "debra_searles"}}).status.tolist() == ["accept_orcid_names"]
