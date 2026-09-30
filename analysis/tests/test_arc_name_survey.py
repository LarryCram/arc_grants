"""Tests for analysis/utils/arc_name_survey.py -- the A-checks and the B difference labels, on real
ARC name strings."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import pytest

from analysis.utils.arc_name_survey import (
    _Parsed, compare_grant_lists, difference_label, levenshtein, potential_anomalies,
)
from src.utils.names import HumanNameParser

P = HumanNameParser()
PARSED = _Parsed()


@pytest.mark.parametrize("first,family,flag", [
    ("Sarah", "O' Shea", "space_after_apostrophe"),
    ("David", "StJohn", "particle_run_in"),
    ("Jan", "DeGier", "particle_run_in"),
    ("Harald", "van_Heerde", "underscore"),
    ("Anthony", "Kinloch FRS, FREng", "comma_in_family"),
    ("Anthony", "Kinloch FRS, FREng", "postnominal_in_field"),
    ("Clare", "Murphy  (nee Paton-Walsh)", "doubled_space"),
    ("Clare", "Murphy  (nee Paton-Walsh)", "nee_bracketed"),
    ("Clare", "Murphy  nee Paton-Walsh", "nee_unbracketed"),
    ("J.A.", "Smith", "initials_only_given"),
    ("Kim-Anh", "Lê Cao", "non_ascii"),
])
def test_potential_anomaly_flags(first, family, flag):
    assert flag in potential_anomalies(first, family, P)


def test_plain_name_has_no_flags():
    assert potential_anomalies("Jennifer", "Smith", P) == []


def test_mcdonald_is_not_internal_capital():
    assert "internal_capital" not in potential_anomalies("John", "McDonald", P)


@pytest.mark.parametrize("a,b,label", [
    (("David", "St John"), ("David", "StJohn"), "separator|same"),
    (("Sarah", "O' Shea"), ("Sarah", "O'Shea"), "separator|same"),
    (("Jan", "de Gier"), ("Jan", "DeGier"), "separator|same"),
    (("Susan", "Walker"), ("Sue", "Walker"), "same|nickname_shaped"),
    (("Mahmuda", "Akhtar"), ("M. Shumi", "Akhtar"), "same|initial_vs_full"),
    (("Kate", "Smith"), ("Kate", "Smith-Miles"), "compound|same"),
    (("Maria", "Seton"), ("Maria", "Sdrolias"), "different|same"),
    (("Kotagiri", "Ramamohanarao"), ("Ramamohanarao", "Kotagiri"), "swap"),
    (("Adil", "Bagirov"), ("Adil", "Baghirov"), "edit1|same"),
    (("Jessica", "Hyles"), ("Ben", "Trevaskis"), "different|different"),
])
def test_difference_labels(a, b, label):
    assert difference_label(PARSED(*a), PARSED(*b)) == label


def test_levenshtein():
    assert levenshtein("smith", "jmith") == 1
    assert levenshtein("wiesel", "vizel") == 3  # w->v, drop e, s->z


def _grant(rows):
    return pd.DataFrame([
        {"grant_code": "G1", "source": s, "first": f, "family": l, "role": "CI",
         "role_in_scope": True, "orcid": None}
        for s, f, l in rows
    ])


def test_b1_rename_and_membership_change():
    g = _grant([("announcement", "Mahmuda", "Akhtar"), ("announcement", "Jessica", "Hyles"),
                ("current", "M. Shumi", "Akhtar"), ("current", "Ben", "Trevaskis")])
    statuses, pairs, ambiguous = compare_grant_lists(g, PARSED)
    st = {(s["first"], s["status"]) for s in statuses}
    assert ("M. Shumi", "rename") in st
    assert ("Jessica", "only_announcement") in st and ("Ben", "only_current") in st
    assert {p["source"] for p in pairs} == {"B1_rename", "B1_unmatched"}
    assert not ambiguous


def test_b1_empty_current_list_is_not_a_deletion():
    g = _grant([("announcement", "Jan", "de Gier")])
    statuses, pairs, _ = compare_grant_lists(g, PARSED)
    assert [s["status"] for s in statuses] == ["no_current_list"]
    assert pairs == []
