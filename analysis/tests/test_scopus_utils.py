"""Tests for analysis/utils/scopus.py's pure pieces (no Scopus calls) and 16_scopus_lookup's status."""
import importlib.util
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analysis.utils.scopus import acif_query, load_university_map, name_clause, search_keys

_spec = importlib.util.spec_from_file_location(
    "scopus_lookup", Path(__file__).resolve().parents[1] / "16_scopus_lookup.py")
_lookup = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_lookup)


def test_search_keys_prefers_full_given_names():
    assert search_keys(["s_ng", "shu_ng", "angus_ng", "k_ng"]) == ["angus_ng", "shu_ng"]


def test_search_keys_falls_back_to_initials():
    assert search_keys(["s_ng", "k_ng"]) == ["k_ng", "s_ng"]


def test_name_clause_splits_at_first_underscore_only():
    assert name_clause("shu-kay_ng") == '(AUTHFIRST("shu-kay") AND AUTHLASTNAME("ng"))'
    assert name_clause("jan_de gier") == '(AUTHFIRST("jan") AND AUTHLASTNAME("de gier"))'


def test_acif_query_ors_names_and_affiliations():
    q = acif_query(["karen_ford", "karen_marsh", "k_ford"], ["ANU"],
                   {"ANU": ["Australian National University"]})
    assert q == ('((AUTHFIRST("karen") AND AUTHLASTNAME("ford")) OR (AUTHFIRST("karen") AND '
                 'AUTHLASTNAME("marsh"))) AND (AFFIL("Australian National University"))')


def test_university_map_covers_every_arc_university():
    m = load_university_map()
    assert m.hep_code.is_unique and len(m) >= 42
    assert dict(zip(m.hep_code, m.scopus_affiliation_id))["GU"] == "60032987"   # Griffith
    assert dict(zip(m.hep_code, m.scopus_affiliation_id))["UQ"] == "60031004"


def test_status():
    s = _lookup.status
    assert s({"O1"}, {"O1", "O2"}, 2) == "confirmed"
    assert s({"O1"}, {"O2"}, 1) == "other_orcid"
    assert s({"O1"}, set(), 3) == "no_scopus_orcid"
    assert s(set(), {"O2"}, 1) == "one_orcid"
    assert s(set(), {"O2", "O3"}, 2) == "several_orcids"
    assert s(set(), set(), 0) == "no_profile"


def test_acif_query_is_order_independent():
    # pybliometrics caches by the query string, so input order must not change it
    names = {"UQ": ["University of Queensland", "The University of Queensland"], "GU": ["Griffith University"]}
    a = acif_query(["shu_ng", "angus_ng", "kay_ng"], ["UQ", "GU"], names)
    b = acif_query(["kay_ng", "shu_ng", "angus_ng"], ["GU", "UQ"],
                   {"GU": ["Griffith University"], "UQ": ["The University of Queensland", "University of Queensland"]})
    assert a == b
