"""Tests for src/utils/scopus.py's pure pieces and src/00d_extract_scopus.py's status() and
ORCID-record readers (no Scopus calls)."""
import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.scopus import acif_query, load_university_map, name_clause, search_keys

_x = importlib.import_module("src.00d_extract_scopus")


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


def test_acif_query_is_order_independent():
    # pybliometrics caches by the query string, so input order must not change it
    names = {"UQ": ["University of Queensland", "The University of Queensland"], "GU": ["Griffith University"]}
    a = acif_query(["shu_ng", "angus_ng", "kay_ng"], ["UQ", "GU"], names)
    b = acif_query(["kay_ng", "shu_ng", "angus_ng"], ["GU", "UQ"],
                   {"GU": ["Griffith University"], "UQ": ["The University of Queensland", "University of Queensland"]})
    assert a == b


def test_status():
    s = _x.status
    assert s({"O1"}, {"O1", "O2"}, 2) == "confirmed"
    assert s({"O1"}, {"O2"}, 1) == "other_orcid"
    assert s({"O1"}, set(), 3) == "no_scopus_orcid"
    assert s(set(), {"O2"}, 1) == "one_orcid"
    assert s(set(), {"O2", "O3"}, 2) == "several_orcids"
    assert s(set(), set(), 0) == "no_profile"


def _record(given, family, others=(), scopus=()):
    return {"person": {"name": {"given-names": {"value": given}, "family-name": {"value": family}},
                       "other-names": {"other-name": [{"content": o} for o in others]},
                       "external-identifiers": {"external-identifier": [
                           {"external-id-type": "Scopus Author ID", "external-id-value": v} for v in scopus]}}}


def test_record_name_keys_include_other_names():
    keys = _x.record_name_keys(_record("S. Rachel", "Skinner", others=["Susan Rachel Skinner"]))
    assert {"rachel_skinner", "susan_skinner"} <= keys


def test_record_scopus_ids():
    assert _x.record_scopus_ids(_record("Kerrie", "Sadiq", scopus=["56529466100", "56529466100"])) == {"56529466100"}
    assert _x.record_scopus_ids(_record("A", "B")) == set()


def test_a_redirected_cached_record_is_not_used():
    # ORCID answers a deprecated ORCID with the record it redirects to (Shaomin Liu's
    # 0000-0001-5019-5182 came back as 0000-0002-9865-9596, "Yuanyuan Chu")
    rec = {"orcid-identifier": {"path": "0000-0002-9865-9596"}, **_record("Yuanyuan", "Chu")}
    assert not _x._usable(rec, "0000-0001-5019-5182")
    assert _x._usable({"orcid-identifier": {"path": "0000-0001-5019-5182"}}, "0000-0001-5019-5182")
    assert not _x._usable({"_error": "404"}, "0000-0001-5019-5182")
