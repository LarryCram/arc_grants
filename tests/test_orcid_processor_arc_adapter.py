"""
Tests for src/utils/orcid_processor_arc_adapter.py -- the thin, project-specific glue between
OrcidProcessor (standalone) and this project's own HumanNameParser/orcid_client.py conventions.

get_record()'s live-fetch path is exercised with an injected in-memory cache, never the real
disk cache or a live ORCID API call.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.orcid_processor_arc_adapter import (
    arc_name_normalizer,
    get_record,
    institution_matched_candidates,
    resolve_institution_overlap,
)


class TestArcNameNormalizer:
    def test_returns_orcid_processor_nameforms_shape(self):
        nf = arc_name_normalizer("Frank Grützner")
        assert nf.family_name_main in ("grutzner", "gruetzner")
        assert nf.first_name_canonical == "frank"

    def test_postnominal_stripped_via_humannameparser(self):
        # Proves this really delegates to HumanNameParser (postnominal stripping is that
        # class's own job, not orcid_processor.py's bare default_name_normalizer).
        nf = arc_name_normalizer("Anthony Thomas AC FAA")
        assert nf.family_name_main == "thomas"

    def test_empty_string(self):
        nf = arc_name_normalizer("")
        assert nf.full_name_key is None


class TestInstitutionMatchedCandidates:
    CANDIDATES = [
        {"orcid": "0000-0001", "institution_names": ["University of Queensland"]},
        {"orcid": "0000-0002", "institution_names": ["Macquarie University"]},
        {"orcid": "0000-0003", "institution_names": []},
    ]

    def test_returns_raw_matched_list_not_collapsed(self):
        matched = institution_matched_candidates(self.CANDIDATES, {"University of Queensland", "Macquarie University"})
        assert {c["orcid"] for c in matched} == {"0000-0001", "0000-0002"}

    def test_zero_matches(self):
        assert institution_matched_candidates(self.CANDIDATES, {"University of Sydney"}) == []

    def test_accepts_list_or_set(self):
        as_list = institution_matched_candidates(self.CANDIDATES, ["University of Queensland"])
        as_set = institution_matched_candidates(self.CANDIDATES, {"University of Queensland"})
        assert as_list == as_set

    def test_candidate_without_institution_names_key_safe(self):
        candidates = [{"orcid": "0000-0001"}]
        assert institution_matched_candidates(candidates, {"University of Queensland"}) == []


class TestResolveInstitutionOverlap:
    CANDIDATES = [
        {"orcid": "0000-0001", "name": "Simon Kelly", "institution_names": ["University of Queensland"]},
        {"orcid": "0000-0002", "name": "Simon Kelly", "institution_names": ["Macquarie University"]},
    ]

    def test_unique_overlap_resolves(self):
        winner = resolve_institution_overlap(self.CANDIDATES, {"University of Queensland"})
        assert winner == "0000-0001"

    def test_case_insensitive(self):
        winner = resolve_institution_overlap(self.CANDIDATES, {"university of queensland"})
        assert winner == "0000-0001"

    def test_no_overlap_returns_none(self):
        winner = resolve_institution_overlap(self.CANDIDATES, {"University of Sydney"})
        assert winner is None

    def test_multiple_overlap_returns_none(self):
        candidates = [
            {"orcid": "0000-0001", "institution_names": ["University of Queensland"]},
            {"orcid": "0000-0002", "institution_names": ["University of Queensland"]},
        ]
        winner = resolve_institution_overlap(candidates, {"University of Queensland"})
        assert winner is None

    def test_candidate_with_no_institutions_never_matches(self):
        candidates = [{"orcid": "0000-0001", "institution_names": []}]
        winner = resolve_institution_overlap(candidates, {"University of Queensland"})
        assert winner is None


class TestGetRecord:
    def test_uses_injected_cache_on_hit_no_live_call(self):
        cache = {"0000-0001": {"person": {"name": {"given-names": {"value": "David"},
                                                     "family-name": {"value": "Smith"}}},
                                "activities-summary": {}}}
        r = get_record("0000-0001", cache=cache)
        assert r.given_names == "David"
        assert r.error is None
