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
    au_country_compatible_candidates,
    get_record,
    institution_compatible_candidates,
    institution_matched_candidates,
    resolve_institution_overlap,
)

# arc_name_normalizer() was removed 2026-09-08 (item #26) -- orcid_processor.py now uses
# names.py's HumanNameParser directly as its own default, so there's no adapter-level name
# normalization left to test here. Equivalent coverage (postnominal stripping, diacritic
# widening, nickname handling) lives in tests/test_names.py's own HumanNameParser tests.


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


class TestInstitutionCompatibleCandidates:
    """2026-09-11, the Kimbal/Ken Marriott case: a candidate with no institution_names data at
    all must be kept, not treated as a non-match -- each candidate's own keep/drop verdict
    depends only on that candidate's own recorded data, never on another candidate's."""

    def test_confirmed_match_alone_is_the_sole_survivor(self):
        candidates = [
            {"orcid": "0000-0001", "institution_names": ["Monash University"]},
            {"orcid": "0000-0002", "institution_names": ["University of Sydney"]},
        ]
        survivors = institution_compatible_candidates(candidates, {"Monash University"})
        assert {c["orcid"] for c in survivors} == {"0000-0001"}

    def test_no_data_candidate_is_kept_not_dropped(self):
        # Real Marriott shape: one candidate has an unrelated (wrong-person) institution match,
        # the true candidate has literally no institution data on file at all.
        candidates = [
            {"orcid": "wrong-person", "institution_names": ["Monash University"]},
            {"orcid": "true-person", "institution_names": []},
        ]
        survivors = institution_compatible_candidates(candidates, {"Monash University"})
        assert {c["orcid"] for c in survivors} == {"wrong-person", "true-person"}

    def test_conflicting_institution_data_is_the_only_thing_that_drops_a_candidate(self):
        candidates = [
            {"orcid": "0000-0001", "institution_names": ["University of Sydney"]},
            {"orcid": "0000-0002", "institution_names": []},
            {"orcid": "0000-0003", "institution_names": []},
        ]
        survivors = institution_compatible_candidates(candidates, {"Monash University"})
        assert {c["orcid"] for c in survivors} == {"0000-0002", "0000-0003"}

    def test_candidate_without_institution_names_key_is_kept(self):
        candidates = [{"orcid": "0000-0001"}]
        survivors = institution_compatible_candidates(candidates, {"Monash University"})
        assert {c["orcid"] for c in survivors} == {"0000-0001"}

    def test_case_insensitive(self):
        candidates = [{"orcid": "0000-0001", "institution_names": ["monash university"]}]
        survivors = institution_compatible_candidates(candidates, {"Monash University"})
        assert {c["orcid"] for c in survivors} == {"0000-0001"}


class TestAuCountryCompatibleCandidates:
    """2026-09-11: the free, local counterpart to _resolve_results()'s live-API AU-or-NULL fix --
    a candidate with no `countries` data on file is kept (unknown, not disqualifying); only a
    confirmed non-AU country actually rules a candidate out."""

    def test_confirmed_au_alone_is_the_sole_survivor(self):
        candidates = [
            {"orcid": "0000-0001", "countries": ["AU"]},
            {"orcid": "0000-0002", "countries": ["US"]},
        ]
        assert {c["orcid"] for c in au_country_compatible_candidates(candidates)} == {"0000-0001"}

    def test_no_country_data_is_kept_not_dropped(self):
        candidates = [
            {"orcid": "0000-0001", "countries": []},
            {"orcid": "0000-0002", "countries": []},
        ]
        survivors = au_country_compatible_candidates(candidates)
        assert {c["orcid"] for c in survivors} == {"0000-0001", "0000-0002"}

    def test_confirmed_non_au_is_dropped(self):
        candidates = [
            {"orcid": "0000-0001", "countries": ["US", "GB"]},
            {"orcid": "0000-0002", "countries": []},
        ]
        survivors = au_country_compatible_candidates(candidates)
        assert {c["orcid"] for c in survivors} == {"0000-0002"}

    def test_missing_countries_key_is_kept(self):
        assert au_country_compatible_candidates([{"orcid": "0000-0001"}]) == [{"orcid": "0000-0001"}]

    def test_case_insensitive(self):
        candidates = [{"orcid": "0000-0001", "countries": ["au"]}]
        assert au_country_compatible_candidates(candidates) == candidates


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
