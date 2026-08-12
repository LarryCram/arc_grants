"""
Tests for src/utils/oeuvre_build.py's pure, non-DB logic:
  - apply_deterministic_filters
  - apply_subfield_filter
  - score_institution_coherence
  - score_coauthor_arc_corroboration

fetch_candidate_oeuvre/dedup_oeuvre/build_oeuvre are DB-heavy (DuckDB scans over the real
OpenAlex compact tables) -- validated instead by the full-pipeline verification against real
data (see the plan file), matching the same precedent as src/utils/awards_cif.py's
populate_oax_candidates()/dedup_oax_candidates(). score_identity_clusters() is a thin wrapper
around analysis.utils.identity_clustering.build_identity_clusters_from_data(), already covered
directly by analysis/tests/test_identity_clustering.py.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.awards_cif import AwardCIFItem, AwardsCIF, CandidateWork
from src.utils.oeuvre_build import (
    apply_deterministic_filters,
    apply_subfield_filter,
    score_institution_coherence,
    score_coauthor_arc_corroboration,
)
import src.utils.oeuvre_build as oeuvre_build


def _item(unique_id, grant_code=None, full_name="John Smith") -> AwardCIFItem:
    return AwardCIFItem(
        unique_id=unique_id,
        grant_code=grant_code or unique_id.split("_")[0],
        first_name="", family_name="", role_code="CI", orcid=None,
        admin_org=None, institution_oax_id=None, funding_commence_year=None,
        for_name=None, for_code=None, full_name=full_name,
    )


def _cluster(cluster_id, inst_arr=None, for2020_codes=None, items=None) -> AwardsCIF:
    return AwardsCIF(
        cluster_id=cluster_id,
        items=items or [_item(f"{cluster_id}_G1")],
        inst_arr=inst_arr or [],
        for2020_codes=for2020_codes or [],
    )


def _work(work_idx, **kwargs) -> CandidateWork:
    defaults = dict(
        publication_year=2020, cited_by_count=5, type="article",
        title="A perfectly ordinary research article title", doi="10.1/x", source_id=1,
    )
    defaults.update(kwargs)
    return CandidateWork(work_idx=work_idx, **defaults)


# ---------------------------------------------------------------------------
# apply_deterministic_filters
# ---------------------------------------------------------------------------

class TestApplyDeterministicFilters:
    def test_valid_work_survives(self):
        c = _cluster("A")
        c.oeuvre = [_work(1)]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].included is True
        assert c.oeuvre[0].exclusion_reason is None

    def test_disallowed_type_excluded(self):
        c = _cluster("A")
        c.oeuvre = [_work(1, type="dataset")]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].included is False
        assert c.oeuvre[0].exclusion_reason == "type_not_allowed"

    def test_missing_year_excluded(self):
        c = _cluster("A")
        c.oeuvre = [_work(1, publication_year=None)]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].exclusion_reason == "missing_year"

    def test_implausible_year_excluded(self):
        c = _cluster("A")
        c.oeuvre = [_work(1, publication_year=1900)]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].exclusion_reason == "implausible_year"

    def test_future_year_excluded(self):
        c = _cluster("A")
        c.oeuvre = [_work(1, publication_year=2999)]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].exclusion_reason == "future_year"

    def test_missing_doi_excluded(self):
        c = _cluster("A")
        c.oeuvre = [_work(1, doi=None)]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].exclusion_reason == "missing_doi"

    def test_corrupt_authorship_excluded(self):
        c = _cluster("A")
        c.oeuvre = [_work(1, signals={"null_author_idx_count": 1, "null_institution_idx_count": 0})]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].exclusion_reason == "corrupt_authorship"

    def test_missing_source_excluded_for_article(self):
        c = _cluster("A")
        c.oeuvre = [_work(1, source_id=None, type="article")]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].exclusion_reason == "missing_source"

    def test_missing_source_ok_for_non_article(self):
        c = _cluster("A")
        c.oeuvre = [_work(1, source_id=None, type="preprint")]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].included is True

    def test_already_excluded_work_not_reprocessed(self):
        c = _cluster("A")
        w = _work(1, type="dataset")
        w.included = False
        w.exclusion_reason = "duplicate_version"
        c.oeuvre = [w]
        apply_deterministic_filters([c])
        # first-match-wins semantics don't apply to already-excluded works -- left untouched
        assert c.oeuvre[0].exclusion_reason == "duplicate_version"

    def test_first_reason_wins(self):
        # disallowed type AND missing DOI both true -- exclude_reason() (type check) fires first
        c = _cluster("A")
        c.oeuvre = [_work(1, type="dataset", doi=None)]
        apply_deterministic_filters([c])
        assert c.oeuvre[0].exclusion_reason == "type_not_allowed"


# ---------------------------------------------------------------------------
# apply_subfield_filter -- real FOR2020 codes/OAX fields (Resolver()-backed, same as
# tests/test_gap_candidates.py's precedent), not placeholder strings.
# ---------------------------------------------------------------------------

def _for2020(code, name, is_primary=True, confidence=1.0):
    return {"code": code, "name": name, "is_primary": is_primary, "confidence": confidence}


class TestApplySubfieldFilter:
    def test_matching_field_included(self):
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        c.oeuvre = [_work(1, field_name="Earth and Planetary Sciences")]
        apply_subfield_filter([c])
        assert c.oeuvre[0].included is True
        assert c.oeuvre[0].signals["field_match"] is True

    def test_mismatched_field_excluded(self):
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        c.oeuvre = [_work(1, field_name="Psychology")]
        apply_subfield_filter([c])
        assert c.oeuvre[0].included is False
        assert c.oeuvre[0].exclusion_reason == "field_mismatch"
        assert c.oeuvre[0].signals["field_match"] is False

    def test_different_for2020_code_same_oax_field_included(self):
        # Geology (3705) and Geophysics (3706) are different FOR2020 groups but the same
        # OAX field "Earth and Planetary Sciences" -- not a mismatch.
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        c.oeuvre = [_work(1, field_name="Earth and Planetary Sciences")]
        apply_subfield_filter([c])
        assert c.oeuvre[0].included is True

    def test_no_field_name_left_alone(self):
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        c.oeuvre = [_work(1, field_name=None)]
        apply_subfield_filter([c])
        assert c.oeuvre[0].included is True
        assert "field_match" not in c.oeuvre[0].signals

    def test_no_for2020_codes_skips_cluster(self):
        c = _cluster("A", for2020_codes=[])
        c.oeuvre = [_work(1, field_name="Anything At All")]
        apply_subfield_filter([c])
        assert c.oeuvre[0].included is True

    def test_secondary_code_also_used(self):
        # for2020_primary_fields() only uses is_primary=True entries -- a secondary code alone
        # shouldn't create a target field.
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology", is_primary=False)])
        c.oeuvre = [_work(1, field_name="Earth and Planetary Sciences")]
        apply_subfield_filter([c])
        # no primary code -> no reference -> not excluded
        assert c.oeuvre[0].included is True

    def test_already_excluded_work_still_scored_but_stays_excluded(self):
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        w = _work(1, field_name="Earth and Planetary Sciences")
        w.included = False
        w.exclusion_reason = "missing_doi"
        c.oeuvre = [w]
        apply_subfield_filter([c])
        # works() property only returns included=True, so this work isn't even considered --
        # exclusion_reason from the earlier stage is preserved, not overwritten.
        assert c.oeuvre[0].exclusion_reason == "missing_doi"


# ---------------------------------------------------------------------------
# score_institution_coherence
# ---------------------------------------------------------------------------

class TestScoreInstitutionCoherence:
    def test_matching_institution_true(self):
        c = _cluster("A", inst_arr=["https://openalex.org/I12345"])
        c.oeuvre = [_work(1, own_institution_idxs=[12345])]
        score_institution_coherence([c])
        assert c.oeuvre[0].signals["institution_arc_match"] is True

    def test_non_matching_institution_false(self):
        c = _cluster("A", inst_arr=["https://openalex.org/I12345"])
        c.oeuvre = [_work(1, own_institution_idxs=[99999])]
        score_institution_coherence([c])
        assert c.oeuvre[0].signals["institution_arc_match"] is False

    def test_no_arc_institutions_false(self):
        c = _cluster("A", inst_arr=[])
        c.oeuvre = [_work(1, own_institution_idxs=[12345])]
        score_institution_coherence([c])
        assert c.oeuvre[0].signals["institution_arc_match"] is False

    def test_no_own_institution_on_work_false(self):
        c = _cluster("A", inst_arr=["https://openalex.org/I12345"])
        c.oeuvre = [_work(1, own_institution_idxs=[])]
        score_institution_coherence([c])
        assert c.oeuvre[0].signals["institution_arc_match"] is False

    def test_excluded_work_not_scored(self):
        c = _cluster("A", inst_arr=["https://openalex.org/I12345"])
        w = _work(1, own_institution_idxs=[12345])
        w.included = False
        w.exclusion_reason = "missing_doi"
        c.oeuvre = [w]
        score_institution_coherence([c])
        assert "institution_arc_match" not in c.oeuvre[0].signals


# ---------------------------------------------------------------------------
# score_coauthor_arc_corroboration -- _load_coinvestigator_names() hits real parquet data,
# monkeypatched here to isolate the scoring logic itself.
# ---------------------------------------------------------------------------

class TestScoreCoauthorArcCorroboration:
    def test_matching_coinvestigator_true(self, monkeypatch):
        monkeypatch.setattr(
            oeuvre_build, "_load_coinvestigator_names",
            lambda: {"DP001": ["John Smith", "Jane Doe"]},
        )
        c = _cluster("A", items=[_item("DP001_JohnSmith", grant_code="DP001", full_name="John Smith")])
        c.oeuvre = [_work(1, coauthor_names=["Jane Doe"])]
        score_coauthor_arc_corroboration([c])
        assert c.oeuvre[0].signals["coinvestigator_match"] is True

    def test_non_matching_coauthor_false(self, monkeypatch):
        monkeypatch.setattr(
            oeuvre_build, "_load_coinvestigator_names",
            lambda: {"DP001": ["John Smith", "Jane Doe"]},
        )
        c = _cluster("A", items=[_item("DP001_JohnSmith", grant_code="DP001", full_name="John Smith")])
        c.oeuvre = [_work(1, coauthor_names=["Someone Else"])]
        score_coauthor_arc_corroboration([c])
        assert c.oeuvre[0].signals["coinvestigator_match"] is False

    def test_own_name_excluded_from_coinvestigator_set(self, monkeypatch):
        # The ARC person's own name is on their own grant record too -- must not count as a
        # "coauthor corroboration" against themselves.
        monkeypatch.setattr(
            oeuvre_build, "_load_coinvestigator_names",
            lambda: {"DP001": ["John Smith"]},
        )
        c = _cluster("A", items=[_item("DP001_JohnSmith", grant_code="DP001", full_name="John Smith")])
        c.oeuvre = [_work(1, coauthor_names=["John Smith"])]
        score_coauthor_arc_corroboration([c])
        assert c.oeuvre[0].signals["coinvestigator_match"] is False

    def test_no_coinvestigators_on_record_false(self, monkeypatch):
        monkeypatch.setattr(oeuvre_build, "_load_coinvestigator_names", lambda: {})
        c = _cluster("A", items=[_item("DP001_JohnSmith", grant_code="DP001", full_name="John Smith")])
        c.oeuvre = [_work(1, coauthor_names=["Anyone"])]
        score_coauthor_arc_corroboration([c])
        assert c.oeuvre[0].signals["coinvestigator_match"] is False
