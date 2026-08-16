"""
Tests for src/utils/oeuvre_build.py's pure, non-DB logic:
  - apply_deterministic_filters
  - apply_subfield_filter
  - score_institution_coherence
  - score_coauthor_arc_corroboration

fetch_candidate_oeuvre/dedup_oeuvre/build_oeuvre are DB-heavy (DuckDB scans over the real
OpenAlex compact tables) -- validated instead by the full-pipeline verification against real
data (see the plan file), matching the same precedent as src/utils/awards_cif.py's
populate_oax_candidates()/dedup_oax_candidates().

score_identity_clusters() (and analysis/utils/identity_clustering.py, its only consumer) was
dropped entirely 2026-08-15 -- confirmed unreliable by construction: it assumed works connected
by a shared coauthor/institution are the same person, but OpenAlex's own merge errors most often
happen between genuinely close real people, exactly the case that would show up as strongly
*connected* by this technique. See build_oeuvre()'s docstring in oeuvre_build.py for the full
reasoning.

fetch_and_filter_stage1/apply_field_filter_stage3 (2026-08-15 full-scale-OOM fix) ARE tested
directly, unlike fetch_candidate_oeuvre/dedup_oeuvre above -- their glob paths are injectable
(auth_glob/work_glob/topic_glob/stage1_path parameters), so small synthetic parquet fixtures
written to tmp_path stand in for the real OpenAlex compact tables, giving full control over every
corruption-check branch without needing production-scale data.
"""
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.awards_cif import AwardCIFItem, AwardsCIF, CandidateWork
from src.utils.oeuvre_build import (
    apply_deterministic_filters,
    apply_subfield_filter,
    score_institution_coherence,
    score_coauthor_arc_corroboration,
    fetch_and_filter_stage1,
    apply_field_filter_stage3,
    compute_subfield_hep_signals,
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


def _cluster(cluster_id, inst_arr=None, for2020_codes=None, items=None, hep_codes=None) -> AwardsCIF:
    return AwardsCIF(
        cluster_id=cluster_id,
        items=items or [_item(f"{cluster_id}_G1")],
        inst_arr=inst_arr or [],
        for2020_codes=for2020_codes or [],
        hep_codes=hep_codes or [],
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


# ---------------------------------------------------------------------------
# fetch_and_filter_stage1 -- synthetic parquet fixtures stand in for the real OpenAlex
# authorships/works/work_topics tables via injectable auth_glob/work_glob/topic_glob.
# ---------------------------------------------------------------------------

# Explicit dtypes so a column that happens to be all-None in a given test (e.g. doi=None on the
# fixture's only row) still round-trips through parquet as the right type -- pandas/pyarrow can't
# infer "string" from a column with no non-null values present, and DuckDB then rejects passing
# that column into exclude_reason(VARCHAR, VARCHAR, VARCHAR).
_WORK_DTYPES = {
    "work_idx": "int64", "publication_year": "Int64", "cited_by_count": "Int64",
    "type": "string", "doi": "string", "title": "string", "source_id": "Int64",
}
_AUTH_DTYPES = {
    "work_idx": "int64", "author_idx": "Int64", "author_name": "string",
    "institution_idx": "Int64", "raw_orcid": "string",
}


def _write_pq(tmp_path, name, rows, dtypes=None) -> str:
    path = tmp_path / name
    df = pd.DataFrame(rows)
    if dtypes:
        df = df.astype({k: v for k, v in dtypes.items() if k in df.columns})
    df.to_parquet(path, index=False)
    return str(path)


def _work_row(work_idx, **kwargs) -> dict:
    defaults = dict(
        publication_year=2020, cited_by_count=5, type="article",
        doi=f"10.1/{work_idx}", title="A perfectly ordinary research article title",
        source_id=1,
    )
    defaults.update(kwargs)
    defaults["work_idx"] = work_idx
    return defaults


def _auth_row(work_idx, author_idx, **kwargs) -> dict:
    defaults = dict(author_name="Someone", institution_idx=1)
    defaults.update(kwargs)
    defaults["work_idx"] = work_idx
    defaults["author_idx"] = author_idx
    return defaults


def _topic_row(work_idx, **kwargs) -> dict:
    defaults = dict(
        subfield_idx=1, subfield_name="Sub", field_name="Field", domain_name="Domain", score=0.9,
    )
    defaults.update(kwargs)
    defaults["work_idx"] = work_idx
    return defaults


class TestFetchAndFilterStage1:
    def _run(self, tmp_path, clusters, auth_rows, work_rows, topic_rows=None):
        auth_glob = _write_pq(tmp_path, "auth.parquet", auth_rows, dtypes=_AUTH_DTYPES)
        work_glob = _write_pq(tmp_path, "works.parquet", work_rows, dtypes=_WORK_DTYPES)
        # read_parquet() can't read a zero-column file, so an "empty" topics table still needs a
        # dummy row with a work_idx that will never match a real test work (LEFT JOIN leaves the
        # real rows' topic columns NULL, which is the intended "no topic data" case).
        topic_glob = _write_pq(tmp_path, "topics.parquet", topic_rows or [_topic_row(-1)])
        con = duckdb.connect()
        survivors_path = tmp_path / "stage1_survivors.parquet"
        exclusions_path = tmp_path / "stage1_exclusions.parquet"
        fetch_and_filter_stage1(
            clusters, con=con, path=survivors_path, exclusions_path=exclusions_path,
            auth_glob=auth_glob, work_glob=work_glob, topic_glob=topic_glob,
        )
        # Reading the persisted parquet back is the test's own assertion boundary (legitimate
        # small-scale "reporting" use of pandas) -- the function itself no longer returns a
        # DataFrame, see fetch_and_filter_stage1()'s docstring for why.
        survivors = pd.read_parquet(survivors_path)
        exclusions_df = pd.read_parquet(exclusions_path)
        return survivors, exclusions_df

    def _cluster_with_candidate(self, cluster_id="A", author_idx=100):
        c = _cluster(cluster_id)
        c.oax_candidates = [f"https://openalex.org/A{author_idx}"]
        return c

    def test_valid_work_survives(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1)],
        )
        assert list(survivors["work_idx"]) == [1]
        assert survivors.iloc[0]["cluster_id"] == "A"
        assert len(exclusions_df) == 0

    def test_field_names_aggregates_all_topics_not_just_best(self, tmp_path):
        # Real finding (2026-08-15): works have up to 3 topics with a near-tied top-vs-2nd score
        # in the median case, so field_names must carry every distinct field, not just the
        # single highest-scoring topic's -- apply_field_filter_stage3() matches against this.
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1)],
            topic_rows=[
                _topic_row(1, field_name="Earth and Planetary Sciences", score=0.51),
                _topic_row(1, field_name="Chemistry", score=0.5),
                _topic_row(1, field_name="Chemistry", score=0.3),  # duplicate field, lower score
            ],
        )
        assert survivors.iloc[0]["field_name"] == "Earth and Planetary Sciences"  # best by score
        assert sorted(survivors.iloc[0]["field_names"]) == ["Chemistry", "Earth and Planetary Sciences"]

    def test_disallowed_type_excluded(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, type="dataset")],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "type_not_allowed"
        assert exclusions_df.iloc[0]["n"] == 1

    def test_missing_year_excluded(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, publication_year=None)],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "missing_year"

    def test_implausible_year_excluded(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, publication_year=1900)],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "implausible_year"

    def test_future_year_excluded(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, publication_year=2999)],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "future_year"

    def test_missing_doi_excluded(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, doi=None)],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "missing_doi"

    def test_corrupt_authorship_null_author_excluded(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100), _auth_row(1, None)],
            work_rows=[_work_row(1)],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "corrupt_authorship"

    def test_corrupt_authorship_null_institution_excluded(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100), _auth_row(1, 999, institution_idx=None)],
            work_rows=[_work_row(1)],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "corrupt_authorship"

    def test_missing_source_excluded_for_article(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, source_id=None, type="article")],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "missing_source"

    def test_missing_source_ok_for_preprint(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, source_id=None, type="preprint")],
        )
        assert list(survivors["work_idx"]) == [1]
        assert len(exclusions_df) == 0

    def test_first_reason_wins(self, tmp_path):
        # disallowed type AND missing DOI both true -- exclude_reason() (type check) fires first
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, type="dataset", doi=None)],
        )
        assert exclusions_df.iloc[0]["reason"] == "type_not_allowed"

    def test_raw_orcid_column_absent_is_a_no_op(self, tmp_path):
        # authorships table has no raw_orcid column at all -- must not hard-fail, and an old-year
        # work survives exactly as it would have before this check existed.
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100)],
            work_rows=[_work_row(1, publication_year=2005)],
        )
        assert list(survivors["work_idx"]) == [1]
        assert len(exclusions_df) == 0

    def test_raw_orcid_impossible_excluded_when_column_present(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100, raw_orcid="0000-0001-2345-6789")],
            work_rows=[_work_row(1, publication_year=2005)],
        )
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["reason"] == "raw_orcid_impossible"

    def test_raw_orcid_present_but_recent_year_survives(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100, raw_orcid="0000-0001-2345-6789")],
            work_rows=[_work_row(1, publication_year=2020)],
        )
        assert list(survivors["work_idx"]) == [1]
        assert len(exclusions_df) == 0

    def test_same_work_multiple_candidates_collapses_to_one_row(self, tmp_path):
        c = self._cluster_with_candidate()
        c.oax_candidates = ["https://openalex.org/A100", "https://openalex.org/A200"]
        survivors, exclusions_df = self._run(
            tmp_path, [c],
            auth_rows=[_auth_row(1, 100), _auth_row(1, 200)],
            work_rows=[_work_row(1)],
        )
        assert len(survivors) == 1
        assert sorted(survivors.iloc[0]["source_author_idxs"]) == [100, 200]

    def test_exclusion_counts_grouped_by_cluster_and_reason(self, tmp_path):
        c1 = self._cluster_with_candidate("A", 100)
        c2 = self._cluster_with_candidate("B", 200)
        survivors, exclusions_df = self._run(
            tmp_path, [c1, c2],
            auth_rows=[_auth_row(1, 100), _auth_row(2, 200)],
            work_rows=[_work_row(1, type="dataset"), _work_row(2, type="dataset")],
        )
        assert len(survivors) == 0
        counts = dict(zip(exclusions_df["cluster_id"], exclusions_df["n"]))
        assert counts == {"A": 1, "B": 1}

    def test_persists_to_given_paths(self, tmp_path):
        c = self._cluster_with_candidate()
        survivors_path = tmp_path / "custom_survivors.parquet"
        exclusions_path = tmp_path / "custom_exclusions.parquet"
        auth_glob = _write_pq(tmp_path, "auth.parquet", [_auth_row(1, 100)], dtypes=_AUTH_DTYPES)
        work_glob = _write_pq(tmp_path, "works.parquet", [_work_row(1)], dtypes=_WORK_DTYPES)
        topic_glob = _write_pq(tmp_path, "topics.parquet", [_topic_row(-1)])
        fetch_and_filter_stage1(
            [c], con=duckdb.connect(), path=survivors_path, exclusions_path=exclusions_path,
            auth_glob=auth_glob, work_glob=work_glob, topic_glob=topic_glob,
        )
        assert survivors_path.exists()
        assert exclusions_path.exists()
        assert len(pd.read_parquet(survivors_path)) == 1


# ---------------------------------------------------------------------------
# apply_field_filter_stage3
# ---------------------------------------------------------------------------

def _survivor_row(cluster_id, work_idx, field_names=None, **kwargs) -> dict:
    defaults = dict(
        source_author_idxs=[100], publication_year=2020, cited_by_count=5, type="article",
        title="A title", doi=f"10.1/{work_idx}", source_id=1,
        subfield_idx=1, subfield_name="Sub", domain_name="Domain",
    )
    defaults.update(kwargs)
    defaults["cluster_id"] = cluster_id
    defaults["work_idx"] = work_idx
    # field_name (singular, display-only, not matched against) also set for schema realism --
    # the first field_names entry when present, matching what fetch_and_filter_stage1() produces.
    defaults["field_name"] = field_names[0] if field_names else None
    defaults["field_names"] = field_names
    return defaults


class TestApplyFieldFilterStage3:
    def test_matching_field_included(self, tmp_path):
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        rows = [_survivor_row("A", 1, field_names=["Earth and Planetary Sciences"])]
        survivors, exclusions_df = self._run(tmp_path, [c], rows)
        assert list(survivors["work_idx"]) == [1]
        assert len(exclusions_df) == 0

    def test_mismatched_field_excluded_with_count(self, tmp_path):
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        rows = [_survivor_row("A", 1, field_names=["Psychology"])]
        survivors, exclusions_df = self._run(tmp_path, [c], rows)
        assert len(survivors) == 0
        assert exclusions_df.iloc[0]["cluster_id"] == "A"
        assert exclusions_df.iloc[0]["reason"] == "field_mismatch"
        assert exclusions_df.iloc[0]["n"] == 1

    def test_secondary_topic_field_match_included(self, tmp_path):
        # Real finding (2026-08-15): a work's best-scoring topic isn't Geology, but a secondary
        # topic is -- must survive, since the top-vs-2nd topic score gap is a near-tie in the
        # median case and shouldn't be treated as a confident, exclusive signal.
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        rows = [_survivor_row("A", 1, field_names=["Psychology", "Earth and Planetary Sciences"])]
        survivors, exclusions_df = self._run(tmp_path, [c], rows)
        assert list(survivors["work_idx"]) == [1]
        assert len(exclusions_df) == 0

    def test_no_field_names_left_alone(self, tmp_path):
        c = _cluster("A", for2020_codes=[_for2020("3705", "Geology")])
        rows = [_survivor_row("A", 1, field_names=None)]
        survivors, exclusions_df = self._run(tmp_path, [c], rows)
        assert list(survivors["work_idx"]) == [1]

    def test_no_for2020_codes_skips_cluster(self, tmp_path):
        c = _cluster("A", for2020_codes=[])
        rows = [_survivor_row("A", 1, field_names=["Anything At All"])]
        survivors, exclusions_df = self._run(tmp_path, [c], rows)
        assert list(survivors["work_idx"]) == [1]
        assert len(exclusions_df) == 0

    def _run(self, tmp_path, clusters, stage1_rows):
        stage1_path = tmp_path / "stage1.parquet"
        pd.DataFrame(stage1_rows).to_parquet(stage1_path, index=False)
        survivors_path = tmp_path / "stage3_survivors.parquet"
        exclusions_path = tmp_path / "stage3_exclusions.parquet"
        apply_field_filter_stage3(
            clusters, con=duckdb.connect(), path=survivors_path,
            exclusions_path=exclusions_path, stage1_path=stage1_path,
        )
        # Reading back is the test's own assertion boundary -- the function itself no longer
        # returns a DataFrame, see apply_field_filter_stage3()'s docstring for why.
        survivors = pd.read_parquet(survivors_path)
        exclusions_df = pd.read_parquet(exclusions_path)
        return survivors, exclusions_df


# ---------------------------------------------------------------------------
# compute_subfield_hep_signals -- 2026-08-16 reframing (per-candidate, per-work subfield+HEP
# signal). _load_institution_hep_crosswalk() is monkeypatched, same pattern as
# score_coauthor_arc_corroboration()'s _load_coinvestigator_names(), so tests are isolated from
# real admin_orgs.csv content/institution IDs.
# ---------------------------------------------------------------------------

class TestComputeSubfieldHepSignals:
    """group_coherent (2026-08-16 redesign) is purely OpenAlex-native -- within one candidate's
    own Stage-3-surviving work-set, does a work's subfield recur in >=1 other work in the same
    group? No AwardsCIF-declared FOR2020 reference is consulted at all for this signal (unlike
    hep_match, which is unchanged). Verified against the real DP0559177_MarkNelson case before
    being wired in -- see the function's own docstring."""

    def _run(self, tmp_path, clusters, stage3_rows, auth_rows, hep_crosswalk, monkeypatch):
        monkeypatch.setattr(oeuvre_build, "_load_institution_hep_crosswalk", lambda: hep_crosswalk)
        stage3_path = tmp_path / "stage3.parquet"
        pd.DataFrame(stage3_rows).to_parquet(stage3_path, index=False)
        auth_glob = _write_pq(tmp_path, "auth.parquet", auth_rows, dtypes=_AUTH_DTYPES)
        out_path = tmp_path / "signals.parquet"
        compute_subfield_hep_signals(
            clusters, con=duckdb.connect(), path=out_path,
            stage3_path=stage3_path, auth_glob=auth_glob,
        )
        return pd.read_parquet(out_path).set_index("work_idx")

    def test_recurring_subfield_is_coherent(self, tmp_path, monkeypatch):
        # One candidate (author_idx 100), 3 works, "Numerical Analysis" recurs across all 3 --
        # a real count-of-3 shared subfield, group_size=3 is exactly the evaluable threshold.
        c = _cluster("A", hep_codes=["ANU"])
        stage3_rows = [
            _survivor_row("A", 1, field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
            _survivor_row("A", 2, field_names=["Mathematics"], subfield_names=["Numerical Analysis", "Applied Mathematics"]),
            _survivor_row("A", 3, field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
        ]
        auth_rows = [_auth_row(w, 100, institution_idx=200) for w in (1, 2, 3)]
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "ANU"}, monkeypatch)
        assert out.loc[1, "group_coherent"] == True
        assert out.loc[2, "group_coherent"] == True
        assert out.loc[3, "group_coherent"] == True

    def test_outlier_subfield_is_not_coherent(self, tmp_path, monkeypatch):
        # Same group, but work 3's subfield ("Clinical Psychology") never appears elsewhere in
        # its own group -- a genuine within-group outlier, correctly False.
        c = _cluster("A", hep_codes=["ANU"])
        stage3_rows = [
            _survivor_row("A", 1, field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
            _survivor_row("A", 2, field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
            _survivor_row("A", 3, field_names=["Psychology"], subfield_names=["Clinical Psychology"]),
        ]
        auth_rows = [_auth_row(w, 100, institution_idx=200) for w in (1, 2, 3)]
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "ANU"}, monkeypatch)
        assert out.loc[1, "group_coherent"] == True
        assert out.loc[2, "group_coherent"] == True
        assert out.loc[3, "group_coherent"] == False

    def test_small_group_is_none(self, tmp_path, monkeypatch):
        # Only 2 works in this candidate's group -- no reliable "rest of the group" to compare
        # against, must be None (not evaluated), never silently False.
        c = _cluster("A", hep_codes=["ANU"])
        stage3_rows = [
            _survivor_row("A", 1, field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
            _survivor_row("A", 2, field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
        ]
        auth_rows = [_auth_row(w, 100, institution_idx=200) for w in (1, 2)]
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "ANU"}, monkeypatch)
        assert out.loc[1, "group_coherent"] is None
        assert out.loc[2, "group_coherent"] is None

    def test_different_candidates_are_separate_groups(self, tmp_path, monkeypatch):
        # Two different author_idx within the same cluster -- each group's coherence must be
        # judged only against its own works, not pooled across candidates.
        c = _cluster("A", hep_codes=["ANU"])
        stage3_rows = [
            _survivor_row("A", 1, source_author_idxs=[100], field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
            _survivor_row("A", 2, source_author_idxs=[100], field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
            _survivor_row("A", 3, source_author_idxs=[100], field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
            _survivor_row("A", 4, source_author_idxs=[999], field_names=["Medicine"], subfield_names=["Cardiology"]),
            _survivor_row("A", 5, source_author_idxs=[999], field_names=["Medicine"], subfield_names=["Cardiology"]),
            _survivor_row("A", 6, source_author_idxs=[999], field_names=["Medicine"], subfield_names=["Cardiology"]),
        ]
        auth_rows = (
            [_auth_row(w, 100, institution_idx=200) for w in (1, 2, 3)]
            + [_auth_row(w, 999, institution_idx=200) for w in (4, 5, 6)]
        )
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "ANU"}, monkeypatch)
        for w in (1, 2, 3, 4, 5, 6):
            assert out.loc[w, "group_coherent"] == True

    def test_no_subfield_data_on_work_is_none(self, tmp_path, monkeypatch):
        c = _cluster("A", hep_codes=["ANU"])
        stage3_rows = [
            _survivor_row("A", 1, field_names=["Mathematics"], subfield_names=None),
            _survivor_row("A", 2, field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
            _survivor_row("A", 3, field_names=["Mathematics"], subfield_names=["Numerical Analysis"]),
        ]
        auth_rows = [_auth_row(w, 100, institution_idx=200) for w in (1, 2, 3)]
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "ANU"}, monkeypatch)
        assert out.loc[1, "group_coherent"] is None

    def test_hep_match_true(self, tmp_path, monkeypatch):
        c = _cluster("A", hep_codes=["ANU"])
        stage3_rows = [_survivor_row("A", 1, field_names=["Earth and Planetary Sciences"], subfield_names=["Geology"])]
        auth_rows = [_auth_row(1, 100, institution_idx=200)]
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "ANU"}, monkeypatch)
        assert out.loc[1, "hep_match"] == True

    def test_hep_mismatch_false(self, tmp_path, monkeypatch):
        c = _cluster("A", hep_codes=["ANU"])
        stage3_rows = [_survivor_row("A", 1, field_names=["Earth and Planetary Sciences"], subfield_names=["Geology"])]
        auth_rows = [_auth_row(1, 100, institution_idx=200)]
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "UNSW"}, monkeypatch)
        assert out.loc[1, "hep_match"] == False

    def test_no_own_institution_data_is_none(self, tmp_path, monkeypatch):
        # Institution present on the authorship row but not in the crosswalk (e.g. foreign
        # institution) -- own_inst_idxs resolves to nothing, hep_match must be None not False.
        c = _cluster("A", hep_codes=["ANU"])
        stage3_rows = [_survivor_row("A", 1, field_names=["Earth and Planetary Sciences"], subfield_names=["Geology"])]
        auth_rows = [_auth_row(1, 100, institution_idx=999)]
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "ANU"}, monkeypatch)
        assert out.loc[1, "hep_match"] is None

    def test_no_reference_hep_on_cluster_is_none(self, tmp_path, monkeypatch):
        # C has no hep_codes at all -- can't evaluate, must not read as False.
        c = _cluster("A", hep_codes=[])
        stage3_rows = [_survivor_row("A", 1, field_names=["Earth and Planetary Sciences"], subfield_names=["Geology"])]
        auth_rows = [_auth_row(1, 100, institution_idx=200)]
        out = self._run(tmp_path, [c], stage3_rows, auth_rows, {200: "ANU"}, monkeypatch)
        assert out.loc[1, "hep_match"] is None
