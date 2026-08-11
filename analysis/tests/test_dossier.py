"""
Tests for analysis/utils/dossier.py -- the Dossier data model itself (construction,
through_year filtering, Counter-based aggregate methods, immutability). Pure Python
dataclass, no DuckDB/parquet needed.

Does NOT cover analysis/utils/dossier_build.py's build_dossier() (assembly from real
persisted tables) -- that would need a synthetic-fixture rig mirroring test_metrics.py's
conftest.py across five source tables (arc_persons, arc_grant_cluster_map, grants_flat,
investigators_raw, oeuvres, annual_metrics); not yet built, a known gap.
"""

import dataclasses

import pytest

from analysis.utils.dossier import (
    AwardContext, CoauthorRecord, Dossier, Percentile, Work, YearRecord,
)


@pytest.fixture
def sample_dossier():
    return Dossier(
        arc_id="DE000000001_TestPerson",
        preferred_name="Test Person",
        name_variants=["T. Person"],
        for_codes=["4611"],
        for_division="Human Society",
        panel="HCA",
        oax_id="https://openalex.org/A5000000001",
        works=[
            Work(work_idx=1, publication_year=2019, cited_by_count=10, type="article", title="A",
                 field_name="Sociology", outlet_name="Antipode",
                 institution_names=["Monash University"], topic_entropy=0.8, novelty_score=0.2),
            Work(work_idx=2, publication_year=2022, cited_by_count=3, type="article", title="B",
                 field_name="Geography", outlet_name="Antipode",
                 institution_names=["Monash University", "UNSW"], topic_entropy=0.3, novelty_score=0.6),
            Work(work_idx=3, publication_year=2024, cited_by_count=0, type="preprint", title="C",
                 field_name="Sociology", institution_names=["Monash University"]),
        ],
        annual_series=[
            YearRecord(year=2019, n_pubs=1, n_citations_snapshot=10, h_index=1, n_works_cumul=1,
                       total_citations_cumul=10, top_field="Sociology", n_highly_cited=0),
            YearRecord(year=2022, n_pubs=1, n_citations_snapshot=3, h_index=2, n_works_cumul=2,
                       total_citations_cumul=13, top_field="Geography", n_highly_cited=0),
        ],
        first_pub_year=2019,
        excluded_work_counts={"type_not_allowed": 2, "filename_artifact": 1},
        award_contexts=[
            AwardContext(
                grant_id="DE000000001", scheme="DECRA", award_year=2022, career_age_at_award=3,
                coauthor_track_record=[
                    CoauthorRecord(coauthor_author_idx=99, coauthor_name="X Y",
                                   coauthor_country_code="US", n_shared_works_pre_award=2,
                                   cumulative_works_at_award=40, cumulative_citations_at_award=500.0),
                    CoauthorRecord(coauthor_author_idx=98, coauthor_name="Z W",
                                   coauthor_country_code="US", n_shared_works_pre_award=1,
                                   cumulative_works_at_award=5, cumulative_citations_at_award=20.0),
                ],
            ),
        ],
    )


def test_frozen_immutable(sample_dossier):
    with pytest.raises(dataclasses.FrozenInstanceError):
        sample_dossier.preferred_name = "Someone Else"


def test_works_through_filters_by_year(sample_dossier):
    assert len(sample_dossier.works_through(2020)) == 1
    assert len(sample_dossier.works_through(2022)) == 2
    assert len(sample_dossier.works_through()) == 3  # None = no filter


def test_by_field_counter(sample_dossier):
    assert dict(sample_dossier.by_field()) == {"Sociology": 2, "Geography": 1}
    assert dict(sample_dossier.by_field(2019)) == {"Sociology": 1}


def test_by_type_counter(sample_dossier):
    assert dict(sample_dossier.by_type()) == {"article": 2, "preprint": 1}


def test_by_outlet_counter_skips_none(sample_dossier):
    """Work C has no outlet_name -- must not show up as a None key."""
    assert dict(sample_dossier.by_outlet()) == {"Antipode": 2}


def test_institution_counts_is_additive_not_deduplicated(sample_dossier):
    """Two works at Monash + one dual-affiliated with UNSW -> Monash counted 3 times,
    not deduplicated to a single membership flag ("two Harvard papers is better than
    one Harvard paper")."""
    counts = sample_dossier.institution_counts()
    assert counts["Monash University"] == 3
    assert counts["UNSW"] == 1


def test_mean_topic_entropy_ignores_missing_values(sample_dossier):
    """Work C has topic_entropy=None -- must be excluded from the mean, not treated as 0."""
    assert sample_dossier.mean_topic_entropy() == pytest.approx((0.8 + 0.3) / 2)


def test_mean_novelty_score(sample_dossier):
    assert sample_dossier.mean_novelty_score() == pytest.approx((0.2 + 0.6) / 2)


def test_mean_conventionality_score_none_when_no_data(sample_dossier):
    assert sample_dossier.mean_conventionality_score() is None


def test_n_excluded_works_sums_all_reasons(sample_dossier):
    assert sample_dossier.n_excluded_works == 3


def test_award_context_coauthor_countries(sample_dossier):
    assert dict(sample_dossier.award_contexts[0].coauthor_countries) == {"US": 2}


def test_empty_dossier_aggregates_are_empty_not_error():
    d = Dossier(arc_id="X", preferred_name="Nobody")
    assert dict(d.by_field()) == {}
    assert d.mean_topic_entropy() is None
    assert d.n_excluded_works == 0
    assert d.works_through(2020) == []


def test_to_markdown_does_not_crash_and_shows_populated_data(sample_dossier):
    md = sample_dossier.to_markdown()
    assert "Test Person" in md
    assert "DECRA" in md
    assert "Antipode" in md  # by_outlet, populated in this fixture


def test_to_markdown_reports_pending_sections_explicitly():
    """When a section genuinely has no data yet (e.g. outlet/novelty pending upstream
    tables), the render must say so explicitly rather than silently omitting the line --
    a coach reading the dossier needs to distinguish "zero" from "not built yet"."""
    d = Dossier(
        arc_id="X", preferred_name="Nobody",
        works=[Work(work_idx=1, publication_year=2020, cited_by_count=0, type="article", title="A")],
        award_contexts=[AwardContext(grant_id="G1", scheme="DECRA", award_year=2020, career_age_at_award=None)],
    )
    md = d.to_markdown()
    assert md.count("not yet available") >= 4  # outlet, institutions, topic entropy, novelty
