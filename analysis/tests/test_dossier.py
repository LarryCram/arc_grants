"""Tests for analysis/utils/dossier.py (the rebuilt Dossier model, 2026-10-09): pure Python, no data.
build() itself (analysis/utils/dossier_build.py) reads the persisted outputs and is checked by running
analysis/23_dossier.py on real ACIFs."""

import dataclasses

import pytest

from analysis.utils.dossier import Award, Dossier, LinkedRecord, Work


def _w(i, y, decision="accept", type_="article", field="Ecology", venue="J", rule="core"):
    return Work(work_idx=i, year=y, type=type_, title=f"t{i}", venue=venue, doi=None, cited_by_count=0,
                authors_count=2, field=field, decision=decision, rule=rule, decided_by="rule", reason=None)


@pytest.fixture
def d():
    return Dossier(
        cluster_id="DE200000001_test_person", name="Test Person", orcids=["0000-0000-0000-0001"], orcid_sources=["arc"],
        awards=[Award("DE200000001", "DE", "DECRA", "DECRA", True, 2020, 3, 2023, 400000.0, "UQ", 1, "Ecology"),
                Award("DP230000001", "DP", None, "CI", False, 2023, 3, 2026, 500000.0, "UQ", 1, "Ecology", ended_early=True)],
        links=[LinkedRecord(1, "Test Person", "0000-0000-0000-0001", "orcid", "accept_name_key", 3, "x")],
        works=[_w(1, 2015), _w(2, 2018), _w(3, 2021, venue="K"), _w(4, 2022, decision="unsure", rule="fits core")],
        rejected={"namesake component": 5},
        citations={1: {2016: 3, 2018: 4}, 2: {2019: 2, 2020: 1, 2021: 5}, 3: {2022: 1}, 4: {2023: 9}})


def test_profile(d):
    assert d.first_grant_year == 2020 and d.last_grant_year == 2023
    assert [Dossier.label(a) for a in d.fellowships] == ["DECRA"]
    assert d.first_pub_year() == 2015 and len(d.unsure()) == 1


def test_citations_and_h_index(d):
    assert d.citations_received(1) == 7 and d.citations_received(2, through_year=2019) == 2
    assert d.h_index(2017) == 1            # work 1 has 3 citations by 2017; work 2 not yet published
    assert d.h_index() == 2                # 7, 8, 1 -> h = 2; unsure work 4 not counted


def test_timeline(d):
    tl = {r["year"]: r for r in d.timeline()}
    assert tl[2015]["works"] == 1 and tl[2022]["unsure"] == 1 and tl[2022]["works"] == 0
    assert tl[2021]["citations"] == 5 and tl[2023]["citations"] == 0      # unsure work's citations not counted
    assert tl[2020]["events"] == ["DECRA* DE200000001"] and tl[2023]["events"] == ["DP DP230000001 (ended early)"]


def test_counts_and_markdown(d):
    assert d.by_venue() == {"J": 2, "K": 1} and d.by_type()["article"] == 3
    md = d.to_markdown(chart="x.png")
    assert "# Test Person" in md and "DECRA 2020" in md and "![time-line](x.png)" in md
    assert "namesake component 5" in md


def test_frozen(d):
    with pytest.raises(dataclasses.FrozenInstanceError):
        d.name = "x"


def test_plot(d, tmp_path):
    assert d.plot_timeline(tmp_path / "t.png") and (tmp_path / "t.png").stat().st_size > 1000
