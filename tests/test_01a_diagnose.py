"""
Tests for src/01a_diagnose.py's pure, non-I/O logic added 2026-08-24:
  - check_A/check_B/check_C's HardCheckResult population (given hand-built DataFrame fixtures)
  - run_diagnostics()
  - verify_cluster()
  - sample_4u_clusters()

The pre-existing A/B/C *console* battery, run against the real persisted population, stays
validated the way it always has been -- by actually running `.venv/bin/python
src/01a_diagnose.py` against awards_cif_arc_only.parquet, not by a unit test. This file covers
only the new pure-function layer added on top of that (HardCheckResult/verify_cluster/
run_diagnostics/sample_4u_clusters), matching tests/test_awards_cif.py's own convention of
hand-built fixtures for pure logic, real-data verification for I/O-heavy functions.
"""
import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# src/01a_diagnose.py has a digit-leading filename -- not a valid dotted module name, so it's
# loaded via importlib, matching the pattern already used elsewhere in this codebase (e.g.
# analysis/utils/dossier_build.py's `_ecr = import_module("analysis.07_analyse_ecr_fellowships")`).
_spec = importlib.util.spec_from_file_location(
    "diag", str(Path(__file__).resolve().parents[1] / "src" / "01a_diagnose.py")
)
diag = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(diag)

from src.utils.awards_cif import sample_4u_clusters


def _persons_row(
    cluster_id,
    orcid_status="HAS_ORCID",
    resolution_status="RESOLVED",
    orcids=(),
    family_names=("smith",),
    full_name_key="j_smith",
    n_grants=1,
    for2020_codes=(),
) -> dict:
    return {
        "cluster_id": cluster_id,
        "orcid_status": orcid_status,
        "resolution_status": resolution_status,
        "orcids": list(orcids),
        "full_names": [f"{cluster_id} Person"],
        "family_names": list(family_names),
        "full_name_key": full_name_key,
        "n_grants": n_grants,
        "for2020_codes": list(for2020_codes),
        "grant_ids": [f"{cluster_id}_G1"],
        "reliability_tier": "1a" if orcid_status == "HAS_ORCID" else "4",
        "gap_candidates": [],
    }


def _persons_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _empty_inv_gmap():
    inv_f = pd.DataFrame(columns=["unique_id", "grant_code", "orcid"])
    gmap = pd.DataFrame(columns=["unique_id", "cluster_id"])
    prep = pd.DataFrame(columns=["unique_id", "family_names", "first_initials"])
    tf_lookup = {}
    return inv_f, gmap, prep, tf_lookup


@pytest.fixture(autouse=True)
def _no_real_confirmed_not_suspicious_csv(tmp_path, monkeypatch):
    # check_A() -> _load_confirmed_not_suspicious() now resolves every row's cluster_id via
    # resolve_cluster_id(), which raises StaleClusterIdError for any row that doesn't belong
    # to the *current* persons DataFrame. Every test in this file uses tiny synthetic
    # single/two-cluster fixtures ("A"/"B"), not real production data, so point at a
    # nonexistent file by default -- same fix as tests/test_awards_cif.py's
    # TestComputeReliability, needed here for the same reason.
    monkeypatch.setattr(
        "src.utils.awards_cif._MANUAL_CONFIRMED_NOT_SUSPICIOUS_CSV",
        tmp_path / "manual_confirmed_not_suspicious.csv",
    )


class TestCheckA:
    def test_multi_orcid_lands_in_result(self):
        persons = _persons_df([
            _persons_row("A", orcid_status="MULTI_ORCID", resolution_status="UNRESOLVED"),
            _persons_row("B"),
        ])
        inv_f, gmap, prep, tf_lookup = _empty_inv_gmap()
        result = diag.HardCheckResult(known_cluster_ids=set(persons["cluster_id"]))
        failures = diag.check_A(persons, gmap, inv_f, prep, tf_lookup, result, verbose=False)
        assert "A" in result.multi_orcid
        assert "B" not in result.multi_orcid
        assert failures >= 1

    def test_unresolved_lands_in_result(self):
        persons = _persons_df([
            _persons_row("A", resolution_status="UNRESOLVED"),
            _persons_row("B"),
        ])
        inv_f, gmap, prep, tf_lookup = _empty_inv_gmap()
        result = diag.HardCheckResult(known_cluster_ids=set(persons["cluster_id"]))
        diag.check_A(persons, gmap, inv_f, prep, tf_lookup, result, verbose=False)
        assert result.unresolved == {"A"}

    def test_within_grant_conflicting_orcid_lands_in_result(self):
        persons = _persons_df([_persons_row("A"), _persons_row("B")])
        inv_f = pd.DataFrame([
            {"unique_id": "A_x", "grant_code": "G1", "orcid": "0000-0001-0001-0001"},
            {"unique_id": "A_y", "grant_code": "G1", "orcid": "0000-0002-0002-0002"},
        ])
        gmap = pd.DataFrame([
            {"unique_id": "A_x", "cluster_id": "A"},
            {"unique_id": "A_y", "cluster_id": "A"},
        ])
        _, _, prep, tf_lookup = _empty_inv_gmap()
        result = diag.HardCheckResult(known_cluster_ids=set(persons["cluster_id"]))
        failures = diag.check_A(persons, gmap, inv_f, prep, tf_lookup, result, verbose=False)
        assert "A" in result.within_grant_orcid_conflict
        assert failures >= 1


class TestCheckB:
    def test_orcid_in_two_clusters_lands_in_full_result_set(self):
        # Regression guard: must reflect the FULL set of implicated clusters, not just a
        # head(10)-limited console-preview slice.
        rows = [_persons_row(f"C{i}", orcids=("0000-0001-0001-0001",)) for i in range(12)]
        persons = _persons_df(rows)
        inv_f, gmap, prep, tf_lookup = _empty_inv_gmap()
        result = diag.HardCheckResult(known_cluster_ids=set(persons["cluster_id"]))
        diag.check_B(persons, gmap, inv_f, prep, result, verbose=False)
        assert result.orcid_in_multiple_clusters == {f"C{i}" for i in range(12)}

    def test_unique_orcids_do_not_land_in_result(self):
        persons = _persons_df([
            _persons_row("A", orcids=("0000-0001-0001-0001",)),
            _persons_row("B", orcids=("0000-0002-0002-0002",)),
        ])
        inv_f, gmap, prep, tf_lookup = _empty_inv_gmap()
        result = diag.HardCheckResult(known_cluster_ids=set(persons["cluster_id"]))
        diag.check_B(persons, gmap, inv_f, prep, result, verbose=False)
        assert result.orcid_in_multiple_clusters == set()


class TestCheckC:
    def test_missing_unique_id_lands_in_result(self):
        persons = _persons_df([_persons_row("A")])
        inv_f = pd.DataFrame([{"unique_id": "A_x", "grant_code": "G1", "orcid": None}])
        gmap = pd.DataFrame(columns=["unique_id", "cluster_id"])  # A_x never made it into a cluster
        result = diag.HardCheckResult(known_cluster_ids=set(persons["cluster_id"]))
        failures = diag.check_C(persons, gmap, inv_f, result, verbose=False)
        assert "A_x" in result.missing_unique_ids
        assert failures >= 1


class TestRunDiagnostics:
    def test_sums_failures_across_a_mixed_fixture(self):
        persons = _persons_df([
            _persons_row("A", orcid_status="MULTI_ORCID", resolution_status="UNRESOLVED"),
            _persons_row("B"),
        ])
        inv_f, gmap, prep, tf_lookup = _empty_inv_gmap()
        result = diag.run_diagnostics(persons, gmap, inv_f, prep, tf_lookup, [], pd.DataFrame(), verbose=False)
        assert result.n_failures >= 1
        assert "A" in result.multi_orcid

    def test_verbose_false_produces_no_stdout(self, capsys):
        persons = _persons_df([_persons_row("A")])
        inv_f, gmap, prep, tf_lookup = _empty_inv_gmap()
        diag.run_diagnostics(persons, gmap, inv_f, prep, tf_lookup, [], pd.DataFrame(), verbose=False)
        captured = capsys.readouterr()
        assert captured.out == ""


class TestVerifyCluster:
    def test_clean_cluster_is_sound(self):
        result = diag.HardCheckResult(known_cluster_ids={"A"})
        verdict = diag.verify_cluster("A", result)
        assert verdict.sound is True
        assert verdict.reasons == []

    def test_absent_from_population(self):
        result = diag.HardCheckResult(known_cluster_ids={"B"})
        verdict = diag.verify_cluster("A", result)
        assert verdict.sound is False
        assert verdict.reasons == ["NOT_FOUND_IN_ARC_ONLY"]

    @pytest.mark.parametrize("field_name,reason", [
        ("multi_orcid", "A1_MULTI_ORCID"),
        ("unresolved", "A2_UNRESOLVED"),
        ("suspicious_for2020", "A3_SUSPICIOUS_FOR2020"),
        ("within_grant_orcid_conflict", "A5_WITHIN_GRANT_ORCID_CONFLICT"),
        ("orcid_in_multiple_clusters", "B1_ORCID_IN_MULTIPLE_CLUSTERS"),
    ])
    def test_each_individual_failure_mode(self, field_name, reason):
        result = diag.HardCheckResult(known_cluster_ids={"A"})
        getattr(result, field_name).add("A")
        verdict = diag.verify_cluster("A", result)
        assert verdict.sound is False
        assert verdict.reasons == [reason]

    def test_two_failures_at_once(self):
        result = diag.HardCheckResult(known_cluster_ids={"A"})
        result.multi_orcid.add("A")
        result.orcid_in_multiple_clusters.add("A")
        verdict = diag.verify_cluster("A", result)
        assert verdict.sound is False
        assert "A1_MULTI_ORCID" in verdict.reasons
        assert "B1_ORCID_IN_MULTIPLE_CLUSTERS" in verdict.reasons


class TestSample4uClusters:
    def _pool(self, n):
        return pd.DataFrame({"cluster_id": [f"C{i}" for i in range(n)]})

    def test_respects_n(self):
        out = sample_4u_clusters(self._pool(100), n=10, seed=1)
        assert len(out) == 10

    def test_n_greater_than_pool_returns_whole_pool(self):
        pool = self._pool(5)
        out = sample_4u_clusters(pool, n=30, seed=1)
        assert sorted(out) == sorted(pool["cluster_id"].tolist())

    def test_deterministic_for_fixed_seed(self):
        pool = self._pool(100)
        out1 = sample_4u_clusters(pool, n=10, seed=42)
        out2 = sample_4u_clusters(pool, n=10, seed=42)
        assert out1 == out2

    def test_output_is_sorted(self):
        out = sample_4u_clusters(self._pool(100), n=10, seed=1)
        assert out == sorted(out)
