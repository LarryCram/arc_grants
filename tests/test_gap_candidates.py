"""
tests/test_gap_candidates.py

Tests for:
  - first_names_compatible  (src/utils/cluster_checks.py)
  - _compute_gap_candidates (src/01_prepare_arc.py)
"""
import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.cluster_checks import first_names_compatible

_spec = importlib.util.spec_from_file_location(
    "prepare_arc",
    Path(__file__).resolve().parents[1] / "src" / "01_prepare_arc.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
_compute_gap_candidates = _mod._compute_gap_candidates


# ── first_names_compatible ────────────────────────────────────────────────────

class TestFirstNamesCompatible:
    def test_initial_matches_full_name_in_s(self):
        # T has only initial; S has matching full name → initial matches first char
        assert first_names_compatible(["james", "j"], ["j"]) is True

    def test_initial_no_match_in_s(self):
        # T initial 'm'; S has 'james' (initial 'j') → no match
        assert first_names_compatible(["james", "j"], ["m"]) is False

    def test_exact_full_name_match(self):
        assert first_names_compatible(["james", "j"], ["james", "j"]) is True

    def test_different_full_names_same_initial_compatible(self):
        # jennifer vs james share initial 'j' → permissively compatible
        assert first_names_compatible(["james", "j"], ["jennifer", "j"]) is True

    def test_different_initial_incompatible(self):
        # maria vs james — no overlap at any level
        assert first_names_compatible(["james", "j"], ["maria", "m"]) is False

    def test_empty_t_returns_false(self):
        assert first_names_compatible(["james", "j"], []) is False

    def test_empty_s_returns_false(self):
        assert first_names_compatible([], ["james", "j"]) is False

    def test_both_bare_initials_matching(self):
        assert first_names_compatible(["j"], ["j"]) is True

    def test_both_bare_initials_not_matching(self):
        assert first_names_compatible(["j"], ["m"]) is False

    def test_full_name_in_t_against_bare_initial_in_s(self):
        # S has only 'j'; T has full name 'james' → james[0] == 'j' in s_inits
        assert first_names_compatible(["j"], ["james", "j"]) is True

    def test_full_name_in_t_against_non_matching_bare_initial_in_s(self):
        assert first_names_compatible(["m"], ["james", "j"]) is False


# ── _compute_gap_candidates ───────────────────────────────────────────────────

def _make_persons(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal persons DataFrame for testing."""
    defaults = {"for_names": [], "orcids": []}
    return pd.DataFrame([{**defaults, **r} for r in rows])


# Two divisions with no adjacency — any pair spans disconnected divisions.
DIV_MAP = {"biology": "Life Sciences", "physics": "Physical Sciences"}
ADJ: set = set()


class TestComputeGapCandidates:
    def test_compatible_pair_both_listed(self):
        persons = _make_persons([
            {"cluster_id": "A", "family_names": ["smith"], "first_names": ["john", "j"]},
            {"cluster_id": "B", "family_names": ["smith"], "first_names": ["john", "j"]},
        ])
        out = _compute_gap_candidates(persons, DIV_MAP, ADJ)
        assert "B" in out.loc[out["cluster_id"] == "A", "gap_candidates"].iloc[0]
        assert "A" in out.loc[out["cluster_id"] == "B", "gap_candidates"].iloc[0]

    def test_division_mismatch_excluded(self):
        persons = _make_persons([
            {"cluster_id": "A", "family_names": ["smith"], "first_names": ["j"],
             "for_names": ["biology"]},
            {"cluster_id": "B", "family_names": ["smith"], "first_names": ["j"],
             "for_names": ["physics"]},
        ])
        out = _compute_gap_candidates(persons, DIV_MAP, ADJ)
        assert out.loc[out["cluster_id"] == "A", "gap_candidates"].iloc[0] == []
        assert out.loc[out["cluster_id"] == "B", "gap_candidates"].iloc[0] == []

    def test_incompatible_names_not_listed(self):
        # Same surname, different full names with different initials → name_incompat fires
        persons = _make_persons([
            {"cluster_id": "A", "family_names": ["smith"], "first_names": ["john", "j"]},
            {"cluster_id": "B", "family_names": ["smith"], "first_names": ["mary", "m"]},
        ])
        out = _compute_gap_candidates(persons, DIV_MAP, ADJ)
        assert out.loc[out["cluster_id"] == "A", "gap_candidates"].iloc[0] == []
        assert out.loc[out["cluster_id"] == "B", "gap_candidates"].iloc[0] == []

    def test_gap2_cross_initial_compatible(self):
        # "T J Smith" vs "J Smith": different first initial but 'j' appears in both
        # first_names sets → Gap 2 candidate (surname-group only, not blocked by initial)
        persons = _make_persons([
            {"cluster_id": "A", "family_names": ["smith"],
             "first_names": ["thomas", "john", "t", "j"]},
            {"cluster_id": "B", "family_names": ["smith"],
             "first_names": ["john", "j"]},
        ])
        out = _compute_gap_candidates(persons, DIV_MAP, ADJ)
        assert "B" in out.loc[out["cluster_id"] == "A", "gap_candidates"].iloc[0]
        assert "A" in out.loc[out["cluster_id"] == "B", "gap_candidates"].iloc[0]

    def test_orcid_clash_excluded(self):
        persons = _make_persons([
            {"cluster_id": "A", "family_names": ["smith"], "first_names": ["j"],
             "orcids": ["0000-0001-0000-0001"]},
            {"cluster_id": "B", "family_names": ["smith"], "first_names": ["j"],
             "orcids": ["0000-0002-0000-0002"]},
        ])
        out = _compute_gap_candidates(persons, DIV_MAP, ADJ)
        assert out.loc[out["cluster_id"] == "A", "gap_candidates"].iloc[0] == []

    def test_three_clusters_partial_compatibility(self):
        # A and B: compatible (same initial, no FOR conflict)
        # A and C: incompatible (different initial)
        # B and C: incompatible (different initial)
        persons = _make_persons([
            {"cluster_id": "A", "family_names": ["smith"], "first_names": ["john", "j"]},
            {"cluster_id": "B", "family_names": ["smith"], "first_names": ["j"]},
            {"cluster_id": "C", "family_names": ["smith"], "first_names": ["mary", "m"]},
        ])
        out = _compute_gap_candidates(persons, DIV_MAP, ADJ)
        a_cands = out.loc[out["cluster_id"] == "A", "gap_candidates"].iloc[0]
        b_cands = out.loc[out["cluster_id"] == "B", "gap_candidates"].iloc[0]
        c_cands = out.loc[out["cluster_id"] == "C", "gap_candidates"].iloc[0]
        assert "B" in a_cands and "C" not in a_cands
        assert "A" in b_cands and "C" not in b_cands
        assert c_cands == []

    def test_singleton_cluster_empty_candidates(self):
        persons = _make_persons([
            {"cluster_id": "A", "family_names": ["jones"], "first_names": ["alice", "a"]},
        ])
        out = _compute_gap_candidates(persons, DIV_MAP, ADJ)
        assert out.loc[out["cluster_id"] == "A", "gap_candidates"].iloc[0] == []
