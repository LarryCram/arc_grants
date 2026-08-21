"""
Tests for merge/split helpers in src/01_prepare_arc.py:
  - arc_name_arrays
  - _merge_by_orcid
  - _split_orcid_conflicts
"""
import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_spec = importlib.util.spec_from_file_location(
    "prepare_arc",
    Path(__file__).resolve().parents[1] / "src" / "01_prepare_arc.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

arc_name_arrays       = _mod.arc_name_arrays
_merge_by_orcid       = _mod._merge_by_orcid
_split_orcid_conflicts = _mod._split_orcid_conflicts


# ---------------------------------------------------------------------------
# arc_name_arrays
# ---------------------------------------------------------------------------

class TestArcNameArrays:
    def test_simple_name(self):
        r = arc_name_arrays("John", "Smith")
        assert "john" in r["first_names"]
        assert "j"    in r["first_names"]
        assert "smith" in r["family_names"]

    def test_initial_only(self):
        r = arc_name_arrays("J", "Smith")
        assert "j" in r["first_names"]
        assert "smith" in r["family_names"]

    def test_middle_name_included(self):
        r = arc_name_arrays("John Paul", "Smith")
        assert "john" in r["first_names"]
        assert "paul" in r["first_names"]

    def test_diacritical_stripped_from_family(self):
        r = arc_name_arrays("Hans", "Müller")
        assert "muller" in r["family_names"]

    def test_postnominal_stripped_from_family(self):
        r = arc_name_arrays("Barry", "Jones AC")
        assert "jones" in r["family_names"]
        assert "ac" not in r["family_names"]

    def test_empty_first_name(self):
        r = arc_name_arrays("", "Smith")
        assert "smith" in r["family_names"]


# ---------------------------------------------------------------------------
# _merge_by_orcid
# ---------------------------------------------------------------------------

def _make_cluster_ids(*pairs):
    """pairs: (unique_id, cluster_id)"""
    return pd.DataFrame(pairs, columns=["unique_id", "cluster_id"])

def _make_raw(*rows):
    """rows: (unique_id, orcid, family_names_list)"""
    return pd.DataFrame(
        [{"unique_id": u, "orcid": o, "family_names": f} for u, o, f in rows]
    )


class TestMergeByOrcid:
    def test_same_orcid_different_clusters_merged(self):
        df_ids = _make_cluster_ids(
            ("G1_Trevor", "cluster_A"),
            ("G1_David",  "cluster_B"),
        )
        df_raw = _make_raw(
            ("G1_Trevor", "0000-0001-0001-0001", ["waite"]),
            ("G1_David",  "0000-0001-0001-0001", ["waite"]),
        )
        result = _merge_by_orcid(df_ids, df_raw)
        assert result["cluster_id"].nunique() == 1
        assert result["cluster_id"].iloc[0] == "cluster_A"

    def test_already_same_cluster_unchanged(self):
        df_ids = _make_cluster_ids(
            ("G1_Trevor", "cluster_A"),
            ("G1_David",  "cluster_A"),
        )
        df_raw = _make_raw(
            ("G1_Trevor", "0000-0001-0001-0001", ["waite"]),
            ("G1_David",  "0000-0001-0001-0001", ["waite"]),
        )
        result = _merge_by_orcid(df_ids, df_raw)
        # No cross-cluster ORCID, nothing changes
        assert set(result["cluster_id"]) == {"cluster_A"}

    def test_different_orcids_not_merged(self):
        df_ids = _make_cluster_ids(
            ("G1_Alice", "cluster_A"),
            ("G1_Bob",   "cluster_B"),
        )
        df_raw = _make_raw(
            ("G1_Alice", "0000-0001-0001-0001", ["smith"]),
            ("G1_Bob",   "0000-0002-0002-0002", ["smith"]),
        )
        result = _merge_by_orcid(df_ids, df_raw)
        assert result["cluster_id"].nunique() == 2

    def test_different_family_names_still_merged_on_orcid(self):
        """Murphy/Paton-Walsh case: family name differs but ORCID is authority."""
        df_ids = _make_cluster_ids(
            ("G1_Murphy",     "cluster_A"),
            ("G1_PatonWalsh", "cluster_B"),
        )
        df_raw = _make_raw(
            ("G1_Murphy",     "0000-0003-0003-0003", ["murphy"]),
            ("G1_PatonWalsh", "0000-0003-0003-0003", ["paton-walsh"]),
        )
        result = _merge_by_orcid(df_ids, df_raw)
        assert result["cluster_id"].nunique() == 1

    def test_no_orcid_records_unchanged(self):
        df_ids = _make_cluster_ids(
            ("G1_NoOrcid", "cluster_A"),
        )
        df_raw = _make_raw(
            ("G1_NoOrcid", None, ["smith"]),
        )
        result = _merge_by_orcid(df_ids, df_raw)
        assert list(result["cluster_id"]) == ["cluster_A"]

    def test_three_clusters_same_orcid_all_merged(self):
        df_ids = _make_cluster_ids(
            ("G1_A", "cluster_A"),
            ("G2_B", "cluster_B"),
            ("G3_C", "cluster_C"),
        )
        df_raw = _make_raw(
            ("G1_A", "0000-0001-0001-0001", ["jones"]),
            ("G2_B", "0000-0001-0001-0001", ["jones"]),
            ("G3_C", "0000-0001-0001-0001", ["jones"]),
        )
        result = _merge_by_orcid(df_ids, df_raw)
        assert result["cluster_id"].nunique() == 1


# ---------------------------------------------------------------------------
# _split_orcid_conflicts
# ---------------------------------------------------------------------------

class TestSplitOrcidConflicts:
    def test_two_orcids_in_cluster_splits(self):
        df_ids = _make_cluster_ids(
            ("G1_Alice", "cluster_A"),
            ("G1_Bob",   "cluster_A"),
        )
        df_raw = _make_raw(
            ("G1_Alice", "0000-0001-0001-0001", ["smith"]),
            ("G1_Bob",   "0000-0002-0002-0002", ["smith"]),
        )
        result = _split_orcid_conflicts(df_ids, df_raw)
        assert result["cluster_id"].nunique() == 2

    def test_single_orcid_cluster_unchanged(self):
        df_ids = _make_cluster_ids(
            ("G1_Alice", "cluster_A"),
            ("G2_Alice", "cluster_A"),
        )
        df_raw = _make_raw(
            ("G1_Alice", "0000-0001-0001-0001", ["smith"]),
            ("G2_Alice", "0000-0001-0001-0001", ["smith"]),
        )
        result = _split_orcid_conflicts(df_ids, df_raw)
        assert result["cluster_id"].nunique() == 1

    def test_no_orcid_cluster_unchanged(self):
        df_ids = _make_cluster_ids(
            ("G1_A", "cluster_A"),
            ("G2_A", "cluster_A"),
        )
        df_raw = _make_raw(
            ("G1_A", None, ["smith"]),
            ("G2_A", None, ["smith"]),
        )
        result = _split_orcid_conflicts(df_ids, df_raw)
        assert result["cluster_id"].nunique() == 1

    def test_no_orcid_record_gets_own_cluster(self):
        """Record with no ORCID in a conflicted cluster gets its own cluster_id."""
        df_ids = _make_cluster_ids(
            ("G1_Alice",   "cluster_A"),
            ("G1_Bob",     "cluster_A"),
            ("G1_Unknown", "cluster_A"),
        )
        df_raw = _make_raw(
            ("G1_Alice",   "0000-0001-0001-0001", ["smith"]),
            ("G1_Bob",     "0000-0002-0002-0002", ["smith"]),
            ("G1_Unknown", None,                  ["smith"]),
        )
        result = _split_orcid_conflicts(df_ids, df_raw)
        assert result["cluster_id"].nunique() == 3
