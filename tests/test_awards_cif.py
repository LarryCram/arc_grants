"""
Tests for src/utils/awards_cif.py's pure, non-I/O logic:
  - _name_forms, _first_initial, _first_name_canonical  (name normalization)
  - _build_awards_cif                                    (item -> cluster aggregation)
  - merge_by_orcid, split_orcid_conflicts, merge_persons_by_orcid  (refine_clusters steps)
  - compute_gap_candidates, compute_reliability          (reliability steps)
  - AwardsCIF.record_event                               (provenance)

Functions that require real DuckDB/parquet/diskcache inputs (load_award_cif_items,
cluster_items, split_multi_name_clusters, promote_low_by_for, merge_same_grant_coinvestigators,
populate_oax_candidates, dedup_oax_candidates, compute_orcid_for, apply_manual_*) are validated
instead by the full-cohort verification against arc_persons.parquet -- see the plan file.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.awards_cif import (
    AwardCIFItem,
    AwardsCIF,
    _name_forms,
    _first_initial,
    _first_name_canonical,
    _build_awards_cif,
    merge_by_orcid,
    split_orcid_conflicts,
    merge_persons_by_orcid,
    compute_gap_candidates,
    compute_reliability,
)


def _item(
    unique_id,
    orcid=None,
    first_names=None,
    family_names=None,
    full_name="John Smith",
    for_name=None,
    for_code=None,
    institution_oax_id=None,
    full_name_key=None,
) -> AwardCIFItem:
    return AwardCIFItem(
        unique_id=unique_id,
        grant_code=unique_id.split("_")[0],
        first_name="",
        family_name="",
        role_code="CI",
        orcid=orcid,
        admin_org=None,
        institution_oax_id=institution_oax_id,
        funding_commence_year=None,
        for_name=for_name,
        for_code=for_code,
        full_name=full_name,
        first_names=first_names or [],
        family_names=family_names or [],
        full_name_key=full_name_key,
    )


# ---------------------------------------------------------------------------
# _name_forms / _first_initial / _first_name_canonical
# ---------------------------------------------------------------------------

class TestNameForms:
    def test_simple_name(self):
        first_names, family_names = _name_forms("John", "Smith")
        assert "john" in first_names
        assert "j" in first_names
        assert family_names == ["smith"]

    def test_diacritical_stripped_from_family(self):
        _, family_names = _name_forms("Hans", "Müller")
        assert family_names == ["muller"]

    def test_compound_surname_not_split(self):
        _, family_names = _name_forms("Anna", "van der Berg")
        assert family_names == ["van der berg"]

    def test_empty_first_name_falls_back_to_family_name(self):
        first_names, family_names = _name_forms("", "Smith")
        assert family_names == ["smith"]
        assert "smith" in first_names
        assert "s" in first_names


class TestFirstInitial:
    def test_prefers_full_token_initial(self):
        assert _first_initial(["john", "j"]) == "j"

    def test_falls_back_to_bare_initial(self):
        assert _first_initial(["j"]) == "j"

    def test_empty_returns_none(self):
        assert _first_initial([]) is None


class TestFirstNameCanonical:
    def test_prefers_longest_full_token(self):
        assert _first_name_canonical(["jon", "jonathan", "j"]) == "jonathan"

    def test_falls_back_to_bare_initial(self):
        assert _first_name_canonical(["j"]) == "j"

    def test_empty_returns_none(self):
        assert _first_name_canonical([]) is None


# ---------------------------------------------------------------------------
# AwardsCIF.record_event
# ---------------------------------------------------------------------------

class TestRecordEvent:
    def test_appends_structured_event(self):
        c = AwardsCIF(cluster_id="A")
        c.record_event("test_event", foo="bar")
        assert c.provenance == [{"event": "test_event", "foo": "bar"}]


# ---------------------------------------------------------------------------
# _build_awards_cif
# ---------------------------------------------------------------------------

class TestBuildAwardsCIF:
    def test_single_item_has_orcid(self):
        it = _item("G1_John", orcid="0000-0001-0001-0001", full_name="John Smith")
        cif = _build_awards_cif("G1_John", [it])
        assert cif.orcid_status == "HAS_ORCID"
        assert cif.orcids == ["0000-0001-0001-0001"]
        assert cif.n_grants == 1
        assert cif.full_names == ["John Smith"]

    def test_multiple_items_no_orcid(self):
        items = [
            _item("G1_John", full_name="John Smith"),
            _item("G2_John", full_name="John Smith"),
        ]
        cif = _build_awards_cif("G1_John", items)
        assert cif.orcid_status == "NO_ORCID"
        assert cif.n_grants == 2
        assert cif.grant_ids == ["G1_John", "G2_John"]

    def test_conflicting_orcids_multi(self):
        items = [
            _item("G1_A", orcid="0000-0001-0001-0001"),
            _item("G2_A", orcid="0000-0002-0002-0002"),
        ]
        cif = _build_awards_cif("G1_A", items)
        assert cif.orcid_status == "MULTI_ORCID"
        assert cif.orcids == ["0000-0001-0001-0001", "0000-0002-0002-0002"]

    def test_full_name_key_is_modal(self):
        items = [
            _item("G1_A", full_name_key="john_smith"),
            _item("G2_A", full_name_key="john_smith"),
            _item("G3_A", full_name_key="j_smith"),
        ]
        cif = _build_awards_cif("G1_A", items)
        assert cif.full_name_key == "john_smith"


# ---------------------------------------------------------------------------
# merge_by_orcid
# ---------------------------------------------------------------------------

class TestMergeByOrcid:
    def test_shared_orcid_merges_two_clusters(self):
        a = _build_awards_cif("cluster_A", [_item("G1_Trevor", orcid="0000-0001-0001-0001")])
        b = _build_awards_cif("cluster_B", [_item("G2_David", orcid="0000-0001-0001-0001")])
        out = merge_by_orcid([a, b])
        assert len(out) == 1
        assert out[0].cluster_id == "cluster_A"
        assert out[0].n_grants == 2
        assert out[0].provenance[-1]["event"] == "orcid_merge"

    def test_no_shared_orcid_unchanged(self):
        a = _build_awards_cif("cluster_A", [_item("G1_X", orcid="0000-0001-0001-0001")])
        b = _build_awards_cif("cluster_B", [_item("G2_Y", orcid="0000-0002-0002-0002")])
        out = merge_by_orcid([a, b])
        assert len(out) == 2

    def test_no_orcid_clusters_unchanged(self):
        a = _build_awards_cif("cluster_A", [_item("G1_X")])
        out = merge_by_orcid([a])
        assert len(out) == 1
        assert out[0].provenance == []

    def test_three_clusters_same_orcid_all_merged(self):
        cs = [
            _build_awards_cif(f"cluster_{x}", [_item(f"G{i}_{x}", orcid="0000-0001-0001-0001")])
            for i, x in enumerate("ABC")
        ]
        out = merge_by_orcid(cs)
        assert len(out) == 1
        assert out[0].n_grants == 3
        assert out[0].cluster_id == "cluster_A"


# ---------------------------------------------------------------------------
# split_orcid_conflicts
# ---------------------------------------------------------------------------

class TestSplitOrcidConflicts:
    def test_two_orcids_splits(self):
        items = [
            _item("G1_Alice", orcid="0000-0001-0001-0001"),
            _item("G1_Bob", orcid="0000-0002-0002-0002"),
        ]
        cif = _build_awards_cif("cluster_A", items)
        out = split_orcid_conflicts([cif])
        assert len(out) == 2
        assert {c.cluster_id for c in out} == {"G1_Alice", "G1_Bob"}
        for c in out:
            assert c.provenance[-1]["event"] == "orcid_conflict_split"

    def test_single_orcid_unchanged(self):
        items = [
            _item("G1_Alice", orcid="0000-0001-0001-0001"),
            _item("G2_Alice", orcid="0000-0001-0001-0001"),
        ]
        cif = _build_awards_cif("cluster_A", items)
        out = split_orcid_conflicts([cif])
        assert len(out) == 1
        assert out[0].cluster_id == "cluster_A"

    def test_no_orcid_unchanged(self):
        items = [_item("G1_A"), _item("G2_A")]
        cif = _build_awards_cif("cluster_A", items)
        out = split_orcid_conflicts([cif])
        assert len(out) == 1

    def test_no_orcid_record_gets_own_cluster(self):
        items = [
            _item("G1_Alice", orcid="0000-0001-0001-0001"),
            _item("G1_Bob", orcid="0000-0002-0002-0002"),
            _item("G1_Unknown", orcid=None),
        ]
        cif = _build_awards_cif("cluster_A", items)
        out = split_orcid_conflicts([cif])
        assert len(out) == 3


# ---------------------------------------------------------------------------
# merge_persons_by_orcid
# ---------------------------------------------------------------------------

class TestMergePersonsByOrcid:
    def test_compatible_initials_merged(self):
        a = _build_awards_cif("cluster_A", [_item("G1_A", orcid="0000-0001-0001-0001", first_names=["john", "j"])])
        b = _build_awards_cif("cluster_B", [_item("G2_B", orcid="0000-0001-0001-0001", first_names=["j"])])
        out = merge_persons_by_orcid([a, b])
        assert len(out) == 1
        assert out[0].provenance[-1]["event"] == "post_enrichment_merge"

    def test_incompatible_initials_not_merged(self):
        a = _build_awards_cif("cluster_A", [_item("G1_A", orcid="0000-0001-0001-0001", first_names=["j"])])
        b = _build_awards_cif("cluster_B", [_item("G2_B", orcid="0000-0001-0001-0001", first_names=["m"])])
        out = merge_persons_by_orcid([a, b])
        assert len(out) == 2

    def test_no_shared_orcid_unchanged(self):
        a = _build_awards_cif("cluster_A", [_item("G1_A", orcid="0000-0001-0001-0001")])
        b = _build_awards_cif("cluster_B", [_item("G2_B", orcid="0000-0002-0002-0002")])
        out = merge_persons_by_orcid([a, b])
        assert len(out) == 2


# ---------------------------------------------------------------------------
# compute_gap_candidates
# ---------------------------------------------------------------------------
# for2020_codes left empty throughout -- division_mismatch_for2020_pairwise() always returns
# False when either side has no primary code to compare, so these tests exercise only the
# name/ORCID incompatibility axes, independent of Resolver()'s live FOR2020->OAX_FIELD mapping.

class TestComputeGapCandidates:
    def test_compatible_pair_both_listed(self):
        a = _build_awards_cif("A", [_item("G1_A", first_names=["john", "j"], family_names=["smith"])])
        b = _build_awards_cif("B", [_item("G1_B", first_names=["john", "j"], family_names=["smith"])])
        out = compute_gap_candidates([a, b])
        by_id = {c.cluster_id: c for c in out}
        assert "B" in by_id["A"].gap_candidates
        assert "A" in by_id["B"].gap_candidates

    def test_incompatible_names_not_listed(self):
        a = _build_awards_cif("A", [_item("G1_A", first_names=["john", "j"], family_names=["smith"])])
        b = _build_awards_cif("B", [_item("G1_B", first_names=["mary", "m"], family_names=["smith"])])
        out = compute_gap_candidates([a, b])
        by_id = {c.cluster_id: c for c in out}
        assert by_id["A"].gap_candidates == []
        assert by_id["B"].gap_candidates == []

    def test_orcid_clash_excluded(self):
        a = _build_awards_cif("A", [_item("G1_A", first_names=["j"], family_names=["smith"], orcid="0000-0001-0001-0001")])
        b = _build_awards_cif("B", [_item("G1_B", first_names=["j"], family_names=["smith"], orcid="0000-0002-0002-0002")])
        out = compute_gap_candidates([a, b])
        by_id = {c.cluster_id: c for c in out}
        assert by_id["A"].gap_candidates == []
        assert by_id["B"].gap_candidates == []

    def test_no_shared_family_name_no_candidates(self):
        a = _build_awards_cif("A", [_item("G1_A", first_names=["alice", "a"], family_names=["jones"])])
        b = _build_awards_cif("B", [_item("G1_B", first_names=["alice", "a"], family_names=["smith"])])
        out = compute_gap_candidates([a, b])
        for c in out:
            assert c.gap_candidates == []

    def test_singleton_family_name_no_candidates(self):
        a = _build_awards_cif("A", [_item("G1_A", first_names=["alice", "a"], family_names=["jones"])])
        out = compute_gap_candidates([a])
        assert out[0].gap_candidates == []


# ---------------------------------------------------------------------------
# compute_reliability
# ---------------------------------------------------------------------------
# for2020_codes left empty throughout -- is_suspicious_for2020() requires a real
# division_mismatch_for2020() to ever flag UNRESOLVED, which needs >=2 distinct primary OAX
# fields; with no codes present it always returns False (RESOLVED), so these tests isolate the
# ORCID/tier logic. The tier-3 test uses a full_name_key guaranteed absent from the real
# oax_tf_full_name.parquet lookup, so tf_lookup.get(..., 1.0) deterministically falls back to
# the "common name" default regardless of that file's actual contents.

class TestComputeReliability:
    def test_has_orcid_arc_source_tier_1a(self):
        c = _build_awards_cif("A", [_item("G1_A", orcid="0000-0001-0001-0001")])
        out = compute_reliability([c])
        assert out[0].reliability_tier == "1a"
        assert out[0].resolution_status == "RESOLVED"

    def test_has_orcid_enriched_source_tier_1b(self):
        c = _build_awards_cif("A", [_item("G1_A", orcid="0000-0001-0001-0001")])
        c.record_event("enriched_orcid", orcid="0000-0001-0001-0001")
        out = compute_reliability([c])
        assert out[0].reliability_tier == "1b"

    def test_has_orcid_manual_source_tier_1c(self):
        c = _build_awards_cif("A", [_item("G1_A", orcid="0000-0001-0001-0001")])
        c.record_event("manual_orcid", orcid="0000-0001-0001-0001")
        out = compute_reliability([c])
        assert out[0].reliability_tier == "1c"

    def test_multi_orcid_forces_unresolved(self):
        c = _build_awards_cif("A", [
            _item("G1_A", orcid="0000-0001-0001-0001"),
            _item("G2_A", orcid="0000-0002-0002-0002"),
        ])
        out = compute_reliability([c])
        assert out[0].orcid_status == "MULTI_ORCID"
        assert out[0].resolution_status == "UNRESOLVED"

    def test_no_orcid_multigrant_common_name_tier_3(self):
        c = _build_awards_cif("A", [
            _item("G1_A", full_name_key="__test_common_name_not_in_tf_table__"),
            _item("G2_A", full_name_key="__test_common_name_not_in_tf_table__"),
        ])
        out = compute_reliability([c])
        assert out[0].reliability_tier == "3"

    def test_no_orcid_singleton_no_gap_tier_4(self):
        c = _build_awards_cif("A", [_item("G1_A")])
        out = compute_reliability([c])
        assert out[0].reliability_tier == "4"

    def test_no_orcid_singleton_with_gap_tier_4u(self):
        c = _build_awards_cif("A", [_item("G1_A")])
        c.gap_candidates = ["B"]
        out = compute_reliability([c])
        assert out[0].reliability_tier == "4u"
