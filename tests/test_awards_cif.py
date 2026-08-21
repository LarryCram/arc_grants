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
    _merge_awards_cifs,
    compute_gap_candidates,
    compute_reliability,
    persist_awards_cif,
    load_awards_cif,
    _load_hep_crosswalk,
    _load_institution_hep_crosswalk,
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
# _merge_awards_cifs data preservation -- regression tests for a real, confirmed live bug
# (2026-08-21): refine_clusters() steps that set fields in place (apply_enriched_orcids,
# apply_manual_orcids set c.orcids without touching c.items) used to have that change silently
# discarded by any later _merge_awards_cifs() call, since it rebuilt the merged cluster purely
# from _build_awards_cif(items). Confirmed on the live persisted population: 44 AwardsCIF carried
# an enriched_orcid/manual_orcid provenance event but had orcids == [].
# ---------------------------------------------------------------------------

class TestMergeAwardsCifsDataPreservation:
    def test_in_place_orcid_survives_a_later_merge(self):
        # Simulates apply_enriched_orcids(): items carry no orcid, but the cluster-level
        # `orcids` was set in place, exactly as that function does.
        enriched = _build_awards_cif("A", [_item("G1_A", first_names=["j"], family_names=["smith"])])
        enriched.orcids = ["0000-0001-0001-0001"]
        enriched.orcid_status = "HAS_ORCID"
        enriched.record_event("enriched_orcid", orcid="0000-0001-0001-0001", confidence="high")

        other = _build_awards_cif("B", [_item("G1_B", first_names=["j"], family_names=["smith"])])

        merged = _merge_awards_cifs(enriched, [other], "manual_merge")
        assert merged.orcids == ["0000-0001-0001-0001"]
        assert merged.orcid_status == "HAS_ORCID"

    def test_in_place_orcid_survives_when_absorbed_not_canonical(self):
        # Same bug, other direction: the enriched cluster is the one being ABSORBED, not kept.
        canonical = _build_awards_cif("A", [_item("G1_A", first_names=["j"], family_names=["smith"])])
        enriched = _build_awards_cif("B", [_item("G1_B", first_names=["j"], family_names=["smith"])])
        enriched.orcids = ["0000-0002-0002-0002"]
        enriched.orcid_status = "HAS_ORCID"
        enriched.record_event("manual_orcid", orcid="0000-0002-0002-0002")

        merged = _merge_awards_cifs(canonical, [enriched], "manual_merge")
        assert merged.orcids == ["0000-0002-0002-0002"]
        assert merged.orcid_status == "HAS_ORCID"

    def test_oax_candidates_and_excluded_flag_survive_a_merge(self):
        a = _build_awards_cif("A", [_item("G1_A")])
        a.oax_candidates = ["A5111111111"]
        b = _build_awards_cif("B", [_item("G1_B")])
        b.oax_candidates = ["A5222222222"]
        b.excluded = True
        b.excluded_reason = "indigenous"

        merged = _merge_awards_cifs(a, [b], "manual_merge")
        assert merged.oax_candidates == ["A5111111111", "A5222222222"]
        assert merged.excluded is True
        assert merged.excluded_reason == "indigenous"

    def test_reliability_invalidated_not_carried_forward_stale(self):
        a = _build_awards_cif("A", [_item("G1_A")])
        a.reliability_tier = "1a"
        a.resolution_status = "RESOLVED"
        b = _build_awards_cif("B", [_item("G1_B")])

        merged = _merge_awards_cifs(a, [b], "manual_merge")
        assert merged.reliability_tier is None
        assert merged.resolution_status == "UNRESOLVED"

    def test_end_to_end_via_merge_persons_by_orcid(self):
        # Same bug, exercised through a real production call site rather than the private
        # function directly, so the regression is caught even if the internals change.
        enriched = _build_awards_cif("cluster_A", [_item("G1_A", first_names=["j"])])
        enriched.orcids = ["0000-0003-0003-0003"]
        enriched.orcid_status = "HAS_ORCID"
        enriched.record_event("enriched_orcid", orcid="0000-0003-0003-0003", confidence="high")
        other = _build_awards_cif("cluster_B", [_item("G2_B", orcid="0000-0003-0003-0003", first_names=["j"])])

        out = merge_persons_by_orcid([enriched, other])
        assert len(out) == 1
        assert out[0].orcids == ["0000-0003-0003-0003"]
        assert out[0].orcid_status == "HAS_ORCID"


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


# ---------------------------------------------------------------------------
# persist_awards_cif / load_awards_cif -- round-trip through a real parquet file (tmp_path),
# never touching PROCESSED_DATA. Confirms this new persistence (2026-08-15 -- AwardsCIF was
# never written to disk anywhere before this) actually preserves every field a caller needs,
# not just that to_parquet()/read_parquet() don't raise.
# ---------------------------------------------------------------------------

class TestPersistAndLoadAwardsCif:
    def _full_cluster(self) -> AwardsCIF:
        c = _build_awards_cif("A", [_item("G1_A", orcid="0000-0001-0001-0001", for_name="Geology", for_code="3705")])
        c.for2020_codes = [{"code": "3705", "name": "Geology", "is_primary": True, "confidence": 1.0}]
        c.orcid_for_codes = [{"code": "3705", "name": "Geology", "count": 4}]
        c.gap_candidates = ["B", "C"]
        c.oax_candidates = ["https://openalex.org/A100", "https://openalex.org/A200"]
        c.reliability_tier = "1a"
        c.resolution_status = "RESOLVED"
        c.excluded = False
        c.excluded_reason = None
        c.record_event("splink_cluster", threshold=0.9)
        return c

    def test_round_trip_preserves_scalar_fields(self, tmp_path):
        c = self._full_cluster()
        path = tmp_path / "awards_cif.parquet"
        persist_awards_cif([c], path=path)
        [loaded] = load_awards_cif(path=path)
        assert loaded.cluster_id == c.cluster_id
        assert loaded.full_name_key == c.full_name_key
        assert loaded.orcid_status == c.orcid_status
        assert loaded.reliability_tier == "1a"
        assert loaded.resolution_status == "RESOLVED"
        assert loaded.n_grants == c.n_grants
        assert loaded.excluded is False
        assert loaded.excluded_reason is None

    def test_round_trip_preserves_list_fields(self, tmp_path):
        c = self._full_cluster()
        path = tmp_path / "awards_cif.parquet"
        persist_awards_cif([c], path=path)
        [loaded] = load_awards_cif(path=path)
        assert loaded.full_names == c.full_names
        assert loaded.orcids == c.orcids
        assert loaded.gap_candidates == ["B", "C"]
        assert loaded.oax_candidates == [
            "https://openalex.org/A100", "https://openalex.org/A200",
        ]

    def test_round_trip_preserves_struct_list_fields(self, tmp_path):
        c = self._full_cluster()
        path = tmp_path / "awards_cif.parquet"
        persist_awards_cif([c], path=path)
        [loaded] = load_awards_cif(path=path)
        assert loaded.for2020_codes == [
            {"code": "3705", "name": "Geology", "is_primary": True, "confidence": 1.0},
        ]
        assert loaded.orcid_for_codes == [{"code": "3705", "name": "Geology", "count": 4}]
        assert len(loaded.provenance) == 1
        assert loaded.provenance[0]["event"] == "splink_cluster"
        assert loaded.provenance[0]["threshold"] == 0.9

    def test_items_and_oeuvre_not_persisted(self, tmp_path):
        # Deliberate -- items (raw per-grant detail) is reconstructable from
        # load_award_cif_items(); oeuvre is populated by later oeuvre_build.py stages, empty at
        # this point in the chain.
        c = self._full_cluster()
        path = tmp_path / "awards_cif.parquet"
        persist_awards_cif([c], path=path)
        [loaded] = load_awards_cif(path=path)
        assert loaded.items == []
        assert loaded.oeuvre == []

    def test_multiple_clusters_round_trip(self, tmp_path):
        c1 = _build_awards_cif("A", [_item("G1_A")])
        c2 = _build_awards_cif("B", [_item("G1_B", full_name="Jane Doe")])
        path = tmp_path / "awards_cif.parquet"
        persist_awards_cif([c1, c2], path=path)
        loaded = load_awards_cif(path=path)
        assert {c.cluster_id for c in loaded} == {"A", "B"}

    def test_heterogeneous_provenance_shapes_round_trip(self, tmp_path):
        # Regression test: a real full-population run crashed here -- pyarrow's list-of-struct
        # inference can't handle provenance events with different kwarg keys across clusters
        # (record_event()'s whole point is a free-form **details per event type), raising
        # "cannot mix list and non-list, non-null values". Fixed by JSON-encoding provenance as a
        # string column instead of a native struct-list column (matches arc_persons.parquet's own
        # cluster_history column, which is VARCHAR for the same reason).
        c1 = _build_awards_cif("A", [_item("G1_A")])
        c1.record_event("splink_cluster", threshold=0.9)
        c2 = _build_awards_cif("B", [_item("G1_B", full_name="Jane Doe")])
        c2.record_event("oax_split_record_dedup", removed=["https://openalex.org/A1", "https://openalex.org/A2"])
        c3 = _build_awards_cif("C", [_item("G1_C", full_name="Alex Lee")])
        # no provenance at all -- empty list, a third distinct shape
        path = tmp_path / "awards_cif.parquet"
        persist_awards_cif([c1, c2, c3], path=path)
        loaded = {c.cluster_id: c for c in load_awards_cif(path=path)}
        assert loaded["A"].provenance == [{"event": "splink_cluster", "threshold": 0.9}]
        assert loaded["B"].provenance == [{
            "event": "oax_split_record_dedup",
            "removed": ["https://openalex.org/A1", "https://openalex.org/A2"],
        }]
        assert loaded["C"].provenance == []


# ---------------------------------------------------------------------------
# _load_hep_crosswalk / _load_institution_hep_crosswalk -- real admin_orgs.csv, not synthetic
# fixtures (same convention as load_award_cif_items()/cluster_items() etc. per this file's own
# module docstring: functions depending on real persisted data are validated against real data,
# not mocked). Asserts on well-known real Australian universities, stable/durable facts.
# ---------------------------------------------------------------------------

class TestHepCrosswalks:
    def test_name_crosswalk_resolves_known_university(self):
        crosswalk = _load_hep_crosswalk()
        assert crosswalk.get("The Australian National University") == "ANU"

    def test_name_crosswalk_resolves_previously_broken_aliases(self):
        # Regression test (2026-08-16): these 3 aliases were found with HEP='y' but blank
        # institution_id/hep_code in admin_orgs.csv, silently resolving to "no HEP" -- fixed
        # directly in the CSV, and the crosswalk itself resolves via the canonical
        # organisationName group defensively regardless (see _load_hep_crosswalk()'s docstring).
        crosswalk = _load_hep_crosswalk()
        assert crosswalk.get("University of Western Sydney") == "WSU"
        assert crosswalk.get("The University of Western Sydney") == "WSU"
        assert crosswalk.get("Northern Territory University") == "CDU"
        assert crosswalk.get("The Flinders University of South Australia") == "FLI"

    def test_name_crosswalk_no_entry_for_non_hep_org(self):
        crosswalk = _load_hep_crosswalk()
        assert "AINSE Limited" not in crosswalk

    def test_institution_crosswalk_resolves_known_university(self):
        crosswalk = _load_institution_hep_crosswalk()
        # Western Sydney University's OpenAlex institution_idx -> WSU
        assert crosswalk.get(63525965) == "WSU"

    def test_institution_crosswalk_only_covers_real_heps(self):
        crosswalk = _load_institution_hep_crosswalk()
        # ~42 real Australian HEPs have a resolvable institution_id, not all 114 admin_orgs rows
        assert 30 <= len(crosswalk) <= 60
