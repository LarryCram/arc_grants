"""
Tests for src/acif/build.py's pure, non-I/O logic: enrich_items(), seed().

load_items(), load_grant_for2020_codes(), load_grant_org_facts(), _admin_orgs_canonical(), and
build_stage_zero() all require real DuckDB/parquet/CSV inputs -- not unit-tested here, same
convention tests/test_awards_cif.py already uses for load_award_cif_items() and friends. Those
are validated by direct inspection against real data instead (see CLAUDE.md's 2026-09-28 session
entries for the specific cases checked: population count, zero-missing-facts, the admin_orgs
DE120101452 case).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.acif.models import AwardCIFItem, AwardsCIF
from src.acif.build import (
    enrich_items,
    seed,
    load_items,
    load_grant_for2020_codes,
    load_grant_org_facts,
    _admin_orgs_canonical,
    build_stage_zero,
    load_manual_orcid_corrections,
    apply_manual_orcid_corrections,
    compute_orcids,
    _family_names_compatible,
    merge_by_orcid,
    render_orcid_mismatch_report,
    UnionFind,
    DATA_PERSISTED,
)
from src.utils.names import HumanNameParser


def _item(unique_id, grant_code=None, admin_org=None, admin_orgs=None, orcid=None,
          full_name="John Smith") -> AwardCIFItem:
    return AwardCIFItem(
        unique_id=unique_id,
        grant_code=grant_code or unique_id.split("_")[0],
        first_name="John",
        family_name="Smith",
        role_code="CI",
        orcid=orcid,
        admin_org=admin_org,
        admin_orgs=admin_orgs or [],
        institution_oax_id=None,
        funding_commence_year=None,
        for_name=None,
        for_code=None,
        full_name=full_name,
    )


class TestSeed:
    def test_one_singleton_per_item(self):
        items = [_item("DP01_JohnSmith"), _item("DP02_JaneDoe")]
        acifs = seed(items)
        assert len(acifs) == 2
        assert {a.cluster_id for a in acifs} == {"DP01_JohnSmith", "DP02_JaneDoe"}
        for acif, item in zip(acifs, items):
            assert acif.items == [item]
            assert acif.cycle_stages == [1]

    def test_empty_input_gives_empty_output(self):
        assert seed([]) == []


class TestEnrichItems:
    def test_grant_mates_get_the_identical_attached_value(self):
        # two items on the SAME grant -- both must receive the identical grant-level facts,
        # not a per-item recomputation (the whole point of computing this once per grant_code).
        items = [
            _item("DP01_JohnSmith", grant_code="DP01"),
            _item("DP01_JaneDoe", grant_code="DP01"),
        ]
        for2020 = {"DP01": [{"code": "4613", "name": "Theory of computation",
                              "is_primary": True, "confidence": 1.0}]}
        org_facts = {"DP01": {"hep_codes": ["UNSW"], "inst_ids": ["https://openalex.org/I1"]}}

        out = enrich_items(items, for2020, org_facts)

        assert len(out) == 2
        for it in out:
            assert it.for2020_codes == for2020["DP01"]
            assert it.hep_codes == ["UNSW"]
            assert it.inst_ids == ["https://openalex.org/I1"]

    def test_missing_grant_defaults_to_empty_not_keyerror(self):
        items = [_item("DP99_NoFacts", grant_code="DP99")]
        out = enrich_items(items, for2020={}, org_facts={})
        assert out[0].for2020_codes == []
        assert out[0].hep_codes == []
        assert out[0].inst_ids == []

    def test_does_not_mutate_input_items(self):
        # AwardCIFItem is frozen -- enrich_items() must return new objects, not attempt to
        # patch the ones it was given (which would raise, since the dataclass is frozen).
        items = [_item("DP01_JohnSmith", grant_code="DP01")]
        out = enrich_items(items, {"DP01": [{"code": "x"}]}, {})
        assert items[0].for2020_codes == []
        assert out[0].for2020_codes == [{"code": "x"}]


# ---------------------------------------------------------------------------
# Real-data integration checks (2026-09-28) -- these hit the actual PROCESSED_DATA parquet/CSV
# files, same convention as tests/test_awards_cif.py's own real-data tests
# (test_institution_crosswalk_only_covers_real_heps etc.): loose range assertions and
# known-value spot-checks, never brittle exact counts that go stale as ARC issues new grants.
# Deliberately lightweight -- exhaustive correctness (population scope, zero-missing-facts,
# the admin_orgs DE120101452 case) was already verified by direct inspection this session; see
# CLAUDE.md's 2026-09-28 entries. These exist so a future code change gets caught by `pytest`,
# not just by remembering to re-run the same ad hoc script by hand.
# ---------------------------------------------------------------------------

class TestLoadItemsRealData:
    def test_scale_and_scope(self):
        items = load_items()
        # loose lower/upper bound -- the real population grows as ARC issues new grants, so an
        # exact count would need updating every rerun; this just guards against "returns nothing"
        # or "scope filter stopped working" (e.g. every KEEP_ROLES/KEEP_SCHEMES row let through).
        assert 50_000 <= len(items) <= 100_000
        for it in items[:200]:
            assert it.unique_id and it.grant_code
            assert it.grant_code[:2] in KEEP_SCHEMES
            assert it.role_code in KEEP_ROLES

    def test_known_item(self):
        items = {it.unique_id: it for it in load_items()}
        it = items["DE120100016_KhoaNguyen"]
        assert it.admin_org == "University of South Australia"
        assert it.for_code == "4613"
        assert it.is_fellowship is True

    def test_admin_orgs_retains_both_when_they_differ(self):
        # the DE120101452 case found live this session (Sydney now, ANU at announcement --
        # confirmed directly by the grant's own former CI).
        items = {it.unique_id: it for it in load_items()}
        it = items["DE120101452_MShumiAkhtar"]
        assert it.admin_org == "The University of Sydney"
        assert set(it.admin_orgs) == {"The Australian National University", "The University of Sydney"}


class TestLoadGrantFor2020CodesRealData:
    def test_shape_and_known_grant(self):
        codes = load_grant_for2020_codes()
        assert len(codes) > 10_000
        entries = codes["DE120100016"]
        assert any(e["code"] == "4613" and e["is_primary"] for e in entries)
        for e in entries:
            assert {"code", "name", "is_primary", "confidence"} <= e.keys()


class TestLoadGrantOrgFactsRealData:
    def test_shape_and_known_multi_hep_grant(self):
        facts = load_grant_org_facts()
        assert len(facts) > 10_000
        f = facts["DE120101452"]
        assert set(f["hep_codes"]) == {"ANU", "USY"}
        assert len(f["inst_ids"]) == 2


class TestAdminOrgsCanonicalRealData:
    def test_known_hep_alias_resolves(self):
        hep_aliases, alias_to_hep, alias_to_inst = _admin_orgs_canonical()
        assert "The University of Sydney" in hep_aliases
        assert alias_to_hep["The University of Sydney"] == "USY"
        assert alias_to_inst["The University of Sydney"].startswith("https://openalex.org/I")

    def test_non_hep_org_absent(self):
        hep_aliases, _, _ = _admin_orgs_canonical()
        assert "AINSE Limited" not in hep_aliases


class TestBuildStageZeroRealData:
    def test_every_acif_is_a_singleton(self):
        acifs = build_stage_zero()
        assert 50_000 <= len(acifs) <= 100_000
        for acif in acifs[:200]:
            assert len(acif.items) == 1
            assert acif.cluster_id == acif.items[0].unique_id
            assert acif.cycle_stages == [1]


class TestRenameCollapseRealData:
    """award_rename_map.parquet consumption in load_items() -- collapsing a confirmed
    announcement/current rename into one item before seeding, per the 2026-09-28/29 design
    decision (no full_name_raws; a real item-level full_name_keys unioning both name forms)."""

    def test_akhtar_case_collapsed_with_unioned_keys(self):
        items = {it.unique_id: it for it in load_items()}
        assert "DE120101452_MahmudaAkhtar" not in items  # the announcement form
        it = items["DE120101452_MShumiAkhtar"]  # the surviving current form
        assert it.full_name == "M. Shumi Akhtar"
        assert set(it.full_name_keys) >= {"mahmuda_akhtar", "shumi_akhtar"}

    def test_no_announcement_only_rows_leak_through(self):
        # every announcement_unique_id present in this population must have been absorbed,
        # never left standing as its own separate item.
        import pandas as pd
        from config.settings import PROCESSED_DATA
        items = load_items()
        all_ids = {it.unique_id for it in items}
        rename_df = pd.read_parquet(PROCESSED_DATA / "award_rename_map.parquet")
        leaked = set(rename_df["announcement_unique_id"]) & all_ids
        assert leaked == set()

    def test_every_surviving_current_id_has_keys_populated(self):
        import pandas as pd
        from config.settings import PROCESSED_DATA
        items = {it.unique_id: it for it in load_items()}
        rename_df = pd.read_parquet(PROCESSED_DATA / "award_rename_map.parquet")
        for current_id in set(rename_df["current_unique_id"]) & set(items):
            assert items[current_id].full_name_keys, f"{current_id} missing full_name_keys"


def _acif(cluster_id, items) -> AwardsCIF:
    return AwardsCIF(cluster_id=cluster_id, items=items, cycle_stages=[1])


class TestManualOrcidCorrections:
    def test_apply_replaces_matching_wrong_orcid(self):
        items = [_item("DP01_JohnSmith", orcid="0000-0000-0000-0001")]
        corrections = {"DP01_JohnSmith": ("0000-0000-0000-0001", "0000-0000-0000-0002")}
        out = apply_manual_orcid_corrections(items, corrections)
        assert out[0].orcid == "0000-0000-0000-0002"

    def test_apply_nulls_when_correct_orcid_unknown(self):
        items = [_item("DP01_JohnSmith", orcid="0000-0000-0000-0001")]
        corrections = {"DP01_JohnSmith": ("0000-0000-0000-0001", None)}
        out = apply_manual_orcid_corrections(items, corrections)
        assert out[0].orcid is None

    def test_apply_ignores_non_matching_current_orcid(self):
        # the item's CURRENT orcid must match wrong_orcid exactly -- if it's already been
        # corrected some other way (or never had the wrong value), leave it alone.
        items = [_item("DP01_JohnSmith", orcid="0000-0000-0000-9999")]
        corrections = {"DP01_JohnSmith": ("0000-0000-0000-0001", "0000-0000-0000-0002")}
        out = apply_manual_orcid_corrections(items, corrections)
        assert out[0].orcid == "0000-0000-0000-9999"

    def test_apply_does_not_mutate_input(self):
        items = [_item("DP01_JohnSmith", orcid="0000-0000-0000-0001")]
        apply_manual_orcid_corrections(
            items, {"DP01_JohnSmith": ("0000-0000-0000-0001", "0000-0000-0000-0002")},
        )
        assert items[0].orcid == "0000-0000-0000-0001"

    def test_load_real_file_shape(self):
        # real-data check: the actual file, not a fixture -- confirms the loader reads the real
        # data_persisted/manual_orcid_corrections.csv correctly.
        corrections = load_manual_orcid_corrections()
        assert "DP150102405_WeiLiu" in corrections
        wrong, correct = corrections["DP150102405_WeiLiu"]
        assert wrong == "0000-0002-7409-0948"
        assert correct is None


class TestComputeOrcids:
    def test_no_orcid(self):
        acif = _acif("DP01_JohnSmith", [_item("DP01_JohnSmith", orcid=None)])
        compute_orcids(acif)
        assert acif.orcid_status == "NO_ORCID"
        assert acif.orcids == []

    def test_has_orcid(self):
        acif = _acif("DP01_JohnSmith", [_item("DP01_JohnSmith", orcid="0000-0000-0000-0001")])
        compute_orcids(acif)
        assert acif.orcid_status == "HAS_ORCID"
        assert acif.orcids == ["0000-0000-0000-0001"]

    def test_multi_orcid(self):
        acif = _acif("DP01_JohnSmith", [
            _item("DP01_JohnSmith", orcid="0000-0000-0000-0001"),
            _item("DP02_JohnSmith", orcid="0000-0000-0000-0002"),
        ])
        compute_orcids(acif)
        assert acif.orcid_status == "MULTI_ORCID"
        assert acif.orcids == ["0000-0000-0000-0001", "0000-0000-0000-0002"]

    def test_recomputed_not_cached(self):
        # calling it again after items change must reflect the new items, not the old result.
        acif = _acif("DP01_JohnSmith", [_item("DP01_JohnSmith", orcid="0000-0000-0000-0001")])
        compute_orcids(acif)
        acif.items.append(_item("DP02_JaneDoe", orcid="0000-0000-0000-0002"))
        compute_orcids(acif)
        assert acif.orcid_status == "MULTI_ORCID"


class TestFamilyNamesCompatible:
    def setup_method(self):
        self.parser = HumanNameParser()

    def test_identical_names_compatible(self):
        assert _family_names_compatible("John Smith", "John Smith", self.parser)

    def test_diacritic_variant_compatible(self):
        # real case from this session: Goetz/Götz already overlap via ParsedName's own
        # diacritic-widened family_names, no extra handling needed here.
        assert _family_names_compatible("Hans Goetz", "Hans Götz", self.parser)

    def test_postnominal_compatible(self):
        assert _family_names_compatible("Jane Elliott", "Jane Elliott AM", self.parser)

    def test_spacing_hyphenation_compatible(self):
        # real case from this session's own scan: "de Gier"/"deGier".
        assert _family_names_compatible("Piet de Gier", "Piet deGier", self.parser)

    def test_genuinely_different_families_incompatible(self):
        # real confirmed conflict case: Wang/Duan.
        assert not _family_names_compatible("Chien Ming Wang", "Wenhui Duan", self.parser)

    def test_name_order_swap_not_treated_as_compatible(self):
        # real case from this project's history (Kotagiri Ramamohanarao / Ramamohanarao
        # Kotagiri) -- deliberately NOT caught here; needs human review, not a guess.
        assert not _family_names_compatible(
            "Kotagiri Ramamohanarao", "Ramamohanarao Kotagiri", self.parser,
        )


class TestMergeByOrcid:
    def test_compatible_pair_merges(self):
        acifs = [
            _acif("DP01_JohnSmith", [_item("DP01_JohnSmith", orcid="0000-0000-0000-0001")]),
            _acif("DP02_JohnSmith", [_item("DP02_JohnSmith", orcid="0000-0000-0000-0001")]),
        ]
        survivors, mismatches = merge_by_orcid(acifs, UnionFind())
        assert len(survivors) == 1
        assert mismatches == []
        assert survivors[0].cluster_id == "DP01_JohnSmith"  # min() of the two ids
        assert len(survivors[0].items) == 2
        assert survivors[0].orcid_status == "HAS_ORCID"

    def test_incompatible_pair_not_merged_and_reported(self):
        acifs = [
            _acif("DP01_ChienMingWang", [_item(
                "DP01_ChienMingWang", orcid="0000-0002-8147-7673", full_name="Chien Ming Wang",
            )]),
            _acif("DP02_WenhuiDuan", [_item(
                "DP02_WenhuiDuan", orcid="0000-0002-8147-7673", full_name="Wenhui Duan",
            )]),
        ]
        survivors, mismatches = merge_by_orcid(acifs, UnionFind())
        assert len(survivors) == 2  # neither merged
        assert len(mismatches) == 1
        assert mismatches[0]["orcid"] == "0000-0002-8147-7673"
        assert sorted(mismatches[0]["groups"]) == [
            ["DP01_ChienMingWang"], ["DP02_WenhuiDuan"],
        ]

    def test_no_orcid_no_merge(self):
        acifs = [
            _acif("DP01_JohnSmith", [_item("DP01_JohnSmith", orcid=None)]),
            _acif("DP02_JaneDoe", [_item("DP02_JaneDoe", orcid=None)]),
        ]
        survivors, mismatches = merge_by_orcid(acifs, UnionFind())
        assert len(survivors) == 2
        assert mismatches == []

    def test_shared_parent_map_used_across_calls(self):
        # the persistent parent-map design: passing the SAME UnionFind across two calls means
        # find() can resolve an id absorbed in the first call, from the second call onward.
        uf = UnionFind()
        acifs = [
            _acif("DP01_JohnSmith", [_item("DP01_JohnSmith", orcid="0000-0000-0000-0001")]),
            _acif("DP02_JohnSmith", [_item("DP02_JohnSmith", orcid="0000-0000-0000-0001")]),
        ]
        merge_by_orcid(acifs, uf)
        assert uf.find("DP02_JohnSmith") == "DP01_JohnSmith"

    def test_render_report_mentions_both_names(self):
        mismatches = [{
            "orcid": "0000-0002-8147-7673",
            "groups": [["DP01_ChienMingWang"], ["DP02_WenhuiDuan"]],
            "names": {"DP01_ChienMingWang": "Chien Ming Wang", "DP02_WenhuiDuan": "Wenhui Duan"},
        }]
        text = render_orcid_mismatch_report(mismatches)
        assert "Chien Ming Wang" in text
        assert "Wenhui Duan" in text


class TestManualOrcidCorrectionsPreventsWrongMerge:
    """Integration test: applying the REAL manual_orcid_corrections.csv before merge_by_orcid()
    must prevent the exact wrong merge this session found by hand (two different real "Wei Liu"s
    wrongly sharing one ORCID). Built as a real, automated test per direct instruction -- not
    re-verified by a throwaway script each time."""

    def test_wei_liu_case_not_merged_after_correction(self):
        wrong_orcid = "0000-0002-7409-0948"
        items = [
            _item("DP150102405_WeiLiu", orcid=wrong_orcid, full_name="Wei Liu"),
            _item("LP0218928_WeiLiu", orcid=wrong_orcid, full_name="Wei Liu"),
        ]
        corrections = load_manual_orcid_corrections()  # the real, current file
        corrected = apply_manual_orcid_corrections(items, corrections)
        assert corrected[0].orcid is None  # the Sydney record's wrong orcid is nulled
        assert corrected[1].orcid == wrong_orcid  # the genuine RMIT record is untouched

        acifs = [_acif(it.unique_id, [it]) for it in corrected]
        survivors, mismatches = merge_by_orcid(acifs, UnionFind())
        assert len(survivors) == 2  # NOT merged, unlike the uncorrected case
        assert mismatches == []  # no orcid shared between them anymore -- nothing to flag either
