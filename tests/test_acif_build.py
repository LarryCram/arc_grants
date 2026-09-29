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
from src.acif.models import AwardCIFItem
from src.acif.build import (
    enrich_items,
    seed,
    load_items,
    load_grant_for2020_codes,
    load_grant_org_facts,
    _admin_orgs_canonical,
    build_stage_zero,
)


def _item(unique_id, grant_code=None, admin_org=None, admin_orgs=None) -> AwardCIFItem:
    return AwardCIFItem(
        unique_id=unique_id,
        grant_code=grant_code or unique_id.split("_")[0],
        first_name="John",
        family_name="Smith",
        role_code="CI",
        orcid=None,
        admin_org=admin_org,
        admin_orgs=admin_orgs or [],
        institution_oax_id=None,
        funding_commence_year=None,
        for_name=None,
        for_code=None,
        full_name="John Smith",
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
