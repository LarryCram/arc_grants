"""
Lightweight real-data tests for src/00c_extract_propensities.py's five table builders.

All five hit real ARC data (grants_flat.parquet, raw_json.csv, grant_summaries.csv,
admin_orgs.csv) -- no pure/mockable logic to split out separately, unlike src/acif/build.py's
enrich_items(). Same convention as tests/test_awards_cif.py's own real-data tests: loose range
assertions and known-value spot-checks, never brittle exact counts that go stale as ARC issues
new grants. Deliberately lightweight -- exhaustive correctness (institution-name canonicalization,
HEP filtering, grant-vs-item-level counting, the announcement/current rename classification) was
verified by direct inspection when this module was built; see CLAUDE.md's 2026-09-19/28 entries.
"""
import importlib.util
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_spec = importlib.util.spec_from_file_location(
    "extract_propensities", Path(__file__).resolve().parents[1] / "src" / "00c_extract_propensities.py"
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


class TestForNameRarity:
    def test_shape_and_range(self):
        df = _mod.build_for_name_rarity()
        # 212 rows at last real build (2026-09-20) -- loose band around it, not an exact pin,
        # since ARC's own for_name vocabulary barely changes grant to grant.
        assert 150 <= len(df) <= 300
        assert set(df.columns) == {"signature_key", "count", "frequency"}
        assert (df["frequency"] > 0).all()
        assert (df["frequency"] <= 1).all()
        # frequencies are count/n_grants over ONE population -- they must sum to 1, not to
        # something per-row-inflated (the real bug this table was built to fix, ported to a
        # grant-level count instead of cluster_items()'s old per-item sig_counts).
        assert abs(df["frequency"].sum() - 1.0) < 1e-9

    def test_known_signature_present(self):
        df = _mod.build_for_name_rarity()
        # "applied developmental psychology" was the single largest signature at last real
        # build (1,197 grants, ~3.9%) -- a coarse sanity check that real data is flowing
        # through, not that the exact count never moves.
        row = df[df["signature_key"] == "applied|developmental|psychology"]
        assert len(row) == 1
        assert row.iloc[0]["frequency"] > 0.01


class TestForNamePairFreq:
    def test_shape_and_range(self):
        df = _mod.build_for_name_pair_freq()
        assert len(df) > 1000
        assert set(df.columns) == {"name_a", "name_b", "count", "frequency"}
        # canonical alphabetical ordering (a < b), never ARC's own primary/secondary flag.
        assert (df["name_a"] < df["name_b"]).all()
        assert abs(df["frequency"].sum() - 1.0) < 1e-9


class TestInstitutionRarity:
    def test_shape_and_known_institutions(self):
        df = _mod.build_institution_rarity()
        # 42 real Australian HEPs, same figure this project's own admin_orgs.csv tests assert.
        assert 30 <= len(df) <= 55
        assert set(df.columns) == {"institution_name", "count", "frequency"}
        names = set(df["institution_name"])
        assert "The University of Melbourne" in names
        assert "The University of Queensland" in names
        # a genuine non-HEP partner org (CSIRO division, botanic garden, etc.) must never
        # appear -- the whole point of this table's HEP filter.
        assert "AINSE Limited" not in names
        # NOT a sum-to-1 check here, deliberately: unlike for_name_rarity (one primary_for_name
        # per grant, a true partition), a single grant can list several institutions in
        # eligible_orgs, so institution_rarity is multi-label -- frequencies legitimately sum to
        # more than 1 (each multi-institution grant "votes" for more than one row).
        assert (df["frequency"] > 0).all() and (df["frequency"] <= 1).all()
        assert df["frequency"].sum() > 1.0  # confirms real multi-institution grants exist


class TestInstitutionPairFreq:
    def test_shape_and_range(self):
        df = _mod.build_institution_pair_freq()
        assert len(df) > 100
        assert set(df.columns) == {"institution_a", "institution_b", "count", "frequency"}
        assert (df["institution_a"] < df["institution_b"]).all()
        assert abs(df["frequency"].sum() - 1.0) < 1e-9


class TestAwardRenameMap:
    def test_shape_and_range(self):
        df = _mod.build_award_rename_map()
        # 1,106 at last real build (2026-09-29) -- loose band, not an exact pin. The original
        # 1,665 (2026-09-28) has since dropped through three real, confirmed fixes: scoping to
        # load_items()'s full population (KEEP_ROLES + HEP-admin_org, not KEEP_SCHEMES alone),
        # a self-pair guard (proper_key() finding a difference the raw regex-based unique_id
        # construction doesn't, e.g. hyphenation), and matching names on the FULL unfiltered
        # investigator lists rather than per-side KEEP_ROLES-filtered ones (independent role
        # filtering was conflating genuine role swaps between two different real people with a
        # same-person rename -- confirmed on LP100100367, Taylor/Gray).
        assert 800 <= len(df) <= 1400
        assert set(df.columns) == {
            "announcement_unique_id", "current_unique_id", "announcement_name", "current_name",
        }
        # every emitted id must be a real, existing investigators_raw.parquet id, not a
        # fabricated/regex-only construction -- this is the whole point of the table.
        real_ids = set(pd.read_parquet(_mod.PROCESSED_DATA / "investigators_raw.parquet")["unique_id"])
        assert set(df["announcement_unique_id"]) <= real_ids
        assert set(df["current_unique_id"]) <= real_ids

    def test_known_rename_case(self):
        # the DE120101452 case this table was built to catch -- confirmed by the grant's own
        # former CI, 2026-09-28.
        df = _mod.build_award_rename_map()
        row = df[df["announcement_unique_id"] == "DE120101452_MahmudaAkhtar"]
        assert len(row) == 1
        assert row.iloc[0]["current_unique_id"] == "DE120101452_MShumiAkhtar"

    def test_proper_parsing_excludes_case_only_differences(self):
        # regression guard for the 284-grant false-positive fix: a pure capitalization
        # difference (e.g. "KalantarZadeh" vs "Kalantarzadeh") must never appear as a rename --
        # it isn't a rename at all, it's the same name the regex-based comparison got wrong.
        df = _mod.build_award_rename_map()
        assert not df["announcement_unique_id"].str.contains("KalantarZadeh", case=False).any()

    def test_ambiguous_multi_person_grant_not_recorded(self):
        # LP230201137: one person dropped, two different people added -- genuinely ambiguous,
        # must never be recorded as a rename (that would misattribute McKinley's name to an
        # unrelated real person).
        df = _mod.build_award_rename_map()
        assert not df["announcement_unique_id"].str.startswith("LP230201137_").any()
        assert not df["current_unique_id"].str.startswith("LP230201137_").any()
