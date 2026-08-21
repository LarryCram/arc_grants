"""
Tests for src/utils/cluster_checks.py's for2020_primary_subfields() -- uses real FOR2020 codes
(Resolver()-backed), same convention as tests/test_gap_candidates.py and
tests/test_oeuvre_build.py's TestApplySubfieldFilter, not placeholder strings.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.cluster_checks import (
    for2020_primary_fields, for2020_primary_subfields,
    division_mismatch_for2020, is_suspicious_for2020, ACCEPTABLE_DIVISION_PAIRS,
    RARE_NAME_TF, first_names_compatible,
)


def _for2020(code, name, is_primary=True, confidence=1.0):
    return {"code": code, "name": name, "is_primary": is_primary, "confidence": confidence}


class TestFor2020PrimarySubfields:
    def test_real_code_resolves_to_subfield(self):
        codes = [_for2020("3705", "Geology")]
        assert for2020_primary_subfields(codes) == {"Geology"}

    def test_finer_grained_than_field(self):
        # Same code resolves to a coarser field and a finer subfield -- the two functions
        # should genuinely differ in granularity, not just in name.
        codes = [_for2020("3705", "Geology")]
        fields = for2020_primary_fields(codes)
        subfields = for2020_primary_subfields(codes)
        assert fields == {"Earth and Planetary Sciences"}
        assert subfields == {"Geology"}
        assert fields != subfields

    def test_secondary_code_not_used(self):
        codes = [_for2020("3705", "Geology", is_primary=False)]
        assert for2020_primary_subfields(codes) == set()

    def test_empty_codes_returns_empty_set(self):
        assert for2020_primary_subfields([]) == set()

    def test_multiple_primary_codes_unioned(self):
        codes = [_for2020("3705", "Geology"), _for2020("4402", "Cultural studies")]
        result = for2020_primary_subfields(codes)
        assert "Geology" in result
        assert len(result) >= 1


# Real, Resolver()-verified FOR2020 group codes, one per division used below -- same convention
# as the rest of this file (no placeholder codes). Verified group->division mapping:
#   3005 Fisheries sciences (30), 3103 Ecology (31), 3403 Macromolecular and materials
#   chemistry (34), 4004 Chemical engineering (40), 4005 Civil engineering (40),
#   4104 Environmental management (41), 4609 Information systems (46).
_FISHERIES_30 = _for2020("3005", "Fisheries sciences")
_ECOLOGY_31 = _for2020("3103", "Ecology")
_CHEM_34 = _for2020("3403", "Macromolecular and materials chemistry")
_CHEMENG_40 = _for2020("4004", "Chemical engineering")
_CIVIL_40 = _for2020("4005", "Civil engineering")
_ENVIRO_41 = _for2020("4104", "Environmental management")
_INFOSYS_46 = _for2020("4609", "Information systems")


class TestDivisionMismatchFor2020:
    """(34,40) Chemical Sciences/Engineering is a real ACCEPTABLE_DIVISION_PAIRS entry
    (z=9.25, one of the empirically-strongest pairs -- see cluster_checks.py's module
    docstring). (40,46) Engineering/Information-Computing-Sciences is the division pair
    from the real DE230100180_WeiWang false-merge that motivated this whole check -- it did
    NOT clear the significance threshold, so it must still flag."""

    def test_single_division_no_mismatch(self):
        assert division_mismatch_for2020([_CHEMENG_40]) is False

    def test_no_primary_codes_no_mismatch(self):
        assert division_mismatch_for2020([]) is False
        assert division_mismatch_for2020([_for2020("4004", "Chemical engineering", is_primary=False)]) is False

    def test_whitelisted_pair_not_flagged(self):
        assert division_mismatch_for2020([_CHEM_34, _CHEMENG_40]) is False

    def test_wei_wang_pair_still_flagged(self):
        # (40, 46) -- the actual division pair from the confirmed DE230100180_WeiWang
        # false-merge. Must NOT be exempted -- this pair never cleared the empirical
        # significance threshold, and this check exists specifically to catch this case.
        assert division_mismatch_for2020([_CIVIL_40, _INFOSYS_46]) is True

    def test_unwhitelisted_pair_flagged(self):
        assert division_mismatch_for2020([_CIVIL_40, _INFOSYS_46]) is True

    def test_three_divisions_all_pairwise_whitelisted_not_flagged(self):
        # (30,31), (30,41), (31,41) are all individually whitelisted -- the real pattern
        # behind a prolific single agricultural/ecological/environmental researcher.
        assert division_mismatch_for2020([_FISHERIES_30, _ECOLOGY_31, _ENVIRO_41]) is False

    def test_four_divisions_not_all_pairwise_whitelisted_flagged(self):
        # Same 3 whitelisted divisions as above, plus Engineering (40) added -- (30,40),
        # (31,40), (40,41) are none of them whitelisted, so the whole cluster still flags.
        # This is the real Hans-Lambers-shaped residual false positive discussed with the
        # user: a genuine single prolific researcher can still be flagged if one grant's
        # division doesn't pairwise-whitelist with the rest -- documented as an accepted,
        # known limitation, not silently hidden.
        codes = [_FISHERIES_30, _ECOLOGY_31, _CHEMENG_40, _ENVIRO_41]
        assert division_mismatch_for2020(codes) is True

    def test_secondary_codes_now_counted(self):
        # Changed 2026-08-17: division_mismatch_for2020() moved from for2020_primary_fields()
        # to for2020_all_fields() -- every FOR2020 code ARC recorded is real evidence of a CI's
        # declared discipline mix, is_primary marks emphasis not exclusivity, so a secondary
        # code now counts toward the division set just like a primary one. (34,46) isn't
        # whitelisted, so this now correctly flags where it previously (wrongly) didn't.
        codes = [_CHEM_34, _for2020("4609", "Information systems", is_primary=False)]
        assert division_mismatch_for2020(codes) is True


class TestAcceptableDivisionPairs:
    def test_is_frozenset_of_frozensets(self):
        assert isinstance(ACCEPTABLE_DIVISION_PAIRS, frozenset)
        assert all(isinstance(p, frozenset) and len(p) == 2 for p in ACCEPTABLE_DIVISION_PAIRS)

    def test_chem_eng_pair_present(self):
        assert frozenset({"34", "40"}) in ACCEPTABLE_DIVISION_PAIRS

    def test_wei_wang_pair_absent(self):
        assert frozenset({"40", "46"}) not in ACCEPTABLE_DIVISION_PAIRS

    def test_indigenous_division_never_appears(self):
        # Regression guard: the first computation of this list accidentally included the
        # 107 Indigenous-focused AwardsCIF that set_aside_indigenous_research() marks
        # excluded=True but leaves in the returned list -- caught and fixed before this
        # constant was hard-wired. Division 45 must never appear in any pair.
        assert not any("45" in pair for pair in ACCEPTABLE_DIVISION_PAIRS)


class TestIsSuspiciousFor2020:
    """The 'any ORCID present' bypass this function used to have was removed 2026-08-16 --
    found wrong via DE230100180_WeiWang, where a single ORCID on 1 of 9 merged grant records
    fully exempted the whole cluster from review. is_suspicious_for2020() no longer takes an
    orcids argument at all -- ORCID presence plays no role in this check anymore."""

    def test_common_name_unwhitelisted_cross_division_is_suspicious(self):
        codes = [_CIVIL_40, _INFOSYS_46]
        tf_lookup = {"wei wang": 1.0}  # common name -- tf well above RARE_NAME_TF
        assert is_suspicious_for2020("wei wang", codes, tf_lookup) is True

    def test_rare_name_cross_division_not_suspicious(self):
        # A genuinely rare name is independent, strong evidence against a collision --
        # verified against the real Oscar Cacho case (tf ~7.2e-7), which has a division
        # pair (Economics/Sociology) not in ACCEPTABLE_DIVISION_PAIRS at all, yet is
        # correctly not flagged purely on name rarity.
        codes = [_CIVIL_40, _INFOSYS_46]
        tf_lookup = {"oscar cacho": RARE_NAME_TF / 10}
        assert is_suspicious_for2020("oscar cacho", codes, tf_lookup) is False

    def test_common_name_whitelisted_pair_not_suspicious(self):
        codes = [_CHEM_34, _CHEMENG_40]
        tf_lookup = {"common name": 1.0}
        assert is_suspicious_for2020("common name", codes, tf_lookup) is False

    def test_single_division_never_suspicious(self):
        tf_lookup = {"common name": 1.0}
        assert is_suspicious_for2020("common name", [_CHEMENG_40], tf_lookup) is False

    def test_missing_full_name_key_still_evaluates_division(self):
        codes = [_CIVIL_40, _INFOSYS_46]
        assert is_suspicious_for2020(None, codes, {}) is True

    def test_full_name_key_not_in_tf_lookup_defaults_common(self):
        # tf_lookup.get(key, 1.0) -- an unknown name defaults to "common" (1.0), not rare,
        # so the division check still applies rather than silently exempting unknown names.
        codes = [_CIVIL_40, _INFOSYS_46]
        assert is_suspicious_for2020("some unseen name", codes, {}) is True


class TestFirstNamesCompatible:
    def test_initial_matches_full_name_in_s(self):
        # T has only initial; S has matching full name -> initial matches first char
        assert first_names_compatible(["james", "j"], ["j"]) is True

    def test_initial_no_match_in_s(self):
        # T initial 'm'; S has 'james' (initial 'j') -> no match
        assert first_names_compatible(["james", "j"], ["m"]) is False

    def test_exact_full_name_match(self):
        assert first_names_compatible(["james", "j"], ["james", "j"]) is True

    def test_different_full_names_same_initial_compatible(self):
        # jennifer vs james share initial 'j' -> permissively compatible
        assert first_names_compatible(["james", "j"], ["jennifer", "j"]) is True

    def test_different_initial_incompatible(self):
        # maria vs james -- no overlap at any level
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
        # S has only 'j'; T has full name 'james' -> james[0] == 'j' in s_inits
        assert first_names_compatible(["j"], ["james", "j"]) is True

    def test_full_name_in_t_against_non_matching_bare_initial_in_s(self):
        assert first_names_compatible(["m"], ["james", "j"]) is False
