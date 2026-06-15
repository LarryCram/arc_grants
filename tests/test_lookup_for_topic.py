"""
Tests for src/utils/lookup_for_topic.py

Covers:
  - div_to_field / field_to_divs (bi-directional, round-trip)
  - group_to_subfield / subfield_to_groups (bi-directional, round-trip)
  - missing codes return None / []
  - forbidden cross-level lookups raise CrossLevelError
  - hierarchy constraint: every group's subfield is within its division's field
  - field_id derivable from first two digits of subfield_id
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.utils.lookup_for_topic import ForTopicLookup, CrossLevelError


@pytest.fixture(scope="module")
def lu():
    return ForTopicLookup()


@pytest.fixture(scope="module")
def lu2020():
    return ForTopicLookup(for_series="2020")


# ---------------------------------------------------------------------------
# Division ↔ OAX field
# ---------------------------------------------------------------------------

class TestDivToField:
    def test_mathematics_2020(self, lu2020):
        r = lu2020.div_to_field("49")
        assert r is not None
        assert r["oax_field"] == "Mathematics"
        assert r["oax_field_id"] == "26"
        assert r["oax_domain"] == "Physical Sciences"

    def test_physics_2020(self, lu2020):
        r = lu2020.div_to_field("51")
        assert r["oax_field"] == "Physics and Astronomy"
        assert r["oax_field_id"] == "31"

    def test_medicine_div32(self, lu2020):
        r = lu2020.div_to_field("32")
        assert r["oax_field"] == "Medicine"
        assert r["oax_field_id"] == "27"

    def test_social_sciences_education(self, lu2020):
        r = lu2020.div_to_field("39")
        assert r["oax_field"] == "Social Sciences"
        assert r["oax_field_id"] == "33"

    def test_agbio_div30(self, lu2020):
        r = lu2020.div_to_field("30")
        assert r["oax_field"] == "Agricultural and Biological Sciences"
        assert r["oax_field_id"] == "11"

    def test_agbio_div31(self, lu2020):
        r = lu2020.div_to_field("31")
        assert r["oax_field_id"] == "11"

    def test_missing_code(self, lu2020):
        assert lu2020.div_to_field("99") is None

    def test_2008_series(self, lu):
        r = lu.div_to_field("01")   # 2008 Mathematics
        assert r["for_series"] == "2008"
        assert r["oax_field"] == "Mathematics"

    def test_leading_zero_normalised(self, lu2020):
        assert lu2020.div_to_field("30") == lu2020.div_to_field("30")


class TestFieldToDivs:
    def test_agbio_has_two_2020_divs(self, lu2020):
        divs = lu2020.field_to_divs("11")
        codes = {r["div_code"] for r in divs}
        assert "30" in codes
        assert "31" in codes

    def test_mathematics_single_div(self, lu2020):
        divs = lu2020.field_to_divs("26")
        assert len(divs) == 1
        assert divs[0]["div_code"] == "49"

    def test_missing_field_id(self, lu2020):
        assert lu2020.field_to_divs("99") == []


class TestDivFieldRoundTrip:
    def test_forward_then_back(self, lu2020):
        for div_code in ["30", "34", "46", "49", "51", "52"]:
            field_row = lu2020.div_to_field(div_code)
            divs = lu2020.field_to_divs(field_row["oax_field_id"])
            codes = {r["div_code"] for r in divs}
            assert div_code in codes, f"div {div_code} missing after round-trip"


# ---------------------------------------------------------------------------
# Group ↔ OAX subfield
# ---------------------------------------------------------------------------

class TestGroupToSubfield:
    def test_analytical_chemistry(self, lu):
        r = lu.group_to_subfield("3401")
        assert r["oax_subfield_id"] == "1602"
        assert r["oax_subfield_name"] == "Analytical Chemistry"
        assert r["oax_field_id"] == "16"

    def test_applied_mathematics(self, lu):
        r = lu.group_to_subfield("4901")
        assert r["oax_subfield_id"] == "2604"
        assert r["oax_subfield_name"] == "Applied Mathematics"

    def test_ecology(self, lu):
        r = lu.group_to_subfield("3103")
        assert r["oax_subfield_id"] == "1105"

    def test_ai(self, lu):
        r = lu.group_to_subfield("4602")
        assert r["oax_subfield_id"] == "1702"
        assert r["oax_subfield_name"] == "Artificial Intelligence"

    def test_epidemiology(self, lu):
        r = lu.group_to_subfield("4202")
        assert r["oax_subfield_id"] == "2713"

    def test_statistics(self, lu):
        r = lu.group_to_subfield("4905")
        assert r["oax_subfield_id"] == "2613"
        assert r["oax_subfield_name"] == "Statistics and Probability"

    def test_missing_group(self, lu):
        assert lu.group_to_subfield("9999") is None


class TestSubfieldToGroups:
    def test_education_has_multiple_groups(self, lu):
        groups = lu.subfield_to_groups("3304")
        codes = {r["group_code"] for r in groups}
        # Div 39 groups 3901-3904 + 3999, and Div 45 aboriginal education groups
        assert "3901" in codes
        assert "4502" in codes  # Aboriginal and TSI education

    def test_law_has_eight_groups(self, lu):
        groups = lu.subfield_to_groups("3308")
        assert len(groups) == 8
        codes = {r["group_code"] for r in groups}
        assert "4801" in codes
        assert "4807" in codes

    def test_missing_subfield(self, lu):
        assert lu.subfield_to_groups("9999") == []


class TestGroupSubfieldRoundTrip:
    def test_forward_then_back(self, lu):
        for group_code in ["3401", "3703", "4602", "4901", "5104", "5201"]:
            sf_row = lu.group_to_subfield(group_code)
            groups = lu.subfield_to_groups(sf_row["oax_subfield_id"])
            codes = {r["group_code"] for r in groups}
            assert group_code in codes, f"group {group_code} missing after round-trip"


# ---------------------------------------------------------------------------
# Hierarchy constraint
# ---------------------------------------------------------------------------

class TestHierarchy:
    def test_subfield_field_matches_division_field(self, lu2020):
        """Every group's oax_field_id must equal its parent division's oax_field_id."""
        failures = []
        for group in lu2020.all_groups():
            div_row = lu2020.div_to_field(group["div_code"])
            assert div_row is not None, f"no division row for {group['div_code']}"
            if group["oax_field_id"] != div_row["oax_field_id"]:
                failures.append(
                    f"group {group['group_code']} subfield {group['oax_subfield_id']} "
                    f"is in OAX field {group['oax_field_id']} "
                    f"but div {group['div_code']} maps to field {div_row['oax_field_id']}"
                )
        assert not failures, "\n" + "\n".join(failures)

    def test_subfield_id_prefix_equals_field_id(self, lu):
        """oax_subfield_id[:2] == oax_field_id for every group row."""
        for group in lu.all_groups():
            assert group["oax_subfield_id"][:2] == group["oax_field_id"], (
                f"group {group['group_code']}: subfield {group['oax_subfield_id']} "
                f"prefix != field_id {group['oax_field_id']}"
            )

    def test_all_213_groups_present(self, lu):
        assert len(lu.all_groups()) == 213

    def test_all_2020_divisions_present(self, lu2020):
        assert len(lu2020.all_divisions()) == 23


# ---------------------------------------------------------------------------
# 2008 → 2020 code upgrade
# ---------------------------------------------------------------------------

class TestUpgradeForCode:
    def test_2020_code_passthrough(self, lu):
        assert lu.upgrade_for_code("4904") == "4904"   # Pure mathematics

    def test_2020_code_large(self, lu):
        assert lu.upgrade_for_code("5201") == "5201"   # Applied psychology

    def test_2008_3digit_padded(self, lu):
        # 2008 "0101" (Pure Mathematics) → 2020 "4904"
        result = lu.upgrade_for_code("0101")
        assert result == "4904"

    def test_2008_4digit(self, lu):
        # 2008 "1303" (Biochemistry) → some 2020 code
        result = lu.upgrade_for_code("1303")
        assert result is not None
        assert result >= "3000"

    def test_2008_psychology_1701(self, lu):
        result = lu.upgrade_for_code("1701")
        assert result == "5201"

    def test_unknown_2008_returns_none(self, lu):
        assert lu.upgrade_for_code("9999") is None

    def test_2008_chain_gives_valid_subfield(self, lu):
        # 2008 "0101" → 2020 "4904" → should find a subfield
        code20 = lu.upgrade_for_code("0101")
        sf = lu.group_to_subfield(code20)
        assert sf is not None
        assert sf["oax_subfield_name"] == "Algebra and Number Theory"


# ---------------------------------------------------------------------------
# Forbidden cross-level lookups
# ---------------------------------------------------------------------------

class TestCrossLevelForbidden:
    def test_div_to_subfield_raises(self, lu):
        with pytest.raises(CrossLevelError):
            lu.div_to_subfield("30")

    def test_group_to_field_raises(self, lu):
        with pytest.raises(CrossLevelError):
            lu.group_to_field("3001")

    def test_error_message_div_to_subfield(self, lu):
        with pytest.raises(CrossLevelError, match="div_to_subfield"):
            lu.div_to_subfield("49")

    def test_error_message_group_to_field(self, lu):
        with pytest.raises(CrossLevelError, match="group_to_field"):
            lu.group_to_field("4901")
