"""
Tests for src/utils/for_resolve.py — the research_classification-backed replacement for
src/utils/lookup_for_topic.py's hand-built FOR/OAX concordance.

Covers:
  - upgrade_for_code / upgrade_for_name: same contract as the old ForTopicLookup-based
    wrappers (2008→2020 upgrade, 2020 passthrough, tolerant None on unmappable input)
  - oax_subfield_name: the new one-hop FOR→OAX_SUBFIELD replacement for the old two-hop
    upgrade_for_code → group_to_subfield chain
  - oax_to_for2020: the new reverse-direction (OAX→FOR2020) helper, unused by the pipeline
    today but added for future ECR/DECRA analysis work
  - resolve()'s LookupError/ValueError/UserWarning contract is swallowed, not leaked
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.for_resolve import (
    oax_field_name,
    oax_subfield_name,
    oax_to_for2020,
    oax_to_for_name,
    resolve_arc_for_entry,
    upgrade_for_code,
    upgrade_for_name,
)


# ---------------------------------------------------------------------------
# upgrade_for_code — 2008 → 2020, 2020 passthrough, tolerant None
# ---------------------------------------------------------------------------

class TestUpgradeForCode:
    def test_2008_converts(self):
        assert upgrade_for_code("0101") == "4904"   # Pure Mathematics

    def test_2008_psychology(self):
        assert upgrade_for_code("1701") == "5201"   # Psychology → Applied psych

    def test_2020_passthrough(self):
        assert upgrade_for_code("4605") == "4605"   # Data management

    def test_2020_passthrough_large(self):
        assert upgrade_for_code("5201") == "5201"

    def test_none_returns_none(self):
        assert upgrade_for_code(None) is None

    def test_empty_returns_none(self):
        assert upgrade_for_code("") is None

    def test_unmappable_returns_none(self):
        assert upgrade_for_code("9999") is None

    def test_non_digit_returns_none(self):
        assert upgrade_for_code("abcd") is None

    def test_wrong_length_returns_none(self):
        assert upgrade_for_code("101") is None


# ---------------------------------------------------------------------------
# upgrade_for_name — 2008 gets official 2020 label, 2020 keeps original name
# ---------------------------------------------------------------------------

class TestUpgradeForName:
    def test_2008_gets_2020_name(self):
        result = upgrade_for_name("0101", "Pure Mathematics")
        assert result == "Pure mathematics"   # official ANZSRC 2020 capitalisation

    def test_2008_psychology_name(self):
        result = upgrade_for_name("1701", "Psychology")
        assert result == "Applied and developmental psychology"

    def test_2020_keeps_original_name(self):
        result = upgrade_for_name("4605", "Data Management and Data Science")
        assert result == "Data Management and Data Science"

    def test_none_code_keeps_name(self):
        assert upgrade_for_name(None, "Some Field") == "Some Field"

    def test_empty_code_keeps_name(self):
        assert upgrade_for_name("", "Some Field") == "Some Field"

    def test_unmappable_keeps_name(self):
        assert upgrade_for_name("9999", "Unknown") == "Unknown"

    def test_none_name_stays_none(self):
        assert upgrade_for_name("4605", None) is None


# ---------------------------------------------------------------------------
# oax_subfield_name — one-hop FOR2008-or-FOR2020 → OAX_SUBFIELD label
# ---------------------------------------------------------------------------

class TestOaxSubfieldName:
    def test_analytical_chemistry(self):
        assert oax_subfield_name("3401") == "Analytical Chemistry"

    def test_applied_mathematics(self):
        assert oax_subfield_name("4901") == "Applied Mathematics"

    def test_2008_code_resolves_directly(self):
        # 2008 "0101" (Pure Mathematics) -> same subfield as its 2020 equivalent "4904",
        # in one resolve() call rather than the old two-hop upgrade-then-lookup chain.
        assert oax_subfield_name("0101") == "Algebra and Number Theory"

    def test_unmappable_returns_none(self):
        assert oax_subfield_name("9999") is None

    def test_none_returns_none(self):
        assert oax_subfield_name(None) is None


# ---------------------------------------------------------------------------
# oax_field_name — one level coarser than oax_subfield_name(), used for cross-division
# coherence checks (2026-08-12) where ANZSRC's own division boundaries split disciplines
# OpenAlex's own field taxonomy groups together.
# ---------------------------------------------------------------------------

class TestOaxFieldName:
    def test_political_science_and_legal_systems_share_a_field(self):
        # ANZSRC divisions 44 (Human Society) and 48 (Law and Legal Studies) are
        # administratively distinct, but both resolve to OAX field "Social Sciences".
        assert oax_field_name("4408") == "Social Sciences"
        assert oax_field_name("4805") == "Social Sciences"

    def test_ecology_and_forestry_share_a_field(self):
        assert oax_field_name("3103") == "Agricultural and Biological Sciences"
        assert oax_field_name("3007") == "Agricultural and Biological Sciences"

    def test_unmappable_returns_none(self):
        assert oax_field_name("9999") is None

    def test_none_returns_none(self):
        assert oax_field_name(None) is None


# ---------------------------------------------------------------------------
# oax_to_for2020 — reverse direction, added for future ECR/DECRA analysis use
# ---------------------------------------------------------------------------

class TestOaxToFor2020:
    def test_topic_reaches_field_precision(self):
        # OAX topic "Natural Language Processing Techniques" -> FOR2020 field (6-digit)
        assert oax_to_for2020("10181") == "460208"

    def test_none_returns_none(self):
        assert oax_to_for2020(None) is None

    def test_unmappable_returns_none(self):
        assert oax_to_for2020("not-a-real-oax-code") is None


# ---------------------------------------------------------------------------
# oax_to_for_name — same reverse direction as oax_to_for2020, .label not .code --
# used by oeuvre_build.py::apply_subfield_filter() to derive a candidate work's own
# FOR-division via for_divisions.csv's for_name-keyed lookup.
# ---------------------------------------------------------------------------

class TestOaxToForName:
    def test_geophysics(self):
        assert oax_to_for_name("Geophysics") == "Geophysics"

    def test_geochemistry_and_petrology_maps_to_geochemistry_group(self):
        # "Geochemistry and Petrology" is one OAX subfield but resolves to the
        # "Geochemistry" FOR2020 group (Petrology has no separate FOR2020 group of its own).
        assert oax_to_for_name("Geochemistry and Petrology") == "Geochemistry"

    def test_none_returns_none(self):
        assert oax_to_for_name(None) is None

    def test_unmappable_returns_none(self):
        assert oax_to_for_name("not-a-real-oax-code") is None


# ---------------------------------------------------------------------------
# resolve_arc_for_entry — ARC's raw field-of-research entry (raw_json.csv) -> FOR2020.
# ARC's own `type` labels (RFCD98/FOR08/FOR20) are not the scheme names Resolver expects
# (FOR1998/FOR2008/FOR2020) -- confirmed by resolving real RFCD98/FOR08 codes from the actual
# ARC corpus and checking the results are sensible, not assumed from the label alone.
# ---------------------------------------------------------------------------

class TestResolveArcForEntry:
    def test_for20_passthrough(self):
        code, name, confidence = resolve_arc_for_entry("3705", "FOR20")
        assert code == "3705"
        assert name == "Geology"
        assert confidence == 1.0

    def test_for08_bridges_to_for2020(self):
        # real ARC entry: FOR08 "100501 Antennas and Propagation" -> FOR2020 "400601"
        code, name, confidence = resolve_arc_for_entry("100501", "FOR08")
        assert code == "400601"
        assert confidence == 1.0

    def test_rfcd98_bridges_to_for2020(self):
        # real ARC entry: RFCD98 "270299 Genetics Not Elsewhere Classified"
        result = resolve_arc_for_entry("270299", "RFCD98")
        assert result is not None
        code, name, confidence = result
        assert code == "310599"
        assert confidence > 0

    def test_leading_zero_preserved(self):
        # FOR08 division 02 (Physical Sciences) -- confirmed present in the real ARC corpus
        # with the leading zero intact; must not be silently dropped by int coercion anywhere
        # in the resolution path.
        code, name, confidence = resolve_arc_for_entry("0204", "FOR08")
        assert name == "Condensed matter physics"

    def test_unrecognised_type_returns_none(self):
        assert resolve_arc_for_entry("3705", "SOME_OTHER_SCHEME") is None

    def test_none_code_returns_none(self):
        assert resolve_arc_for_entry(None, "FOR20") is None

    def test_none_type_returns_none(self):
        assert resolve_arc_for_entry("3705", None) is None

    def test_unmappable_code_returns_none(self):
        assert resolve_arc_for_entry("9999", "FOR20") is None
