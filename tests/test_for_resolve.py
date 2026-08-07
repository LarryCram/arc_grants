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
    oax_subfield_name,
    oax_to_for2020,
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
