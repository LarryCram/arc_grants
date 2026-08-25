"""
Tests for src/utils/names.py -- name tokenisation/parsing helpers. Diacritic-specific tests
(strip_diacriticals, diacritic_variants, the corpus-grounded equivalence table) moved to
tests/test_name_diacritic_variants.py (2026-08-25), matching the source module split.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.utils.names import norm_alpha, strip_parens, strip_postnominals, tokens


class TestStripPostnominals:
    def test_single(self):
        assert strip_postnominals("Finch AO") == "Finch"

    def test_double(self):
        assert strip_postnominals("Raston AO FAA") == "Raston"

    def test_oam(self):
        assert strip_postnominals("Heine OAM") == "Heine"

    def test_am(self):
        assert strip_postnominals("Blackmore AM") == "Blackmore"

    def test_ac(self):
        assert strip_postnominals("Thomas AC") == "Thomas"

    def test_no_postnominal(self):
        assert strip_postnominals("Smith") == "Smith"

    def test_compound_no_postnominal(self):
        assert strip_postnominals("Sen Gupta") == "Sen Gupta"

    def test_all_caps_name_untouched(self):
        # all-caps single-word family names are NOT post-nominals
        assert strip_postnominals("WANG") == "WANG"
        assert strip_postnominals("NGUYEN") == "NGUYEN"

    def test_empty(self):
        assert strip_postnominals("") == ""


class TestStripParens:
    def test_nee(self):
        assert strip_parens("Murphy (née Paton-Walsh)") == "Murphy"

    def test_alias(self):
        assert strip_parens("Pas (née Izgorodina)") == "Pas"

    def test_no_parens(self):
        assert strip_parens("Smith") == "Smith"

    def test_multiple_parens(self):
        assert strip_parens("Jones (Bob) (Jr)") == "Jones"


class TestNormAlpha:
    def test_basic(self):
        assert norm_alpha("Smith") == "smith"

    def test_hyphen_removed(self):
        assert norm_alpha("O'Brien") == "obrien"

    def test_diacritical_stripped(self):
        assert norm_alpha("Müller") == "muller"

    def test_compound(self):
        assert norm_alpha("Sen Gupta") == "sengupta"

    def test_paren_stripped(self):
        assert norm_alpha("Pas (née Izgorodina)") == "pas"


class TestTokens:
    def test_basic(self):
        assert tokens("David Smith") == ["david", "smith"]

    def test_initial(self):
        assert tokens("D. Smith") == ["d", "smith"]

    def test_diacritical(self):
        assert tokens("Müller") == ["muller"]

    def test_hyphenated(self):
        assert tokens("O'Brien") == ["o", "brien"]
