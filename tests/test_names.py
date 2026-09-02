"""
Tests for src/utils/names.py -- name tokenisation/parsing helpers. Diacritic-specific tests
(strip_diacriticals, diacritic_variants, the corpus-grounded equivalence table) moved to
tests/test_name_diacritic_variants.py (2026-08-25), matching the source module split.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.utils.names import (
    norm_alpha, strip_parens, strip_postnominals, tokens, parse_given,
    HumanNameParser, ParsedName,
)


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


class TestParseGivenIsPureDelegation:
    """parse_given() is now a thin delegation to HumanNameParser.parse_given_legacy() -- these
    confirm the historical, already-relied-upon 5-tuple shape/behavior is unchanged."""

    def test_postnominal_stripped(self):
        assert parse_given("Anthony Thomas AC FAA") == ("anthony", None, "anthony", "a", None)

    def test_first_and_middle(self):
        assert parse_given("David John Smith") == ("david", "john", "david john", "d", "j")

    def test_empty(self):
        assert parse_given("") == (None, None, None, None, None)


class TestHumanNameParser:
    """The real, sole implementation of this project's name-parsing chain -- see ParsedName's
    own docstring for the ASCII-reduced-vs-non-ASCII-raw design."""

    def setup_method(self):
        self.p = HumanNameParser()

    def test_ascii_bridges_real_spelling_difference(self):
        # ARC recorded "Gruetzner", OpenAlex recorded "Grützner" -- genuinely different letters,
        # not just different encodings; the ASCII-reduced path must generate both spellings so
        # the two sides become comparable.
        r = self.p.parse("Frank Grützner")
        assert "gruetzner" in r.family_names
        assert "grutzner" in r.family_names

    def test_non_latin_ascii_path_empties_but_raw_path_survives(self):
        # A name with no Latin-script form at all: the ASCII-reduced path (real, necessary
        # spelling-bridging value for Latin names) has nothing to work with and correctly
        # produces nothing -- but the non-ASCII raw key must NOT also come back empty, or this
        # person becomes structurally unmatchable by name at all.
        r = self.p.parse("Иван Иванов")
        assert r.given_tokens == ()
        assert r.family_names == ()
        assert r.full_name_key is None
        assert r.full_name_key_raw == "иван_иванов"

    def test_last_name_only_fallback(self):
        # No given name at all -- HumanName recognizes "Dr." as a title prefix (consuming what
        # would otherwise be the "first" slot), leaving only a surname. The family name's own
        # first letter is used as a stand-in given-name token, matching
        # awards_cif.py::_name_forms()'s existing, tested convention exactly.
        r = self.p.parse("Dr. Smith")
        assert r.given_tokens == ("s",)
        assert r.family_name_main == "smith"

    def test_postnominal_and_contamination_combined(self):
        # Postnominal stripping + soft-hyphen removal + zero-width-space stripping all working
        # together through the full class, not just the lower-level primitive in isolation.
        r = self.p.parse("Glenn Summ­ary​hayes OL OAM")
        assert r.given_tokens[0] == "glenn"
        assert r.family_name_main == "summaryhayes"

    def test_parsed_name_fields_are_tuples_not_sets(self):
        # This project already found and fixed a real non-determinism bug from set()-based
        # given-name dedup (PYTHONHASHSEED-dependent iteration order flipping which token wins a
        # length tie between reruns) -- every "set-like" ParsedName field must be a tuple.
        r = self.p.parse("Xiao Dong Chen")
        assert isinstance(r.given_tokens, tuple)
        assert isinstance(r.family_names, tuple)
        assert isinstance(r.given_tokens_raw, tuple)

    def test_deterministic_across_repeated_calls(self):
        # Same real-world case that caused the set()-based bug (Xiao/Dong length tie) -- confirm
        # repeated parses of the identical input always pick the same first_name_canonical.
        results = {self.p.parse("Xiao Dong Chen").first_name_canonical for _ in range(20)}
        assert len(results) == 1

    def test_canonicalize_exposed_standalone(self):
        assert self.p.canonicalize("Smith–Jones") == "Smith-Jones"

    def test_diacritic_variants_exposed_standalone(self):
        assert self.p.diacritic_variants("Müller") == ("muller", "mueller")
