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
    norm_alpha, strip_parens, tokens, parse_given,
    HumanNameParser, ParsedName,
)


class TestPostnominalHandling:
    """Postnominal suffixes are handled entirely by HumanName's own native suffix_acronyms
    registration (names.py's _POSTNOMINAL_ACRONYMS) -- the old custom strip_postnominals()
    pre-stripping regex was removed 2026-09-08. See names.py's own comment for the incident: an
    early test of this native mechanism registered only 4 of the ~19 needed acronyms, concluded
    native handling didn't work, and built a parallel custom regex instead of just registering
    the rest. Tested here through the real parse() pipeline (family_name_main), not a standalone
    stripping function that no longer exists."""

    def setup_method(self):
        self.p = HumanNameParser()

    def test_single_postnominal(self):
        assert self.p.parse("John Finch AO").family_name_main == "finch"

    def test_stacked_space_separated(self):
        assert self.p.parse("Anthony Thomas AC FAA").family_name_main == "thomas"

    def test_stacked_comma_separated(self):
        assert self.p.parse("Anthony Kinloch FRS, FREng").family_name_main == "kinloch"

    def test_oam(self):
        assert self.p.parse("Peter Heine OAM").family_name_main == "heine"

    def test_ol_and_oam_combined(self):
        # Real case: Glenn Summerhayes OL OAM (PNG archaeology honour + Australian honour).
        assert self.p.parse("Glenn Summerhayes OL OAM").family_name_main == "summerhayes"

    def test_pharmacist(self):
        assert self.p.parse("John Smith, Pharmacist").family_name_main == "smith"

    def test_no_postnominal_unaffected(self):
        assert self.p.parse("David Wang").family_name_main == "wang"
        assert self.p.parse("Van Nguyen").family_name_main == "nguyen"

    def test_known_residual_gap_bare_surname_plus_suffix(self):
        # Accepted, documented limitation (names.py's own comment): with NO given name at all,
        # HumanName's grammar has too few tokens to tell there's no first name, and misparses --
        # confirmed to not occur in real ARC data (only 2 rows have an empty first_name, neither
        # with a postnominal) and, on the OAX side, indistinguishable from OpenAlex's own
        # unresolved PDF-extraction/disambiguation noise (checked directly 2026-09-08 -- e.g.
        # 'Kevin AM' carries OpenAlex's own alternate spelling 'Kevin Am', i.e. even OpenAlex
        # hasn't resolved whether this is a name or an acronym). Not fixed; asserted here so a
        # future change to this behaviour is a deliberate choice, not a silent regression.
        assert self.p.parse("Raston AO FAA").family_name_main == "ao"


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


class TestNicknameTokens:
    """nickname_tokens (2026-09-08): a quoted/parenthesized nickname, kept as its own field --
    deliberately NOT folded into given_tokens (see ParsedName's own docstring for why: it's a
    different kind of relationship to the family name, its own combinatorial axis, not one more
    interchangeable given-name candidate)."""

    def setup_method(self):
        self.p = HumanNameParser()

    def test_parenthesized_nickname_extracted(self):
        r = self.p.parse("William (Bill) Harley")
        assert "bill" in r.nickname_tokens

    def test_quoted_nickname_extracted(self):
        r = self.p.parse('John "Johnny" Smith')
        assert "johnny" in r.nickname_tokens

    def test_nickname_does_not_affect_given_tokens_or_canonical(self):
        # The corrected design (2026-09-08): a real case, Yingzi (Jenny) Wang -- the nickname
        # must never override the structural first name for canonical/blocking-key purposes.
        r = self.p.parse("Yingzi (Jenny) Wang")
        assert "jenny" not in r.given_tokens
        assert r.first_name_canonical == "yingzi"
        assert r.full_name_key == "yingzi_wang"
        assert "jenny" in r.nickname_tokens

    def test_multiword_nickname_splits_into_separate_tokens(self):
        # Real ARC case -- a compound nickname splits the same way a compound given name does
        # ("Jean-Baptiste" -> ["jean","baptiste"]), not kept as one joined string.
        r = self.p.parse("Jafar (Seyed Ruhollah) Shojaii")
        assert "seyed" in r.nickname_tokens
        assert "ruhollah" in r.nickname_tokens

    def test_two_existing_given_tokens_nickname_stays_independent(self):
        # Real ARC case that disproved the originally-specified "fuse onto the second token"
        # rule: Carys is not "Wen Carys". given_tokens is completely unaffected either way.
        r = self.p.parse("Xi Wen (Carys) Chan")
        assert "carys" in r.nickname_tokens
        assert "carys" not in r.given_tokens
        assert set(r.given_tokens) >= {"xi", "wen"}

    def test_numeric_content_rejected(self):
        # OpenAlex display_name noise: an author-disambiguation numeric suffix, not a nickname.
        r = self.p.parse("Ying Zhang (40767)")
        assert r.nickname_tokens == ()

    def test_year_annotation_rejected(self):
        r = self.p.parse("John Hart (1946-)")
        assert r.nickname_tokens == ()

    def test_overlong_content_rejected(self):
        r = self.p.parse("Jane Smith (" + "x" * 31 + ")")
        assert r.nickname_tokens == ()

    def test_no_nickname_present(self):
        r = self.p.parse("Robert Smith")
        assert r.nickname_tokens == ()

    def test_nickname_tokens_is_a_tuple(self):
        r = self.p.parse("William (Bill) Harley")
        assert isinstance(r.nickname_tokens, tuple)


class TestMaidenNameHandling:
    """A '(nee X)'/'(née X)' marker lands in HumanName's .nickname field structurally
    identically to a genuine given-name nickname -- distinguished here (2026-09-08) since it's
    semantically a FORMER FAMILY name, not a given-name alternative. Real case: 'Leesa Costello
    (nee Bonniface)', see CLAUDE.md's 4u-review history."""

    def setup_method(self):
        self.p = HumanNameParser()

    def test_nee_widens_family_names_not_nickname_tokens(self):
        r = self.p.parse("Judy Brown (nee Field)")
        assert "field" in r.family_names
        assert r.nickname_tokens == ()

    def test_nee_with_accent(self):
        r = self.p.parse("Murphy (née Paton-Walsh)")
        assert "paton-walsh" in r.family_names

    def test_nee_does_not_override_family_name_main(self):
        # The primary/current surname stays the representative scalar -- the maiden name only
        # widens the matching set, same "additive, never overriding" treatment as nickname_tokens.
        r = self.p.parse("Judy Brown (nee Field)")
        assert r.family_name_main == "brown"
        assert r.full_name_key == "judy_brown"

    def test_genuine_nickname_not_treated_as_maiden_name(self):
        r = self.p.parse("William (Bill) Harley")
        assert "bill" not in r.family_names
        assert "bill" in r.nickname_tokens
