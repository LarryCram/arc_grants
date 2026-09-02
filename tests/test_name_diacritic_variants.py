"""
Tests for src/utils/name_diacritic_variants.py -- diacritic/special-character handling for ARC
and OpenAlex name strings. Split out of test_names.py (2026-08-25), matching the source module
split.

2026-08-26: the corpus-wide bare<->digraph equivalence table (build_diacritic_variant_table() /
persist_/load_diacritic_variant_table()) was removed as a real, confirmed bug -- see the source
module's own docstring. Its tests are removed along with it; diacritic_variants() no longer
takes a `table` argument.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import unicodedata

import pytest
from src.utils.name_diacritic_variants import (
    diacritic_variants, expand_diacritic_variants, strip_diacriticals,
    canonicalize_name_punctuation,
)


class TestStripDiacriticals:
    """strip_diacriticals() is a thin wrapper over diacritic_variants()[0] -- always lowercase,
    per this codebase's convention that all string-matching code operates on lowercase and
    .title() is reserved for restoring case at display time only. These assertions reflect that
    convention, not an earlier case-preserving contract -- the case-preserving behaviour was
    never load-bearing anywhere (every real caller already chained its own .lower() immediately
    after calling this function)."""
    def test_accent(self):
        assert strip_diacriticals("Müller") == "muller"

    def test_exotic_hyphen(self):
        # U+2011 non-breaking hyphen → ASCII hyphen
        assert strip_diacriticals("Bunde‑Birouste") == "bunde-birouste"

    def test_turkish_dotless_i(self):
        assert strip_diacriticals("Anbarcı") == "anbarci"

    def test_turkish_dotted_I(self):
        assert strip_diacriticals("İstanbul") == "istanbul"

    def test_plain_ascii(self):
        assert strip_diacriticals("Smith") == "smith"

    def test_no_decomp_fallback_not_silently_dropped(self):
        # ø/ß/ł/œ/æ/ð/þ have no NFD canonical decomposition -- confirmed real bug (2026-08-25):
        # NFD-based stripping alone silently DROPS them rather than substituting a bare letter.
        assert strip_diacriticals("Sørensen") == "sorensen"
        assert strip_diacriticals("Løken") == "loken"
        assert strip_diacriticals("Włodkowic") == "wlodkowic"
        assert strip_diacriticals("Straße") == "strase"
        assert strip_diacriticals("Bjæland") == "bjaeland"
        assert strip_diacriticals("Þórsson") == "thorsson"

    def test_all_caps_input(self):
        # Real OAX display_names are sometimes ALL CAPS -- must normalise the same as mixed case.
        assert strip_diacriticals("MÜLLER") == "muller"
        assert strip_diacriticals("STRASSE") == "strasse"

    def test_empty(self):
        assert strip_diacriticals("") == ""


class TestDiacriticVariants:
    """diacritic_variants() -- the single consolidated entry point (2026-08-25) replacing the
    old split between strip_diacriticals()/expand_diacritic_variants(). Returns an ORDERED list,
    shortest-first. Deliberately local (2026-08-26): every variant returned is derived purely
    from the input string's own characters -- see TestNoCrossNameLeakage below."""
    def test_bare_and_digraph_from_literal_umlaut(self):
        # Confirmed real case: DP0345157_HansMuhlhaus -- ARC has bare 'muhlhaus' (no diacritic to
        # expand from), every one of his real OpenAlex fragments has family_names_display
        # containing BOTH forms because the literal ü in "Mühlhaus" generates both.
        assert diacritic_variants("Mühlhaus") == ["muhlhaus", "muehlhaus"]

    def test_ordering_shortest_first(self):
        variants = diacritic_variants("Mühlhaus")
        assert len(variants[0]) <= len(variants[-1])

    def test_ss_eszett_ordering_bug_regression(self):
        # Real bug caught by testing before shipping: folding ß->s via the no-decomposition
        # fallback BEFORE the bare/digraph cartesian-product step ran meant the digraph "ss"
        # alternative was silently never generated at all ("Straße" produced only "strase").
        assert diacritic_variants("Straße") == ["strase", "strasse"]

    def test_no_spurious_folds_on_unrelated_names(self):
        # A blind substring-fold approach ("ue"->"u" everywhere) was tried and rejected because
        # it mangles ordinary non-German names -- confirmed here it does NOT happen without a
        # literal diacritic character actually present in the input.
        assert diacritic_variants("Fuentes") == ["fuentes"]
        assert diacritic_variants("Guerrero") == ["guerrero"]
        assert diacritic_variants("Rousseau") == ["rousseau"]

    def test_no_decomp_letters_no_digraph_convention(self):
        # ł/œ/æ/ð/þ have a bare fallback but no real digraph convention -- single-element result.
        assert diacritic_variants("Włodkowic") == ["wlodkowic"]

    def test_all_caps_input_normalised(self):
        assert diacritic_variants("MÜHLHAUS") == ["muhlhaus", "muehlhaus"]

    def test_turkish_capital_i_no_combining_mark_artifact(self):
        # Python's own str.lower() turns İ into "i" + combining dot (U+0307), not plain "i" --
        # confirmed directly; must not leak into the result.
        assert diacritic_variants("İstanbul") == ["istanbul"]

    def test_empty_string(self):
        assert diacritic_variants("") == []

    def test_plain_ascii_single_element(self):
        assert diacritic_variants("Smith") == ["smith"]


class TestNoCrossNameLeakage:
    """2026-08-26: a former version of diacritic_variants() also consulted a corpus-wide
    bare<->digraph equivalence table, keyed on the bare-folded string regardless of whether the
    INPUT itself had a diacritic. That let one unrelated person's real diacritic name (e.g.
    "Christian Bäker", externally confirmed real via a 2014 co-authored journal article) inject a
    spurious "baeker" spelling into every OTHER "Baker" in the whole ARC/OAX population --
    confirmed concretely for Baker/Wang/Walker/Zhu/Wu/Xu/Xue. Removed outright, not merely
    gated -- these tests assert the specific real collision strings never appear for a plain
    ASCII name with no diacritic character of its own, on any current or future mechanism."""
    def test_plain_common_surnames_never_widened(self):
        assert diacritic_variants("Baker") == ["baker"]
        assert diacritic_variants("Wang") == ["wang"]
        assert diacritic_variants("Walker") == ["walker"]
        assert diacritic_variants("Zhu") == ["zhu"]
        assert diacritic_variants("Wu") == ["wu"]
        assert diacritic_variants("Xu") == ["xu"]
        assert diacritic_variants("Xue") == ["xue"]

    def test_no_table_parameter_accepted(self):
        # diacritic_variants() is single-argument now -- passing a second positional argument
        # must be a TypeError, not silently ignored (would mask a caller that still thinks a
        # table exists).
        with pytest.raises(TypeError):
            diacritic_variants("Baker", {"baker": ["baeker"]})


class TestCanonicalizeUnicodeHardening:
    """2026-09-02: canonicalize_name_punctuation() hardened per a detailed Unicode-normalization
    review (NFC/NFKC ingestion hygiene, zero-width character stripping, a real soft-hyphen bug).
    See the function's own docstring for the full audit of what was already handled vs. genuinely
    missing before this session."""

    def test_nfc_nfd_equivalence(self):
        # The same visual name, encoded two different (both legal) ways -- a precomposed
        # code point (NFC, typical of Crossref/REST APIs) vs. base letter + combining mark
        # (NFD, typical of macOS-originated file paths). Must compare equal after canonicalizing.
        nfc = "Müller"
        nfd = unicodedata.normalize("NFD", nfc)
        assert nfc != nfd  # sanity: genuinely different byte sequences to start with
        assert canonicalize_name_punctuation(nfc) == canonicalize_name_punctuation(nfd)

    def test_ligature_decomposed(self):
        # U+FB01 LATIN SMALL LIGATURE FI -- a real artifact from legacy PDF/typesetting
        # extraction, not merely a display glyph; must decompose to plain "f"+"i" or a plain
        # substring search for "first" would silently miss it.
        assert canonicalize_name_punctuation("ﬁrst") == "first"

    def test_zero_width_space_stripped(self):
        assert canonicalize_name_punctuation("Wil​liam") == "William"

    def test_zero_width_joiner_and_bom_stripped(self):
        assert canonicalize_name_punctuation("A‌‍﻿B") == "AB"

    def test_soft_hyphen_stripped_not_substituted(self):
        # Real bug fix: a soft hyphen (U+00AD) is normally invisible and must be REMOVED, not
        # turned into a visible "-" -- substituting it would wrongly read as a hyphenated
        # compound name (two tokens) instead of one continuous word.
        assert canonicalize_name_punctuation("Wil­liam") == "William"

    def test_real_en_dash_still_becomes_visible_hyphen(self):
        # Contrast with the soft hyphen above: a genuine en-dash/em-dash/minus-sign IS a real,
        # intended hyphen in a hyphenated surname, and must still become a visible ASCII "-".
        assert canonicalize_name_punctuation("Smith–Jones") == "Smith-Jones"
        assert canonicalize_name_punctuation("Smith—Jones") == "Smith-Jones"

    def test_nfkc_does_not_disturb_existing_special_cased_diacritics(self):
        # Confirms the reasoning in canonicalize_name_punctuation()'s own docstring: NFKC must
        # be a no-op on every character this module already special-cases (none of them carry a
        # Unicode *compatibility* decomposition, only -- for some -- a canonical one, which NFKC
        # round-trips back to the identical character).
        for ch in "üöäßøåłœæðþ":
            assert canonicalize_name_punctuation(ch) == ch


class TestExpandDiacriticVariants:
    """expand_diacritic_variants() -- thin wrapper over diacritic_variants(), kept for its one
    real production caller (00c_prepare_oax.py's _parse_name(), which relies on this function's
    own internal lowering and never lowercases externally itself)."""
    def test_matches_diacritic_variants(self):
        assert expand_diacritic_variants("Mühlhaus") == diacritic_variants("Mühlhaus")
