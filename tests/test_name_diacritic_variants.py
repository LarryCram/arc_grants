"""
Tests for src/utils/name_diacritic_variants.py -- diacritic/special-character handling and the
corpus-grounded bare<->digraph equivalence table. Split out of test_names.py (2026-08-25),
matching the source module split.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.utils.name_diacritic_variants import (
    build_diacritic_variant_table, diacritic_variants, expand_diacritic_variants,
    load_diacritic_variant_table, persist_diacritic_variant_table, strip_diacriticals,
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
    shortest-first."""
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
        # diacritic-bearing sibling actually present in the input.
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

    def test_table_widens_result(self):
        table = {"muhlhaus": ["muehlhaus", "grutzner"]}
        assert diacritic_variants("Muhlhaus", table) == ["grutzner", "muhlhaus", "muehlhaus"]

    def test_table_none_is_noop(self):
        assert diacritic_variants("Muhlhaus", None) == diacritic_variants("Muhlhaus")

    def test_table_no_match_is_noop(self):
        table = {"someothername": ["irrelevant"]}
        assert diacritic_variants("Muhlhaus", table) == ["muhlhaus"]

    def test_ascii_only_digraph_widened_by_table(self):
        # The gap this table exists to close: OpenAlex's own curated spelling can be ASCII-only
        # digraph (no literal ü anywhere) -- without the table, "Muehlhaus" alone would never
        # generate "muhlhaus", since there's no diacritic character to expand from.
        table = {"muehlhaus": ["muhlhaus"], "muhlhaus": ["muehlhaus"]}
        assert diacritic_variants("Muehlhaus", table) == ["muhlhaus", "muehlhaus"]


class TestExpandDiacriticVariants:
    """expand_diacritic_variants() -- thin wrapper over diacritic_variants(), kept for its one
    real production caller (00c_prepare_oax.py's _parse_name(), which relies on this function's
    own internal lowering and never lowercases externally itself)."""
    def test_matches_diacritic_variants(self):
        assert expand_diacritic_variants("Mühlhaus") == diacritic_variants("Mühlhaus")

    def test_table_passthrough(self):
        table = {"muhlhaus": ["grutzner"]}
        assert expand_diacritic_variants("Muhlhaus", table) == diacritic_variants("Muhlhaus", table)


class TestBuildDiacriticVariantTable:
    """build_diacritic_variant_table() -- scans a corpus of raw name strings for tokens with a
    literal diacritic character and builds a bare<->digraph equivalence table for CONFIRMED
    roots only."""
    def test_confirms_real_pairing(self):
        corpus = ["Hans Mühlhaus", "H.-B. Mühlhaus"]
        table = build_diacritic_variant_table(corpus)
        assert table["muhlhaus"] == ["muehlhaus"]
        assert table["muehlhaus"] == ["muhlhaus"]

    def test_no_entry_for_names_without_diacritic_sibling(self):
        # Confirmed real safety property: an ordinary name never gets a spurious entry just for
        # containing "ue"/"ae"/etc as an incidental substring.
        corpus = ["Random Fuentes", "John Smith", "Maria Guerrero"]
        table = build_diacritic_variant_table(corpus)
        assert "fuentes" not in table
        assert "guerrero" not in table
        assert table == {}

    def test_multiple_roots(self):
        corpus = ["Hans Mühlhaus", "Frank Grützner", "Random Fuentes"]
        table = build_diacritic_variant_table(corpus)
        assert set(table.keys()) == {"muhlhaus", "muehlhaus", "grutzner", "gruetzner"}

    def test_given_name_diacritics_also_captured(self):
        # Not just family names -- any token, per the 2026-08-25 "apply to all names" direction.
        corpus = ["Björn Ohlsson"]
        table = build_diacritic_variant_table(corpus)
        assert "bjorn" in table
        assert "bjoern" in table["bjorn"]

    def test_empty_corpus(self):
        assert build_diacritic_variant_table([]) == {}

    def test_none_and_empty_strings_in_corpus_skipped(self):
        assert build_diacritic_variant_table([None, "", "Hans Mühlhaus"]) == {
            "muhlhaus": ["muehlhaus"], "muehlhaus": ["muhlhaus"],
        }


class TestPersistLoadDiacriticVariantTable:
    def test_round_trip(self, tmp_path):
        table = {
            "muhlhaus": ["muehlhaus"], "muehlhaus": ["muhlhaus"],
            "grutzner": ["gruetzner"], "gruetzner": ["grutzner"],
        }
        path = tmp_path / "table.csv"
        persist_diacritic_variant_table(table, path)
        loaded = load_diacritic_variant_table(path)
        assert loaded == table

    def test_missing_file_returns_empty(self, tmp_path):
        assert load_diacritic_variant_table(tmp_path / "does_not_exist.csv") == {}

    def test_csv_format_is_one_row_per_pair(self, tmp_path):
        table = {"muhlhaus": ["muehlhaus", "grutzner"]}
        path = tmp_path / "table.csv"
        persist_diacritic_variant_table(table, path)
        lines = path.read_text().splitlines()
        assert lines[0] == "variant,counterpart"
        assert len(lines) == 3  # header + 2 rows, one per counterpart
