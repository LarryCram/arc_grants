"""
Tests for pure helper functions in src/00a_analyse_arc.py.
"""
import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_spec = importlib.util.spec_from_file_location(
    "analyse_arc",
    Path(__file__).resolve().parents[1] / "src" / "00a_analyse_arc.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_norm_family   = _mod._norm_family
_first_initial = _mod._first_initial
_full_first    = _mod._full_first
_make_name_key = _mod._make_name_key
_orcid_status  = _mod._orcid_status
_fmt           = _mod._fmt


class TestNormFamily:
    def test_lowercases(self):
        assert _norm_family("Smith") == "smith"

    def test_strips_diacriticals(self):
        assert _norm_family("Müller") == "muller"

    def test_strips_whitespace(self):
        assert _norm_family("  Jones  ") == "jones"

    def test_none_returns_empty(self):
        assert _norm_family(None) == ""

    def test_empty_returns_empty(self):
        assert _norm_family("") == ""


class TestFirstInitial:
    def test_simple(self):
        assert _first_initial("John") == "j"

    def test_initial_only(self):
        assert _first_initial("J") == "j"

    def test_with_diacritical(self):
        assert _first_initial("Éric") == "e"

    def test_none_returns_none(self):
        assert _first_initial(None) is None

    def test_empty_returns_none(self):
        assert _first_initial("") is None


class TestFullFirst:
    def test_returns_longest_token_of_four_plus(self):
        assert _full_first("John") == "john"

    def test_prefers_longer_token(self):
        assert _full_first("Jo Jonathan") == "jonathan"

    def test_ignores_short_tokens(self):
        # "Jo" is 2 chars, below threshold
        assert _full_first("Jo") is None

    def test_three_chars_below_threshold(self):
        assert _full_first("Tom") is None

    def test_none_returns_none(self):
        assert _full_first(None) is None

    def test_empty_returns_none(self):
        assert _full_first("") is None


class TestMakeNameKey:
    def test_standard(self):
        assert _make_name_key("Smith", "John") == "smith_j"

    def test_diacritical_family(self):
        assert _make_name_key("Müller", "Hans") == "muller_h"

    def test_empty_family_returns_none(self):
        assert _make_name_key("", "John") is None

    def test_empty_first_returns_none(self):
        assert _make_name_key("Smith", "") is None

    def test_none_first_returns_none(self):
        assert _make_name_key("Smith", None) is None


class TestOrcidStatus:
    def test_none_when_all_null(self):
        s = pd.Series([None, None])
        assert _orcid_status(s) == "none"

    def test_single_when_one_orcid(self):
        s = pd.Series(["0000-0001-0001-0001", None, "0000-0001-0001-0001"])
        assert _orcid_status(s) == "single"

    def test_conflict_when_two_distinct(self):
        s = pd.Series(["0000-0001-0001-0001", "0000-0002-0002-0002"])
        assert _orcid_status(s) == "conflict"

    def test_single_when_exactly_one_unique(self):
        s = pd.Series(["0000-0001-0001-0001"])
        assert _orcid_status(s) == "single"


class TestFmt:
    def test_percentage(self):
        assert _fmt(1, 4) == "      1  (25.0%)"

    def test_zero(self):
        assert _fmt(0, 100) == "      0  (0.0%)"

    def test_full(self):
        assert _fmt(100, 100) == "    100  (100.0%)"
