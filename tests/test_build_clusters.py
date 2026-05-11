"""
Tests for Layer 0 cluster building logic in src/10_build_clusters.py.
"""
import importlib.util
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.utils.names import norm_alpha, strip_postnominals

module_path = Path(__file__).resolve().parents[1] / "src" / "10_build_clusters.py"
spec = importlib.util.spec_from_file_location("build_clusters", module_path)
build_clusters = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(build_clusters)

UnionFind = build_clusters.UnionFind
first_names_compatible = build_clusters.first_names_compatible
_build_arc_parsed = build_clusters._build_arc_parsed
_build_norm_np_parsed = build_clusters._build_norm_np_parsed


# ── first_names_compatible ────────────────────────────────────────────────────

class TestFirstNamesCompatible:
    def test_identical(self):
        assert first_names_compatible("David", "David")

    def test_initial_covers_full(self):
        assert first_names_compatible("D", "David")
        assert first_names_compatible("David", "D")

    def test_initial_dot(self):
        assert first_names_compatible("D.", "David")

    def test_incompatible_full_names(self):
        assert not first_names_compatible("David", "Diana")

    def test_empty_is_compatible(self):
        assert first_names_compatible("", "David")
        assert first_names_compatible("David", "")

    def test_middle_token_matches(self):
        # "C. Fred" — token "fred" matches "Fred"
        assert first_names_compatible("C. Fred", "Fred")

    def test_initial_wrong_letter(self):
        assert not first_names_compatible("J", "David")

    def test_no_initial_no_match(self):
        # neither token is a single-char initial → correctly incompatible
        assert not first_names_compatible("Alexander", "Alex")

    def test_diacritical_stripped(self):
        assert first_names_compatible("André", "Andre")


# ── ParsedName construction ───────────────────────────────────────────────────

class TestBuildArcParsed:
    def test_last_from_column(self):
        # arc form must use arc_family directly as last, not nameparser inference
        p = _build_arc_parsed("David", "Sen Gupta")
        assert p.last == "Sen Gupta"

    def test_first_parsed(self):
        p = _build_arc_parsed("David", "Smith")
        assert p.first == "David"

    def test_title_stripped(self):
        p = _build_arc_parsed("Prof David", "Smith")
        assert p.title in ("Prof", "Prof.")
        assert "Prof" not in p.first

    def test_middle_captured(self):
        p = _build_arc_parsed("David James", "Smith")
        assert "james" in p.middle or "James" in p.middle

    def test_postnominal_already_stripped(self):
        # post-nominals should be stripped before calling this
        family = strip_postnominals("Smith AO")
        p = _build_arc_parsed("John", family)
        assert p.last == "Smith"


class TestBuildNormNpParsed:
    def test_lowercase(self):
        p = _build_norm_np_parsed("David", "Smith")
        assert p.first == "david"
        assert p.last == "smith"

    def test_diacritical_stripped(self):
        p = _build_norm_np_parsed("André", "Müller")
        assert p.first == "andre"
        assert p.last == "muller"

    def test_compound_family_split(self):
        # nameparser splits "Sen Gupta" → last="gupta", middle contains "sen"
        p = _build_norm_np_parsed("Alexander", "Sen Gupta")
        assert p.last == "gupta"
        assert "sen" in p.middle

    def test_paren_stripped(self):
        p = _build_norm_np_parsed("Clare", "Murphy (née Paton-Walsh)")
        assert p.last == "murphy"


# ── UnionFind ─────────────────────────────────────────────────────────────────

class TestUnionFind:
    def test_initial_separate(self):
        uf = UnionFind(3)
        assert uf.find(0) != uf.find(1)

    def test_union(self):
        uf = UnionFind(3)
        uf.union(0, 1)
        assert uf.find(0) == uf.find(1)
        assert uf.find(0) != uf.find(2)

    def test_transitivity(self):
        uf = UnionFind(3)
        uf.union(0, 1)
        uf.union(1, 2)
        assert uf.find(0) == uf.find(2)

    def test_idempotent(self):
        uf = UnionFind(3)
        uf.union(0, 1)
        uf.union(0, 1)
        assert uf.find(0) == uf.find(1)
