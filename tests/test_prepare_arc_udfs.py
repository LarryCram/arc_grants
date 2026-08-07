"""
Tests for the FOR code/name normalisation UDFs used by src/01_prepare_arc.py, now provided
by src/utils/for_resolve.py (research_classification-backed) instead of the old
ForTopicLookup-based module-level wrappers.

upgrade_for_code(code) -> str | None
    2008 group code  → 2020 group code
    2020 group code  → passthrough
    None / empty     → None
    unmappable 2008  → None  (SQL COALESCE falls back to original)

upgrade_for_name(code, name) -> str | None
    converted code   → official ANZSRC 2020 group name
    2020 code        → original name unchanged
    None / unmappable → original name unchanged
"""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.for_resolve import upgrade_for_code, upgrade_for_name


@pytest.fixture(scope="module")
def con():
    """DuckDB connection with both UDFs registered — mirrors what main() does."""
    c = duckdb.connect()
    c.create_function("upgrade_for_code", upgrade_for_code,
                      ["VARCHAR"], "VARCHAR",
                      null_handling='special')
    c.create_function("upgrade_for_name", upgrade_for_name,
                      ["VARCHAR", "VARCHAR"], "VARCHAR",
                      null_handling='special')
    return c


# ---------------------------------------------------------------------------
# upgrade_for_code — pure Python
# ---------------------------------------------------------------------------

class TestUpgradeForCodePy:
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
        # 2008 codes that have no 4-digit 2020 group (beyond "1701" special case)
        # Any genuinely unknown code → None
        assert upgrade_for_code("9999") is None


# ---------------------------------------------------------------------------
# upgrade_for_name — pure Python
# ---------------------------------------------------------------------------

class TestUpgradeForNamePy:
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
        # 2020 code, None name → returns None (original)
        assert upgrade_for_name("4605", None) is None


# ---------------------------------------------------------------------------
# SQL integration — verify DuckDB UDF registration and COALESCE behaviour
# ---------------------------------------------------------------------------

class TestSQLIntegration:
    def test_code_2008_sql(self, con):
        r = con.execute("SELECT upgrade_for_code('0101')").fetchone()[0]
        assert r == "4904"

    def test_code_2020_passthrough_sql(self, con):
        r = con.execute("SELECT upgrade_for_code('4605')").fetchone()[0]
        assert r == "4605"

    def test_code_null_sql(self, con):
        r = con.execute("SELECT upgrade_for_code(NULL)").fetchone()[0]
        assert r is None

    def test_coalesce_fallback_for_unmappable(self, con):
        # COALESCE(upgrade_for_code(code), code) keeps original when no mapping
        r = con.execute("""
            SELECT COALESCE(upgrade_for_code('9999'), '9999')
        """).fetchone()[0]
        assert r == "9999"

    def test_name_2008_sql(self, con):
        r = con.execute("SELECT upgrade_for_name('0101', 'Pure Mathematics')").fetchone()[0]
        assert r == "Pure mathematics"

    def test_name_2020_unchanged_sql(self, con):
        r = con.execute(
            "SELECT upgrade_for_name('4605', 'Data Management and Data Science')"
        ).fetchone()[0]
        assert r == "Data Management and Data Science"

    def test_name_null_code_sql(self, con):
        r = con.execute("SELECT upgrade_for_name(NULL, 'Some Field')").fetchone()[0]
        assert r == "Some Field"
