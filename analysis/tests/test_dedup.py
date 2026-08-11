"""
Tests for analysis/utils/dedup.py: create_deduped_works() and count_exclusions().

test_metrics.py covers the specific preprint+article worked example against real
03_annual_metrics.py-shaped data; this module covers dedup/exclusion behaviour more
broadly in isolation (multi-person grouping, type-priority tiebreak, exclusion-before-dedup
interaction, year-range filtering, per-arc_id exclusion counts).
"""

import duckdb
import pytest

from analysis.utils.dedup import create_deduped_works, count_exclusions


@pytest.fixture
def con():
    return duckdb.connect()


def _load(con, rows, table="src"):
    con.execute(f"""
        CREATE TABLE {table} (
            arc_id VARCHAR, work_idx BIGINT, publication_year BIGINT, cited_by_count BIGINT,
            type VARCHAR, title VARCHAR, field_name VARCHAR, subfield_name VARCHAR,
            domain_name VARCHAR, doi VARCHAR
        )
    """)
    con.executemany(f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    return table


def test_dedup_is_scoped_per_arc_id(con):
    """Two different people sharing an identical title must NOT merge into each other."""
    table = _load(con, [
        ("P1", 1, 2020, 10, "article", "A shared generic-sounding paper title here", None, None, None, None),
        ("P2", 2, 2020, 5,  "article", "A shared generic-sounding paper title here", None, None, None, None),
    ])
    create_deduped_works(con, table, "out")
    rows = con.execute("SELECT arc_id, work_idx FROM out ORDER BY arc_id").fetchall()
    assert rows == [("P1", 1), ("P2", 2)]


def test_type_priority_tiebreak_book_chapter_over_preprint(con):
    table = _load(con, [
        ("P1", 1, 2019, 3, "preprint",     "A sufficiently long and distinctive paper title", None, None, None, None),
        ("P1", 2, 2020, 7, "book-chapter", "A sufficiently long and distinctive paper title", None, None, None, None),
    ])
    create_deduped_works(con, table, "out")
    rows = con.execute("SELECT work_idx, cited_by_count, type FROM out").fetchall()
    assert rows == [(2, 10, "book-chapter")]  # book-chapter wins tiebreak, citations summed


def test_excluded_work_does_not_merge_with_kept_work(con):
    """A junk/excluded-type record sharing a title with a real kept work must vanish
    entirely, not survive by merging into (and inflating) the real one."""
    table = _load(con, [
        ("P1", 1, 2020, 10, "article", "A sufficiently long and distinctive paper title", None, None, None, None),
        ("P1", 2, 2020, 99, "dataset", "A sufficiently long and distinctive paper title", None, None, None, None),
    ])
    create_deduped_works(con, table, "out")
    rows = con.execute("SELECT work_idx, cited_by_count FROM out").fetchall()
    assert rows == [(1, 10)]  # dataset excluded entirely -- its 99 citations must not leak in


def test_filename_artifact_excluded_from_dedup_output(con):
    table = _load(con, [
        ("P1", 1, 2025, 0, "article", "3093538.pdf", None, None, None, None),
        ("P1", 2, 2025, 0, "article", "3093538.pdf", None, None, None, None),
    ])
    create_deduped_works(con, table, "out")
    rows = con.execute("SELECT * FROM out").fetchall()
    assert rows == []


def test_year_range_filter_still_applies(con):
    table = _load(con, [
        ("P1", 1, 1700, 1, "article", "A sufficiently long and distinctive paper title", None, None, None, None),
        ("P1", 2, 2020, 1, "article", "Another sufficiently long distinctive title here", None, None, None, None),
    ])
    create_deduped_works(con, table, "out", min_year=1950, max_year=2026)
    rows = con.execute("SELECT work_idx FROM out").fetchall()
    assert rows == [(2,)]


def test_count_exclusions_per_arc_id_and_reason(con):
    table = _load(con, [
        ("P1", 1, 2020, 1, "article", "A sufficiently long and distinctive paper title", None, None, None, None),
        ("P1", 2, 2020, 1, "dataset", "A dataset that should be excluded by type here", None, None, None, None),
        ("P1", 3, 2021, 1, "dataset", "Another dataset excluded by type here too yes", None, None, None, None),
        ("P2", 4, 2020, 1, "article", "5551234.pdf", None, None, None, None),
    ])
    count_exclusions(con, table, "excl")
    rows = dict(((a, r), n) for a, r, n in con.execute("SELECT arc_id, reason, n FROM excl ORDER BY arc_id, reason").fetchall())
    assert rows == {
        ("P1", "type_not_allowed"): 2,
        ("P2", "filename_artifact"): 1,
    }


def test_count_exclusions_empty_when_nothing_excluded(con):
    table = _load(con, [
        ("P1", 1, 2020, 1, "article", "A sufficiently long and distinctive paper title", None, None, None, None),
    ])
    count_exclusions(con, table, "excl")
    rows = con.execute("SELECT * FROM excl").fetchall()
    assert rows == []
