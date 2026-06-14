"""
Tests for arc_author_map construction.

Validates:
  - Numeric author_idx extraction from OAX URL
  - Primary and secondary IDs both appear in map
  - is_primary flag is correct
  - Shared author_idx across two ARC persons is detectable
  - Empty secondary_oax_ids array produces no extra rows
"""

import pytest


EXTRACT_SQL = """
TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT)
"""

BUILD_MAP_SQL = """
WITH primary_ids AS (
    SELECT
        arc_id,
        TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) AS author_idx,
        TRUE AS is_primary
    FROM resolved
    WHERE oax_id IS NOT NULL
),
secondary_ids AS (
    SELECT
        arc_id,
        TRY_CAST(regexp_replace(unnest(secondary_oax_ids), 'https://openalex.org/A', '') AS BIGINT) AS author_idx,
        FALSE AS is_primary
    FROM resolved
    WHERE resolved_by IN ('oax_orcid_dedup', 'oax_topic_dedup')
)
SELECT arc_id, author_idx, is_primary
FROM primary_ids   WHERE author_idx IS NOT NULL
UNION ALL
SELECT arc_id, author_idx, is_primary
FROM secondary_ids WHERE author_idx IS NOT NULL
"""


def test_author_idx_extraction(con):
    """Strip URL prefix and cast to BIGINT."""
    rows = con.execute(f"""
        SELECT arc_id, {EXTRACT_SQL} AS author_idx
        FROM resolved ORDER BY arc_id
    """).fetchall()
    assert rows[0] == ("DP001", 100)
    assert rows[1] == ("DP002", 200)
    assert rows[2] == ("DP003", 300)
    assert rows[3] == ("DP004", 400)


def test_author_idx_large_number(con):
    """Extraction works for large IDs (real OAX IDs are 10-digit)."""
    con.execute("CREATE TEMP TABLE big_id AS SELECT 'https://openalex.org/A5027429235' AS oax_id")
    result = con.execute("""
        SELECT TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT)
        FROM big_id
    """).fetchone()[0]
    assert result == 5027429235


def test_build_map_row_counts(con):
    """DP002 (oax_topic_dedup) has one secondary → 5 total rows.
    DP003 (orcid) secondary is excluded by the resolved_by filter."""
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    total, primaries, secondaries = con.execute("""
        SELECT COUNT(*), SUM(is_primary::INT), SUM((NOT is_primary)::INT)
        FROM arc_author_map
    """).fetchone()
    assert total == 5     # 4 primaries + 1 secondary (DP002 only)
    assert primaries == 4
    assert secondaries == 1


def test_orcid_resolved_secondary_excluded(con):
    """DP003 resolved_by='orcid' → secondary author_idx=301 must NOT appear in map."""
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    row = con.execute(
        "SELECT * FROM arc_author_map WHERE arc_id='DP003' AND author_idx=301"
    ).fetchone()
    assert row is None


def test_split_record_secondary_included(con):
    """DP002 resolved_by='oax_topic_dedup' → secondary author_idx=201 IS included."""
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    row = con.execute(
        "SELECT is_primary FROM arc_author_map WHERE arc_id='DP002' AND author_idx=201"
    ).fetchone()
    assert row is not None
    assert row[0] is False


def test_build_map_secondary_is_primary_false(con):
    """Secondary entry for DP002 has is_primary=FALSE."""
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    row = con.execute("""
        SELECT is_primary FROM arc_author_map
        WHERE arc_id = 'DP002' AND author_idx = 201
    """).fetchone()
    assert row is not None
    assert row[0] is False


def test_build_map_primary_is_primary_true(con):
    """Primary entry for DP002 has is_primary=TRUE."""
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    row = con.execute("""
        SELECT is_primary FROM arc_author_map
        WHERE arc_id = 'DP002' AND author_idx = 200
    """).fetchone()
    assert row is not None
    assert row[0] is True


def test_empty_secondary_produces_no_extra_rows(con):
    """DP001, DP003, DP004 have empty secondary_oax_ids — map has exactly one row each."""
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    counts = con.execute("""
        SELECT arc_id, COUNT(*) AS n
        FROM arc_author_map
        WHERE arc_id IN ('DP001', 'DP003', 'DP004')
        GROUP BY arc_id
    """).fetchall()
    for arc_id, n in counts:
        assert n == 1, f"{arc_id} should have 1 map row, got {n}"


def test_no_shared_author_idx_in_clean_data(con):
    """In clean data no author_idx appears for more than one arc_id."""
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    shared = con.execute("""
        SELECT author_idx, COUNT(DISTINCT arc_id) AS n
        FROM arc_author_map
        GROUP BY author_idx
        HAVING COUNT(DISTINCT arc_id) > 1
    """).fetchall()
    assert shared == [], f"Unexpected shared author_idxs: {shared}"


def test_shared_author_idx_is_detectable(con):
    """If two ARC persons share an author_idx it is flagged correctly."""
    # Add a bad row: DP001 also mapped to author_idx 200 (same as DP002 primary)
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    con.execute("INSERT INTO arc_author_map VALUES ('DP001', 200, TRUE)")
    shared = con.execute("""
        SELECT author_idx, COUNT(DISTINCT arc_id) AS n
        FROM arc_author_map
        GROUP BY author_idx HAVING COUNT(DISTINCT arc_id) > 1
    """).fetchall()
    assert len(shared) == 1
    assert shared[0][0] == 200
