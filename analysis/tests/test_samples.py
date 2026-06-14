"""
Tests for 00_samples.py logic.

Exercises the actual DuckDB SAMPLE syntax so syntax errors are caught
before running against real data.
"""

import pytest
import duckdb


@pytest.fixture
def sample_con():
    """DuckDB connection with 100 synthetic arc_ids to sample from."""
    c = duckdb.connect()
    c.execute("""
    CREATE TABLE resolved AS
    SELECT 'ARC' || LPAD(CAST(i AS VARCHAR), 4, '0') AS arc_id
    FROM generate_series(1, 100) t(i)
    """)
    return c


def test_sample_10_syntax(sample_con):
    """USING SAMPLE 10 (reservoir, 42) produces exactly 10 rows."""
    sample_con.execute(
        "CREATE TABLE s10 AS SELECT arc_id FROM resolved USING SAMPLE 10 (reservoir, 42)"
    )
    n = sample_con.execute("SELECT COUNT(*) FROM s10").fetchone()[0]
    assert n == 10


def test_sample_1000_capped_at_population(sample_con):
    """Sampling 1000 from 100 rows returns all 100."""
    sample_con.execute(
        "CREATE TABLE s1000 AS SELECT arc_id FROM resolved USING SAMPLE 1000 (reservoir, 42)"
    )
    n = sample_con.execute("SELECT COUNT(*) FROM s1000").fetchone()[0]
    assert n == 100


def test_sample_is_reproducible(sample_con):
    """Same seed produces identical sample."""
    sample_con.execute(
        "CREATE TABLE s_a AS SELECT arc_id FROM resolved USING SAMPLE 10 (reservoir, 42)"
    )
    sample_con.execute(
        "CREATE TABLE s_b AS SELECT arc_id FROM resolved USING SAMPLE 10 (reservoir, 42)"
    )
    diff = sample_con.execute(
        "SELECT COUNT(*) FROM (SELECT arc_id FROM s_a EXCEPT SELECT arc_id FROM s_b)"
    ).fetchone()[0]
    assert diff == 0


def test_samples_parquet_schema(sample_con):
    """Output table has correct columns: arc_id, in_sample_10, in_sample_1000."""
    sample_con.execute(
        "CREATE TABLE s10   AS SELECT arc_id FROM resolved USING SAMPLE 10   (reservoir, 42)"
    )
    sample_con.execute(
        "CREATE TABLE s1000 AS SELECT arc_id FROM resolved USING SAMPLE 1000 (reservoir, 42)"
    )
    sample_con.execute("""
    CREATE TABLE samples AS
    SELECT
        r.arc_id,
        (s10.arc_id  IS NOT NULL) AS in_sample_10,
        (s1k.arc_id  IS NOT NULL) AS in_sample_1000
    FROM resolved r
    LEFT JOIN s10        ON s10.arc_id = r.arc_id
    LEFT JOIN s1000 s1k  ON s1k.arc_id = r.arc_id
    """)
    cols = {row[0] for row in sample_con.execute("DESCRIBE samples").fetchall()}
    assert "arc_id"        in cols
    assert "in_sample_10"  in cols
    assert "in_sample_1000" in cols

    # Every s10 member is also in s1000 (reservoir, 42 makes s1000 = full population here)
    in_10_not_1000 = sample_con.execute("""
        SELECT COUNT(*) FROM samples WHERE in_sample_10 AND NOT in_sample_1000
    """).fetchone()[0]
    assert in_10_not_1000 == 0
