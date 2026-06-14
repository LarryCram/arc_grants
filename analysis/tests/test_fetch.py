"""
Tests for 01_fetch_oeuvres logic.

Validates:
  - Authorships join: correct works fetched per ARC person
  - Deduplication: work appearing under primary AND secondary → one row, is_primary=TRUE
  - Works only under secondary → included with is_primary=FALSE
  - Best-topic selection: highest-score topic per work
  - Works with no topic: still included (left join), topic columns NULL
  - No year filter: pre-2000 works are included
"""

import pytest

BUILD_MAP_SQL = """
WITH primary_ids AS (
    SELECT arc_id,
           TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) AS author_idx,
           TRUE AS is_primary
    FROM resolved
),
secondary_ids AS (
    SELECT arc_id,
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

FETCH_SQL = """
WITH arc_authorships AS (
    SELECT a.work_idx, m.arc_id, m.is_primary, a.author_name
    FROM authorships a
    JOIN arc_author_map m ON m.author_idx = a.author_idx
),
deduped AS (
    SELECT
        arc_id, work_idx,
        MAX(is_primary::INT)::BOOL AS is_primary_author_id,
        MAX(author_name) AS author_name
    FROM arc_authorships
    GROUP BY arc_id, work_idx
),
best_topic AS (
    SELECT work_idx, topic_idx, subfield_name, field_name, domain_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY work_idx ORDER BY score DESC) AS rn
        FROM topics
    ) WHERE rn = 1
)
SELECT
    d.arc_id, d.work_idx, d.is_primary_author_id, d.author_name,
    w.publication_year, w.cited_by_count, w.type,
    bt.field_name, bt.subfield_name, bt.domain_name
FROM deduped d
JOIN works w ON w.work_idx = d.work_idx
LEFT JOIN best_topic bt ON bt.work_idx = d.work_idx
ORDER BY d.arc_id, w.publication_year
"""


@pytest.fixture
def oeuvres(con):
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP_SQL}")
    con.execute(f"CREATE TABLE oeuvres AS {FETCH_SQL}")
    return con


def test_dp001_gets_three_works(oeuvres):
    """DP001 primary author_idx=100 has works 1001, 1002, 1003."""
    n = oeuvres.execute("SELECT COUNT(*) FROM oeuvres WHERE arc_id='DP001'").fetchone()[0]
    assert n == 3


def test_dp002_gets_three_works_after_dedup(oeuvres):
    """DP002 has primary works 2001+2002 and secondary adds 2003; 2002 appears once."""
    work_idxs = sorted(r[0] for r in oeuvres.execute(
        "SELECT work_idx FROM oeuvres WHERE arc_id='DP002'"
    ).fetchall())
    assert work_idxs == [2001, 2002, 2003]


def test_shared_work_is_primary(oeuvres):
    """Work 2002 appears under both primary (200) and secondary (201) → is_primary=TRUE."""
    row = oeuvres.execute(
        "SELECT is_primary_author_id FROM oeuvres WHERE arc_id='DP002' AND work_idx=2002"
    ).fetchone()
    assert row is not None
    assert row[0] is True


def test_secondary_only_work_is_not_primary(oeuvres):
    """Work 2003 is only under secondary author_idx=201 → is_primary=FALSE."""
    row = oeuvres.execute(
        "SELECT is_primary_author_id FROM oeuvres WHERE arc_id='DP002' AND work_idx=2003"
    ).fetchone()
    assert row is not None
    assert row[0] is False


def test_co_author_not_in_oeuvres(oeuvres):
    """author_idx=999 (co-author, not ARC) should not appear as an arc_id."""
    rows = oeuvres.execute("SELECT DISTINCT arc_id FROM oeuvres").fetchall()
    arc_ids = {r[0] for r in rows}
    assert "999" not in arc_ids
    assert len(arc_ids) == 4  # DP001–DP004


def test_best_topic_highest_score(oeuvres):
    """Work 1001 has two topics (scores 0.9, 0.6) — should use field_name for score=0.9."""
    row = oeuvres.execute(
        "SELECT field_name FROM oeuvres WHERE arc_id='DP001' AND work_idx=1001"
    ).fetchone()
    assert row[0] == "Physics and Astronomy"


def test_work_without_topic_still_included(oeuvres):
    """Work 3002 has no topic entry — should appear with NULL field_name."""
    row = oeuvres.execute(
        "SELECT work_idx, field_name FROM oeuvres WHERE arc_id='DP003' AND work_idx=3002"
    ).fetchone()
    assert row is not None
    assert row[1] is None


def test_no_year_filter_pre2000_work(oeuvres):
    """Work 4001 published 1998 — must be present (no year filter in fetch)."""
    row = oeuvres.execute(
        "SELECT publication_year FROM oeuvres WHERE arc_id='DP004' AND work_idx=4001"
    ).fetchone()
    assert row is not None
    assert row[0] == 1998


def test_dp004_all_six_works(oeuvres):
    """DP004 has works from 1998 to 2020 — all 6 should be present."""
    n = oeuvres.execute("SELECT COUNT(*) FROM oeuvres WHERE arc_id='DP004'").fetchone()[0]
    assert n == 6


def test_orcid_secondary_work_excluded(oeuvres):
    """Work 3003 belongs to author_idx=301, which is DP003's secondary but
    resolved_by='orcid' — should NOT appear in DP003's oeuvre."""
    row = oeuvres.execute(
        "SELECT work_idx FROM oeuvres WHERE arc_id='DP003' AND work_idx=3003"
    ).fetchone()
    assert row is None


def test_dp003_only_primary_works(oeuvres):
    """DP003 should have exactly 2 works (3001, 3002) — not 3003."""
    work_idxs = sorted(r[0] for r in oeuvres.execute(
        "SELECT work_idx FROM oeuvres WHERE arc_id='DP003'"
    ).fetchall())
    assert work_idxs == [3001, 3002]
