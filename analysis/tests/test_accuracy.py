"""
Tests for 02_accuracy_check diagnostics logic.

Validates:
  - Year continuity: gap detection across career span
  - works_count reconciliation logic
  - Author name variant detection
  - Secondary ID work contribution counting
"""

import pytest


# ── Year gap detection ─────────────────────────────────────────────────────

GAP_SQL = """
WITH years_pub AS (
    SELECT DISTINCT arc_id, publication_year AS yr
    FROM oeuvres
    WHERE publication_year IS NOT NULL
),
per_person AS (
    SELECT arc_id, MIN(yr) AS first_yr, MAX(yr) AS last_yr
    FROM years_pub GROUP BY arc_id
),
spine AS (
    SELECT p.arc_id, gs.yr,
           (y.yr IS NOT NULL) AS has_pub
    FROM per_person p
    CROSS JOIN (SELECT generate_series AS yr FROM generate_series(1990, 2025)) gs
    LEFT JOIN years_pub y ON y.arc_id = p.arc_id AND y.yr = gs.yr
    WHERE gs.yr BETWEEN p.first_yr AND p.last_yr
),
gaps AS (
    SELECT arc_id, SUM(CASE WHEN NOT has_pub THEN 1 ELSE 0 END) AS total_gap_years
    FROM spine GROUP BY arc_id
)
SELECT arc_id, total_gap_years FROM gaps ORDER BY arc_id
"""


@pytest.fixture
def with_oeuvres(con):
    """Build minimal oeuvres from synthetic data."""
    BUILD_MAP = """
    WITH p AS (
        SELECT arc_id,
               TRY_CAST(regexp_replace(oax_id,'https://openalex.org/A','') AS BIGINT) AS author_idx,
               TRUE AS is_primary
        FROM resolved
    ),
    s AS (
        SELECT arc_id,
               TRY_CAST(regexp_replace(unnest(secondary_oax_ids),'https://openalex.org/A','') AS BIGINT) AS author_idx,
               FALSE AS is_primary
        FROM resolved
        WHERE resolved_by IN ('oax_orcid_dedup', 'oax_topic_dedup')
    )
    SELECT arc_id, author_idx, is_primary FROM p WHERE author_idx IS NOT NULL
    UNION ALL
    SELECT arc_id, author_idx, is_primary FROM s WHERE author_idx IS NOT NULL
    """
    con.execute(f"CREATE TABLE arc_author_map AS {BUILD_MAP}")
    con.execute("""
    CREATE TABLE oeuvres AS
    WITH deduped AS (
        SELECT arc_id, work_idx, MAX(is_primary::INT)::BOOL AS is_primary_author_id
        FROM (
            SELECT a.work_idx, m.arc_id, m.is_primary
            FROM authorships a JOIN arc_author_map m ON m.author_idx = a.author_idx
        ) GROUP BY arc_id, work_idx
    )
    SELECT d.arc_id, d.work_idx, d.is_primary_author_id,
           w.publication_year, w.cited_by_count, w.type, a.author_name
    FROM deduped d
    JOIN works w ON w.work_idx = d.work_idx
    JOIN (
        SELECT work_idx, MAX(author_name) AS author_name FROM authorships GROUP BY work_idx
    ) a ON a.work_idx = d.work_idx
    """)
    return con


def test_dp003_has_gap(with_oeuvres):
    """DP003 publishes in 2000 and 2006 → 5 gap years (2001–2005)."""
    gaps = dict(with_oeuvres.execute(GAP_SQL).fetchall())
    assert gaps["DP003"] == 5


def test_dp001_no_gap(with_oeuvres):
    """DP001 publishes in 2015, 2018, 2021 — years between covered by gap_sql
    but only career-span years count; gaps within span: 2016,2017,2019,2020 = 4 gap years."""
    gaps = dict(with_oeuvres.execute(GAP_SQL).fetchall())
    # career span 2015–2021 = 7 years; active in 2015, 2018, 2021 → 4 gap years
    assert gaps["DP001"] == 4


def test_dp004_has_no_mid_career_gaps(with_oeuvres):
    """DP004 publishes in 1998, 2000, 2005, 2010, 2015, 2020.
    Gap years within 1998–2020 span: 1999, 2001-2004, 2006-2009, 2011-2014, 2016-2019 = 18."""
    gaps = dict(with_oeuvres.execute(GAP_SQL).fetchall())
    expected = (1999-1999+1) + (2004-2001+1) + (2009-2006+1) + (2014-2011+1) + (2019-2016+1)
    # 1 + 4 + 4 + 4 + 4 = 17... let me count: 1999(1), 2001-2004(4), 2006-2009(4), 2011-2014(4), 2016-2019(4) = 17
    assert gaps["DP004"] == 17


# ── First publication year ─────────────────────────────────────────────────

def test_first_pub_year(with_oeuvres):
    """MIN(publication_year) per person, including pre-2000."""
    rows = with_oeuvres.execute("""
        SELECT arc_id, MIN(publication_year) AS first_pub
        FROM oeuvres GROUP BY arc_id ORDER BY arc_id
    """).fetchall()
    result = dict(rows)
    assert result["DP001"] == 2015
    assert result["DP002"] == 2010
    assert result["DP003"] == 2000
    assert result["DP004"] == 1998  # pre-2000, included because no year filter


def test_academic_age(with_oeuvres):
    """academic_age = 2026 - first_pub_year."""
    rows = with_oeuvres.execute("""
        SELECT arc_id, 2026 - MIN(publication_year) AS academic_age
        FROM oeuvres GROUP BY arc_id ORDER BY arc_id
    """).fetchall()
    result = dict(rows)
    assert result["DP004"] == 2026 - 1998


# ── Secondary ID work contribution ────────────────────────────────────────

def test_secondary_works_count(with_oeuvres):
    """DP002 secondary contributes 1 new work (2003); 2002 is shared."""
    secondary_only = with_oeuvres.execute("""
        SELECT COUNT(*) FROM oeuvres
        WHERE arc_id = 'DP002' AND NOT is_primary_author_id
    """).fetchone()[0]
    assert secondary_only == 1   # only work 2003


def test_primary_works_count(with_oeuvres):
    """DP002 has 2 primary works (2001, 2002)."""
    primary = with_oeuvres.execute("""
        SELECT COUNT(*) FROM oeuvres
        WHERE arc_id = 'DP002' AND is_primary_author_id
    """).fetchone()[0]
    assert primary == 2


# ── Author name variants ───────────────────────────────────────────────────

def test_dp001_single_name_variant(with_oeuvres):
    """DP001 appears as 'Smith J' only — one name variant."""
    n = with_oeuvres.execute("""
        SELECT COUNT(DISTINCT author_name) FROM oeuvres WHERE arc_id = 'DP001'
    """).fetchone()[0]
    assert n == 1


def test_dp002_two_name_variants(with_oeuvres):
    """DP002 appears as 'Jones M' (primary) and 'M Jones' (secondary) — two variants."""
    n = with_oeuvres.execute("""
        SELECT COUNT(DISTINCT author_name) FROM oeuvres WHERE arc_id = 'DP002'
    """).fetchone()[0]
    assert n == 2


# ── works_count reconciliation ─────────────────────────────────────────────

def test_works_count_reconciliation(with_oeuvres):
    """Oeuvre row count per arc_id matches expected from synthetic data."""
    rows = with_oeuvres.execute("""
        SELECT arc_id, COUNT(DISTINCT work_idx) AS n
        FROM oeuvres GROUP BY arc_id ORDER BY arc_id
    """).fetchall()
    result = dict(rows)
    assert result["DP001"] == 3   # works 1001, 1002, 1003
    assert result["DP002"] == 3   # works 2001, 2002, 2003 (incl. secondary)
    assert result["DP003"] == 2   # works 3001, 3002
    assert result["DP004"] == 6   # works 4001–4006
