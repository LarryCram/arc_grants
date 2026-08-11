"""
Tests for 03_annual_metrics logic.

Validates:
  - H-index SQL formula: MAX(rank) WHERE cited_by_count >= rank
  - Specific known H-index values
  - H-index is monotonically non-decreasing over time (can't go down)
  - first_pub_year uses ALL years (pre-2000 included)
  - n_pubs counts distinct works per year (new publications only)
  - cumulative n_works grows over time
  - career_age_at_year = year - first_pub_year

The dedup/exclusion step used to build `deduped_works` is NOT re-implemented here --
it calls the real analysis.utils.dedup.create_deduped_works(), the same function
03_annual_metrics.py itself calls, so these tests exercise the actual production code
path rather than a hand-copied SQL string that can silently drift out of sync with it
(see analysis/tests/test_dedup.py for dedup/exclusion-specific tests).
"""

import pytest

from analysis.utils.dedup import create_deduped_works

H_INDEX_SQL = """
MAX(rk) FILTER (WHERE cited_by_count >= rk)
"""

MIN_PUB_YEAR = 1950
MAX_PUB_YEAR = 2026

CUMUL_SQL = """
WITH years AS (
    SELECT generate_series AS year FROM generate_series(2000, 2024)
),
first_pub AS (
    SELECT arc_id, MIN(publication_year) AS first_pub_year FROM deduped_works GROUP BY arc_id
),
cumul AS (
    SELECT
        w.arc_id, y.year, w.work_idx, w.cited_by_count,
        ROW_NUMBER() OVER (
            PARTITION BY w.arc_id, y.year
            ORDER BY w.cited_by_count DESC
        ) AS rk
    FROM deduped_works w
    CROSS JOIN years y
    WHERE w.publication_year <= y.year
)
SELECT
    c.arc_id,
    c.year,
    f.first_pub_year,
    c.year - f.first_pub_year AS career_age,
    MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h_index,
    COUNT(DISTINCT work_idx) AS n_works_cumul
FROM cumul c
JOIN first_pub f ON f.arc_id = c.arc_id
GROUP BY c.arc_id, c.year, f.first_pub_year
ORDER BY c.arc_id, c.year
"""

ANNUAL_PUBS_SQL = """
SELECT arc_id, publication_year AS year, COUNT(DISTINCT work_idx) AS n_pubs
FROM deduped_works
WHERE publication_year BETWEEN 2000 AND 2024
GROUP BY arc_id, publication_year
ORDER BY arc_id, year
"""


@pytest.fixture
def with_oeuvres(con):
    """Build oeuvres directly from synthetic tables (skipping file I/O)."""
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

    BEST_TOPIC = """
    SELECT work_idx, field_name FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY work_idx ORDER BY score DESC) AS rn
        FROM topics
    ) WHERE rn = 1
    """
    con.execute(f"""
    CREATE TABLE oeuvres AS
    WITH deduped AS (
        SELECT arc_id, work_idx,
               MAX(is_primary::INT)::BOOL AS is_primary_author_id,
               MAX(author_name) AS author_name
        FROM (
            SELECT a.work_idx, m.arc_id, m.is_primary, a.author_name
            FROM authorships a
            JOIN arc_author_map m ON m.author_idx = a.author_idx
        )
        GROUP BY arc_id, work_idx
    )
    SELECT d.arc_id, d.work_idx, d.is_primary_author_id,
           w.publication_year, w.cited_by_count, w.type, w.title, w.doi,
           bt.field_name, NULL::VARCHAR AS subfield_name, NULL::VARCHAR AS domain_name
    FROM deduped d
    JOIN works w ON w.work_idx = d.work_idx
    LEFT JOIN ({BEST_TOPIC}) bt ON bt.work_idx = d.work_idx
    """)

    create_deduped_works(con, "oeuvres", "deduped_works")
    con.execute(f"CREATE TABLE metrics AS {CUMUL_SQL}")
    return con


# ── H-index formula unit tests (isolated) ─────────────────────────────────

def test_h_index_formula_basic(con):
    """[50, 20, 5] → H=3 (all three papers have ≥3 citations)."""
    h = con.execute("""
        SELECT MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h
        FROM (VALUES (50,1),(20,2),(5,3)) t(cited_by_count, rk)
    """).fetchone()[0]
    assert h == 3


def test_h_index_formula_gap(con):
    """[50, 20, 2] → H=2 (third paper has only 2 citations but rank=3 < 3)."""
    h = con.execute("""
        SELECT MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h
        FROM (VALUES (50,1),(20,2),(2,3)) t(cited_by_count, rk)
    """).fetchone()[0]
    assert h == 2


def test_h_index_formula_all_zero(con):
    """All zero citations → H=NULL (no paper satisfies cited_by_count >= rank)."""
    h = con.execute("""
        SELECT MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h
        FROM (VALUES (0,1),(0,2),(0,3)) t(cited_by_count, rk)
    """).fetchone()[0]
    assert h is None


def test_h_index_formula_single_paper(con):
    """[1] → H=1; [0] → H=NULL."""
    h1 = con.execute("""
        SELECT MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h
        FROM (VALUES (1,1)) t(cited_by_count, rk)
    """).fetchone()[0]
    assert h1 == 1

    h0 = con.execute("""
        SELECT MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h
        FROM (VALUES (0,1)) t(cited_by_count, rk)
    """).fetchone()[0]
    assert h0 is None


def test_h_index_formula_classic_example(con):
    """Classic: [100,50,25,10,5,2,1] → H=5."""
    h = con.execute("""
        SELECT MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h
        FROM (VALUES (100,1),(50,2),(25,3),(10,4),(5,5),(2,6),(1,7)) t(cited_by_count,rk)
    """).fetchone()[0]
    assert h == 5


# ── Integration tests against synthetic oeuvres ───────────────────────────

def test_dp001_h_index_at_2021(with_oeuvres):
    """DP001 has works [50,20,5] → cumulative H at 2021 should be 3."""
    h = with_oeuvres.execute(
        "SELECT h_index FROM metrics WHERE arc_id='DP001' AND year=2021"
    ).fetchone()[0]
    assert h == 3


def test_dp001_h_index_at_2018(with_oeuvres):
    """DP001 by end of 2018 has works [50,20] → H=2."""
    h = with_oeuvres.execute(
        "SELECT h_index FROM metrics WHERE arc_id='DP001' AND year=2018"
    ).fetchone()[0]
    assert h == 2


def test_dp001_h_index_at_2015(with_oeuvres):
    """DP001 by end of 2015 has only work 1001 (cited=50) → H=1."""
    h = with_oeuvres.execute(
        "SELECT h_index FROM metrics WHERE arc_id='DP001' AND year=2015"
    ).fetchone()[0]
    assert h == 1


def test_dp001_h_index_before_first_pub(with_oeuvres):
    """DP001 first publishes in 2015 → no metrics row exists for year 2014."""
    row = with_oeuvres.execute(
        "SELECT h_index FROM metrics WHERE arc_id='DP001' AND year=2014"
    ).fetchone()
    assert row is None  # person absent from metrics until their first publication year


def test_h_index_monotone(with_oeuvres):
    """H-index must never decrease from one year to the next."""
    rows = with_oeuvres.execute("""
        SELECT arc_id, year, h_index FROM metrics
        WHERE h_index IS NOT NULL
        ORDER BY arc_id, year
    """).fetchall()
    by_person = {}
    for arc_id, year, h in rows:
        by_person.setdefault(arc_id, []).append((year, h))
    for arc_id, series in by_person.items():
        for i in range(1, len(series)):
            prev_h = series[i-1][1] or 0
            curr_h = series[i][1] or 0
            assert curr_h >= prev_h, (
                f"{arc_id} H-index decreased: year {series[i-1][0]}={prev_h} "
                f"→ year {series[i][0]}={curr_h}"
            )


def test_dp004_first_pub_year_pre2000(with_oeuvres):
    """DP004 has work 4001 from 1998 → first_pub_year=1998 (within plausible range 1950–2026)."""
    fpy = with_oeuvres.execute(
        "SELECT MIN(first_pub_year) FROM metrics WHERE arc_id='DP004'"
    ).fetchone()[0]
    assert fpy == 1998


def test_career_age_at_year(with_oeuvres):
    """DP004 first_pub=1998; career_age at year 2020 = 22."""
    age = with_oeuvres.execute(
        "SELECT career_age FROM metrics WHERE arc_id='DP004' AND year=2020"
    ).fetchone()[0]
    assert age == 22


def test_n_works_cumul_grows(with_oeuvres):
    """n_works_cumul for DP001 grows: 1 (2015), 2 (2018), 3 (2021)."""
    rows = with_oeuvres.execute("""
        SELECT year, n_works_cumul FROM metrics
        WHERE arc_id='DP001' AND year IN (2015,2018,2021)
        ORDER BY year
    """).fetchall()
    assert dict(rows) == {2015: 1, 2018: 2, 2021: 3}


def test_n_pubs_per_year(with_oeuvres):
    """Annual n_pubs for DP001: 1 in 2015, 1 in 2018, 1 in 2021, 0 in other years."""
    rows = with_oeuvres.execute(ANNUAL_PUBS_SQL).fetchall()
    dp001 = {year: n for arc_id, year, n in rows if arc_id == "DP001"}
    assert dp001.get(2015) == 1
    assert dp001.get(2018) == 1
    assert dp001.get(2021) == 1
    assert dp001.get(2020) is None   # no publication that year → not in table


# ── Title-based deduplication ─────────────────────────────────────────────

# ── Title-based dedup, against the real create_deduped_works() -- see
# analysis/tests/test_dedup.py for the fuller dedup/exclusion-specific suite. These two
# stay here since they're the same scenario 03_annual_metrics.py's own docstring/history
# used as its worked example (preprint+article pair, plus one genuinely different title).

def _title_dedup_fixture(con):
    con.execute("""
        CREATE TABLE title_dedup_src AS SELECT * FROM (VALUES
            ('P1', 1, 2022, 5,  'preprint', 'Quantum entanglement revisited in modern physics', NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR),
            ('P1', 2, 2023, 20, 'article',  'Quantum entanglement revisited in modern physics', NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR),
            ('P1', 3, 2021, 10, 'article',  'A completely different paper about something else', NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)
        ) t(arc_id, work_idx, publication_year, cited_by_count, type, title, field_name, subfield_name, domain_name, doi)
    """)
    create_deduped_works(con, "title_dedup_src", "title_dedup_out")
    return con.execute("SELECT arc_id, work_idx, publication_year, cited_by_count FROM title_dedup_out ORDER BY work_idx").fetchall()


def test_title_dedup_keeps_article_with_earliest_year_and_summed_citations(con):
    """Preprint (2022, c=5) + article (2023, c=20) same title → keep article's work_idx,
    year = 2022 (earliest), citations = 25 (summed, not article-only -- each version's
    citations are real and additive, see analysis/utils/dedup.py)."""
    rows = _title_dedup_fixture(con)
    assert len(rows) == 2
    quantum = next(r for r in rows if r[1] == 2)
    assert quantum[2] == 2022
    assert quantum[3] == 25


def test_title_dedup_unique_title_unchanged(con):
    """Paper with unique title is unaffected by title dedup."""
    rows = _title_dedup_fixture(con)
    other = next(r for r in rows if r[1] == 3)
    assert other[2] == 2021
    assert other[3] == 10
