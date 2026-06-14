"""
Compute per-person, per-year bibliometric metrics from oeuvres.parquet.

No OAX scan needed — reads only the persisted oeuvres parquet.

Outputs:
  annual_metrics.parquet  — arc_id × year (2000–2025), cumulative and annual stats
  collab_metrics.parquet  — arc_id × year × country/institution co-author counts

H-index note: computed cumulatively for works published up to and including each year,
using Feb-2026 snapshot citation counts. This gives the "H-index as of year Y based on
snapshot citations" — not the true historical H-index which would require year-by-year
citation data from the references parquet.

n_highly_cited: populated as NULL until citation_quantiles.parquet is built by
04b_citation_quantiles.py, then backfilled.

Usage:
  python analysis/03_annual_metrics.py [--sample 10|1000]
"""

import argparse
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from config.settings import PROCESSED_DATA, OPENALEX_DIR, OUTPUT_ROOT

ANALYSIS_OUT   = OUTPUT_ROOT / "analysis"
PERSONS        = str(PROCESSED_DATA / "arc_persons.parquet")
AUTH_GLOB      = str(OPENALEX_DIR / "authorships" / "*.parquet")
QUANTILES      = str(ANALYSIS_OUT / "citation_quantiles.parquet")
OUT_ANNUAL     = str(ANALYSIS_OUT / "annual_metrics.parquet")
OUT_COLLAB     = str(ANALYSIS_OUT / "collab_metrics.parquet")

# Works with publication_year outside this range are treated as OAX data errors
# and excluded from all metrics (but remain in oeuvres.parquet for provenance).
MIN_PUB_YEAR = 1950
MAX_PUB_YEAR = 2026


def oeuvres_path(sample_n):
    if sample_n:
        return str(ANALYSIS_OUT / f"oeuvres_sample-{sample_n}.parquet")
    return str(ANALYSIS_OUT / "oeuvres.parquet")


def main(sample_n=None):
    label   = f"sample-{sample_n}" if sample_n else "full"
    oeuvres = oeuvres_path(sample_n)
    out_ann = OUT_ANNUAL if not sample_n else str(ANALYSIS_OUT / f"annual_metrics_{label}.parquet")
    out_col = OUT_COLLAB if not sample_n else str(ANALYSIS_OUT / f"collab_metrics_{label}.parquet")

    print(f"=== 03_annual_metrics ({label}) ===")

    con = duckdb.connect()
    con.execute("SET threads TO 8")
    con.execute("SET memory_limit = '16GB'")

    has_quantiles = Path(QUANTILES).exists()
    if not has_quantiles:
        print("  Note: citation_quantiles.parquet not found — n_highly_cited will be NULL")
        print("        Run 04b_citation_quantiles.py then re-run this script to fill it in.")

    # ── Annual metrics ─────────────────────────────────────────────────────
    print("Computing annual metrics...")

    # first_pub_year: use plausible years only so academic age isn't skewed by
    # OAX data errors (e.g. works dated 1767 or 1934 for living researchers).
    con.execute(f"""
    CREATE TABLE first_pub AS
    SELECT arc_id, MIN(publication_year) AS first_pub_year
    FROM read_parquet('{oeuvres}')
    WHERE publication_year BETWEEN {MIN_PUB_YEAR} AND {MAX_PUB_YEAR}
    GROUP BY arc_id
    """)

    # Deduplicated works — two levels:
    # 1. Exclude implausible years (OAX data errors).
    # 2. Title-based dedup: same normalised title within one person's oeuvre →
    #    keep the most recent publication_year (preprint→final), then prefer
    #    article > review > book-chapter > book > preprint, then lowest work_idx.
    #    Titles < 20 chars are not deduplicated (too short to be distinctive).
    con.execute(f"""
    CREATE TABLE deduped_works AS
    WITH base AS (
        SELECT DISTINCT
            arc_id, work_idx, publication_year, cited_by_count,
            type, field_name, subfield_name, domain_name, title
        FROM read_parquet('{oeuvres}')
        WHERE publication_year BETWEEN {MIN_PUB_YEAR} AND {MAX_PUB_YEAR}
    ),
    ranked AS (
        SELECT *,
            -- earliest year across all versions of this title = when paper was first public
            MIN(publication_year) OVER (
                PARTITION BY arc_id,
                    CASE WHEN title IS NOT NULL AND length(title) >= 20
                         THEN lower(regexp_replace(title, '[^a-zA-Z0-9]', '', 'g'))
                         ELSE CAST(work_idx AS VARCHAR)
                    END
            ) AS first_pub_year_for_title,
            ROW_NUMBER() OVER (
                PARTITION BY arc_id,
                    CASE WHEN title IS NOT NULL AND length(title) >= 20
                         THEN lower(regexp_replace(title, '[^a-zA-Z0-9]', '', 'g'))
                         ELSE CAST(work_idx AS VARCHAR)
                    END
                ORDER BY
                    CASE type
                        WHEN 'article'      THEN 1
                        WHEN 'review'       THEN 2
                        WHEN 'book-chapter' THEN 3
                        WHEN 'book'         THEN 4
                        WHEN 'preprint'     THEN 5
                        ELSE                     6
                    END ASC,
                    work_idx ASC
            ) AS title_rn
        FROM base
    )
    SELECT arc_id, work_idx,
           first_pub_year_for_title AS publication_year,
           cited_by_count, type, field_name, subfield_name, domain_name
    FROM ranked WHERE title_rn = 1
    """)
    n_base = con.execute("SELECT COUNT(*) FROM deduped_works").fetchone()[0]
    print(f"  deduped_works: {n_base:,} rows after year filter + title dedup")

    # Cumulative H-index and citation totals for each arc_id × year
    # Using CROSS JOIN with years spine — each person × each year gets all their
    # works published up to that year, ranked by citations.
    con.execute("""
    CREATE TABLE annual AS
    WITH years AS (
        SELECT generate_series AS year
        FROM generate_series(2000, 2025)
    ),
    -- cumulative works: each work × each year from publication_year to 2025
    cumul AS (
        SELECT
            w.arc_id,
            y.year,
            w.work_idx,
            w.cited_by_count,
            w.field_name,
            ROW_NUMBER() OVER (
                PARTITION BY w.arc_id, y.year
                ORDER BY w.cited_by_count DESC
            ) AS rk
        FROM deduped_works w
        CROSS JOIN years y
        WHERE w.publication_year <= y.year
    ),
    -- H-index: max rank where cited_by_count >= rank
    h_idx AS (
        SELECT arc_id, year,
               MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h_index,
               COUNT(DISTINCT work_idx) AS n_works_cumul,
               SUM(cited_by_count) AS total_citations_cumul
        FROM cumul
        GROUP BY arc_id, year
    ),
    -- Annual new publications (works published in that specific year)
    annual_pubs AS (
        SELECT arc_id, publication_year AS year,
               COUNT(DISTINCT work_idx) AS n_pubs,
               SUM(cited_by_count) AS n_citations_snapshot,
               MODE(field_name) AS top_field
        FROM deduped_works
        WHERE publication_year BETWEEN 2000 AND 2025
        GROUP BY arc_id, publication_year
    )
    SELECT
        h.arc_id,
        h.year,
        f.first_pub_year,
        (h.year - f.first_pub_year) AS career_age_at_year,
        COALESCE(p.n_pubs, 0)                AS n_pubs,
        COALESCE(p.n_citations_snapshot, 0)  AS n_citations_snapshot,
        h.h_index,
        h.n_works_cumul,
        h.total_citations_cumul,
        p.top_field,
        NULL::INTEGER AS n_highly_cited       -- filled by backfill step below
    FROM h_idx h
    LEFT JOIN annual_pubs p ON p.arc_id = h.arc_id AND p.year = h.year
    LEFT JOIN first_pub   f ON f.arc_id = h.arc_id
    ORDER BY h.arc_id, h.year
    """)

    # Backfill n_highly_cited from quantiles if available
    if has_quantiles:
        print("  Backfilling n_highly_cited from citation_quantiles.parquet...")
        con.execute(f"""
        CREATE TABLE quantiles AS
        SELECT field_name, publication_year, p90, p99
        FROM read_parquet('{QUANTILES}')
        """)
        con.execute("""
        CREATE TABLE annual_hc AS
        SELECT
            w.arc_id,
            y.year AS metric_year,
            COUNT(*) FILTER (WHERE w.cited_by_count >= q.p90) AS n_hc_p90,
            COUNT(*) FILTER (WHERE w.cited_by_count >= q.p99) AS n_hc_p99
        FROM deduped_works w
        CROSS JOIN (SELECT generate_series AS year FROM generate_series(2000,2025)) y
        LEFT JOIN quantiles q
            ON q.field_name = w.field_name
            AND q.publication_year = w.publication_year
        WHERE w.publication_year <= y.year
        GROUP BY w.arc_id, y.year
        """)
        # Replace the NULL column
        con.execute("""
        CREATE TABLE annual2 AS
        SELECT
            a.*  EXCLUDE (n_highly_cited),
            COALESCE(hc.n_hc_p90, 0) AS n_highly_cited,
            COALESCE(hc.n_hc_p99, 0) AS n_highly_cited_p99
        FROM annual a
        LEFT JOIN annual_hc hc ON hc.arc_id = a.arc_id AND hc.metric_year = a.year
        """)
        con.execute(f"DROP TABLE annual")
        con.execute(f"ALTER TABLE annual2 RENAME TO annual")

    con.execute(f"""
    COPY (SELECT * FROM annual) TO '{out_ann}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT arc_id) FROM read_parquet('{out_ann}')").fetchone()
    print(f"  annual_metrics: {n[0]:,} rows, {n[1]:,} persons → {out_ann}")

    # ── Collaboration metrics ──────────────────────────────────────────────
    # For each ARC work, find all co-authors (other author_idx on same work_idx)
    # from the authorships parquet, extract their institution and country.
    print("Computing collaboration metrics (requires authorships scan)...")

    # Get ARC work_idxs (2000-2025 only for collab metrics)
    con.execute("""
    CREATE TABLE arc_work_ids AS
    SELECT DISTINCT arc_id, work_idx, publication_year AS year
    FROM deduped_works
    WHERE publication_year BETWEEN 2000 AND 2025
    """)

    # Need the arc_author_map to exclude the ARC person's own authorship row
    resolved_file = str(PROCESSED_DATA / "arc_oax_resolved.parquet")
    con.execute(f"""
    CREATE TABLE arc_author_idxs AS
    WITH primary_ids AS (
        SELECT TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) AS author_idx
        FROM read_parquet('{resolved_file}')
    ),
    secondary_ids AS (
        SELECT TRY_CAST(regexp_replace(unnest(secondary_oax_ids), 'https://openalex.org/A', '') AS BIGINT) AS author_idx
        FROM read_parquet('{resolved_file}')
    )
    SELECT author_idx FROM primary_ids   WHERE author_idx IS NOT NULL
    UNION
    SELECT author_idx FROM secondary_ids WHERE author_idx IS NOT NULL
    """)

    con.execute(f"""
    COPY (
        WITH co_auths AS (
            SELECT
                aw.arc_id,
                aw.year,
                a.country_code,
                a.institution_name,
                a.ror,
                COUNT(DISTINCT aw.work_idx) AS n_shared_works
            FROM arc_work_ids aw
            JOIN read_parquet('{AUTH_GLOB}') a ON a.work_idx = aw.work_idx
            WHERE a.author_idx NOT IN (SELECT author_idx FROM arc_author_idxs)
              AND a.country_code IS NOT NULL
            GROUP BY aw.arc_id, aw.year, a.country_code, a.institution_name, a.ror
        )
        SELECT * FROM co_auths ORDER BY arc_id, year, n_shared_works DESC
    ) TO '{out_col}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n2 = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT arc_id) FROM read_parquet('{out_col}')").fetchone()
    print(f"  collab_metrics: {n2[0]:,} rows, {n2[1]:,} persons → {out_col}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, choices=[10, 1000], default=None)
    args = parser.parse_args()
    main(sample_n=args.sample)
