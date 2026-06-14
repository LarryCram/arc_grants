"""
Build field × year citation quantile table for "highly cited" normalization.

Scans all works + topics — run once, output persisted.

Output:
  citation_quantiles.parquet — (field_name, publication_year, n_works, p90, p99)

After running, re-run 03_annual_metrics.py to backfill n_highly_cited columns.

Usage:
  python analysis/04b_citation_quantiles.py
"""

import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from config.settings import OPENALEX_DIR, OUTPUT_ROOT

ANALYSIS_OUT  = OUTPUT_ROOT / "analysis"
ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)

WORK_GLOB  = str(OPENALEX_DIR / "works" / "*.parquet")
TOPIC_GLOB = str(OPENALEX_DIR / "topics" / "*.parquet")
OUT        = str(ANALYSIS_OUT / "citation_quantiles.parquet")


def main():
    print("=== 04b_citation_quantiles ===")
    con = duckdb.connect()
    con.execute("SET threads TO 8")
    con.execute("SET memory_limit = '20GB'")

    # Join works to best-topic, then compute quantiles per (field_name, year)
    # "Best topic" = highest score topic for each work
    print("Scanning works + topics (slow — all OAX works)...")
    t0 = time.time()

    con.execute(f"""
    COPY (
        WITH best_topic AS (
            SELECT work_idx, field_name
            FROM (
                SELECT work_idx, field_name,
                       ROW_NUMBER() OVER (PARTITION BY work_idx ORDER BY score DESC) AS rn
                FROM read_parquet('{TOPIC_GLOB}')
            )
            WHERE rn = 1
        ),
        work_field AS (
            SELECT
                w.work_idx,
                w.publication_year,
                w.cited_by_count,
                bt.field_name
            FROM read_parquet('{WORK_GLOB}') w
            JOIN best_topic bt ON bt.work_idx = w.work_idx
            WHERE w.publication_year BETWEEN 1990 AND 2025
              AND w.cited_by_count IS NOT NULL
              AND bt.field_name IS NOT NULL
        )
        SELECT
            field_name,
            publication_year,
            COUNT(*) AS n_works,
            QUANTILE_CONT(cited_by_count, 0.90) AS p90,
            QUANTILE_CONT(cited_by_count, 0.99) AS p99
        FROM work_field
        GROUP BY field_name, publication_year
        ORDER BY field_name, publication_year
    ) TO '{OUT}' (FORMAT PARQUET)
    """)

    elapsed = time.time() - t0
    n = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT field_name) FROM read_parquet('{OUT}')").fetchone()
    print(f"Done in {elapsed:.0f}s — {n[0]:,} (field × year) rows, {n[1]:,} fields → {OUT}")

    # Sanity: show a few rows
    sample = con.execute(f"""
        SELECT field_name, publication_year, n_works, ROUND(p90,1) AS p90, ROUND(p99,1) AS p99
        FROM read_parquet('{OUT}')
        WHERE publication_year = 2020
        ORDER BY n_works DESC
        LIMIT 8
    """).fetchall()
    print("\nSample — 2020 quantiles by field (top 8 by n_works):")
    print(f"  {'field':45s}  {'n_works':>8}  {'p90':>6}  {'p99':>6}")
    for r in sample:
        print(f"  {str(r[0]):45s}  {r[2]:>8,}  {r[3]:>6}  {r[4]:>6}")

    print("\nNow re-run 03_annual_metrics.py to backfill n_highly_cited.")


if __name__ == "__main__":
    main()
