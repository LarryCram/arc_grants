"""
Build Australian and world publication baselines from OAX parquets.

Scans authorships (for AU country_code) and works — run once, output persisted.

Outputs:
  au_works.parquet    — (work_idx, publication_year, cited_by_count, field_name)
                        for every work with ≥1 Australian-affiliated author
  au_annual.parquet   — (year, n_pubs, total_citations, h_index) for AU as a whole
  world_annual.parquet — (year, n_pubs, total_citations) for all OAX works

Usage:
  python analysis/04_au_baseline.py
"""

import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from config.settings import OPENALEX_DIR, OUTPUT_ROOT

ANALYSIS_OUT  = OUTPUT_ROOT / "analysis"
ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)

AUTH_GLOB  = str(OPENALEX_DIR / "authorships" / "*.parquet")
WORK_GLOB  = str(OPENALEX_DIR / "works" / "*.parquet")
TOPIC_GLOB = str(OPENALEX_DIR / "topics" / "*.parquet")

OUT_AU_WORKS  = str(ANALYSIS_OUT / "au_works.parquet")
OUT_AU_ANNUAL = str(ANALYSIS_OUT / "au_annual.parquet")
OUT_WORLD     = str(ANALYSIS_OUT / "world_annual.parquet")


def main():
    print("=== 04_au_baseline ===")
    con = duckdb.connect()
    con.execute("SET threads TO 8")
    con.execute("SET memory_limit = '20GB'")

    # ── 1. Australian works ────────────────────────────────────────────────
    # Find all work_idxs with at least one AU-affiliated author
    print("Scanning authorships for AU country_code (slow)...")
    t0 = time.time()
    con.execute(f"""
    CREATE TABLE au_work_ids AS
    SELECT DISTINCT work_idx
    FROM read_parquet('{AUTH_GLOB}')
    WHERE country_code = 'AU'
    """)
    n_au = con.execute("SELECT COUNT(*) FROM au_work_ids").fetchone()[0]
    print(f"  {n_au:,} AU-affiliated works found — {time.time()-t0:.0f}s")

    # Join to works for year/citation/type and to topics for field
    print("Joining works and topics for AU works...")
    t1 = time.time()
    con.execute(f"""
    CREATE TABLE au_best_topic AS
    SELECT work_idx, field_name
    FROM (
        SELECT t.work_idx, t.field_name,
               ROW_NUMBER() OVER (PARTITION BY t.work_idx ORDER BY t.score DESC) AS rn
        FROM read_parquet('{TOPIC_GLOB}') t
        JOIN au_work_ids a ON a.work_idx = t.work_idx
    )
    WHERE rn = 1
    """)

    con.execute(f"""
    COPY (
        SELECT
            w.work_idx,
            w.publication_year,
            w.cited_by_count,
            w.type,
            bt.field_name
        FROM read_parquet('{WORK_GLOB}') w
        JOIN au_work_ids a ON a.work_idx = w.work_idx
        LEFT JOIN au_best_topic bt ON bt.work_idx = w.work_idx
        WHERE w.publication_year IS NOT NULL
        ORDER BY w.publication_year
    ) TO '{OUT_AU_WORKS}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"  au_works written — {time.time()-t1:.0f}s → {OUT_AU_WORKS}")

    # ── 2. AU annual summary ───────────────────────────────────────────────
    print("Computing au_annual...")
    con.execute(f"""
    COPY (
        WITH ranked AS (
            SELECT
                publication_year AS year,
                cited_by_count,
                ROW_NUMBER() OVER (PARTITION BY publication_year ORDER BY cited_by_count DESC) AS rk
            FROM read_parquet('{OUT_AU_WORKS}')
            WHERE publication_year BETWEEN 2000 AND 2025
        )
        SELECT
            year,
            COUNT(*) AS n_pubs,
            SUM(cited_by_count) AS total_citations,
            MAX(rk) FILTER (WHERE cited_by_count >= rk) AS h_index
        FROM ranked
        GROUP BY year
        ORDER BY year
    ) TO '{OUT_AU_ANNUAL}' (FORMAT PARQUET)
    """)
    print(f"  au_annual → {OUT_AU_ANNUAL}")

    # ── 3. World annual summary ────────────────────────────────────────────
    print("Computing world_annual (scanning all works)...")
    t2 = time.time()
    con.execute(f"""
    COPY (
        SELECT
            publication_year AS year,
            COUNT(*) AS n_pubs,
            SUM(cited_by_count) AS total_citations
        FROM read_parquet('{WORK_GLOB}')
        WHERE publication_year BETWEEN 2000 AND 2025
        GROUP BY publication_year
        ORDER BY publication_year
    ) TO '{OUT_WORLD}' (FORMAT PARQUET)
    """)
    print(f"  world_annual — {time.time()-t2:.0f}s → {OUT_WORLD}")

    # Quick sanity
    au  = con.execute(f"SELECT year, n_pubs, h_index FROM read_parquet('{OUT_AU_ANNUAL}') WHERE year IN (2010, 2020)").fetchall()
    wld = con.execute(f"SELECT year, n_pubs FROM read_parquet('{OUT_WORLD}') WHERE year IN (2010, 2020)").fetchall()
    print("\nSanity check:")
    for r in au:  print(f"  AU  {r[0]}: n_pubs={r[1]:,}  h_index={r[2]}")
    for r in wld: print(f"  WLD {r[0]}: n_pubs={r[1]:,}")


if __name__ == "__main__":
    main()
