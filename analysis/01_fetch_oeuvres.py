"""
Fetch all OAX works for each resolved ARC person.

Scans authorships, works, and topics parquets — run once, output persisted.
No year filter: all years are fetched so that pre-2000 works contribute to
H-index baselines and first_pub_year (academic age).

Usage:
  python analysis/01_fetch_oeuvres.py [--sample 10|1000]

Output: $OUTPUT_ROOT/analysis/oeuvres.parquet
"""

import argparse
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from config.settings import PROCESSED_DATA, OPENALEX_DIR, OUTPUT_ROOT

ANALYSIS_OUT = OUTPUT_ROOT / "analysis"
ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)

RESOLVED   = str(PROCESSED_DATA / "arc_oax_resolved.parquet")
SAMPLES    = str(ANALYSIS_OUT / "samples.parquet")
AUTH_GLOB  = str(OPENALEX_DIR / "authorships" / "*.parquet")
WORK_GLOB  = str(OPENALEX_DIR / "works" / "*.parquet")
TOPIC_GLOB = str(OPENALEX_DIR / "topics" / "*.parquet")
OUT        = str(ANALYSIS_OUT / "oeuvres.parquet")


def build_author_map(con, sample_n):
    """Build (author_idx BIGINT, arc_id, is_primary) from arc_oax_resolved."""
    if sample_n:
        col = f"in_sample_{sample_n}"
        sample_clause = f"AND arc_id IN (SELECT arc_id FROM read_parquet('{SAMPLES}') WHERE {col})"
    else:
        sample_clause = ""

    con.execute(f"""
    CREATE TABLE arc_author_map AS
    WITH primary_ids AS (
        SELECT
            arc_id,
            TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) AS author_idx,
            TRUE AS is_primary
        FROM read_parquet('{RESOLVED}')
        WHERE oax_id IS NOT NULL {sample_clause}
    ),
    secondary_ids AS (
        SELECT
            arc_id,
            TRY_CAST(regexp_replace(unnest(secondary_oax_ids), 'https://openalex.org/A', '') AS BIGINT) AS author_idx,
            FALSE AS is_primary
        FROM read_parquet('{RESOLVED}')
        -- Only genuine OAX split-records share the same real person's publications.
        -- Other resolved_by values have secondary IDs that are disambiguation
        -- alternatives (different people with similar names) — exclude them.
        WHERE resolved_by IN ('oax_orcid_dedup', 'oax_topic_dedup') {sample_clause}
    )
    SELECT arc_id, author_idx, is_primary FROM primary_ids   WHERE author_idx IS NOT NULL
    UNION ALL
    SELECT arc_id, author_idx, is_primary FROM secondary_ids WHERE author_idx IS NOT NULL
    """)

    n_idx, n_arc = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT arc_id) FROM arc_author_map"
    ).fetchone()
    print(f"  arc_author_map: {n_idx} author_idx rows for {n_arc} ARC persons")


def main(sample_n=None):
    label = f"sample-{sample_n}" if sample_n else "full"
    out   = OUT if not sample_n else str(ANALYSIS_OUT / f"oeuvres_{label}.parquet")

    print(f"=== 01_fetch_oeuvres ({label}) ===")
    con = duckdb.connect()
    con.execute("SET threads TO 8")
    con.execute("SET memory_limit = '20GB'")

    t0 = time.time()

    # ── 1. Build author map ────────────────────────────────────────────────
    print("Building arc_author_map...")
    build_author_map(con, sample_n)

    # ── 2. Scan authorships ────────────────────────────────────────────────
    # Hash-join: arc_author_map (small build side) × authorships (probe side)
    print("Scanning authorships (slow)...")
    t1 = time.time()
    con.execute(f"""
    CREATE TABLE arc_authorships AS
    SELECT
        a.work_idx,
        m.arc_id,
        m.is_primary,
        a.author_name
    FROM read_parquet('{AUTH_GLOB}') a
    JOIN arc_author_map m ON m.author_idx = a.author_idx
    """)
    n_auth = con.execute("SELECT COUNT(*), COUNT(DISTINCT work_idx) FROM arc_authorships").fetchone()
    print(f"  {n_auth[0]:,} authorship rows, {n_auth[1]:,} distinct works — {time.time()-t1:.0f}s")

    # Deduplicate: same (arc_id, work_idx) from both primary and secondary → keep primary
    con.execute("""
    CREATE TABLE arc_works AS
    SELECT
        arc_id,
        work_idx,
        MAX(is_primary::INT)::BOOL AS is_primary_author_id,
        MAX(author_name) AS author_name
    FROM arc_authorships
    GROUP BY arc_id, work_idx
    """)
    n_works = con.execute("SELECT COUNT(*), COUNT(DISTINCT work_idx) FROM arc_works").fetchone()
    print(f"  After dedup: {n_works[0]:,} (arc_id, work_idx) pairs, {n_works[1]:,} unique works")

    # ── 3. Join works ──────────────────────────────────────────────────────
    print("Joining works...")
    t2 = time.time()
    con.execute(f"""
    CREATE TABLE arc_work_details AS
    SELECT
        aw.arc_id,
        aw.work_idx,
        aw.is_primary_author_id,
        aw.author_name,
        w.publication_year,
        w.cited_by_count,
        w.type,
        w.doi,
        w.title,
        w.authors_count
    FROM arc_works aw
    JOIN read_parquet('{WORK_GLOB}') w ON w.work_idx = aw.work_idx
    """)
    print(f"  Works joined — {time.time()-t2:.0f}s")

    # ── 4. Join topics (best topic per work by score) ──────────────────────
    # Filter topics to only the work_idxs we care about before ranking,
    # so we don't rank topics for all 200M+ OAX works.
    print("Joining topics...")
    t3 = time.time()
    con.execute(f"""
    CREATE TABLE arc_best_topic AS
    SELECT work_idx, topic_idx, subfield_name, field_name, domain_name
    FROM (
        SELECT
            t.work_idx, t.topic_idx, t.subfield_name, t.field_name, t.domain_name,
            ROW_NUMBER() OVER (PARTITION BY t.work_idx ORDER BY t.score DESC) AS rn
        FROM read_parquet('{TOPIC_GLOB}') t
        JOIN arc_work_details d ON d.work_idx = t.work_idx
    )
    WHERE rn = 1
    """)
    print(f"  Topics joined — {time.time()-t3:.0f}s")

    # ── 5. Final output ────────────────────────────────────────────────────
    print(f"Writing {out} ...")
    con.execute(f"""
    COPY (
        SELECT
            d.arc_id,
            d.work_idx,
            d.is_primary_author_id,
            d.author_name,
            d.publication_year,
            d.cited_by_count,
            d.type,
            d.doi,
            d.title,
            d.authors_count,
            bt.topic_idx,
            bt.subfield_name,
            bt.field_name,
            bt.domain_name
        FROM arc_work_details d
        LEFT JOIN arc_best_topic bt ON bt.work_idx = d.work_idx
        ORDER BY d.arc_id, d.publication_year
    ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    n_final = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT arc_id), COUNT(DISTINCT work_idx)
        FROM read_parquet('{out}')
    """).fetchone()
    print(
        f"Done in {time.time()-t0:.0f}s — "
        f"{n_final[0]:,} rows, {n_final[1]:,} ARC persons, "
        f"{n_final[2]:,} unique works → {out}"
    )

    # Quick year-range sanity check
    yr = con.execute(f"""
        SELECT MIN(publication_year), MAX(publication_year),
               COUNT(*) FILTER (WHERE publication_year < 2000) AS pre2000
        FROM read_parquet('{out}')
    """).fetchone()
    print(f"  Year range: {yr[0]}–{yr[1]}, pre-2000 works: {yr[2]:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, choices=[10, 1000], default=None,
                        help="Restrict to sample-10 or sample-1000 persons")
    args = parser.parse_args()
    main(sample_n=args.sample)
