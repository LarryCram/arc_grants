"""
02_profile_openalex_authors.py

Profile the OpenAlex authors entity parquet to understand what is available
for disambiguation. Covers total size, ORCID coverage, AU-affiliated authors,
name alternatives, and works-count distribution.

INPUT:
    OPENALEX_DIR/authors/*.parquet   -- OpenAlex author entity table (partitioned)

OUTPUT:
    OUTPUT_ROOT/profiles/openalex_authors_profile.txt

SCHEMA NOTES (Feb 2026 snapshot):
    id                       VARCHAR    full URL  https://openalex.org/AXXXXXXXXX
    display_name             VARCHAR
    display_name_alternatives VARCHAR[] array of name variants
    orcid                    VARCHAR    full URL  https://orcid.org/XXXX-XXXX-XXXX-XXXX  (or NULL)
    works_count              BIGINT
    cited_by_count           BIGINT
    summary_stats            STRUCT     h_index, i10_index, 2yr_mean_citedness
    last_known_institutions  STRUCT[]   {id, ror, display_name, country_code, type, lineage[]}
    affiliations             STRUCT[]   {institution STRUCT, years BIGINT[]}
    counts_by_year           STRUCT[]   {year, works_count, oa_works_count, cited_by_count}
    partition_id             INTEGER
"""

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROFILES_OUT
from src.utils.io import setup_stdout_utf8


GLOB = str(OAX_AUTHORS / "*.parquet")


def q(con, sql: str):
    return con.execute(sql).fetchone()[0]


def main():
    setup_stdout_utf8()
    con = duckdb.connect()

    print(f"Reading: {GLOB}")

    # ── Totals ───────────────────────────────────────────────────────────────
    total = q(con, f"SELECT count(*) FROM read_parquet('{GLOB}')")
    n_partitions = q(con, f"SELECT count(DISTINCT partition_id) FROM read_parquet('{GLOB}')")

    # ── ORCID coverage ───────────────────────────────────────────────────────
    n_has_orcid = q(con, f"""
        SELECT count(*) FROM read_parquet('{GLOB}')
        WHERE orcid IS NOT NULL AND orcid != ''
    """)

    # Malformed: has orcid but doesn't match expected URL pattern
    n_bad_orcid = q(con, f"""
        SELECT count(*) FROM read_parquet('{GLOB}')
        WHERE orcid IS NOT NULL AND orcid != ''
          AND NOT regexp_matches(orcid,
              '^https://orcid\\.org/\\d{{4}}-\\d{{4}}-\\d{{4}}-\\d{{3}}[0-9X]$')
    """)

    # ── AU-affiliated authors ─────────────────────────────────────────────────
    # last_known_institutions is an array; unnest and check country_code
    n_au = q(con, f"""
        SELECT count(DISTINCT id)
        FROM (
            SELECT id, unnest(last_known_institutions) AS inst
            FROM read_parquet('{GLOB}')
        )
        WHERE inst.country_code = 'AU'
    """)

    n_au_orcid = q(con, f"""
        SELECT count(DISTINCT t.id)
        FROM (
            SELECT id, unnest(last_known_institutions) AS inst
            FROM read_parquet('{GLOB}')
            WHERE orcid IS NOT NULL AND orcid != ''
        ) t
        WHERE t.inst.country_code = 'AU'
    """)

    # ── Name alternatives ─────────────────────────────────────────────────────
    n_has_alts = q(con, f"""
        SELECT count(*) FROM read_parquet('{GLOB}')
        WHERE len(display_name_alternatives) > 0
    """)

    avg_alts = q(con, f"""
        SELECT round(avg(len(display_name_alternatives)), 2)
        FROM read_parquet('{GLOB}')
    """)

    max_alts = q(con, f"""
        SELECT max(len(display_name_alternatives))
        FROM read_parquet('{GLOB}')
    """)

    # ── Works count distribution ──────────────────────────────────────────────
    wc_dist = con.execute(f"""
        SELECT
            count_bucket,
            count(*) AS authors
        FROM (
            SELECT
                CASE
                    WHEN works_count = 0  THEN '0'
                    WHEN works_count = 1  THEN '1'
                    WHEN works_count <= 5 THEN '2-5'
                    WHEN works_count <= 10 THEN '6-10'
                    WHEN works_count <= 50 THEN '11-50'
                    WHEN works_count <= 200 THEN '51-200'
                    ELSE '>200'
                END AS count_bucket,
                CASE
                    WHEN works_count = 0  THEN 0
                    WHEN works_count = 1  THEN 1
                    WHEN works_count <= 5 THEN 2
                    WHEN works_count <= 10 THEN 3
                    WHEN works_count <= 50 THEN 4
                    WHEN works_count <= 200 THEN 5
                    ELSE 6
                END AS sort_key
            FROM read_parquet('{GLOB}')
        )
        GROUP BY count_bucket, sort_key
        ORDER BY sort_key
    """).fetchall()

    # ── AU works-count distribution ───────────────────────────────────────────
    wc_au_dist = con.execute(f"""
        SELECT
            count_bucket,
            count(*) AS authors
        FROM (
            SELECT
                t.id,
                CASE
                    WHEN t.works_count = 0  THEN '0'
                    WHEN t.works_count = 1  THEN '1'
                    WHEN t.works_count <= 5 THEN '2-5'
                    WHEN t.works_count <= 10 THEN '6-10'
                    WHEN t.works_count <= 50 THEN '11-50'
                    WHEN t.works_count <= 200 THEN '51-200'
                    ELSE '>200'
                END AS count_bucket,
                CASE
                    WHEN t.works_count = 0  THEN 0
                    WHEN t.works_count = 1  THEN 1
                    WHEN t.works_count <= 5 THEN 2
                    WHEN t.works_count <= 10 THEN 3
                    WHEN t.works_count <= 50 THEN 4
                    WHEN t.works_count <= 200 THEN 5
                    ELSE 6
                END AS sort_key
            FROM (
                SELECT DISTINCT s.id, s.works_count
                FROM (
                    SELECT id, works_count, unnest(last_known_institutions) AS inst
                    FROM read_parquet('{GLOB}')
                ) s
                WHERE s.inst.country_code = 'AU'
            ) t
        )
        GROUP BY count_bucket, sort_key
        ORDER BY sort_key
    """).fetchall()

    # ── Institution type breakdown for AU authors ────────────────────────────
    au_inst_types = con.execute(f"""
        SELECT inst.type AS inst_type, count(DISTINCT id) AS authors
        FROM (
            SELECT id, unnest(last_known_institutions) AS inst
            FROM read_parquet('{GLOB}')
        )
        WHERE inst.country_code = 'AU'
        GROUP BY inst.type
        ORDER BY authors DESC
    """).fetchall()

    # ── Build profile ─────────────────────────────────────────────────────────
    lines = []
    p = lines.append

    p("=" * 60)
    p("OPENALEX AUTHORS PROFILE  (Feb 2026 snapshot)")
    p("=" * 60)

    p(f"\n── Source ──────────────────────────────────────────────")
    p(f"  Partitions:              {n_partitions:>10,}")
    p(f"  Total authors:           {total:>10,}")

    p(f"\n── ORCID Coverage (global) ─────────────────────────────")
    p(f"  Has ORCID:               {n_has_orcid:>10,}  ({100*n_has_orcid/total:.1f}%)")
    p(f"  No ORCID:                {total - n_has_orcid:>10,}  ({100*(total - n_has_orcid)/total:.1f}%)")
    p(f"  Malformed ORCID URLs:    {n_bad_orcid:>10,}")
    p(f"  Note: orcid stored as full URL (https://orcid.org/XXXX-...)")
    p(f"        strip prefix before matching ARC ORCIDs")

    p(f"\n── AU-Affiliated Authors ────────────────────────────────")
    p(f"  (last_known_institutions.country_code = 'AU')")
    p(f"  AU authors:              {n_au:>10,}  ({100*n_au/total:.1f}% of global)")
    p(f"  AU authors with ORCID:   {n_au_orcid:>10,}  ({100*n_au_orcid/n_au:.1f}% of AU)")
    p(f"  AU without ORCID:        {n_au - n_au_orcid:>10,}  ({100*(n_au - n_au_orcid)/n_au:.1f}% of AU)")

    p(f"\n  AU institution type breakdown:")
    for inst_type, cnt in au_inst_types:
        p(f"    {(inst_type or 'unknown'):<20} {cnt:>8,}")

    p(f"\n── Name Alternatives ───────────────────────────────────")
    p(f"  Has alternatives:        {n_has_alts:>10,}  ({100*n_has_alts/total:.1f}%)")
    p(f"  Avg alternatives:        {avg_alts:>10}")
    p(f"  Max alternatives:        {max_alts:>10,}")

    p(f"\n── Works Count Distribution (global) ───────────────────")
    for bucket, cnt in wc_dist:
        p(f"    {bucket:<10} {cnt:>10,}  ({100*cnt/total:.1f}%)")

    p(f"\n── Works Count Distribution (AU authors) ───────────────")
    for bucket, cnt in wc_au_dist:
        p(f"    {bucket:<10} {cnt:>10,}  ({100*cnt/n_au:.1f}%)")

    p(f"\n── Disambiguation Readiness ────────────────────────────")
    p(f"  ARC investigators (in-scope):     62,747")
    p(f"  ARC ORCID coverage:               44.5%  (~27,900 with ORCID)")
    p(f"  OAX AU authors with ORCID:        {n_au_orcid:>10,}")
    p(f"  Layer 1 candidate pool (AU+ORCID): {n_au_orcid:>9,}")

    p("\n" + "=" * 60)

    profile_text = "\n".join(lines)
    PROFILES_OUT.mkdir(parents=True, exist_ok=True)
    out_path = PROFILES_OUT / "openalex_authors_profile.txt"
    out_path.write_text(profile_text, encoding="utf-8")

    print("\n" + profile_text)
    print(f"\nProfile saved to: {out_path}")


if __name__ == "__main__":
    main()
