"""
Accuracy and quality diagnostics for the ARC oeuvres.

Prints to stdout — no parquet output. Run after 01_fetch_oeuvres.py.

Usage:
  python analysis/02_accuracy_check.py [--sample 10|1000]

Key checks:
  1. Shared OAX author_idx across multiple ARC persons (linkage error flag)
  2. Extra works contributed by secondary (split-record) author IDs
  3. works_count reconciliation vs OAX author record
  4. Author name consistency
  5. Work type distribution
  6. Year continuity: first_pub_year, gaps, academic age
"""

import argparse
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from config.settings import PROCESSED_DATA, OPENALEX_DIR, OUTPUT_ROOT

ANALYSIS_OUT  = OUTPUT_ROOT / "analysis"
RESOLVED      = str(PROCESSED_DATA / "arc_oax_resolved.parquet")
PERSONS       = str(PROCESSED_DATA / "arc_persons.parquet")
GRANTS_FLAT   = str(PROCESSED_DATA / "grants_flat.parquet")
AUTHORS_GLOB  = str(OPENALEX_DIR / "authors" / "partition_*.parquet")
SAMPLES       = str(ANALYSIS_OUT / "samples.parquet")


def oeuvres_path(sample_n):
    if sample_n:
        return str(ANALYSIS_OUT / f"oeuvres_sample-{sample_n}.parquet")
    return str(ANALYSIS_OUT / "oeuvres.parquet")


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def main(sample_n=None):
    oeuvres = oeuvres_path(sample_n)
    label   = f"sample-{sample_n}" if sample_n else "full"
    print(f"=== 02_accuracy_check ({label}) — oeuvres: {oeuvres} ===")

    if not Path(oeuvres).exists():
        print(f"ERROR: oeuvres file not found: {oeuvres}")
        print("Run 01_fetch_oeuvres.py first" +
              (f" --sample {sample_n}" if sample_n else ""))
        sys.exit(1)

    con = duckdb.connect()

    # ── 1. Shared author_idx across ARC persons ────────────────────────────
    section("1. Shared OAX author_idx across ARC persons")
    con.execute(f"""
    CREATE TABLE author_map AS
    WITH primary_ids AS (
        SELECT arc_id,
               TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) AS author_idx,
               TRUE AS is_primary
        FROM read_parquet('{RESOLVED}')
    ),
    secondary_ids AS (
        SELECT arc_id,
               TRY_CAST(regexp_replace(unnest(secondary_oax_ids), 'https://openalex.org/A', '') AS BIGINT) AS author_idx,
               FALSE AS is_primary
        FROM read_parquet('{RESOLVED}')
        WHERE resolved_by IN ('oax_orcid_dedup', 'oax_topic_dedup')
    )
    SELECT * FROM primary_ids   WHERE author_idx IS NOT NULL
    UNION ALL
    SELECT * FROM secondary_ids WHERE author_idx IS NOT NULL
    """)

    # Check 1: same PRIMARY author_idx appearing for two arc_ids with different
    # family names — this is a genuine linkage error.
    # Many arc_ids sharing a secondary author_idx is expected (same split-record
    # merged across all clusters of a prolific researcher with many grants).
    shared = con.execute(f"""
        WITH primaries AS (
            SELECT m.author_idx, m.arc_id,
                   regexp_replace(r.oax_id, 'https://openalex.org/A', '') AS oax_numeric
            FROM author_map m
            JOIN read_parquet('{RESOLVED}') r ON r.arc_id = m.arc_id
            WHERE m.is_primary
        )
        SELECT author_idx,
               COUNT(DISTINCT arc_id) AS n_arc,
               LIST(arc_id ORDER BY arc_id) AS arc_ids
        FROM primaries
        GROUP BY author_idx
        HAVING COUNT(DISTINCT arc_id) > 1
        ORDER BY n_arc DESC
    """).fetchall()
    if shared:
        print(f"  WARNING: {len(shared)} PRIMARY author_idx values linked to >1 ARC person")
        print("  (same OAX author resolved as primary for two different ARC clusters — likely error)")
        for row in shared[:10]:
            print(f"    author_idx={row[0]}  n_arc={row[1]}  arc_ids={row[2]}")
    else:
        print("  OK — no primary author_idx shared across multiple ARC persons")

    # ── 2. Secondary ID coverage ───────────────────────────────────────────
    section("2. Extra works from secondary (split-record) author IDs")
    n_with_secondary = con.execute(f"""
        SELECT COUNT(DISTINCT arc_id)
        FROM read_parquet('{RESOLVED}')
        WHERE len(secondary_oax_ids) > 0
    """).fetchone()[0]
    print(f"  ARC persons with ≥1 secondary OAX ID: {n_with_secondary:,}")

    secondary_works = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE NOT is_primary_author_id) AS secondary_works,
            COUNT(*) AS total_works,
            ROUND(100.0 * COUNT(*) FILTER (WHERE NOT is_primary_author_id) / COUNT(*), 1) AS pct
        FROM read_parquet('{oeuvres}')
    """).fetchone()
    print(f"  Works from secondary IDs: {secondary_works[0]:,} / {secondary_works[1]:,} total ({secondary_works[2]}%)")

    # ── 3. works_count reconciliation ─────────────────────────────────────
    section("3. works_count reconciliation (OAX global works_count vs oeuvres row count)")
    # gap = oax_global_works_count - oeuvre_works
    #   negative: we have MORE works than OAX says (bad — contamination)
    #   positive: we have FEWER (expected — pre-2000, non-AU affiliations, etc.)
    # Flags saved to CSV for batch review → corrections go to manual_resolutions.csv
    recon = con.execute(f"""
        WITH oax_global AS (
            SELECT id AS oax_id,
                   works_count            AS oax_works_count,
                   summary_stats.h_index  AS oax_h_index,
                   display_name           AS oax_display_name
            FROM read_parquet('{AUTHORS_GLOB}')
        ),
        oeuvre_counts AS (
            SELECT arc_id, COUNT(DISTINCT work_idx) AS oeuvre_works
            FROM read_parquet('{oeuvres}')
            GROUP BY arc_id
        ),
        arc_names AS (
            SELECT cluster_id AS arc_id, full_names[1] AS arc_name
            FROM read_parquet('{PERSONS}')
        )
        SELECT
            r.arc_id,
            an.arc_name,
            r.oax_id,
            og.oax_display_name,
            oc.oeuvre_works,
            og.oax_works_count,
            og.oax_h_index,
            (og.oax_works_count - oc.oeuvre_works)                                    AS gap,
            ROUND(oc.oeuvre_works * 1.0 / NULLIF(og.oax_works_count, 0), 2)           AS ratio,
            r.resolved_by,
            CASE
                WHEN og.oax_works_count >= 10
                 AND oc.oeuvre_works < 0.5 * og.oax_works_count THEN 'under_coverage'
                -- Split-record cases (oax_*_dedup) combine multiple OAX records into one
                -- oeuvre; comparing against the primary record's works_count alone is
                -- meaningless, so over_coverage is suppressed for them.
                WHEN oc.oeuvre_works > og.oax_works_count + 20
                 AND r.resolved_by NOT IN ('oax_orcid_dedup', 'oax_topic_dedup') THEN 'over_coverage'
                ELSE NULL
            END AS flag
        FROM read_parquet('{RESOLVED}') r
        LEFT JOIN oeuvre_counts oc ON oc.arc_id  = r.arc_id
        LEFT JOIN oax_global    og ON og.oax_id  = r.oax_id
        LEFT JOIN arc_names     an ON an.arc_id  = r.arc_id
        WHERE r.oax_id IS NOT NULL
        ORDER BY gap ASC NULLS LAST
    """).fetchall()

    # cols: arc_id[0] arc_name[1] oax_id[2] oax_display_name[3] oeuvre_works[4]
    #        oax_works_count[5] oax_h_index[6] gap[7] ratio[8] resolved_by[9] flag[10]
    n_limit = 10 if sample_n == 10 else 20
    print(f"  Top {n_limit} by gap ascending (negative = oeuvre has MORE than OAX global):")
    print(f"  {'arc_id':32s}  {'oeuvre':>7}  {'oax_global':>10}  {'oax_h':>6}  {'ratio':>6}  {'gap':>7}  oax_name")
    for row in recon[:n_limit]:
        print(f"  {str(row[0]):32s}  {str(row[4] or '?'):>7}  {str(row[5] or '?'):>10}  "
              f"{str(row[6] or '?'):>6}  {str(row[8] or '?'):>6}  {str(row[7] or '?'):>7}  {row[3] or ''}")

    flagged = [row for row in recon if row[10] is not None]
    n_under = sum(1 for r in flagged if r[10] == 'under_coverage')
    n_over  = sum(1 for r in flagged if r[10] == 'over_coverage')
    print(f"\n  Flagged: {n_under} under-coverage (oeuvre < 50% of oax_global, wc≥10)"
          f"  |  {n_over} over-coverage (oeuvre > oax_global + 20)")

    if flagged:
        import csv
        flag_path = ANALYSIS_OUT / f"accuracy_flags_{label}.csv"
        with open(flag_path, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["arc_id", "arc_name", "oax_id", "oax_display_name",
                        "oeuvre_works", "oax_works_count", "oax_h_index",
                        "gap", "ratio", "resolved_by", "flag"])
            w.writerows(flagged)
        print(f"  Flags written → {flag_path}")
        if sample_n == 10:
            print()
            for row in flagged:
                print(f"    [{row[10]}] {str(row[0]):32s}  arc={row[1]}"
                      f"  oeuvre={row[4]}  oax={row[5]}  ratio={row[8]}"
                      f"  resolved_by={row[9]}  oax_name={row[3]}")

    # ── 4. Author name consistency (sample only) ───────────────────────────
    if sample_n == 10:
        section("4. Author names in oeuvres vs ARC name (sample-10)")
        rows = con.execute(f"""
            SELECT
                o.arc_id,
                p.full_names[1] AS arc_name,
                COUNT(DISTINCT o.author_name) AS n_name_variants,
                LIST(DISTINCT o.author_name ORDER BY o.author_name)[1:5] AS name_samples
            FROM read_parquet('{oeuvres}') o
            JOIN read_parquet('{PERSONS}') p ON p.cluster_id = o.arc_id
            GROUP BY o.arc_id, p.full_names
            ORDER BY o.arc_id
        """).fetchall()
        for row in rows:
            print(f"  {str(row[1]):35s}  variants={row[2]}  {row[3]}")

    # ── 5. Work type distribution ──────────────────────────────────────────
    section("5. Work type distribution")
    types = con.execute(f"""
        SELECT type, COUNT(*) AS n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (), 1) AS pct
        FROM read_parquet('{oeuvres}')
        GROUP BY type ORDER BY n DESC
    """).fetchall()
    for row in types:
        print(f"  {str(row[0]):30s}  {row[1]:>8,}  {row[2]:>5}%")

    # ── 6. Year continuity ────────────────────────────────────────────────
    section("6. Year continuity and academic age")
    yr_stats = con.execute(f"""
        WITH per_person AS (
            SELECT
                arc_id,
                MIN(publication_year) AS first_pub_year,
                MAX(publication_year) AS last_pub_year,
                COUNT(DISTINCT publication_year) AS active_years,
                COUNT(DISTINCT work_idx) AS total_works
            FROM read_parquet('{oeuvres}')
            WHERE publication_year IS NOT NULL
            GROUP BY arc_id
        )
        SELECT
            MIN(first_pub_year) AS earliest,
            MAX(first_pub_year) AS latest_first_pub,
            ROUND(AVG(first_pub_year), 1) AS avg_first_pub,
            ROUND(AVG(2025 - first_pub_year), 1) AS avg_academic_age_2025,
            COUNT(*) FILTER (WHERE first_pub_year > 2000) AS n_post2000_debut,
            COUNT(*) FILTER (WHERE first_pub_year < 1990) AS n_pre1990_debut
        FROM per_person
    """).fetchone()
    assert yr_stats is not None
    print(f"  first_pub_year range: {yr_stats[0]}–{yr_stats[1]}, mean: {yr_stats[2]}")
    print(f"  Mean academic age at 2025 (2025 − first_pub): {yr_stats[3]} years")
    print(f"  Persons first published after 2000: {yr_stats[4]:,}")
    print(f"  Persons first published before 1990: {yr_stats[5]:,}")

    # Year gap detection (gaps ≥3 consecutive missing years mid-career)
    gap_cases = con.execute(f"""
        WITH years_pub AS (
            SELECT DISTINCT arc_id, publication_year AS yr
            FROM read_parquet('{oeuvres}')
            WHERE publication_year BETWEEN 1980 AND 2025
        ),
        per_person AS (
            SELECT arc_id, MIN(yr) AS first_yr, MAX(yr) AS last_yr
            FROM years_pub GROUP BY arc_id
        ),
        -- generate all years in career span, flag which have publications
        spine AS (
            SELECT p.arc_id, gs.yr,
                   (y.yr IS NOT NULL) AS has_pub
            FROM per_person p
            CROSS JOIN (SELECT generate_series AS yr FROM generate_series(1980, 2025)) gs
            LEFT JOIN years_pub y ON y.arc_id = p.arc_id AND y.yr = gs.yr
            WHERE gs.yr BETWEEN p.first_yr AND p.last_yr
        ),
        -- count consecutive gap years per person
        gaps AS (
            SELECT arc_id, SUM(CASE WHEN NOT has_pub THEN 1 ELSE 0 END) AS total_gap_years,
                   MAX(CASE WHEN NOT has_pub THEN 1 ELSE 0 END) AS has_any_gap
            FROM spine
            GROUP BY arc_id
        )
        SELECT
            COUNT(*) FILTER (WHERE total_gap_years >= 3) AS persons_with_gap3,
            COUNT(*) FILTER (WHERE total_gap_years >= 5) AS persons_with_gap5,
            COUNT(*) AS total_persons
        FROM gaps
    """).fetchone()
    print(f"\n  Persons with ≥3 career gap years: {gap_cases[0]:,} / {gap_cases[2]:,}")
    print(f"  Persons with ≥5 career gap years: {gap_cases[1]:,} / {gap_cases[2]:,}")
    print("  (Gaps may indicate missing OAX records — consider extending secondary IDs)")

    # If sample-10, show per-person year profiles
    if sample_n == 10:
        print("\n  Per-person year profile (sample-10):")
        profiles = con.execute(f"""
            SELECT
                arc_id,
                MIN(publication_year) AS first_pub,
                MAX(publication_year) AS last_pub,
                COUNT(DISTINCT work_idx) AS n_works,
                COUNT(DISTINCT publication_year) AS active_years,
                (2025 - MIN(publication_year)) AS academic_age_2025
            FROM read_parquet('{oeuvres}')
            WHERE publication_year IS NOT NULL
            GROUP BY arc_id ORDER BY arc_id
        """).fetchall()
        for row in profiles:
            print(f"  {str(row[0]):30s}  {row[1]}–{row[2]}  works={row[3]:4d}"
                  f"  active_yrs={row[4]}  acad_age_2025={row[5]}")

    # ── 7. Work-level quality flags ────────────────────────────────────────
    # Separate from author flags: these are OAX work metadata errors within
    # an otherwise correctly linked oeuvre.
    #   implausible_year : publication_year < 1950 (OAX year error, e.g. 1934 not 1984)
    #   future_year      : publication_year > 2026
    #   domain_outlier   : work's domain not among the person's established domains
    #                      (≥5% of works or top-3); only fires for persons with ≥20 works
    section("7. Work-level quality flags (OAX data errors within correct oeuvres)")
    work_flags = con.execute(f"""
        WITH arc_names AS (
            SELECT cluster_id AS arc_id, full_names[1] AS arc_name, for_names
            FROM read_parquet('{PERSONS}')
        ),
        domain_counts AS (
            SELECT arc_id, domain_name,
                   COUNT(*) AS n,
                   SUM(COUNT(*)) OVER (PARTITION BY arc_id) AS total
            FROM read_parquet('{oeuvres}')
            WHERE domain_name IS NOT NULL
            GROUP BY arc_id, domain_name
        ),
        established_domains AS (
            -- domains that make up ≥5% of works OR are in person's top 3
            SELECT arc_id, domain_name
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY arc_id ORDER BY n DESC) AS rk
                FROM domain_counts
            )
            WHERE rk <= 3 OR (n * 1.0 / total) >= 0.05
        ),
        person_total AS (
            SELECT arc_id, COUNT(*) AS total_works
            FROM read_parquet('{oeuvres}')
            GROUP BY arc_id
        ),
        flagged AS (
            SELECT
                o.arc_id,
                an.arc_name,
                o.work_idx,
                an.for_names,
                o.title,
                o.publication_year,
                o.field_name,
                o.domain_name,
                pt.total_works,
                CASE
                    WHEN o.publication_year < 1950           THEN 'implausible_year'
                    WHEN o.publication_year > 2026           THEN 'future_year'
                    WHEN o.domain_name IS NOT NULL
                     AND ed.arc_id IS NULL
                     AND pt.total_works >= 20               THEN 'domain_outlier'
                    ELSE NULL
                END AS work_flag
            FROM read_parquet('{oeuvres}') o
            LEFT JOIN arc_names      an ON an.arc_id  = o.arc_id
            LEFT JOIN established_domains ed
                   ON ed.arc_id = o.arc_id AND ed.domain_name = o.domain_name
            LEFT JOIN person_total   pt ON pt.arc_id  = o.arc_id
        )
        SELECT arc_id, arc_name, work_idx, for_names, title, publication_year,
               field_name, domain_name, total_works, work_flag
        FROM flagged
        WHERE work_flag IS NOT NULL
        ORDER BY work_flag, arc_id, publication_year
    """).fetchall()

    # cols: arc_id[0] arc_name[1] work_idx[2] for_names[3] title[4]
    #        publication_year[5] field_name[6] domain_name[7] total_works[8] work_flag[9]
    n_year   = sum(1 for r in work_flags if r[9] in ('implausible_year', 'future_year'))
    n_domain = sum(1 for r in work_flags if r[9] == 'domain_outlier')
    print(f"  Year flags (implausible/future): {n_year:,}  |  Domain outliers: {n_domain:,}")

    if work_flags:
        import csv as _csv
        wf_path = ANALYSIS_OUT / f"work_flags_{label}.csv"
        with open(wf_path, "w", newline="") as fh:
            w = _csv.writer(fh)
            w.writerow(["arc_name", "work_idx", "arc_for_names",
                        "publication_year", "field_name", "domain_name",
                        "total_works", "work_flag"])
            # for_names is a list — serialise as semicolon-separated string
            for row in work_flags:
                r = list(row)
                r[3] = "; ".join(r[3]) if r[3] else ""
                # drop arc_id[0] and title[4]
                w.writerow([r[1], r[2], r[3], r[5], r[6], r[7], r[8], r[9]])
        print(f"  Flags written → {wf_path}")
        if sample_n == 10:
            print()
            for row in work_flags:
                pub = row[5]
                title_short = (row[4] or '')[:60]
                print(f"    [{row[9]}] {str(row[0]):28s}  year={pub}"
                      f"  domain={row[7] or 'None'}  title={title_short!r}")

    # ── 8. Duplicate works by normalised title + year ──────────────────────
    # Detects preprint/final-version pairs assigned to different work_idx
    # (and different author_idx split-records) in OAX.
    # Normalisation: lowercase, strip non-alphanumeric, require title ≥ 20 chars.
    section("8. Duplicate works — same normalised title + year (preprint/final)")
    dupes = con.execute(f"""
        WITH norm AS (
            SELECT
                arc_id,
                work_idx,
                publication_year,
                type,
                lower(regexp_replace(title, '[^a-zA-Z0-9]', '', 'g')) AS norm_title
            FROM read_parquet('{oeuvres}')
            WHERE title IS NOT NULL
              AND publication_year IS NOT NULL
              AND length(title) >= 20
        ),
        groups AS (
            SELECT
                arc_id,
                norm_title,
                publication_year,
                COUNT(DISTINCT work_idx)            AS n_versions,
                LIST(DISTINCT work_idx ORDER BY work_idx) AS work_idxs,
                LIST(DISTINCT type    ORDER BY type)      AS types
            FROM norm
            GROUP BY arc_id, norm_title, publication_year
            HAVING COUNT(DISTINCT work_idx) > 1
        )
        SELECT arc_id, publication_year, n_versions, work_idxs, types, norm_title
        FROM groups
        ORDER BY arc_id, publication_year
    """).fetchall()

    n_affected = len(set(r[0] for r in dupes))
    n_extra    = sum(r[2] - 1 for r in dupes)
    print(f"  Duplicate title+year groups: {len(dupes):,}  "
          f"({n_extra:,} extra copies)  affecting {n_affected:,} persons")

    if dupes:
        import csv as _csv
        dp_path = ANALYSIS_OUT / f"title_dupes_{label}.csv"
        with open(dp_path, "w", newline="") as fh:
            w = _csv.writer(fh)
            w.writerow(["arc_id", "publication_year", "n_versions",
                        "work_idxs", "types", "norm_title_60"])
            for row in dupes:
                w.writerow([row[0], row[1], row[2],
                            " | ".join(str(x) for x in row[3]),
                            " | ".join(row[4]),
                            row[5][:60]])
        print(f"  Written → {dp_path}")
        if sample_n in (10, 1000):
            for row in dupes[:10]:
                print(f"  {str(row[0]):30s}  {row[1]}  "
                      f"n={row[2]}  types={row[4]}  "
                      f"title={row[5][:50]!r}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, choices=[10, 1000], default=10)
    parser.add_argument("--full", action="store_true", help="Run against full oeuvres (no sample)")
    args = parser.parse_args()
    main(sample_n=None if args.full else args.sample)
