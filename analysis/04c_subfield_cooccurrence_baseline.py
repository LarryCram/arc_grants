"""
Build a subfield x subfield co-occurrence rarity baseline, cumulative by year, over the
full global OpenAlex corpus.

Adapted from a sibling project's already-validated implementation
(/home/lc/Projects/AustralianUniversities/university_data_loader/researcher/loaders/novelty.py,
Steps 1-3), built there for Uzzi-style per-work novelty scoring against the same Jul26
OpenAlex snapshot this project uses (confirmed identical underlying data, different directory
layout only). Reused here for a different purpose -- the oeuvre-decontamination plan's signal 3
("does this person's internal field-split look like a known real interdisciplinary pattern, or
a rare combination more likely explained by two blended identities?") needs exactly this same
rarity baseline, just consumed differently (per-person oeuvre-wide, not per-work).

Design choices carried over verbatim from that sibling implementation, validated there against
real diagnosed failures -- see its METHODOLOGY.md for the full incident record:
  - Subfield-level (252 categories), not topic-level (4,516). Topic-level was tried first and
    abandoned after live inspection showed it dominated by classifier noise -- e.g. a
    coffee/sleep study's "rarest combination" traced to two near-zero-score garbage leftover
    topics, never touching the work's real (confidently-identified) subject.
  - W_0 = 0.1: a subfield below 10% share of a work's total topic-score mass is dropped as
    "not strong in this work" before pairing, then survivors are renormalized to sum to 1.
  - Pair weight = SUM (not product) of the two normalized shares -- product would unfairly
    punish two genuinely-present-but-modest subfields relative to one dominant + one middling.
  - Cumulative by publication year (not binned) -- avoids look-ahead bias; a pair only counts
    as "seen before" if it had already occurred elsewhere up to and including that year.

Background corpus is the full global OpenAlex corpus, not any Australian-researcher subset --
deliberately not reusing the sibling's own oax_researcher_oeuvre population, which is
independently documented (same METHODOLOGY.md) to carry the same blended-idx contamination this
project is trying to detect; using it as the "genuine researcher" baseline would be circular.

Does NOT reuse the sibling's oax_topic_for2020_division table (a separate, unaudited
topic->FOR2020-division mapping) -- this project's FOR-code mapping is research_classification
via src/utils/for_resolve.py (see CLAUDE.md's "FOR Code Handling"); this script stays entirely
within OpenAlex's own subfield taxonomy and never touches FOR codes at all.

KNOWN LIMITATION, carried over unsolved from the sibling implementation: some pairs surfaced as
"rare" here may be classifier-leakage artifacts, not genuine rarity -- OpenAlex's topic
classifier can fire confidently on the wrong subfield when a word means something different in
another field's vocabulary (the sibling's own METHODOLOGY.md documents e.g. "storage" firing a
high-confidence but wrong subfield tag on an unrelated electrocatalysis paper). The sibling built
a `leak_ratio` diagnostic for this but never shipped it as a production filter; this script
doesn't attempt to either. Do not treat a low cumulative_weight as proof of genuine rarity without
spot-checking the actual works driving it -- this is exactly the kind of check the decontamination
plan's own verification section already calls for before trusting any flagged signal.

Simpler than the sibling's own Step 1 here: arc_grants' compact/work_topics is already exploded
to one row per (work_idx, topic_idx) with subfield_idx/subfield_name pre-joined, so no
unnest(topics)/topics.parquet join is needed the way the sibling's nested source format requires.

Only builds the baseline table (Steps 1-3 equivalent: work-subfield shares -> pair weights by
year -> cumulative). Scoring a specific person's oeuvre against this baseline is separate,
new consumer logic, not yet built here.

Output:
  subfield_cooccurrence_baseline.parquet
    (subfield_lo, subfield_hi, subfield_lo_name, subfield_hi_name, publication_year,
     cumulative_weight, cumulative_observed_pairs)

Usage:
  python analysis/04c_subfield_cooccurrence_baseline.py --sample-year 2020   # fast smoke test
  python analysis/04c_subfield_cooccurrence_baseline.py                     # full corpus
"""

import argparse
import os
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from config.settings import OPENALEX_COMPACT_DIR, OUTPUT_ROOT
from analysis.utils.dedup import MIN_PUB_YEAR, MAX_PUB_YEAR

ANALYSIS_OUT = OUTPUT_ROOT / "analysis"
ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)

WORK_GLOB  = str(OPENALEX_COMPACT_DIR / "works" / "*.parquet")
TOPIC_GLOB = str(OPENALEX_COMPACT_DIR / "work_topics" / "*.parquet")

W_0 = 0.1  # minimum per-work subfield share to be considered "strong" -- see module docstring


def _elapsed(t0: float) -> str:
    s = time.time() - t0
    return f"{s:.0f}s" if s < 60 else f"{s/60:.1f}m"


DUCKDB_TMP = "/home/lc/k/tmp/arc_grants_duckdb"  # own subfolder -- /home/lc/k/tmp is shared with other work


def main(sample_year: int | None = None):
    print("=== 04c_subfield_cooccurrence_baseline ===")
    os.makedirs(DUCKDB_TMP, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    con.execute("SET threads TO 8")
    # A prior run without an explicit temp_directory filled system swap; fixed by setting one
    # (below) plus starting conservative here. Confirmed the fix works as intended: a 12GB cap
    # then failed with a clean DuckDB OutOfMemoryException (11.1/11.1 GiB used) instead of
    # silently pressuring system memory -- safe failure mode, just too tight a limit. Raised to
    # 24GB accordingly (still well under this machine's 62GB total).
    con.execute("SET memory_limit = '24GB'")
    con.execute(f"SET temp_directory = '{DUCKDB_TMP}'")
    con.execute("SET preserve_insertion_order = false")

    # Same plausible-year bounds as analysis/utils/dedup.py -- without this, garbage years
    # (found directly: publication_year up to 2050 present in the raw corpus) pollute the
    # "latest year" used for cumulative lookups and sanity checks with near-empty stub years.
    if sample_year:
        print(f"SAMPLE MODE: restricted to publication_year = {sample_year}")
        year_filter = f"WHERE publication_year = {sample_year}"
    else:
        year_filter = f"WHERE publication_year BETWEEN {MIN_PUB_YEAR} AND {MAX_PUB_YEAR}"

    print(f"Step 1: (work_idx, subfield, normalized_share, year), W_0={W_0}...")
    t0 = time.time()
    con.execute(f"""
    CREATE TEMP TABLE wanted_works AS
    SELECT work_idx, publication_year FROM read_parquet('{WORK_GLOB}') {year_filter}
    """)
    n_wanted = con.execute("SELECT COUNT(*) FROM wanted_works").fetchone()[0]
    print(f"  {n_wanted:,} candidate works  [{_elapsed(t0)}]")

    con.execute(f"""
    CREATE TEMP TABLE work_subfield_year AS
    WITH raw_shares AS (
        SELECT t.work_idx, t.subfield_idx, t.subfield_name, SUM(t.score) AS share
        FROM read_parquet('{TOPIC_GLOB}') t
        JOIN wanted_works w ON w.work_idx = t.work_idx
        WHERE t.subfield_idx IS NOT NULL
        GROUP BY t.work_idx, t.subfield_idx, t.subfield_name
    ),
    kept AS (
        SELECT work_idx, subfield_idx, subfield_name, share
        FROM raw_shares
        WHERE share >= {W_0}
    )
    SELECT k.work_idx, k.subfield_idx, k.subfield_name,
           k.share / SUM(k.share) OVER (PARTITION BY k.work_idx) AS normalized_share,
           w.publication_year
    FROM kept k
    JOIN wanted_works w USING (work_idx)
    """)
    n_wsy = con.execute("SELECT COUNT(*) FROM work_subfield_year").fetchone()[0]
    print(f"  {n_wsy:,} (work, subfield, year) rows above W_0  [{_elapsed(t0)}]")

    print("\nStep 2: global subfield-pair weights by year (sum, not product)...")
    t0 = time.time()
    con.execute("""
    CREATE TEMP TABLE pairs_by_year AS
    SELECT
        LEAST(a.subfield_idx, b.subfield_idx) AS subfield_lo,
        GREATEST(a.subfield_idx, b.subfield_idx) AS subfield_hi,
        a.publication_year AS publication_year,
        SUM(a.normalized_share + b.normalized_share) AS weight,
        COUNT(*) AS n_pairs
    FROM work_subfield_year a
    JOIN work_subfield_year b ON a.work_idx = b.work_idx AND a.subfield_idx < b.subfield_idx
    GROUP BY 1, 2, a.publication_year
    """)
    n_pairs_year = con.execute("SELECT COUNT(*) FROM pairs_by_year").fetchone()[0]
    print(f"  {n_pairs_year:,} (pair, year) rows  [{_elapsed(t0)}]")

    print("\nStep 3: cumulative pair weight by year (avoids look-ahead bias) + subfield names...")
    t0 = time.time()
    out_name = f"subfield_cooccurrence_baseline_sample{sample_year}.parquet" if sample_year else "subfield_cooccurrence_baseline.parquet"
    OUT = str(ANALYSIS_OUT / out_name)
    con.execute(f"""
    COPY (
        WITH cum AS (
            SELECT subfield_lo, subfield_hi, publication_year,
                   SUM(weight) OVER (
                       PARTITION BY subfield_lo, subfield_hi ORDER BY publication_year
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS cumulative_weight,
                   SUM(n_pairs) OVER (
                       PARTITION BY subfield_lo, subfield_hi ORDER BY publication_year
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS cumulative_observed_pairs
            FROM pairs_by_year
        ),
        names AS (
            SELECT DISTINCT subfield_idx, subfield_name FROM work_subfield_year
        )
        SELECT c.subfield_lo, c.subfield_hi,
               nlo.subfield_name AS subfield_lo_name, nhi.subfield_name AS subfield_hi_name,
               c.publication_year, c.cumulative_weight, c.cumulative_observed_pairs
        FROM cum c
        JOIN names nlo ON nlo.subfield_idx = c.subfield_lo
        JOIN names nhi ON nhi.subfield_idx = c.subfield_hi
    ) TO '{OUT}' (FORMAT PARQUET)
    """)
    n_cum = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUT}')").fetchone()[0]
    print(f"  {n_cum:,} cumulative (pair, year) rows  [{_elapsed(t0)}] -> {OUT}")

    print("\nSanity check: known disciplinary-neighbor pairs should be common (high weight);")
    print("implausible cross-domain pairs should be rare (low weight), at the latest year available.")
    sample = con.execute(f"""
        SELECT subfield_lo_name, subfield_hi_name, publication_year, cumulative_weight, cumulative_observed_pairs
        FROM read_parquet('{OUT}')
        WHERE publication_year = (SELECT MAX(publication_year) FROM read_parquet('{OUT}'))
        ORDER BY cumulative_weight DESC
        LIMIT 5
    """).fetchall()
    print("\nMost common pairs (latest year):")
    for r in sample:
        print(f"  {r[0]:35s} x {r[1]:35s}  weight={r[3]:.1f}  n={r[4]:,}")

    rare = con.execute(f"""
        SELECT subfield_lo_name, subfield_hi_name, publication_year, cumulative_weight, cumulative_observed_pairs
        FROM read_parquet('{OUT}')
        WHERE publication_year = (SELECT MAX(publication_year) FROM read_parquet('{OUT}'))
          AND cumulative_observed_pairs >= 5
        ORDER BY cumulative_weight ASC
        LIMIT 5
    """).fetchall()
    print("\nRarest pairs with >=5 observations (latest year, avoids single-fluke noise):")
    for r in rare:
        print(f"  {r[0]:35s} x {r[1]:35s}  weight={r[3]:.1f}  n={r[4]:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-year", type=int, default=None,
                        help="Restrict to one publication_year for a fast smoke test before the full corpus run")
    args = parser.parse_args()
    main(sample_year=args.sample_year)
