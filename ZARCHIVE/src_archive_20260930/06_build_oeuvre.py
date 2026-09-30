"""
src/06_build_oeuvre.py

Builds and persists the AwardsCIF population, then runs the oeuvre-build pipeline through
Stage 3 (field filter) and the per-candidate subfield+HEP signal computation (2026-08-16
reframing). "Stage 4 (coauthor pull, gated at coauthor_count < 30)" in this sentence used to name
a still-pending stage -- corrected 2026-08-25: this was `score_identity_clusters()`
(shared-coauthor/shared-institution union-find grouping), which was actually built, tested, and
then dropped entirely on 2026-08-15 as confirmed unreliable by construction (see
`oeuvre_build.py::build_oeuvre()`'s own docstring), not merely left unconnected. What IS wired in
and working: `score_institution_coherence()` and `score_coauthor_arc_corroboration()` (the
ARC-co-investigator-as-coauthor check). The genuinely still-open piece is combining those two
real signals into a calibrated weighted inclusion score (roadmap step 4) -- "needs empirical
calibration against known contamination cases, not done" per that same docstring.

AwardsCIF() is the pipeline going forward -- a refactor of 01_prepare_arc.py/03_link_arc_oax.py/
04_resolve_links.py onto a proper dataclass, not a side experiment -- so this script's output
lands in PROCESSED_DATA alongside arc_persons.parquet, not a separate location.

2026-08-21: step 1 used to be build_awards_cif_population() -- rebuilding the ENTIRE ARC-side
population from raw data on every run, purely to reach the two OAX-enrichment steps this script
actually needed, and silently overwriting 01_prepare_arc.py's own output at the same path in the
process (the incident that led to 01's ARC-only/OAX-enriched split -- see its own docstring).
This script now just loads the enriched population's already-built output directly.

2026-08-25: that output used to come from a separate script, 03b_enrich_awards_cif.py -- archived
(ZARCHIVE/src_archive_20260825/) once 04_resolve_links.py absorbed its job, fixing a confirmed
165/22,563-case disagreement between 03b's own independently-computed oax_candidates and
04's own independently-computed resolution. AWARDS_CIF_PARQUET is now 04_resolve_links.py's
output, not 03b's -- see that file's own docstring for the consolidated design.

Steps:
  1. load_awards_cif(AWARDS_CIF_PARQUET) -- 04_resolve_links.py's own output, freshness-
     checked against it below rather than rebuilt here
  2. fetch_and_filter_stage1() -- PROCESSED_DATA/oeuvre_stage1_{survivors,exclusions}.parquet
  3. apply_field_filter_stage3() -- PROCESSED_DATA/oeuvre_stage3_{survivors,exclusions}.parquet
  4. compute_subfield_hep_signals() -- PROCESSED_DATA/oeuvre_subfield_hep_signals.parquet
  5. compute_and_persist_idf_tables() -- PROCESSED_DATA/work_tf_*.parquet (fast, in-process;
     the prerequisite piling's feature vectors are weighted by).

Piling itself (assigning each Stage-3 survivor work to a pile, then channeling piles to ACIFs)
is NOT run by this script -- see run_piling.sh at the repo root. 2026-08-26/27: first wired in
as a step inside this script's own process (persist_piling_results(), one shared DuckDB
connection looping over every batch sequentially), then pulled back out the very next day after
three consecutive crashes at full population scale (10.4M Stage-3 survivor rows, up from the
2.55M this was originally built against) -- a handful of common-name mega-pools (WeiZhang,
YanYan, JunWang, all 60,000-80,000+ works) made a single long-lived process both memory-risky
(one crash took the whole IDE down with it) and non-resumable (every kill meant restarting from
batch 1, since piling output was one growing file rewritten in full on every batch). Rebuilt as
a genuinely separate, resumable, parallel job queue instead -- see work_piling.py's own
"Persisted pipeline stage" section for the full account, and CLAUDE.md.

Usage:
  .venv/bin/python src/06_build_oeuvre.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from config.settings import DUCKDB_TMP_DIR, PROCESSED_DATA
from src.utils.pipeline_freshness import assert_fresh, AWARDS_CIF_SOURCE
from src.utils.awards_cif import load_awards_cif, AWARDS_CIF_PARQUET

_RESOLVE_LINKS_SOURCE = Path(__file__).resolve().parent / "04_resolve_links.py"
from src.utils.oeuvre_build import (
    fetch_and_filter_stage1,
    apply_field_filter_stage3,
    compute_subfield_hep_signals,
    STAGE1_SURVIVORS,
    STAGE1_EXCLUSIONS,
    STAGE3_SURVIVORS,
    STAGE3_EXCLUSIONS,
    STAGE_SUBFIELD_HEP_SIGNALS,
)
from src.utils.work_piling import compute_and_persist_idf_tables


def _elapsed(t0: float) -> str:
    s = time.time() - t0
    return f"{s:.0f}s" if s < 60 else f"{s / 60:.1f}m"


def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    con.execute("SET threads TO 8")
    con.execute("SET memory_limit = '24GB'")
    con.execute(f"SET temp_directory = '{DUCKDB_TMP_DIR}'")

    print("=== 06_build_oeuvre: Step 1 -- load enriched AwardsCIF population ===")
    # AWARDS_CIF_PARQUET is 04_resolve_links.py's output, not rebuilt here -- refuse to
    # run oeuvre-building against a stale enrichment (e.g. 01/03/04 edited or rerun since).
    assert_fresh(
        "06_build_oeuvre (awards_cif.parquet)",
        outputs=[AWARDS_CIF_PARQUET],
        inputs=[
            PROCESSED_DATA / "arc_oax_links.parquet", _RESOLVE_LINKS_SOURCE, AWARDS_CIF_SOURCE,
        ],
    )
    clusters = load_awards_cif(AWARDS_CIF_PARQUET)
    print(f"  {len(clusters):,} AwardsCIF loaded [{_elapsed(t0)}]")

    print("=== Step 2 -- fetch_and_filter_stage1 ===")
    fetch_and_filter_stage1(
        clusters, con=con, path=STAGE1_SURVIVORS, exclusions_path=STAGE1_EXCLUSIONS,
    )
    print(f"  [{_elapsed(t0)}]")

    print("=== Step 3 -- apply_field_filter_stage3 ===")
    apply_field_filter_stage3(
        clusters, con=con, path=STAGE3_SURVIVORS, exclusions_path=STAGE3_EXCLUSIONS,
        stage1_path=STAGE1_SURVIVORS,
    )
    print(f"  [{_elapsed(t0)}]")

    print("=== Step 4 -- compute_subfield_hep_signals ===")
    compute_subfield_hep_signals(
        clusters, con=con, path=STAGE_SUBFIELD_HEP_SIGNALS, stage3_path=STAGE3_SURVIVORS,
    )
    print(f"  [{_elapsed(t0)}]")

    print("=== Step 5 -- compute_and_persist_idf_tables ===")
    compute_and_persist_idf_tables(con=con, survivors_path=STAGE3_SURVIVORS)
    print(f"  [{_elapsed(t0)}]")
    print("  Piling itself is a separate step -- see run_piling.sh")

    con.close()
    print(f"=== Done, total [{_elapsed(t0)}] ===")


if __name__ == "__main__":
    main()
