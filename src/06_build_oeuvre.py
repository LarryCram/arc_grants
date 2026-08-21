"""
src/06_build_oeuvre.py

Builds and persists the AwardsCIF population, then runs the oeuvre-build pipeline through
Stage 3 (field filter) and the per-candidate subfield+HEP signal computation (2026-08-16
reframing). Stage 4 (coauthor pull, gated at coauthor_count < 30) and the Weights stage are not
yet wired into this driver (see the plan file's "Not addressed by this fix" section).

AwardsCIF() is the pipeline going forward -- a refactor of 01_prepare_arc.py/03_link_arc_oax.py/
04_resolve_links.py onto a proper dataclass, not a side experiment -- so this script's output
lands in PROCESSED_DATA alongside arc_persons.parquet, not a separate location.

2026-08-21: step 1 used to be build_awards_cif_population() -- rebuilding the ENTIRE ARC-side
population from raw data on every run, purely to reach the two OAX-enrichment steps this script
actually needed, and silently overwriting 01_prepare_arc.py's own output at the same path in the
process (the incident that led to 01/03b's ARC-only/OAX-enriched split -- see their docstrings).
This script now just loads 03b_enrich_awards_cif.py's already-enriched output directly.

Steps:
  1. load_awards_cif(AWARDS_CIF_PARQUET) -- 03b_enrich_awards_cif.py's own output, freshness-
     checked against it below rather than rebuilt here
  2. fetch_and_filter_stage1() -- PROCESSED_DATA/oeuvre_stage1_{survivors,exclusions}.parquet
  3. apply_field_filter_stage3() -- PROCESSED_DATA/oeuvre_stage3_{survivors,exclusions}.parquet
  4. compute_subfield_hep_signals() -- PROCESSED_DATA/oeuvre_subfield_hep_signals.parquet

Usage:
  .venv/bin/python src/06_build_oeuvre.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from config.settings import DUCKDB_TMP_DIR, PROCESSED_DATA
from src.utils.pipeline_freshness import assert_fresh
from src.utils.awards_cif import load_awards_cif, AWARDS_CIF_PARQUET
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
    # AWARDS_CIF_PARQUET is 03b_enrich_awards_cif.py's output, not rebuilt here -- refuse to
    # run oeuvre-building against a stale enrichment (e.g. 01/03/03b edited or rerun since).
    assert_fresh(
        "06_build_oeuvre (awards_cif.parquet)",
        outputs=[AWARDS_CIF_PARQUET],
        inputs=[PROCESSED_DATA / "arc_oax_links.parquet"],
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

    con.close()
    print(f"=== Done, total [{_elapsed(t0)}] ===")


if __name__ == "__main__":
    main()
