"""
src/06_build_oeuvre.py

Builds and persists the AwardsCIF population, then runs the oeuvre-build pipeline through
Stage 3 (field filter) -- the 2026-08-15 full-scale-OOM fix. Currently stops after Stage 3;
Stage 4 (coauthor pull, gated at coauthor_count < 30) and the Weights stage are not yet wired
into this driver (see the plan file's "Not addressed by this fix" section).

AwardsCIF() is the pipeline going forward -- a refactor of 01_prepare_arc.py/03_link_arc_oax.py/
04_resolve_links.py onto a proper dataclass, not a side experiment -- so this script's output
lands in PROCESSED_DATA alongside arc_persons.parquet, not a separate location.

Steps:
  1. build_awards_cif_population() -- the full step-1 chain (load_award_cif_items -> cluster_items
     -> refine_clusters -> set_aside_indigenous_research -> populate_oax_candidates ->
     dedup_oax_candidates -> compute_orcid_for -> compute_gap_candidates -> compute_reliability)
  2. persist_awards_cif() -- PROCESSED_DATA/awards_cif.parquet (never persisted before this)
  3. fetch_and_filter_stage1() -- PROCESSED_DATA/oeuvre_stage1_{survivors,exclusions}.parquet
  4. apply_field_filter_stage3() -- PROCESSED_DATA/oeuvre_stage3_{survivors,exclusions}.parquet

Usage:
  .venv/bin/python src/06_build_oeuvre.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from config.settings import DUCKDB_TMP_DIR
from src.utils.awards_cif import build_awards_cif_population, persist_awards_cif, AWARDS_CIF_PARQUET
from src.utils.oeuvre_build import (
    fetch_and_filter_stage1,
    apply_field_filter_stage3,
    STAGE1_SURVIVORS,
    STAGE1_EXCLUSIONS,
    STAGE3_SURVIVORS,
    STAGE3_EXCLUSIONS,
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

    print("=== 06_build_oeuvre: Step 1 -- build_awards_cif_population ===")
    clusters = build_awards_cif_population(con)
    print(f"  {len(clusters):,} AwardsCIF built [{_elapsed(t0)}]")

    print("=== Step 2 -- persist_awards_cif ===")
    persist_awards_cif(clusters, path=AWARDS_CIF_PARQUET)
    print(f"  [{_elapsed(t0)}]")

    print("=== Step 3 -- fetch_and_filter_stage1 ===")
    fetch_and_filter_stage1(
        clusters, con=con, path=STAGE1_SURVIVORS, exclusions_path=STAGE1_EXCLUSIONS,
    )
    print(f"  [{_elapsed(t0)}]")

    print("=== Step 4 -- apply_field_filter_stage3 ===")
    apply_field_filter_stage3(
        clusters, con=con, path=STAGE3_SURVIVORS, exclusions_path=STAGE3_EXCLUSIONS,
        stage1_path=STAGE1_SURVIVORS,
    )
    print(f"  [{_elapsed(t0)}]")

    con.close()
    print(f"=== Done, total [{_elapsed(t0)}] ===")


if __name__ == "__main__":
    main()
