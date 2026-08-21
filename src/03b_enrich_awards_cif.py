"""
src/03b_enrich_awards_cif.py

Enriches the checkable ARC-only population (awards_cif_arc_only.parquet, produced by
01_prepare_arc.py) with OAX candidates -- reads arc_oax_links.parquet (03_link_arc_oax.py's own
output) and dedups the OAX side, producing the final, OAX-enriched awards_cif.parquet that
06_build_oeuvre.py, work_piling.py, dossier_build.py, and 01a_diagnose.py all read.

Deliberately its own stage, separate from both 01 and 06:
  - 01_prepare_arc.py must stay OAX-independent (see its own docstring for the 2026-08-21
    incident this avoids -- a genuine over-bundling bug, not a true circular dependency, but one
    that meant no ARC-only checkable checkpoint ever actually existed on disk).
  - 06_build_oeuvre.py used to rebuild the ENTIRE population from raw ARC data on every run
    (via the old build_awards_cif_population()) purely to reach this one enrichment step --
    wasteful, and it silently overwrote 01's output at the same path. It now just loads this
    stage's output.

Output: awards_cif.parquet (PROCESSED_DATA) -- the final, OAX-candidate-enriched population.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.utils.pipeline_freshness import assert_fresh
from src.utils.awards_cif import (
    load_awards_cif,
    enrich_with_oax_candidates,
    persist_awards_cif,
    ARC_ONLY_PARQUET,
    AWARDS_CIF_PARQUET,
)

_ARC_OAX_LINKS = PROCESSED_DATA / "arc_oax_links.parquet"


def main():
    # Pre-flight, not a check on this script's OWN output (which is about to be unconditionally
    # rebuilt below regardless -- checking it here would hard-fail on this script's very first
    # ever run, before it's had a chance to create it). Mirrors the same pattern
    # 03_link_arc_oax.py uses on its own primary input: verify arc_oax_links.parquet (03's
    # output, this script's real dependency) isn't older than ITS OWN sources -- if 01 was
    # rerun after 03 last ran, or 03 has simply never run, that's caught here.
    assert_fresh(
        "03b_enrich_awards_cif (arc_oax_links.parquet)",
        outputs=[_ARC_OAX_LINKS],
        inputs=[ARC_ONLY_PARQUET, PROCESSED_DATA / "openalex_authors_prep.parquet"],
    )

    clusters = load_awards_cif(ARC_ONLY_PARQUET)
    print(f"  Loaded {len(clusters):,} ARC-only AwardsCIF from {ARC_ONLY_PARQUET.name}")
    clusters = enrich_with_oax_candidates(clusters)
    persist_awards_cif(clusters, AWARDS_CIF_PARQUET)
    n_with_candidates = sum(1 for c in clusters if c.oax_candidates)
    print(f"  {n_with_candidates:,} / {len(clusters):,} clusters have >=1 OAX candidate")
    print(f"  Saved -> {AWARDS_CIF_PARQUET}")


if __name__ == "__main__":
    main()
