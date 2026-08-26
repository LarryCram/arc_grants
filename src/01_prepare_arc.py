"""
src/01_prepare_arc.py

Thin driver onto src/utils/awards_cif.py's ARC-only population builder -- this file used to
contain a full, independent second implementation of ARC-side name parsing, Splink dedupe, and
the ten-step refine_clusters() sequence, duplicating (and, as found 2026-08-21, silently
drifting from) awards_cif.py's own validated logic. AwardsCIF is now the sole core: there is
exactly one implementation of ARC-side identity resolution in this codebase, here or anywhere
else, and this script's only job is to run it and persist the result.

The full former implementation is preserved, not deleted, at
ZARCHIVE/src_archive_20260821/01_prepare_arc.py -- per this project's established convention
(see src_archive_20260520/) for retired pipeline stages.

2026-08-21 correction: this script previously called build_awards_cif_population(), which
silently also populated oax_candidates by reading arc_oax_links.parquet -- 03_link_arc_oax.py's
OWN output. That meant "01" depended on "03" having already run, and 06_build_oeuvre.py would
later overwrite this script's output at the same path with its own OAX-enriched rebuild, so no
genuinely ARC-only, checkable-before-OAX artifact ever actually existed on disk. Fixed by calling
build_arc_only_population() (no OAX dependency at all) and persisting to a distinctly-named file,
awards_cif_arc_only.parquet -- 03_link_arc_oax.py reads THIS file, never awards_cif.parquet.
awards_cif.parquet itself is now produced by 04_resolve_links.py (2026-08-25: absorbed from a
former separate stage, src/03b_enrich_awards_cif.py, archived -- see 04's own docstring for why),
which runs after 03 and enriches this file's output with oax_candidates.

Output:
    awards_cif_arc_only.parquet   (PROCESSED_DATA) -- the ARC/ORCID-only identity population,
                                                        checkable before any OAX connection
    arc_grant_cluster_map.parquet (PROCESSED_DATA) -- unique_id -> cluster_id, for consumers
                                                        that only need the grant/cluster mapping
"""

import importlib.util
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.utils.awards_cif import (
    build_arc_only_population,
    persist_awards_cif,
    persist_grant_cluster_map,
    ARC_ONLY_PARQUET,
    GRANT_CLUSTER_MAP_PARQUET,
)

# 00c_prepare_oax.py's filename starts with a digit, so it can't be imported with a normal
# `import` statement -- load it by path instead (same technique tests/test_01a_diagnose.py
# already uses for the same reason).
_spec = importlib.util.spec_from_file_location(
    "prepare_oax", Path(__file__).resolve().parent / "00c_prepare_oax.py"
)
prepare_oax = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(prepare_oax)

def main():
    prepare_oax.ensure_fresh()
    prepare_oax.scan_for_new_diacritics()
    clusters = build_arc_only_population()
    persist_awards_cif(clusters, ARC_ONLY_PARQUET)
    persist_grant_cluster_map(clusters, GRANT_CLUSTER_MAP_PARQUET)


if __name__ == "__main__":
    main()
