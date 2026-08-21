"""
src/01_prepare_arc.py

Thin driver onto src/utils/awards_cif.py's AwardsCIF population builder -- this file used to
contain a full, independent second implementation of ARC-side name parsing, Splink dedupe, and
the ten-step refine_clusters() sequence, duplicating (and, as found 2026-08-21, silently
drifting from) awards_cif.py's own validated logic. AwardsCIF is now the sole core: there is
exactly one implementation of ARC-side identity resolution in this codebase, here or anywhere
else, and this script's only job is to run it and persist the result.

The full former implementation is preserved, not deleted, at
ZARCHIVE/src_archive_20260821/01_prepare_arc.py -- per this project's established convention
(see src_archive_20260520/) for retired pipeline stages.

Not yet ported from the old implementation (flagged, not silently dropped):
    - _export_manual_splits_template() -- regenerated a human-review candidate list
      (data_persisted/manual_splits.csv) of clusters suspected of merging 2+ real people.
      Confirmed 2026-08-21 (cross-session, see CLAUDE.md) that this file has never actually had
      a human-confirmed row in its entire history, so nothing operationally depends on it running
      here today -- but the mechanism itself is real and worth rebuilding against AwardsCIF
      objects rather than a DataFrame, as a follow-up.
    - _diagnostic_report() -- printed a same-session A/B case breakdown of suspicious clusters;
      superseded in spirit by 01a_diagnose.py, which already runs against AwardsCIF's own output.

Output:
    awards_cif.parquet          (PROCESSED_DATA) -- the sole ARC-side identity population
    arc_grant_cluster_map.parquet (PROCESSED_DATA) -- unique_id -> cluster_id, for consumers
                                                        that only need the grant/cluster mapping
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.utils.awards_cif import (
    build_awards_cif_population,
    persist_awards_cif,
    persist_grant_cluster_map,
    AWARDS_CIF_PARQUET,
    GRANT_CLUSTER_MAP_PARQUET,
)


def main():
    clusters = build_awards_cif_population()
    persist_awards_cif(clusters, AWARDS_CIF_PARQUET)
    persist_grant_cluster_map(clusters, GRANT_CLUSTER_MAP_PARQUET)


if __name__ == "__main__":
    main()
