#!/usr/bin/env bash
# Run the current Splink-based ARC<->OAX identity pipeline end to end.
# 00a_extract_arc.py / 00b_extract_oax.py are standalone, order-independent prep steps (renamed
# 2026-09-15 from 00_extract_arc.py/02_prepare_oax.py -- the "00" prefix marks them as outside
# the main sequential chain, not run-before-01 in any data-dependency sense: 01_ only reads
# 00a_'s output, never 00b_'s). 00b_extract_oax.py is a loader, not a separate step here --
# 01_prepare_arc.py calls prepare_oax.ensure_fresh() itself, which only rebuilds
# openalex_authors_prep.parquet when stale relative to authorships_hep.parquet/works_hep.parquet
# (a few times a year, not every run). Run src/00b_extract_oax.py directly only to force a
# rebuild after a new snapshot.
#
# STILL BROKEN, now for a second reason: steps 03/04 below (03_link_arc_oax.py,
# 04_resolve_links.py) were both archived -- 04_resolve_links.py to ZARCHIVE/src_archive_20260909/
# (2026-09-09, found structurally broken), 03_link_arc_oax.py to
# ZARCHIVE/src_archive_20260918/ (2026-09-18, superseded by src/utils/acif_oax_linker.py's
# AcifOaxLinker -- block()/fd_score()/coawardee_corroborate(), no Splink). Neither replacement
# (04_filter_candidates.py, itself also archived 2026-09-18, or AcifOaxLinker) has an equivalent
# single-command "run everything, write final output" entry point yet. Do not run this script
# end-to-end -- see analysis/11_build_results_db.py / analysis/12_acif_report.py for the current
# working output pipeline (results.db), and src/utils/acif_oax_linker.py's own __main__ for how
# to run AcifOaxLinker's block()/fd_score()/coawardee_corroborate() stages directly.
#
# Usage: ./run_pipeline.sh 2>&1 | tee /tmp/pipeline_run.log
set -e
cd "$(dirname "$0")"
PY=.venv/bin/python
echo "=== 00a: extract ARC raw data ===" && $PY src/00a_extract_arc.py
echo "=== 01: prepare ARC-only population (loads OAX prep as needed) ===" && $PY src/01_prepare_arc.py
echo "=== 03/04: BROKEN, see comment above -- ARC<->OAX linking is now AcifOaxLinker, not these archived scripts ==="
echo "=== ALL DONE (partially -- see above) ==="
