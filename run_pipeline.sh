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
# KNOWN BROKEN as of 2026-09-15: step 04 below references 04_resolve_links.py, which was
# archived to ZARCHIVE/src_archive_20260909/ after being found structurally broken -- its
# replacement (04_filter_candidates.py) does not yet have an equivalent single-command
# resolve-everything entry point. Do not run this script end-to-end until that refactor lands.
#
# Usage: ./run_pipeline.sh 2>&1 | tee /tmp/pipeline_run.log
set -e
cd "$(dirname "$0")"
PY=.venv/bin/python
echo "=== 00a: extract ARC raw data ===" && $PY src/00a_extract_arc.py
echo "=== 01: prepare ARC-only population (loads OAX prep as needed) ===" && $PY src/01_prepare_arc.py
echo "=== 03: link ARC <-> OAX ===" && $PY src/03_link_arc_oax.py
echo "=== 04: resolve links (also builds awards_cif.parquet) ===" && $PY src/04_resolve_links.py
echo "=== ALL DONE ==="
