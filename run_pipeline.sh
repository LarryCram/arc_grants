#!/usr/bin/env bash
# Run the current Splink-based ARC<->OAX identity pipeline end to end.
# 00c_prepare_oax.py is a loader, not a separate step here -- 01_prepare_arc.py calls
# prepare_oax.ensure_fresh() itself, which only rebuilds openalex_authors_prep.parquet when
# stale relative to authorships_hep.parquet/works_hep.parquet (a few times a year, not every
# run). Run src/00c_prepare_oax.py directly only to force a rebuild after a new snapshot.
#
# Usage: ./run_pipeline.sh 2>&1 | tee /tmp/pipeline_run.log
set -e
cd "$(dirname "$0")"
PY=.venv/bin/python
echo "=== 00: extract ARC raw data ===" && $PY src/00_extract_arc.py
echo "=== 01: prepare ARC-only population (loads OAX prep as needed) ===" && $PY src/01_prepare_arc.py
echo "=== 03: link ARC <-> OAX ===" && $PY src/03_link_arc_oax.py
echo "=== 04: resolve links (also builds awards_cif.parquet) ===" && $PY src/04_resolve_links.py
echo "=== ALL DONE ==="
