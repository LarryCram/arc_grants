#!/usr/bin/env bash
# Run matching pipeline layers 2 → 3 → 16 → 17b → 17d sequentially.
# Assumes layer 1.5 has already completed.
# Usage: ./run_pipeline.sh 2>&1 | tee /tmp/pipeline_run.log
set -e
cd "$(dirname "$0")"
PY=.venv/bin/python
echo "=== Layer 2 ===" && $PY src/14_layer2_names.py
echo "=== Layer 3 ===" && $PY src/15_layer3_for.py
echo "=== Layer 16 ===" && $PY src/16_absent_humanities.py
echo "=== Layer 5 (17b) ===" && $PY src/17b_parquet_oax.py
echo "=== Layer 5d (17d) ===" && $PY src/17d_resolve.py
echo "=== ALL DONE ==="
