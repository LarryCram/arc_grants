#!/usr/bin/env bash
# Run work_piling.py's per-batch worker in parallel, one OS process per batch.
# Run from project root: bash run_piling.sh
#
# Prerequisite (fast, in-process, run by 06_build_oeuvre.py's own Step 5 already):
#   .venv/bin/python -m src.utils.work_piling                # compute_and_persist_idf_tables()
#
# Generate the batch list first (cheap, idempotent -- re-run any time membership changes):
#   .venv/bin/python -m src.utils.work_piling --write-batches
#
# Then run this script. Resumable for free: a batch whose output file already exists is
# skipped, never re-dispatched -- safe to re-run after a kill/crash with no wasted work.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$SCRIPT_DIR/.venv/bin/python"

BATCH_DIR="$($PYTHON -c "import sys; sys.path.insert(0, '$SCRIPT_DIR'); from src.utils.work_piling import PILING_BATCHES_DIR; print(PILING_BATCHES_DIR)")"
OUT_DIR="$($PYTHON -c "import sys; sys.path.insert(0, '$SCRIPT_DIR'); from src.utils.work_piling import PILING_RESULTS_DIR; print(PILING_RESULTS_DIR)")"
mkdir -p "$OUT_DIR"

TODO_LIST="$SCRIPT_DIR/piling_batches_todo.txt"
ls "$BATCH_DIR"/batch_*.txt > "$TODO_LIST" 2>/dev/null || true

if [ ! -s "$TODO_LIST" ]; then
    echo "No batch files found in $BATCH_DIR -- run first:"
    echo "  .venv/bin/python -m src.utils.work_piling --write-batches"
    exit 1
fi

JOBS=$(grep -E '^PILING_PARALLEL_JOBS=' "$SCRIPT_DIR/.env" 2>/dev/null | cut -d= -f2 || echo 12)

NUM_BATCHES=$(wc -l < "$TODO_LIST")
echo "Processing $NUM_BATCHES batches with $JOBS parallel jobs"

# Capped per-worker thread counts (see work_piling.py's own comment on this) -- 12 concurrent
# OS processes x these caps stays well within a 24-core machine's budget rather than each
# process independently trying to claim every core for itself.
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1

cat "$TODO_LIST" | xargs -P "$JOBS" -I {} bash -c '
    batch="{}"
    idx=$(basename "$batch" .txt | sed "s/^batch_//")
    out="'"$OUT_DIR"'/batch_${idx}.parquet"
    if [ -f "$out" ]; then
        echo "  skip batch_${idx} (already done)"
        exit 0
    fi
    "'"$PYTHON"'" -u -m src.utils.work_piling --batch "$batch" --out "$out" || true
'

rm -f "$TODO_LIST"
echo "=== run_piling.sh done ==="
