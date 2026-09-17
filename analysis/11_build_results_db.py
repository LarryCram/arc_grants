"""
analysis/11_build_results_db.py

(Re)build PROCESSED_DATA/results.db -- the output-facing database driving per-ACIF reporting.
See analysis/utils/results_db.py for the table definitions and design rationale.

Usage:
  .venv/bin/python analysis/11_build_results_db.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.utils.results_db import RESULTS_DB, build_results_db

if __name__ == "__main__":
    counts = build_results_db()
    print(f"Built {RESULTS_DB}")
    for table, n in counts.items():
        print(f"  {table}: {n:,} rows")
