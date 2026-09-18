"""
analysis/13_summary_tables.py

Population-level summary tables for publication (scheme breakdown, fellowship tiers, CI/F
split) -- see analysis/utils/summary_tables.py for the table definitions and design rationale.

Usage:
  .venv/bin/python analysis/13_summary_tables.py [--out FILE]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from analysis.utils.results_db import RESULTS_DB
from analysis.utils.summary_tables import build_summary_report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", help="Write output to this file instead of stdout")
    args = ap.parse_args()

    if not RESULTS_DB.exists():
        sys.exit(f"{RESULTS_DB} does not exist -- run analysis/11_build_results_db.py first")

    con = duckdb.connect(str(RESULTS_DB), read_only=True)
    output = build_summary_report(con)

    if args.out:
        Path(args.out).write_text(output)
        print(f"Wrote {args.out}")
    else:
        print(output)


if __name__ == "__main__":
    main()
