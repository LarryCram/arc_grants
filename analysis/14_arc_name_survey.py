"""
analysis/14_arc_name_survey.py

Statistics of ARC investigator names as ARC provides them (raw_json.csv), in-scope only.
See analysis/utils/arc_name_survey.py. Diagnostic only.

Outputs (PROCESSED_DATA/arc_name_survey/):
  potential_anomalies.parquet  one row per (distinct name record, flag)
  entry_statuses.parquet       announcement/current status of every entry on in-scope grants
  name_differences.parquet     one row per B pair (source, names, grant or ORCID, label)
  arc_name_survey.md           counts and examples

Usage:
  .venv/bin/python analysis/14_arc_name_survey.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import PROCESSED_DATA
from analysis.utils.arc_name_survey import run_survey, render_report

OUT_DIR = PROCESSED_DATA / "arc_name_survey"

if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    res = run_survey()
    res["anomalies"].to_parquet(OUT_DIR / "potential_anomalies.parquet", index=False)
    res["statuses"].to_parquet(OUT_DIR / "entry_statuses.parquet", index=False)
    res["differences"].to_parquet(OUT_DIR / "name_differences.parquet", index=False)
    report = render_report(res)
    (OUT_DIR / "arc_name_survey.md").write_text(report, encoding="utf-8")
    print(f"Wrote {OUT_DIR}/arc_name_survey.md")
