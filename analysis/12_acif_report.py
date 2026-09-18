"""
analysis/12_acif_report.py

CLI reporting tool over results.db (see analysis/utils/results_db.py). Markdown output only
for now, per direct 2026-09-17 direction (IDE-based development, cmd-line/md output) -- PDF is
a planned follow-on, not built yet. render_acif_markdown() already returns a plain string, so
a later --pdf flag can pipe that string through pandoc (or similar) without touching this
module's own logic.

Usage:
  # dashboard: category counts across reliability_tier x resolution_status x selection_status
  .venv/bin/python analysis/12_acif_report.py

  # one ACIF, printed to stdout
  .venv/bin/python analysis/12_acif_report.py --cluster-id DP0877196_IanPaulsen

  # one ACIF, written to a file
  .venv/bin/python analysis/12_acif_report.py --cluster-id DP0877196_IanPaulsen --out /tmp/report.md

  # a category: list matching cluster_ids (no full report)
  .venv/bin/python analysis/12_acif_report.py --tier 4u --status RESOLVED

  # a category: render N sample reports concatenated into one file, for spot review
  .venv/bin/python analysis/12_acif_report.py --tier 4u --sample 5 --out /tmp/sample.md
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from analysis.utils.acif_report import category_summary, list_cluster_ids, render_acif_markdown, markdown_table
from analysis.utils.results_db import RESULTS_DB


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--cluster-id", help="Render a full report for one ACIF")
    ap.add_argument("--tier", help="Filter by reliability_tier (e.g. 1a, 4u)")
    ap.add_argument("--status", help="Filter by resolution_status (RESOLVED/UNRESOLVED)")
    ap.add_argument(
        "--selection-status",
        help="Filter by selection_status (accepted/no_accepted/no_candidates)",
    )
    ap.add_argument("--sample", type=int, help="Render this many sample reports from the filtered category")
    ap.add_argument("--limit", type=int, help="Cap how many cluster_ids --tier/--status lists")
    ap.add_argument("--out", help="Write output to this file instead of stdout")
    args = ap.parse_args()

    if not RESULTS_DB.exists():
        sys.exit(f"{RESULTS_DB} does not exist -- run analysis/11_build_results_db.py first")

    con = duckdb.connect(str(RESULTS_DB), read_only=True)

    if args.cluster_id:
        output = render_acif_markdown(con, args.cluster_id)
    elif args.tier or args.status or args.selection_status:
        ids = list_cluster_ids(
            con,
            tier=args.tier,
            status=args.status,
            selection_status=args.selection_status,
            limit=args.sample or args.limit,
        )
        if args.sample:
            output = "\n\n---\n\n".join(render_acif_markdown(con, cid) for cid in ids)
        else:
            output = f"# {len(ids)} matching ACIF(s)\n\n" + "\n".join(f"- `{cid}`" for cid in ids)
    else:
        output = "# results.db category summary\n\n" + markdown_table(category_summary(con))

    if args.out:
        Path(args.out).write_text(output)
        print(f"Wrote {args.out}")
    else:
        print(output)


if __name__ == "__main__":
    main()
