"""
analysis/23_dossier.py -- write the dossier of one or more ACIFs (analysis/utils/dossier.py): a
markdown page and a time-line chart per person in processed/dossiers/.

Usage: .venv/bin/python analysis/23_dossier.py <cluster_id or name text> [...]
       e.g. DP0773667_warren_burt  "sarah legge"
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.utils.dossier_build import DossierBuilder, find_acifs
from config.settings import PROCESSED_DATA

OUT = PROCESSED_DATA / "dossiers"


def main():
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    OUT.mkdir(parents=True, exist_ok=True)
    b = DossierBuilder()
    for arg in args:
        ids = [arg] if arg in b.acifs.index else find_acifs(arg)
        if not ids:
            print(f"no ACIF matches {arg!r}")
        for cid in ids:
            d = b.build(cid)
            safe = cid.replace(" ", "_").replace("'", "")
            chart = d.plot_timeline(OUT / f"{safe}.png")
            (OUT / f"{safe}.md").write_text(d.to_markdown(chart=Path(chart).name if chart else None), encoding="utf-8")
            print(OUT / f"{safe}.md")


if __name__ == "__main__":
    main()
