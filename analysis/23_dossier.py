"""
analysis/23_dossier.py -- write the dossier of one or more ACIFs (analysis/utils/dossier.py) to
processed/dossiers/: a markdown page and a time-line chart per person, plus a self-contained HTML page
(chart embedded) that opens in any browser.

Usage: .venv/bin/python analysis/23_dossier.py <cluster_id or name text> [...]
       e.g. DP0773667_warren_burt  "sarah legge"
       .venv/bin/python analysis/23_dossier.py --random 10 [--seed 1]   (kept ACIFs drawn at random;
       also writes random_<seed>.html, an index of the drawn dossiers)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import base64

import markdown

from analysis.utils.dossier_build import DossierBuilder, find_acifs
from config.settings import PROCESSED_DATA

OUT = PROCESSED_DATA / "dossiers"
CSS = """body{font-family:system-ui,sans-serif;max-width:1150px;margin:24px auto;padding:0 16px;color:#222}
table{border-collapse:collapse;margin:8px 0;font-size:13px}th,td{border:1px solid #ccc;padding:3px 7px;text-align:left}
th{background:#f2f2f2}img{max-width:100%}h1{margin-bottom:4px}h2{border-bottom:1px solid #ddd;padding-bottom:3px}"""


def write_html(md_text: str, chart, path: Path, title: str) -> None:
    """A self-contained HTML page: the markdown rendered, the chart embedded as a data URI."""
    if chart:
        b64 = base64.b64encode(Path(chart).read_bytes()).decode()
        md_text = md_text.replace(f"]({Path(chart).name})", f"](data:image/png;base64,{b64})")
    body = markdown.markdown(md_text, extensions=["tables"])
    path.write_text(f"<!doctype html><html><head><meta charset='utf-8'><title>{title}</title><style>{CSS}</style>"
                    f"</head><body>{body}</body></html>", encoding="utf-8")


def main():
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    OUT.mkdir(parents=True, exist_ok=True)
    b = DossierBuilder()
    targets, index = [], None
    if args[0] == "--random":
        n = int(args[1])
        seed = int(args[args.index("--seed") + 1]) if "--seed" in args else 1
        kept = b.acifs[~b.acifs.excluded].index.to_series()
        targets = list(kept.sample(n, random_state=seed))
        index = OUT / f"random_{seed}.html"
    else:
        for arg in args:
            ids = [arg] if arg in b.acifs.index else find_acifs(arg)
            if not ids:
                print(f"no ACIF matches {arg!r}")
            targets += ids
    rows = []
    for cid in targets:
        d = b.build(cid)
        safe = cid.replace(" ", "_").replace("'", "")
        chart = d.plot_timeline(OUT / f"{safe}.png")
        md = d.to_markdown(chart=Path(chart).name if chart else None)
        (OUT / f"{safe}.md").write_text(md, encoding="utf-8")
        write_html(md, chart, OUT / f"{safe}.html", d.name)
        print(OUT / f"{safe}.html")
        linked = ", ".join(sorted({l.stage for l in d.links})) or "not linked"
        rows.append(f"| [{d.name}]({safe}.html) | {d.first_grant_year}-{d.last_grant_year} | {d.main_division or ''} | "
                    f"{linked} | {len(d.accepted()):,} | {len(d.unsure()):,} | {sum(d.rejected.values()):,} | {d.h_index()} |")
    if index:
        md = "\n".join([f"# Random dossiers (seed {Path(index).stem.split('_')[1]})", "",
                         "| person | grants | main field | linked by | accepted | unsure | rejected | h-index |",
                         "|---|---|---|---|---|---|---|---|"] + rows)
        write_html(md, None, index, "Random dossiers")
        print(index)


if __name__ == "__main__":
    main()
