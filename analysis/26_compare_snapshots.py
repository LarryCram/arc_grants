"""
analysis/26_compare_snapshots.py -- the same pipeline run on two OpenAlex snapshots, side by side
(2026-10-21: Jul26 in /home/lc/k/WORKING_ARC_PROJECT, Oct26 in /home/lc/k/WORKING_ARC_PROJECT_oct26).

Reads each folder's processed/acifs_arc.parquet, oax_link/accepted_links.parquet and
oeuvre/acif_works_classified.parquet; writes <new>/processed/snapshot_comparison.md:
  - ACIFs linked, by linker stage (the first stage that linked each ACIF);
  - ACIFs whose set of linked OpenAlex records changed, and how (same / more / fewer / different);
  - accepted, rejected and unsure works per ACIF, and the change per ACIF (distribution and the
    largest gains and losses, with names);
  - ACIFs linked in one snapshot only.
Nothing is decided here; it is a report.

Usage: .venv/bin/python analysis/26_compare_snapshots.py [OLD_DATA_ROOT NEW_DATA_ROOT]
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd

OLD = Path(sys.argv[1]) if len(sys.argv) > 2 else Path("/home/lc/k/WORKING_ARC_PROJECT")
NEW = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("/home/lc/k/WORKING_ARC_PROJECT_oct26")
STAGE_ORDER = ["orcid", "name", "works", "scopus"]


def load(root: Path, con) -> dict:
    p = root / "processed"
    acifs = pd.read_parquet(p / "acifs_arc.parquet", columns=["cluster_id", "full_names", "excluded"])
    acifs = acifs[~acifs.excluded]
    acifs["name"] = acifs.full_names.map(lambda x: list(x)[0] if len(x) else "")
    links = pd.read_parquet(p / "oax_link" / "accepted_links.parquet")
    works = con.execute(f"""SELECT cluster_id, decision, count(*) AS n
        FROM read_parquet('{p}/oeuvre/acif_works_classified.parquet') GROUP BY 1, 2""").df()
    works = works.pivot(index="cluster_id", columns="decision", values="n").fillna(0).astype(int)
    return {"acifs": acifs.set_index("cluster_id"), "links": links, "works": works}


def first_stage(links: pd.DataFrame) -> pd.Series:
    rank = {s: i for i, s in enumerate(STAGE_ORDER)}
    l = links.assign(r=links.stage.map(rank))
    return l.sort_values("r").groupby("cluster_id").stage.first()


def md_table(df: pd.DataFrame) -> list[str]:
    cols = list(df.columns)
    out = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    for r in df.itertuples(index=False):
        out.append("| " + " | ".join(f"{v:,}" if isinstance(v, (int,)) and not isinstance(v, bool) else str(v)
                                     for v in r) + " |")
    return out


def main():
    con = duckdb.connect()
    o, n = load(OLD, con), load(NEW, con)
    ids = o["acifs"].index.intersection(n["acifs"].index)
    L = [f"# Snapshot comparison: {OLD.name} (old) vs {NEW.name} (new)", "",
         f"Kept ACIFs: old {len(o['acifs']):,}, new {len(n['acifs']):,}, in both {len(ids):,}.", ""]

    so, sn = first_stage(o["links"]), first_stage(n["links"])
    t = pd.DataFrame({"old": so.reindex(ids).fillna("unlinked").value_counts(),
                      "new": sn.reindex(ids).fillna("unlinked").value_counts()}).fillna(0).astype(int)
    t = t.reindex([s for s in STAGE_ORDER + ["unlinked"] if s in t.index])
    t["change"] = t.new - t.old
    L += ["## ACIFs by the stage that linked them", ""] + md_table(t.reset_index(names="stage")) + [""]
    cross = pd.crosstab(so.reindex(ids).fillna("unlinked"), sn.reindex(ids).fillna("unlinked"))
    L += ["Old stage (rows) against new stage (columns):", ""] + md_table(cross.reset_index(names="old \\ new")) + [""]

    ro = o["links"].groupby("cluster_id").author_idx.apply(frozenset)
    rn = n["links"].groupby("cluster_id").author_idx.apply(frozenset)
    def kind(c):
        a, b = ro.get(c, frozenset()), rn.get(c, frozenset())
        if not a and not b:
            return "unlinked in both"
        if not a:
            return "linked in new only"
        if not b:
            return "linked in old only"
        if a == b:
            return "same records"
        if a < b:
            return "records added"
        if b < a:
            return "records dropped"
        return "some records differ" if a & b else "entirely different records"
    k = pd.Series({c: kind(c) for c in ids})
    L += ["## Linked OpenAlex records per ACIF", ""] + md_table(k.value_counts().reset_index().rename(
        columns={"index": "change", "count": "ACIFs"})) + [""]

    cols = ["accept", "reject", "unsure"]
    wo = o["works"].reindex(ids).fillna(0).astype(int).reindex(columns=cols, fill_value=0)
    wn = n["works"].reindex(ids).fillna(0).astype(int).reindex(columns=cols, fill_value=0)
    tot = pd.DataFrame({"old": wo.sum(), "new": wn.sum()})
    tot["change"] = tot.new - tot.old
    L += ["## Works (ACIF x work rows) by step-5 decision", ""] + md_table(tot.reset_index(names="decision")) + [""]
    d = (wn.accept - wo.accept)
    bins = pd.cut(d, [-10**9, -50, -10, -1, 0, 10, 50, 10**9],
                  labels=["<= -51", "-50..-11", "-10..-1", "0", "1..10", "11..50", ">= 51"])
    L += ["Change in accepted works per ACIF:", ""] + md_table(
        bins.value_counts().reindex(bins.cat.categories).reset_index().rename(columns={"index": "change", "count": "ACIFs"})) + [""]
    nm = n["acifs"].name
    for title, sel in [("Largest losses", d.nsmallest(25)), ("Largest gains", d.nlargest(25))]:
        rows = pd.DataFrame({"ACIF": sel.index, "name": nm.reindex(sel.index).values,
                             "old accepted": wo.accept.reindex(sel.index).values,
                             "new accepted": wn.accept.reindex(sel.index).values,
                             "records": k.reindex(sel.index).values})
        L += [f"### {title} in accepted works", ""] + md_table(rows) + [""]
    for title, sel in [("Linked in old only", k[k == "linked in old only"].index),
                       ("Linked in new only", k[k == "linked in new only"].index)]:
        L += [f"### {title} ({len(sel):,})", ""] + [f"- {c} ({nm.get(c, '')}): old stage {so.get(c, '-')}, "
                                                     f"new stage {sn.get(c, '-')}" for c in sorted(sel)[:60]] + [""]
    out = NEW / "processed" / "snapshot_comparison.md"
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
