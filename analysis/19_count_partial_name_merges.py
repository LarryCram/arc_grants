"""
analysis/19_count_partial_name_merges.py -- counts what partial merges inside the name stage's
flagged groups would add (2026-10-06, user: count before building). Nothing is written back.

For each group the name stage's checks flagged (src/acif/name_merge.py), two parts are compatible when the
pair on its own raises no flag (group_checks() on the two parts). A partial merge takes a largest
set of parts that are all pairwise compatible (a maximum clique) and that also raises no flag as a
set (interleaving and the DECRA rules are set properties); the other parts are left out. Rounds:
after the first set is taken, the same is tried on the parts left over.

Reported per group: no compatible pair; a unique largest clean set; several largest sets with
different members (ambiguous -- which parts go together is not settled by the checks); a largest
set that is still flagged as a set. Plus whether shared co-investigators connect the chosen set.

Output: PROCESSED_DATA/name_merge_trial/partial_merge_count.md (+ .parquet, one row per group)
Usage: .venv/bin/python analysis/19_count_partial_name_merges.py
"""

import sys
from collections import Counter
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import PROCESSED_DATA
from src.acif.build import build_acifs
from src.acif.name_merge import coinvestigators, load_name_merge_inputs, n_components, part_facts

# 2026-10-06: the partial merges are now built into the name stage (src/acif/name_merge.py);
# this script reports them from the build's own report (status "partial", partial_sets).

OUT = PROCESSED_DATA / "name_merge_trial"


def main():
    pre, _, _ = build_acifs(names=False)
    acifs, _, report = build_acifs()
    rep = report["names"]
    g = rep["groups"]
    g = g[g.partial_status.notna()]
    inp = load_name_merge_inputs()
    coinv_of = coinvestigators(pre)
    by_id = {a.cluster_id: a for a in pre}
    rows = []
    for r in g.itertuples():
        sets = list(r.partial_sets)
        first = list(sets[0]) if sets else []
        rows.append({
            "first_id": r.first_id, "names": r.names, "n_parts": r.n_parts, "flags": list(r.flags),
            "status": r.partial_status, "first_set_size": len(first), "n_sets_merged": len(sets),
            "acifs_saved": sum(len(st) - 1 for st in sets), "parts_left_out": len(r.parts_left_out),
            "set_coawardee_linked": bool(first) and n_components(
                [part_facts(by_id[c], inp, coinv_of) for c in first],
                lambda p, q: bool(p["coinv"] & q["coinv"])) == 1,
            "chosen": first, "left_out": list(r.parts_left_out),
        })
    df = pd.DataFrame(rows)
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT / "partial_merge_count.parquet", index=False)
    text = render(df, {"n_after": rep["n_after"] + int(df.acifs_saved.sum())})
    (OUT / "partial_merge_count.md").write_text(text, encoding="utf-8")
    print(text)


def render(df, rep):
    n_after = rep["n_after"]
    saved = int(df.acifs_saved.sum())
    L = ["# Partial merges inside the name stage's flagged groups", "",
         f"Flagged groups: {len(df):,} ({int(df.n_parts.sum()):,} ACIFs). Name stage without partial merges: {n_after:,} ACIFs.", "",
         "| first round | groups | ACIFs in them |", "|---|---|---|"]
    for s, sub in df.groupby("status"):
        L.append(f"| {s} | {len(sub):,} | {int(sub.n_parts.sum()):,} |")
    u = df[df.status == "unique"]
    L += ["", f"Groups with a unique largest clean set: {len(u):,}",
          f"- ACIFs saved (all rounds): {saved:,} -> build has {n_after - saved:,} ACIFs",
          f"- parts left out of those groups: {int(u.parts_left_out.sum()):,}",
          f"- groups needing a second set: {int((u.n_sets_merged > 1).sum()):,}",
          f"- first set connected by shared co-investigators: {int(u.set_coawardee_linked.sum()):,}",
          "", "Unique groups by flag (a group can have two):", ""]
    for f, c in Counter(f for fl in u["flags"] for f in fl).most_common():
        L.append(f"- {f}: {c:,}")
    L += ["", "First set size vs group size (unique groups):", ""]
    for (n, k), c in Counter(zip(u.n_parts, u.first_set_size)).most_common(15):
        L.append(f"- {k} of {n} parts: {c:,}")
    for s in ["unique", "ambiguous", "no_compatible_pair", "largest_set_flagged"]:
        ex = df[df.status == s].sort_values("n_parts").head(10)
        if len(ex):
            L += ["", f"## Examples: {s}", ""]
            for r in ex.itertuples():
                L.append(f"- {r.n_parts} parts, {', '.join(r.names[:4])} [{', '.join(r.flags)}]"
                         + (f" -- merge {', '.join(r.chosen)}; leave {', '.join(r.left_out)}" if s == "unique" else ""))
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    main()
