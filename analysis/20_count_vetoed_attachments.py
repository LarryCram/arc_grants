"""
analysis/20_count_vetoed_attachments.py -- counts what attaching the no-ORCID parts of the name
stage's vetoed groups to one ORCID side would add (2026-10-06, user: count before building).
Nothing is written back.

A vetoed group (src/acif/name_merge.py, status orcid_veto) holds 2+ ACIFs with different ORCIDs
(the "sides") plus, often, ACIFs with no ORCID. A no-ORCID part has evidence for a side when they
share a co-investigator (identified by the co-investigator's ACIF in the finished build) or a
university on a single-organisation grant. It would be attached when:
  - it has evidence for exactly one side, and
  - the pair (part + side) raises no name-stage flag (group_checks()).
Then each side with its attached parts is checked as a set; a side whose set is flagged attaches
nothing. Reported too: parts with evidence for 2+ sides (ambiguous), parts with none, and parts
whose only evidence is a shared FOR2020 group (information only -- not used to attach).

Output: PROCESSED_DATA/name_merge_trial/vetoed_attach_count.md (+ .parquet, one row per part)
Usage: .venv/bin/python analysis/20_count_vetoed_attachments.py
"""

import sys
from collections import Counter
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import PROCESSED_DATA
from src.acif.build import build_acifs
from src.acif.name_merge import coinvestigators, group_checks, load_name_merge_inputs, part_facts

OUT = PROCESSED_DATA / "name_merge_trial"


def evidence(p, q) -> list[str]:
    ev = []
    if p["coinv"] & q["coinv"]:
        ev.append("coinvestigator")
    if p["unis"] & q["unis"]:
        ev.append("university")
    return ev


def main():
    acifs, _, report = build_acifs()
    g = report["names"]["groups"]
    vetoed = g[g.status == "orcid_veto"]
    inp = load_name_merge_inputs()
    coinv_of = coinvestigators(acifs)
    by_id = {a.cluster_id: a for a in acifs}

    rows, group_rows = [], []
    for grp in vetoed.itertuples():
        parts = {c: part_facts(by_id[c], inp, coinv_of) for c in grp.parts}
        sides = [c for c in grp.parts if parts[c]["orcids"]]
        loose = [c for c in grp.parts if not parts[c]["orcids"]]
        attach: dict[str, list[str]] = {s: [] for s in sides}
        prow = []
        for c in loose:
            p = parts[c]
            ev = {s: evidence(p, parts[s]) for s in sides}
            with_ev = [s for s in sides if ev[s]]
            ok = [s for s in with_ev
                  if not group_checks(sorted(p["main_keys"] | parts[s]["main_keys"]), [p, parts[s]], inp)["flags"]]
            for_only = [s for s in sides if not ev[s] and p["for"] & parts[s]["for"]]
            if len(with_ev) == 0:
                status = "no_evidence"
            elif len(with_ev) > 1:
                status = "ambiguous"
            elif not ok:
                status = "pair_flagged"
            else:
                status = "attach"
                attach[ok[0]].append(c)
            prow.append({"group": grp.first_id, "part": c, "names": p["names"], "status": status,
                         "side": ok[0] if status == "attach" else None,
                         "evidence": ev[with_ev[0]] if len(with_ev) == 1 else sorted({e for s in with_ev for e in ev[s]}),
                         "n_sides_with_evidence": len(with_ev), "for_only_sides": len(for_only),
                         "other_sides_without_unis": sum(1 for s in sides if s not in with_ev and not parts[s]["unis"])})
        for s, cs in attach.items():
            if cs:
                set_parts = [parts[s]] + [parts[c] for c in cs]
                flags = group_checks(sorted(set().union(*(x["main_keys"] for x in set_parts))), set_parts, inp)["flags"]
                if flags:
                    for r in prow:
                        if r["side"] == s:
                            r["status"], r["side"] = "side_set_flagged", None
        rows += prow
        group_rows.append({"group": grp.first_id, "n_parts": grp.n_parts, "n_sides": len(sides),
                           "n_loose": len(loose),
                           "n_attached": sum(r["status"] == "attach" for r in prow)})
    df, gdf = pd.DataFrame(rows), pd.DataFrame(group_rows)
    OUT.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT / "vetoed_attach_count.parquet", index=False)
    text = render(df, gdf, report["n_names"])
    (OUT / "vetoed_attach_count.md").write_text(text, encoding="utf-8")
    print(text)


def render(df, gdf, n_build):
    att = df[df.status == "attach"]
    L = ["# No-ORCID parts of the name stage's vetoed groups", "",
         f"Vetoed groups: {len(gdf):,} ({int(gdf.n_parts.sum()):,} ACIFs): ORCID sides "
         f"{int(gdf.n_sides.sum()):,}, no-ORCID parts {int(gdf.n_loose.sum()):,}. "
         f"Groups with no no-ORCID part: {int((gdf.n_loose == 0).sum()):,}.", "",
         "| no-ORCID part | parts |", "|---|---|"]
    for s, c in df.status.value_counts().items():
        L.append(f"| {s} | {c:,} |")
    L += ["", f"Attached: {len(att):,} parts in {int((gdf.n_attached > 0).sum()):,} groups "
          f"-> build {n_build:,} -> {n_build - len(att):,} ACIFs", "",
          "Evidence of attached parts:", ""]
    for e, c in Counter(" + ".join(x) for x in att.evidence).most_common():
        L.append(f"- {e}: {c:,}")
    L += ["", f"Attached parts where another side has no single-organisation university (so could not "
          f"show university evidence): {int((att.other_sides_without_unis > 0).sum()):,}; of those, attached on "
          f"university evidence alone: {int(((att.other_sides_without_unis > 0) & att.evidence.map(lambda e: list(e) == ['university'])).sum()):,}"]
    ne = df[df.status == "no_evidence"]
    L += ["", f"No-evidence parts sharing a FOR2020 group with some side (information only): "
          f"{int((ne.for_only_sides > 0).sum()):,} of {len(ne):,}", ""]
    for s in ["attach", "ambiguous", "pair_flagged", "side_set_flagged", "no_evidence"]:
        ex = df[df.status == s].head(12)
        if len(ex):
            L += [f"## Examples: {s}", ""]
            for r in ex.itertuples():
                L.append(f"- {r.part} ({', '.join(r.names)}) in {r.group}"
                         + (f" -> {r.side} [{', '.join(r.evidence)}]" if s == "attach" else
                            f" [{', '.join(r.evidence)}]" if len(r.evidence) else ""))
            L.append("")
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    main()
