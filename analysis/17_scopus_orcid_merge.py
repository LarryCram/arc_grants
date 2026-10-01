"""
analysis/17_scopus_orcid_merge.py -- Scopus merge pass one, as a what-if: merge ACIFs by ORCID
where the ORCID is ARC's own or one found through Scopus (rules in analysis/utils/scopus_merge.py).
Nothing is written back to the pipeline; the ACIF build is unchanged.

Needs analysis/16_scopus_lookup.py --all to have been run on the same build.

Outputs (PROCESSED_DATA/scopus/):
    pass1_decisions.parquet   one row per ACIF: whether a Scopus ORCID was accepted (and whether
                              its names came from the ORCID cache or orcid_bulk.parquet), and why not
    pass1_merged.parquet      one row per ACIF after pass one: cluster_id, members, how joined
    pass1_report.md           counts and examples

Usage: .venv/bin/python analysis/17_scopus_orcid_merge.py
"""

import sys
from pathlib import Path

import diskcache
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import DISKCACHE_DIR, PROCESSED_DATA
from analysis.utils.scopus_merge import _orcid_or_none, decide, load_bulk_names, load_scopus_rejections
from src.acif.build import UnionFind, build_stage_zero, merge_by_key, merge_by_orcid

OUT = PROCESSED_DATA / "scopus"


def main():
    acifs, arc_mismatches = merge_by_orcid(build_stage_zero(), UnionFind({}))
    summary = pd.read_parquet(OUT / "acif_scopus_summary.parquet")
    profiles = pd.read_parquet(OUT / "acif_scopus_profiles.parquet")
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    wanted = {o for o in map(_orcid_or_none, profiles.orcid) if o and o not in cache}
    bulk = load_bulk_names(wanted)
    rejected = load_scopus_rejections(pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet"))
    d = decide(acifs, summary, profiles, cache, bulk, rejected)
    d.to_parquet(OUT / "pass1_decisions.parquet", index=False)

    accepted = dict(zip(d.loc[d.decision == "accepted", "cluster_id"], d.loc[d.decision == "accepted", "scopus_orcid"]))

    def key(a):
        if a.orcid_status == "HAS_ORCID":
            return a.orcids[0]
        return accepted.get(a.cluster_id)

    before = {a.cluster_id: a for a in acifs}
    uf = UnionFind({})
    merged, mismatches = merge_by_key(acifs, uf, key)
    report(d, acifs, merged, uf, before, mismatches, arc_mismatches)


def report(d, acifs, merged, uf, before, mismatches, arc_mismatches):
    groups = {}
    for cid in before:
        groups.setdefault(uf.find(cid), []).append(cid)
    joined = {r: m for r, m in groups.items() if len(m) > 1}
    kinds = {"arc+scopus": 0, "scopus_only": 0}
    rows = []
    for root, members in joined.items():
        arc = [c for c in members if before[c].orcid_status == "HAS_ORCID"]
        kind = "arc+scopus" if arc else "scopus_only"
        kinds[kind] += 1
        orcid = before[arc[0]].orcids[0] if arc else d.set_index("cluster_id").loc[members[0], "scopus_orcid"]
        rows.append({"root": root, "orcid": orcid, "kind": kind, "n_acifs": len(members),
                     "n_fragments_added": len([c for c in members if before[c].orcid_status == "NO_ORCID"]),
                     "members": sorted(members),
                     "names": sorted({it.full_name for c in members for it in before[c].items})})
    j = pd.DataFrame(rows)
    j.to_parquet(OUT / "pass1_merged.parquet", index=False)

    no = d[d.orcid_status == "NO_ORCID"]
    lines = ["# Scopus merge pass one (what-if)", "",
             f"ACIFs after the ARC ORCID merge: {len(acifs):,} "
             f"({(d.orcid_status == 'HAS_ORCID').sum():,} with an ARC ORCID, {len(no):,} without)", "",
             "## Scopus ORCID decisions for the ACIFs without an ARC ORCID", "",
             "| decision | ACIFs |", "|---|---|"]
    lines += [f"| {k} | {v:,} |" for k, v in no.decision.value_counts().items()]
    acc = no[no.decision == "accepted"]
    lines += ["", f"Accepted, names read from: " + ", ".join(f"{k} {v:,}" for k, v in acc.name_source.value_counts().items())]
    lines += ["", "## Merging by ORCID (ARC or accepted Scopus)", "",
              f"- ACIFs after pass one: {len(merged):,} (was {len(acifs):,}; {len(acifs) - len(merged):,} fewer)",
              f"- groups that merged: {len(j):,} -- {kinds['arc+scopus']:,} joined no-ORCID fragments to an "
              f"ARC-ORCID ACIF, {kinds['scopus_only']:,} joined no-ORCID fragments to each other",
              f"- fragments absorbed: {int(j.n_fragments_added.sum()) if len(j) else 0:,}",
              f"- ORCID groups not merged because names don't link: {len(mismatches):,} "
              f"(ARC-only merge had {len(arc_mismatches):,})", ""]
    has = d[d.orcid_status == "HAS_ORCID"]
    lines += ["## ACIFs with an ARC ORCID: what Scopus said", "", "| lookup status | ACIFs |", "|---|---|"]
    lines += [f"| {k} | {v:,} |" for k, v in has.lookup_status.value_counts().items()]
    if len(j):
        lines += ["", "## Largest merges", ""]
        for r in j.sort_values("n_acifs", ascending=False).head(15).itertuples():
            lines.append(f"- {r.orcid} ({r.kind}, {r.n_acifs} ACIFs): {', '.join(r.names[:6])}")
        lines += ["", "## Sample of scopus_only merges", ""]
        for r in j[j.kind == "scopus_only"].head(15).itertuples():
            lines.append(f"- {r.orcid}: {r.members} -- {', '.join(r.names)}")
    if mismatches:
        lines += ["", "## ORCID groups whose names don't link (not merged)", ""]
        for m in mismatches[:20]:
            lines.append(f"- {m['orcid']}: {m['names']}")
    text = "\n".join(lines) + "\n"
    (OUT / "pass1_report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
