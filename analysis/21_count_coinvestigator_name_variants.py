"""
analysis/21_count_coinvestigator_name_variants.py -- counts pairs of ACIFs in the finished build
that share a family name and a co-investigator but were not joined by the name stage because
their given names differ (2026-10-06, user: count before building). Nothing is written back.

Candidates: two ACIFs whose records share a family name (arc_names.parquet family_names, the
parser's output) and that have a co-investigator in common (the co-investigator's ACIF in the
finished build), not on a common grant (two people on one grant are two people), and whose main
name keys don't overlap (those were the name stage's business). Each pair is put in one name
category from the parser's own fields:
    middle_or_compound   they share a full-given-name key that is not a main key (Hai-Bin/Bin Yu)
    initial_only         one side has no full given name and its initial matches the other's
    same_initial         full given names differ, same initial (Mike/Michael, or two people)
    different_initial    full given names with different initials (Bob/Robert, or two people)
A pair "passes" when it carries at most one ORCID and raises no name-stage flag other than
several_main_names (the name difference is the point here). Pairs are then chained into groups
(A~B, B~C) and a group is counted as mergeable only if it carries at most one ORCID.

Output: PROCESSED_DATA/name_merge_trial/coinv_variant_count.md (+ .parquet, one row per pair)
Usage: .venv/bin/python analysis/21_count_coinvestigator_name_variants.py
"""

import sys
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import PROCESSED_DATA
from src.acif.build import UnionFind, _item_orcids, build_acifs
from src.acif.name_merge import coinvestigators, group_checks, load_name_merge_inputs, part_facts

OUT = PROCESSED_DATA / "name_merge_trial"


def name_facts(acifs):
    a = pd.read_parquet(PROCESSED_DATA / "arc_names.parquet",
                        columns=["unique_id", "family_names", "first_name_canonical", "full_name_keys",
                                 "full_name_key", "given_tokens"])
    rec = {r.unique_id: r for r in a.itertuples(index=False)}
    out = {}
    for ac in acifs:
        fam, full_given, initials, keys = set(), set(), set(), set()
        for it in ac.items:
            r = rec[it.unique_id]
            fam |= set(r.family_names)
            if r.first_name_canonical and len(r.first_name_canonical) > 1:
                full_given.add(r.first_name_canonical)
            initials |= {t[0] for t in r.given_tokens if t}
            keys |= {k for k in r.full_name_keys if len(k.split("_", 1)[0]) > 1}
        out[ac.cluster_id] = {"family": fam, "given": full_given, "initials": initials, "keys": keys}
    return out


def category(p, q, main_p, main_q) -> str:
    if (p["keys"] & q["keys"]) - (main_p | main_q):
        return "middle_or_compound"
    if not p["given"] or not q["given"]:
        return "initial_only" if p["initials"] & q["initials"] else "no_given_match"
    if {g[0] for g in p["given"]} & {g[0] for g in q["given"]}:
        return "same_initial"
    return "different_initial"


def main():
    acifs, _, report = build_acifs()
    inp = load_name_merge_inputs()
    coinv_of = coinvestigators(acifs)
    facts = {a.cluster_id: part_facts(a, inp, coinv_of) for a in acifs}
    names = name_facts(acifs)
    grants = {a.cluster_id: {it.grant_code for it in a.items} for a in acifs}
    n_grants_of = {cid: len(g) for cid, g in grants.items()}

    by_family = defaultdict(set)
    for cid, n in names.items():
        for f in n["family"]:
            by_family[f].add(cid)

    rows, seen = [], set()
    for fam, cids in by_family.items():
        for x, y in combinations(sorted(cids), 2):
            if (x, y) in seen:
                continue
            seen.add((x, y))
            p, q = facts[x], facts[y]
            shared = p["coinv"] & q["coinv"]
            if not shared or grants[x] & grants[y] or p["main_keys"] & q["main_keys"]:
                continue
            orcids = p["orcids"] | q["orcids"]
            flags = [f for f in group_checks(sorted(p["main_keys"] | q["main_keys"]), [p, q], inp)["flags"]
                     if f != "several_main_names"]
            rows.append({
                "a": x, "b": y, "names_a": p["names"], "names_b": q["names"],
                "category": category(names[x], names[y], p["main_keys"], q["main_keys"]),
                "n_shared_coinv": len(shared),
                "max_coinv_grants": max(n_grants_of[c] for c in shared),
                "orcid_conflict": len(orcids) > 1, "flags": flags,
                "passes": len(orcids) <= 1 and not flags,
            })
    df = pd.DataFrame(rows)

    # chain passing pairs; a group is mergeable only with at most one ORCID
    ok = df[df.passes]
    uf = UnionFind()
    for r in ok.itertuples():
        uf.union(r.a, r.b)
    by_id = {a.cluster_id: a for a in acifs}
    comps = defaultdict(list)
    for c in set(ok.a) | set(ok.b):
        comps[uf.find(c)].append(c)
    comp_rows = [{"n": len(cs), "n_orcids": len(set().union(*(_item_orcids(by_id[c]) for c in cs)))}
                 for cs in comps.values()]
    cdf = pd.DataFrame(comp_rows)

    OUT.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT / "coinv_variant_count.parquet", index=False)
    text = render(df, cdf, report["n_names"])
    (OUT / "coinv_variant_count.md").write_text(text, encoding="utf-8")
    print(text)


def render(df, cdf, n_build):
    L = ["# Same family name + shared co-investigator, different given names", "",
         f"Candidate pairs: {len(df):,} (finished build: {n_build:,} ACIFs).", "",
         "| category | pairs | pass | ORCID conflict | flagged | pass with 2+ shared co-inv. |",
         "|---|---|---|---|---|---|"]
    for c, sub in df.groupby("category"):
        L.append(f"| {c} | {len(sub):,} | {int(sub.passes.sum()):,} | {int(sub.orcid_conflict.sum()):,} | "
                 f"{int((sub["flags"].map(len) > 0).sum()):,} | {int((sub.passes & (sub.n_shared_coinv >= 2)).sum()):,} |")
    good = cdf[cdf.n_orcids <= 1] if len(cdf) else cdf
    saved = int((good.n - 1).sum()) if len(good) else 0
    L += ["", f"Passing pairs chained into groups: {len(cdf):,} groups; with at most one ORCID "
          f"{len(good):,} (ACIFs saved {saved:,} -> {n_build - saved:,}); with 2+ ORCIDs {len(cdf) - len(good):,}.", "",
          "Shared co-investigator's own grant count (passing pairs; a busy co-investigator is weaker evidence):", ""]
    ps = df[df.passes]
    for lab, c in pd.cut(ps.max_coinv_grants, [0, 2, 5, 10, 20, 10**6],
                         labels=["1-2", "3-5", "6-10", "11-20", "21+"]).value_counts().sort_index().items():
        L.append(f"- {lab}: {c:,}")
    for cat in ["middle_or_compound", "initial_only", "same_initial", "different_initial", "no_given_match"]:
        ex = df[(df.category == cat) & df.passes].sort_values("n_shared_coinv", ascending=False).head(15)
        if len(ex):
            L += ["", f"## Passing examples: {cat}", ""]
            for r in ex.itertuples():
                L.append(f"- {', '.join(r.names_a)} ({r.a}) ~ {', '.join(r.names_b)} ({r.b}) -- "
                         f"{r.n_shared_coinv} shared co-inv.")
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    main()
