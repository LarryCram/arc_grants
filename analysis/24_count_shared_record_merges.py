"""
analysis/24_count_shared_record_merges.py -- count (do not apply) a post-linking ACIF merge: ACIFs
whose accepted OpenAlex links share a record are taken as one person (2026-10-09, user: "do the count
first"; the Paul Young case -- 25 ACIFs the ARC build kept apart; 9 link to the UQ virologist's
record, 14 to the Sydney pharmacist's).

Groups: connected components of ACIFs joined through shared accepted records
(processed/oax_link/accepted_links.parquet). A group would be refused when
  orcid_conflict   its ACIFs carry 2+ different ORCIDs
  career checks    the ARC build's own year checks on the merged grants, raised only by the merge
                   (src/acif/name_merge.py::year_problems: two DECRAs, a DECRA after an FT/FL, an
                   award 10+ years before a DECRA, a span over 40 years, interleaved single-
                   organisation universities)
Also reported, not applied: the ARC name stage's field test (rare_for: a pair of ACIFs sharing no
FOR2020 group and no field pair co-occurring on grants more than by chance, lift >= 1).
Reported per group: size, how its ACIFs were linked, whether they share a main name, the records.
Writes processed/shared_record_merges.md.

Usage: .venv/bin/python analysis/24_count_shared_record_merges.py
"""

import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, OAX_LINK_DIR, PROCESSED_DATA
from src.acif.name_merge import YEAR_FLAGS, load_name_merge_inputs, unlinked_pairs, year_problems

OUT = PROCESSED_DATA / "shared_record_merges.md"


def components(edges: pd.DataFrame) -> dict[str, int]:
    """ACIF -> component id, ACIFs joined when they share an author_idx."""
    parent = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for _, g in edges.groupby("author_idx"):
        cs = list(g.cluster_id)
        for c in cs[1:]:
            parent[find(c)] = find(cs[0])
        find(cs[0])
    roots = {}
    return {c: roots.setdefault(find(c), len(roots)) for c in list(parent)}


def main():
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    a = pd.read_parquet(ACIFS_ARC).set_index("cluster_id")
    a = a[~a.excluded]
    acc = acc[acc.cluster_id.isin(a.index)]
    shared = acc[acc.author_idx.map(acc.author_idx.value_counts()) > 1]
    comp = components(shared[["cluster_id", "author_idx"]])
    inp = load_name_merge_inputs()
    rec = pd.read_parquet(ACIF_ARC_RECORDS)
    grants = defaultdict(list)
    for r in rec.itertuples():
        us = set()
        if r.grant_code in inp.single_org:
            orgs = list(r.admin_orgs) if r.admin_orgs is not None and len(r.admin_orgs) else [r.admin_org]
            us = {inp.crosswalk.get(o, o) for o in orgs if o} & inp.hep_names
        grants[r.cluster_id].append((r.grant_code, None if pd.isna(r.funding_commence_year) else int(r.funding_commence_year),
                                     frozenset(us)))
    main = {}
    uid_cluster = dict(zip(rec.unique_id, rec.cluster_id))
    for uid, k in inp.main_keys.items():
        c = uid_cluster.get(uid)
        if c is not None:
            main.setdefault(c, set()).add(k)
    stage = acc.groupby("cluster_id").stage.agg(lambda s: "+".join(sorted(set(s))))
    prep = pd.read_parquet(PROCESSED_DATA / "openalex_authors_prep.parquet", columns=["author_idx", "full_name"]).set_index("author_idx").full_name

    rows = []
    by_comp = defaultdict(list)
    for c, k in comp.items():
        by_comp[k].append(c)
    for k, cs in by_comp.items():
        orcids = {o for c in cs for o in a.loc[c, "orcids"]}
        whole = year_problems([g for c in cs for g in grants[c]])
        before = set().union(*(year_problems(grants[c]) for c in cs))
        flags = [f for f in YEAR_FLAGS if f in whole and f not in before]
        if len(orcids) > 1:
            flags = ["orcid_conflict"] + flags
        fparts = [{"for": {f["name"] for f in a.loc[c, "for2020_codes"]}} for c in cs]
        bad_for, tested_for = unlinked_pairs(fparts, "for", inp.for_lift)
        keys = [main.get(c, set()) for c in cs]
        same_name = bool(set.intersection(*keys)) if all(keys) else False
        recs = sorted(set(shared[shared.cluster_id.isin(cs)].author_idx))
        rows.append({"acifs": len(cs), "ids": sorted(cs), "refusals": flags, "same_main_name": same_name,
                     "rare_for": bad_for > 0, "for_pairs_unlinked": bad_for, "for_pairs": tested_for,
                     "stages": Counter(stage[c] for c in cs), "records": recs,
                     "names": sorted({n for c in cs for n in a.loc[c, "full_names"]}),
                     "years": (min(int(a.loc[c, "first_year"]) for c in cs), max(int(a.loc[c, "last_year"]) for c in cs))})
    d = pd.DataFrame(rows)
    ok = d[d.refusals.map(len) == 0]
    L = ["# Post-linking merge by shared OpenAlex record: count only", "",
         f"- accepted (ACIF, record) links: {len(acc):,}; records accepted for 2+ ACIFs: {shared.author_idx.nunique():,}",
         f"- groups of ACIFs joined through shared records: {len(d):,}, holding {int(d.acifs.sum()):,} ACIFs",
         f"- **would merge** (no ORCID conflict, no career check raised by the merge): {len(ok):,} groups, "
         f"{int(ok.acifs.sum()):,} ACIFs -> {len(ok):,} people (kept ACIFs {len(a):,} -> {len(a) - int(ok.acifs.sum()) + len(ok):,})",
         f"- refused: {len(d) - len(ok):,} groups ({int(d.acifs.sum() - ok.acifs.sum()):,} ACIFs)", "",
         "| refused because (a group can have several) | groups |", "|---|---|"]
    for f, n in Counter(f for fs in d.refusals for f in fs).most_common():
        L.append(f"| {f} | {n:,} |")
    L += ["", f"- the ARC name stage's field test (not applied here) would flag {int(d.rare_for.sum()):,} of the {len(d):,} groups; "
              f"of the {len(ok):,} that would merge: {int(ok.rare_for.sum()):,} ({int(ok[ok.rare_for].acifs.sum()):,} ACIFs)"]
    L += ["", "| group size | groups | would merge |", "|---|---|---|"]
    for s, sub in d.groupby(d.acifs.clip(upper=6)):
        L.append(f"| {s if s < 6 else '6+'} | {len(sub):,} | {int((sub.refusals.map(len) == 0).sum()):,} |")
    L += ["", f"- groups whose ACIFs share no main name (different first given names on the grants): "
              f"{int((~d.same_main_name).sum()):,} (would merge: {int((~ok.same_main_name).sum()):,})", "",
          "How the ACIFs of mergeable groups were linked:", ""]
    st = Counter(s for ss in ok.stages for s, n in ss.items() for _ in range(n))
    L += [f"- {s}: {n:,}" for s, n in st.most_common()]

    def show(sub, n):
        out = []
        for r in sub.head(n).itertuples():
            out.append(f"- {', '.join(r.names)} ({r.acifs} ACIFs, grants {r.years[0]}-{r.years[1]}; linked by "
                       f"{dict(r.stages)}; records {', '.join(f'A{x} {prep.get(x, '')}' for x in r.records[:3])}"
                       f"{' ...' if len(r.records) > 3 else ''}){' -- refused: ' + ', '.join(r.refusals) if r.refusals else ''}")
        return out
    L += ["", "Largest groups that would merge:", ""] + show(ok.sort_values("acifs", ascending=False), 25)
    L += ["", "Groups that would merge but the field test would flag (pairs with unrelated fields / pairs tested):", ""]
    for r in ok[ok.rare_for].sort_values("acifs", ascending=False).itertuples():
        L.append(f"- {', '.join(r.names)} ({r.acifs} ACIFs; {r.for_pairs_unlinked}/{r.for_pairs}; record "
                 f"{', '.join(f'A{x} {prep.get(x, '')}' for x in r.records[:2])})")
    L += ["", "Groups that would merge although their ACIFs share no main name:", ""] + show(ok[~ok.same_main_name], 40)
    L += ["", "Refused groups (largest first):", ""] + show(d[d.refusals.map(len) > 0].sort_values("acifs", ascending=False), 40)
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L[:20]))
    print(OUT)


if __name__ == "__main__":
    main()
