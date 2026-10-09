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
Partial version (as the name stage does in flagged groups): two ACIFs of a group are compatible when
the pair raises no check (ORCID conflict, field test, career checks); the unique largest set of pairwise-
compatible ACIFs that raises no check as a set is merged, then the same on the rest. Counted twice: with
all career checks, and with the shared record overriding the interleaved-universities check.
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
from itertools import combinations

from src.acif.name_merge import YEAR_FLAGS, load_name_merge_inputs, maximal_cliques, unlinked_pairs, year_problems

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


def pair_flags(group, facts, inp, skip=()) -> list[str]:
    """Checks a set of ACIFs raises when merged: ORCID conflict, field test, career checks (new ones)."""
    out = []
    if len({o for c in group for o in facts[c]["orcids"]}) > 1:
        out.append("orcid_conflict")
    if unlinked_pairs([{"for": facts[c]["for"]} for c in group], "for", inp.for_lift)[0]:
        out.append("rare_for")
    whole = year_problems([g for c in group for g in facts[c]["grants"]])
    before = set().union(*(year_problems(facts[c]["grants"]) for c in group))
    out += [f for f in YEAR_FLAGS if f in whole and f not in before and f not in skip]
    return out


def partial(cs, facts, inp, skip=()) -> tuple[list[set], list[str]]:
    """(sets merged, ACIFs left out) by the unique-largest-compatible-set rule, repeated on the rest."""
    left, sets = list(cs), []
    while len(left) >= 2:
        adj = {c: set() for c in left}
        for x, y in combinations(left, 2):
            if not pair_flags([x, y], facts, inp, skip):
                adj[x].add(y)
                adj[y].add(x)
        cl = [c for c in maximal_cliques(left, adj) if len(c) >= 2]
        if not cl:
            break
        k = max(len(c) for c in cl)
        clean = [c for c in cl if len(c) == k and not pair_flags(c, facts, inp, skip)]
        if len(clean) != 1:
            break
        sets.append(clean[0])
        left = [c for c in left if c not in clean[0]]
    merged = set().union(*sets) if sets else set()
    return sets, [c for c in cs if c not in merged]


def leave_odd_out(cs, facts, inp, skip=()) -> tuple[set, list[str]]:
    """(set merged, ACIFs left out): each ACIF is tested against the rest of its group taken as one
    (field test on its FOR set vs the union of the others'; ORCID; career checks); those failing are
    left out, and the rest merge if 2+ remain and raise no ORCID or career check together (the field
    test is not re-applied pairwise inside the rest: a large group's grants span many fields)."""
    out = []
    for c in cs:
        rest = [x for x in cs if x != c]
        merged_rest = {"orcids": set().union(*(facts[x]["orcids"] for x in rest)),
                       "for": set().union(*(facts[x]["for"] for x in rest)),
                       "grants": [g for x in rest for g in facts[x]["grants"]]}
        f = dict(facts, __rest__=merged_rest)
        if pair_flags([c, "__rest__"], f, inp, skip):
            out.append(c)
    keep = [c for c in cs if c not in out]
    if len(keep) >= 2 and not [f for f in pair_flags(keep, facts, inp, skip) if f != "rare_for"]:
        return set(keep), out
    return set(), list(cs)


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

    facts = {c: {"orcids": set(a.loc[c, "orcids"]), "for": {f["name"] for f in a.loc[c, "for2020_codes"]},
                 "grants": grants[c]} for c in comp}
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
        sets_a, out_a = partial(sorted(cs), facts, inp)
        sets_b, out_b = partial(sorted(cs), facts, inp, skip=("interleaved_universities",))
        set_c, out_c = leave_odd_out(sorted(cs), facts, inp)
        set_d, out_d = leave_odd_out(sorted(cs), facts, inp, skip=("interleaved_universities",))
        keys = [main.get(c, set()) for c in cs]
        same_name = bool(set.intersection(*keys)) if all(keys) else False
        recs = sorted(set(shared[shared.cluster_id.isin(cs)].author_idx))
        rows.append({"acifs": len(cs), "ids": sorted(cs), "refusals": flags, "same_main_name": same_name,
                     "rare_for": bad_for > 0, "sets_a": sets_a, "left_a": out_a,
                     "sets_c": [set_c] if set_c else [], "left_c": out_c, "sets_d": [set_d] if set_d else [], "left_d": out_d, "sets_b": sets_b, "left_b": out_b, "for_pairs_unlinked": bad_for, "for_pairs": tested_for,
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
    for tag, label in (("a", "with all career checks"), ("b", "shared record overrides interleaved universities"),
                       ("c", "leave-odd-out, all career checks"), ("d", "leave-odd-out, record overrides interleaved universities")):
        n_sets = int(d[f"sets_{tag}"].map(len).sum())
        n_in = int(d[f"sets_{tag}"].map(lambda ss: sum(len(x) for x in ss)).sum())
        n_left = int(d[f"left_{tag}"].map(len).sum())
        L += ["", f"- **partial version, {label}**: {n_sets:,} merged sets from {n_in:,} ACIFs -> kept ACIFs "
                  f"{len(a):,} -> {len(a) - n_in + n_sets:,}; ACIFs left out of their group: {n_left:,} "
                  f"(groups with nothing merged: {int((d[f'sets_{tag}'].map(len) == 0).sum()):,})"]
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
    L += ["", "Leave-odd-out version (record overrides interleaved universities): ACIFs left out, with their grant "
              "fields and years:", ""]
    for r in d[d.left_d.map(len) > 0].sort_values("acifs", ascending=False).itertuples():
        L.append(f"- {', '.join(r.names)} ({r.acifs} ACIFs; merged {[len(x) for x in r.sets_d]}; record "
                 f"A{r.records[0]} {prep.get(r.records[0], '')}):")
        for c in r.left_d:
            fl = [f for f in pair_flags([c] + sorted(r.sets_d[0]), facts, inp, ("interleaved_universities",))] if r.sets_d else []
            L.append(f"  - {c}: {int(a.loc[c, 'first_year'])}-{int(a.loc[c, 'last_year'])}, "
                     f"{'; '.join(sorted(facts[c]['for']))[:110]}" + (f" -- vs the merged set: {', '.join(fl)}" if fl else ""))
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
