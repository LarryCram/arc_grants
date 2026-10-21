"""
Merge evidence from the linker (2026-10-21, user: resolve the open ARC-phase decisions): ACIFs whose
accepted OpenAlex links share a record are taken as one person, when the merged set passes checks.
Rules (counted first in analysis/24_count_shared_record_merges.py; this is its "leave-odd-out, record
overrides interleaved universities" variant plus two name tests):

  group           ACIFs joined through shared accepted records (connected components)
  polluted record a record whose OpenAlex name lists several people ("Harris, Jeffrey H. (Oak Ridge,
                  TN); Hender, Timothy C. ...") joins nothing
  name tests      an ACIF with a full given name is left out when none of its main given names fits
                  the record's own given names (equal without separators, nickname-related, a clipped
                  form of 4+ letters, or one of the record's name forms -- "Mitchell, Jane Margaret"
                  fits Jane and Margaret); a record with initials only fits on the first initial. A
                  group whose remaining ACIFs hold full given names that fit neither each other nor
                  the record's name forms is not merged (Huawei / Hui Zhao on "H.C. Zhao")
  same grant      ACIFs that share a grant are two investigators on it (00a joins one person's two
                  name forms on a grant): both are left out (Mark / Thomas Gabbott, George / Malcolm
                  Smith -- OpenAlex lists the co-investigator's name among the record's alternatives)
  pairs           two ACIFs are compatible when they share a FOR2020 group or a co-investigator (the
                  name stage's lift rescue is not used: it let econometrics join psychology, and
                  materials engineering join physiology, on one record), carry no conflicting ORCID,
                  and raise no career check when merged; the shared record overrides the
                  interleaved-universities check (joint or successive posts: Graeme Hugo, George
                  Kuczera, Zhao Yang Dong)
  set             the unique largest set of pairwise-compatible ACIFs that raises no ORCID or career
                  check as a set is merged; then the same on the rest. Equally large sets (James
                  Smith: two Curtin civil-engineering grants and two UQ gender-studies grants on one
                  record) merge nothing -- a missed merge, not a wrong one. (A first version tested
                  each ACIF against the rest taken as one, which let such symmetric splits through.)

Output: one row per (merge set, record): set_id, unique_ids (the records of every ACIF in the set),
cluster_ids, author_idx, names; and the left-out ACIFs with their reason.
"""

from __future__ import annotations

from collections import defaultdict

import pandas as pd

from itertools import combinations

from src.acif.name_merge import YEAR_FLAGS, maximal_cliques, year_problems
from src.oax.orcid_link import _compact
from src.utils.names import given_names_related

NICK_MIN_CLIPPED = 4


def components(edges: pd.DataFrame) -> dict[str, int]:
    """cluster_id -> component id; ACIFs joined when they share an author_idx."""
    parent: dict[str, str] = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for _, g in edges.groupby("author_idx"):
        cs = sorted(g.cluster_id)
        for c in cs[1:]:
            parent[find(c)] = find(cs[0])
        find(cs[0])
    roots: dict[str, int] = {}
    return {c: roots.setdefault(find(c), len(roots)) for c in sorted(parent)}


def _given(keys) -> set[str]:
    return {k.split("_", 1)[0] for k in keys if "_" in k}


def _fits(a: str, b: str) -> bool:
    return _compact(a) == _compact(b) or given_names_related(a, b, NICK_MIN_CLIPPED)


def name_fits_record(acif_given: set[str], record_keys) -> bool:
    """An ACIF's main given names against the record's own name forms (module docstring)."""
    if not acif_given:
        return True
    rg = _given(record_keys)
    full = {g for g in rg if len(g) > 1}
    if not full:
        return any(g[0] in rg for g in acif_given)
    return any(_fits(a, b) for a in acif_given for b in full)


def given_names_agree(givens: list[set[str]], record_keys) -> bool:
    """Every pair of ACIFs with full given names fits each other or both appear in the record's
    name forms."""
    full_rec = {g for g in _given(record_keys) if len(g) > 1}
    have = [g for g in givens if g]
    for i in range(len(have)):
        for j in range(i + 1, len(have)):
            a, b = have[i], have[j]
            if any(_fits(x, y) for x in a for y in b):
                continue
            if a & full_rec and b & full_rec:
                continue
            return False
    return True


def set_flags(group, facts, skip=("interleaved_universities",)) -> list[str]:
    """ORCID conflict and career checks a set of ACIFs raises when merged (new ones only)."""
    out = []
    if len({o for c in group for o in facts[c]["orcids"]}) > 1:
        out.append("orcid_conflict")
    whole = year_problems([g for c in group for g in facts[c]["grants"]])
    before = set().union(*(year_problems(facts[c]["grants"]) for c in group))
    return out + [f for f in YEAR_FLAGS if f in whole and f not in before and f not in skip]


def compatible(a, b, facts) -> bool:
    fa, fb = facts[a], facts[b]
    linked = bool(fa["for"] & fb["for"]) or b in fa["coinv"] or a in fb["coinv"]
    return linked and not set_flags([a, b], facts)


def merge_sets(cs, facts) -> tuple[list[list[str]], dict[str, str]]:
    """(sets merged, {left-out ACIF: reason}) by the unique-largest-compatible-set rule."""
    left, sets = sorted(cs), []
    while len(left) >= 2:
        adj = {c: {d for d in left if d != c and compatible(c, d, facts)} for c in left}
        cl = [c for c in maximal_cliques(left, adj) if len(c) >= 2]
        if not cl:
            break
        k = max(len(c) for c in cl)
        top = [c for c in cl if len(c) == k and not set_flags(sorted(c), facts)]
        if len(top) != 1:
            break
        sets.append(sorted(top[0]))
        left = [c for c in left if c not in top[0]]
    reason = "no_compatible_set" if not sets else "outside_the_merged_set"
    return sets, {c: reason for c in left}


def decide(links: pd.DataFrame, facts: dict, givens: dict, record_names: dict, record_keys: dict):
    """links: accepted (cluster_id, author_idx). facts: cluster_id -> {orcids, for, grants, coinv};
    givens: cluster_id -> main given names; record_names / record_keys: author_idx -> OpenAlex name /
    name keys. Returns (sets, left): sets rows (set_id, cluster_ids, author_idxs); left rows
    (cluster_id, group, reason)."""
    shared = links[links.author_idx.map(links.author_idx.value_counts()) > 1]
    polluted = {a for a, n in record_names.items() if isinstance(n, str) and ";" in n}
    shared = shared[~shared.author_idx.isin(polluted)]
    comp = components(shared[["cluster_id", "author_idx"]])
    recs_of = shared.groupby("cluster_id").author_idx.apply(set)
    by_comp = defaultdict(list)
    for c, k in comp.items():
        by_comp[k].append(c)
    sets, left = [], []
    for k, cs in sorted(by_comp.items()):
        recs = sorted(set().union(*(recs_of[c] for c in cs)))
        rkeys = set().union(*(set(record_keys.get(r, ())) for r in recs))
        ok = [c for c in cs if all(name_fits_record(givens.get(c, set()), record_keys.get(r, ())) for r in recs_of[c])]
        left += [{"cluster_id": c, "group": k, "reason": "given_name_differs_from_record"} for c in cs if c not in ok]
        gcount = defaultdict(set)
        for c in ok:
            for g in {x[0] for x in facts[c]["grants"]}:
                gcount[g].add(c)
        same = set().union(*[v for v in gcount.values() if len(v) > 1]) if gcount else set()
        left += [{"cluster_id": c, "group": k, "reason": "shares_a_grant"} for c in ok if c in same]
        ok = [c for c in ok if c not in same]
        if len(ok) >= 2 and not given_names_agree([givens.get(c, set()) for c in ok], rkeys):
            left += [{"cluster_id": c, "group": k, "reason": "given_names_differ"} for c in ok]
            continue
        if len(ok) < 2:
            left += [{"cluster_id": c, "group": k, "reason": "alone"} for c in ok]
            continue
        found, out = merge_sets(ok, facts)
        left += [{"cluster_id": c, "group": k, "reason": r} for c, r in out.items()]
        for i, keep in enumerate(found):
            sets.append({"set_id": f"{k}.{i}", "cluster_ids": keep,
                         "author_idxs": sorted(set().union(*(recs_of[c] for c in keep)))})
    return pd.DataFrame(sets, columns=["set_id", "cluster_ids", "author_idxs"]), \
        pd.DataFrame(left, columns=["cluster_id", "group", "reason"])
