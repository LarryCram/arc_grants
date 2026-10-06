"""
Name stage of the ACIF build (2026-10-06, user): the last stage of build_acifs(), after the hand
stage. Rules as settled in the trial (analysis/18_trail_name_merge.py, 2026-10-03), with one
change: a group that any check flags is NOT merged -- only clean groups are. Flagged groups are
reported for later steps (partial merges, review), not guessed at.

  - Groups: ACIFs that share a record's MAIN name key (00a's full_name_key in arc_names.parquet:
    first_name_canonical + family name, e.g. jack_smith), chained (build.key_components()). Keys
    from middle or compound given tokens don't link, nor do initial-only main keys (j_smith).
  - Refused, whole group (build.merge_by_key()): names_do_not_link; orcid_veto (2+ different
    ORCIDs -- ARC, Scopus or hand -- among the group's ACIFs); kept_apart (the group would put a
    manual_confirmed_distinct.csv pair in one ACIF); flagged (any check below).
  - Checks compare the group's ACIFs ("parts") pair by pair, no chaining. Lift = how often a pair
    appears together on one grant against chance, count_ab * n_grants / (count_a * count_b), from
    00c's tables; lift >= LIFT_MIN is evidence of a link.
      several_main_names   the group holds 2+ different main keys and no part already holds them all
      rare_for             some pair of parts (both with FOR codes) shares no FOR2020 group and no
                           cross pair of their groups has lift >= LIFT_MIN
      interleaved_universities  two universities, each with 2+ of the group's grants, whose grant
                           years interleave (neither run ends within INTERLEAVE_TOL years of the
                           other's start)
      award_10y_before_decra  an award starting > DE_LEAD_YEARS before a DECRA (DE)
      two_decras           two different DE grants
      decra_after_ft_fl    a DE starting after an FT or FL
      span_over_40y        first-to-last grant span > MAX_SPAN years
    A flag counts only when no single part already has the problem (introduced by the merge).
    University evidence uses grants with one eligible organisation only (grants_flat
    n_eligible_orgs == 1): on a multi-organisation grant ARC doesn't say which organisation is the
    investigator's.
  - Partial merges in flagged groups (2026-10-06, user): two parts are compatible when the pair
    on its own raises no flag. The largest set of pairwise-compatible parts (a maximum clique) is
    merged when it is the only largest set and raises no flag as a set (interleaving and the
    DECRA rules are set properties); the other parts are left out, and the same is tried on them.
    Two or more equally large sets (ambiguous), no compatible pair, or a largest set that is still
    flagged: nothing is merged. Status "partial" in the report.
  - Information only, never refuses: rare_institutions (some pair of parts shares no university
    and no cross pair has lift >= LIFT_MIN), and co-awardee links (pairs of parts sharing a
    co-investigator, identified by the co-investigator's ACIF before this stage).
"""

from __future__ import annotations

import importlib
from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations

import pandas as pd

from config.settings import PROCESSED_DATA
from src.acif.build import UnionFind, _item_orcids, apply_unions, key_components, merge_by_key
from src.acif.models import AwardsCIF

LIFT_MIN = 1.0
INTERLEAVE_TOL = 2
DE_LEAD_YEARS = 10
MAX_SPAN = 40
FLAGS = ["several_main_names", "rare_for", "interleaved_universities",
         "award_10y_before_decra", "two_decras", "decra_after_ft_fl", "span_over_40y"]
YEAR_FLAGS = ["interleaved_universities", "award_10y_before_decra", "two_decras",
              "decra_after_ft_fl", "span_over_40y"]


@dataclass
class NameMergeInputs:
    main_keys: dict[str, str]          # unique_id -> main name key (full given name only)
    crosswalk: dict[str, str]          # organisation alias -> canonical name
    hep_names: set[str]                # canonical names that are HEPs
    single_org: set[str]               # grant codes with one eligible organisation
    for_lift: dict[tuple[str, str], float]
    uni_lift: dict[tuple[str, str], float]


# ── inputs ───────────────────────────────────────────────────────────────────

def main_keys() -> dict[str, str]:
    """unique_id -> the record's main name key from 00a (full_name_key, else the raw key),
    only where it has a full given name."""
    a = pd.read_parquet(PROCESSED_DATA / "arc_names.parquet",
                        columns=["unique_id", "full_name_key", "full_name_key_raw"])
    out = {}
    for r in a.itertuples(index=False):
        k = r.full_name_key or r.full_name_key_raw
        if isinstance(k, str) and "_" in k and len(k.split("_", 1)[0]) > 1:
            out[r.unique_id] = k
    return out


def lift_table(pairs: pd.DataFrame, a: str, b: str, margins: pd.DataFrame, name: str) -> dict:
    n = round((margins["count"] / margins["frequency"]).iloc[0])
    c = dict(zip(margins[name], margins["count"]))
    return {(r[a], r[b]): r["count"] * n / (c[r[a]] * c[r[b]])
            for _, r in pairs.iterrows() if r[a] in c and r[b] in c}


def single_org_grants() -> set[str]:
    """Grants with exactly one eligible organisation -- the only ones whose administering
    university can be taken as the investigator's own."""
    g = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet", columns=["grant_code", "n_eligible_orgs"])
    return set(g.loc[g.n_eligible_orgs == 1, "grant_code"])


def load_name_merge_inputs() -> NameMergeInputs:
    x00c = importlib.import_module("src.00c_extract_propensities")
    crosswalk, hep_names = x00c._load_institution_name_crosswalk()
    P = PROCESSED_DATA
    return NameMergeInputs(
        main_keys=main_keys(), crosswalk=crosswalk, hep_names=hep_names,
        single_org=single_org_grants(),
        for_lift=lift_table(pd.read_parquet(P / "for_name_pair_freq.parquet"), "name_a", "name_b",
                            pd.read_parquet(P / "for2020_group_rarity.parquet"), "name"),
        uni_lift=lift_table(pd.read_parquet(P / "institution_pair_freq.parquet"), "institution_a",
                            "institution_b", pd.read_parquet(P / "institution_rarity.parquet"),
                            "institution_name"),
    )


# ── facts per ACIF ───────────────────────────────────────────────────────────

def coinvestigators(acifs) -> dict[str, set[str]]:
    """unique_id -> the ACIF ids of the other in-scope investigators on its grant."""
    acif_of = {it.unique_id: a.cluster_id for a in acifs for it in a.items}
    by_grant = defaultdict(set)
    for u in acif_of:
        by_grant[u.split("_", 1)[0]].add(u)
    return {u: {acif_of[v] for v in by_grant[u.split("_", 1)[0]] if acif_of[v] != acif_of[u]}
            for u in acif_of}


def part_facts(acif, inp: NameMergeInputs, coinv_of) -> dict:
    unis, grants = set(), []
    for it in acif.items:
        us = set()
        if it.grant_code in inp.single_org:
            us = {inp.crosswalk.get(o, o) for o in (it.admin_orgs or [it.admin_org]) if o} & inp.hep_names
        unis |= us
        grants.append((it.grant_code, it.funding_commence_year, frozenset(us)))
    return {
        "id": acif.cluster_id,
        "names": sorted({it.full_name for it in acif.items}),
        "main_keys": {inp.main_keys[it.unique_id] for it in acif.items if it.unique_id in inp.main_keys},
        "for": {e["name"] for it in acif.items for e in (it.for2020_codes or []) if e.get("name")},
        "unis": unis,
        "coinv": set().union(*(coinv_of.get(it.unique_id, set()) for it in acif.items)),
        "orcids": _item_orcids(acif),
        "grants": grants,
    }


# ── pair tests ───────────────────────────────────────────────────────────────

def best_lift(xs, ys, lifts) -> float:
    best = 0.0
    for x in xs:
        for y in ys:
            if x != y:
                best = max(best, lifts.get((min(x, y), max(x, y)), 0.0))
    return best


def unlinked_pairs(parts, field, lifts, seen=None) -> tuple[int, int]:
    """(pairs with no evidence, pairs tested) among parts that have `field`; appends each
    non-sharing pair's best lift to `seen` if given."""
    have = [p for p in parts if p[field]]
    bad = tested = 0
    for p, q in combinations(have, 2):
        tested += 1
        if p[field] & q[field]:
            continue
        b = best_lift(p[field], q[field], lifts)
        if seen is not None:
            seen.append(b)
        if b < LIFT_MIN:
            bad += 1
    return bad, tested


def n_components(parts, linked) -> int:
    uf = UnionFind()
    for p in parts:
        uf.find(p["id"])
    for p, q in combinations(parts, 2):
        if linked(p, q):
            uf.union(p["id"], q["id"])
    return len({uf.find(p["id"]) for p in parts})


def _scheme(grant_code: str) -> str:
    return "".join(ch for ch in grant_code[:2] if ch.isalpha())


def year_problems(grants) -> set[str]:
    out = set()
    yrs = [(g, y) for g, y, _ in grants if y]
    de = {(g, y) for g, y in yrs if _scheme(g) == "DE"}
    if len({g for g, _ in de}) > 1:
        out.add("two_decras")
    for _, dy in de:
        if any(y < dy - DE_LEAD_YEARS for g, y in yrs if _scheme(g) != "DE"):
            out.add("award_10y_before_decra")
        if any(_scheme(g) in ("FT", "FL") and y < dy for g, y in yrs):
            out.add("decra_after_ft_fl")
    if yrs and max(y for _, y in yrs) - min(y for _, y in yrs) > MAX_SPAN:
        out.add("span_over_40y")
    by_uni = defaultdict(list)
    for g, y, us in grants:
        if y and len(us) == 1:
            by_uni[next(iter(us))].append(y)
    runs = {u: (min(v), max(v)) for u, v in by_uni.items() if len(v) >= 2}
    for (a, (a0, a1)), (b, (b0, b1)) in combinations(sorted(runs.items()), 2):
        if not (a1 <= b0 + INTERLEAVE_TOL or b1 <= a0 + INTERLEAVE_TOL):
            out.add("interleaved_universities")
    return out


def group_checks(keys, parts, inp: NameMergeInputs, seen_for=None, seen_uni=None) -> dict:
    """Every check and information field for one group of parts; row["flags"] lists the flags."""
    row = {"several_main_names": len(keys) > 1 and not any(p["main_keys"] >= set(keys) for p in parts)}
    bad, tested = unlinked_pairs(parts, "for", inp.for_lift, seen_for)
    row.update(for_pairs_unlinked=bad, for_pairs=tested, rare_for=bad > 0)
    bad, tested = unlinked_pairs(parts, "unis", inp.uni_lift, seen_uni)
    row.update(uni_pairs_unlinked=bad, uni_pairs=tested, rare_institutions=bad > 0)
    row["coawardee_some_link"] = any(p["coinv"] & q["coinv"] for p, q in combinations(parts, 2))
    row["coawardee_all_linked"] = n_components(parts, lambda p, q: bool(p["coinv"] & q["coinv"])) == 1
    whole = year_problems([g for p in parts for g in p["grants"]])
    before = set().union(*(year_problems(p["grants"]) for p in parts))
    for k in YEAR_FLAGS:
        row[k] = k in whole and k not in before
    row["flags"] = [f for f in FLAGS if row[f]]
    return row


# ── partial merges in flagged groups ─────────────────────────────────────────

def maximal_cliques(nodes, adj) -> list[set]:
    """Bron-Kerbosch with pivoting; adj[i] is the set of nodes compatible with i."""
    out = []

    def bk(r, p, x):
        if not p and not x:
            out.append(r)
            return
        u = max(p | x, key=lambda v: len(adj[v] & p))
        for v in sorted(p - adj[u]):
            bk(r | {v}, p & adj[v], x & adj[v])
            p = p - {v}
            x = x | {v}

    bk(set(), set(nodes), set())
    return out


def _keys_of(parts, idx):
    return sorted(set().union(*(parts[i]["main_keys"] for i in idx)))


def best_set(parts, idx, inp: NameMergeInputs) -> tuple[str, set, int]:
    """(status, chosen indices, number of largest sets) among parts[idx]: status is "unique",
    "ambiguous", "no_compatible_pair" or "largest_set_flagged"."""
    adj = {i: set() for i in idx}
    for i, j in combinations(idx, 2):
        if not group_checks(_keys_of(parts, (i, j)), [parts[i], parts[j]], inp)["flags"]:
            adj[i].add(j)
            adj[j].add(i)
    cliques = [c for c in maximal_cliques(idx, adj) if len(c) >= 2]
    if not cliques:
        return "no_compatible_pair", set(), 0
    k = max(len(c) for c in cliques)
    top = [c for c in cliques if len(c) == k]
    clean = [c for c in top if not group_checks(_keys_of(parts, c), [parts[i] for i in c], inp)["flags"]]
    if not clean:
        return "largest_set_flagged", set(), len(top)
    if len(clean) > 1:
        return "ambiguous", set(), len(clean)
    return "unique", clean[0], 1


def partial_sets(parts, inp: NameMergeInputs) -> tuple[str, list[set]]:
    """The first round's status and every set to merge (rounds on the parts left over, while the
    largest set is unique)."""
    left = list(range(len(parts)))
    sets, first = [], None
    while len(left) >= 2:
        status, chosen, _ = best_set(parts, left, inp)
        first = first or status
        if status != "unique":
            break
        sets.append(chosen)
        left = [i for i in left if i not in chosen]
    return first, sets


# ── the stage ────────────────────────────────────────────────────────────────

def name_merge(acifs: list[AwardsCIF], uf: UnionFind, inputs: NameMergeInputs | None = None,
               distinct=None) -> tuple[list[AwardsCIF], dict]:
    """Merge the clean name groups, then the unique largest clean sets inside flagged groups.
    Returns (acifs, report): report["groups"] has one row per name group of 2+ ACIFs -- status
    (merged / partial / orcid_veto / names_do_not_link / kept_apart / flagged), parts, names,
    keys, for groups that reached the checks every check field, and for flagged groups
    partial_status (first round), partial_sets and parts_left_out."""
    if inputs is None:
        inputs = load_name_merge_inputs()
    if distinct is None:
        from src.acif.hand import load_hand_distinct
        distinct = load_hand_distinct()
    apart = [frozenset(p) for p in distinct]
    coinv_of = coinvestigators(acifs)
    facts = {a.cluster_id: part_facts(a, inputs, coinv_of) for a in acifs}
    checked: dict[str, dict] = {}

    def check(keys, group):
        recs = {it.unique_id for a in group for it in a.items}
        if any(p <= recs for p in apart):
            return "kept_apart"
        row = group_checks(keys, [facts[a.cluster_id] for a in group], inputs)
        checked[group[0].cluster_id] = row
        return "flagged" if row["flags"] else None

    merged, refused = merge_by_key(acifs, uf, lambda a: facts[a.cluster_id]["main_keys"], check=check)
    reason_of = {}
    for m in refused:
        for sub in m["groups"]:
            for cid in sub:
                reason_of[cid] = m["reason"]

    partial: dict[str, tuple[str, list[list[str]]]] = {}
    to_merge = []
    for m in refused:
        if m["reason"] != "flagged":
            continue
        ids = sorted(c for sub in m["groups"] for c in sub)
        parts = [facts[c] for c in ids]
        status, sets = partial_sets(parts, inputs)
        named = [sorted(ids[i] for i in st) for st in sets]
        partial[ids[0]] = (status, named)
        for st in named:
            to_merge += [(st[0], c) for c in st[1:]]
    merged = apply_unions(merged, uf, to_merge)

    rows = []
    for keys, group in key_components(acifs, lambda a: facts[a.cluster_id]["main_keys"]):
        first = group[0].cluster_id
        row = {"first_id": first, "status": reason_of.get(first, "merged"),
               "n_parts": len(group), "n_records": sum(len(a.items) for a in group),
               "n_orcids": len(set().union(*(facts[a.cluster_id]["orcids"] for a in group))),
               "names": sorted({n for a in group for n in facts[a.cluster_id]["names"]}),
               "main_keys": sorted(keys), "parts": [a.cluster_id for a in group]}
        row.update(checked.get(first, {}))
        if first in partial:
            status, named = partial[first]
            row["partial_status"], row["partial_sets"] = status, named
            row["parts_left_out"] = sorted(set(row["parts"]) - {c for st in named for c in st})
            if named:
                row["status"] = "partial"
        rows.append(row)
    groups = pd.DataFrame(rows)
    report = {"groups": groups, "n_before": len(acifs), "n_after": len(merged),
              "status_counts": groups["status"].value_counts().to_dict() if len(groups) else {}}
    return merged, report
