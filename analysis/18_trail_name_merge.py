"""
analysis/18_trail_name_merge.py -- a trial of a simple name-based merge on top of the ACIF build,
and a check of every merged ACIF it would produce. Nothing is written back to the build.

Decisions (asked, 2026-10-03):
  - Start: the build after both Scopus passes (src/acif/build.py::build_acifs()).
  - Name rule: ACIFs that share a record's MAIN name key (00a's full_name_key in arc_names.parquet:
    first_name_canonical + family name, e.g. jack_smith) are one group, chained (A~B, B~C -> one
    group; build.key_components()). Keys from middle or compound given tokens don't link (third
    run: Hai-Bin Yu's key bin_yu joined Bin Yu; Wen Qi Zhang's qi_zhang joined Qi Zhang), and
    initial-only main keys (j_smith) don't link (first run: t_pietsch joined Tamson and Timothy
    Pietsch). Limitation: a renamed record links on its final name's main key only.
  - Start includes the hand stage (src/acif/hand.py: manual_orcids, manual_merges).
  - ORCID veto on, whole group: a group whose ACIFs carry 2+ different ORCIDs (ARC, Scopus or hand)
    is not merged at all and is counted as vetoed (build.merge_by_key()'s own rule). A group that
    would put a hand keep-apart pair (manual_confirmed_distinct.csv) in one ACIF is not merged
    either ("kept_apart").
  - Checks compare the ACIFs that were joined ("parts" = the ACIFs as they stood after pass two),
    pair by pair -- every pair must be linked directly, no chaining (third run: Paul Young's 28
    parts passed because a chain of overlaps connected a UQ virologist, a Sydney pharmacologist and
    a Monash engineer). Lift = how often a pair appears together on one grant against chance:
    count_ab * n_grants / (count_a * count_b), from 00c's tables. Lift >= 1 is evidence of a link;
    lift < 1 is no evidence either way (co-listing measures shared awards, not people moving), so
    a flag means "no evidence links these parts", for review:
      several_main_names   the group holds 2+ different main keys (a part already carrying two,
                           e.g. through an ORCID, bridges them)
      rare_for             some pair of parts (both with FOR codes) shares no FOR2020 group and no
                           cross pair of their groups has lift >= 1 (for_name_pair_freq,
                           for2020_group_rarity)
      rare_institutions    some pair of parts shares no administering university (admin_org,
                           current or at announcement, canonical HEP) and no cross pair has
                           lift >= 1 (institution_pair_freq, institution_rarity). Only grants
                           with ONE eligible organisation count (grants_flat.n_eligible_orgs == 1;
                           2026-10-03, user): on a multi-organisation grant ARC doesn't say which
                           organisation is the investigator's, so its admin_org is no evidence about
                           the person. INFORMATION ONLY, not a flag (2026-10-03, user): what
                           remains is mostly people who moved, and co-listing says nothing about
                           moves.
      interleaved_universities  two universities, each with 2+ of the group's grants, whose grant
                           years interleave (neither run ends within INTERLEAVE_TOL years of the
                           other's start) -- one person who moved shows A-then-B, two people
                           A,B,A,B. Only grants with one eligible organisation count (same reason).
                           Co-awards and adjunct posts can also interleave: a warning only.
  - Co-awardees are evidence FOR a merge, not a check: reported as groups where some pair of parts
    shares a co-investigator, and where shared co-investigators connect all the parts
    (co-investigators identified by their own ACIF after pass two).
  - Year checks on the merged ACIF: an award starting >10 years before a DECRA (DE); two different
    DE grants; a DE starting after an FT or FL; first-to-last span > 40 years.
  Every flag counts only when no single part already has the problem ("introduced by the merge").

Outputs (PROCESSED_DATA/name_merge_trial/):
    trial_groups.parquet   one row per name group of 2+ ACIFs: status (merged / vetoed), sizes,
                           names, flags, share of part pairs with no FOR / university evidence
    trial_report.md        overall contraction stats, flag counts, distributions, examples

Usage: .venv/bin/python analysis/18_trail_name_merge.py
"""

import importlib
import sys
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import PROCESSED_DATA
from src.acif.build import UnionFind, _item_orcids, build_acifs, key_components, merge_by_key
from src.acif.hand import load_hand_distinct

OUT = PROCESSED_DATA / "name_merge_trial"
LIFT_MIN = 1.0
INTERLEAVE_TOL = 2
DE_LEAD_YEARS = 10
MAX_SPAN = 40
FLAGS = ["several_main_names", "rare_for", "interleaved_universities",
         "award_10y_before_decra", "two_decras", "decra_after_ft_fl", "span_over_40y"]
INFO = ["rare_institutions"]   # reported, not a flag (2026-10-03, user)


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


def coinvestigators(acifs) -> dict[str, set[str]]:
    """unique_id -> the ACIF ids (after pass two) of the other in-scope investigators on its grant."""
    acif_of = {it.unique_id: a.cluster_id for a in acifs for it in a.items}
    by_grant = defaultdict(set)
    for u in acif_of:
        by_grant[u.split("_", 1)[0]].add(u)
    return {u: {acif_of[v] for v in by_grant[u.split("_", 1)[0]] if acif_of[v] != acif_of[u]}
            for u in acif_of}


def single_org_grants() -> set[str]:
    """Grants with exactly one eligible organisation -- the only ones whose administering
    university can be taken as the investigator's own."""
    g = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet", columns=["grant_code", "n_eligible_orgs"])
    return set(g.loc[g.n_eligible_orgs == 1, "grant_code"])


def part_facts(acif, mk, crosswalk, hep_names, coinv_of, single) -> dict:
    unis, grants = set(), []
    for it in acif.items:
        us = set()
        if it.grant_code in single:
            us = {crosswalk.get(o, o) for o in (it.admin_orgs or [it.admin_org]) if o} & hep_names
        unis |= us
        grants.append((it.grant_code, it.funding_commence_year, frozenset(us)))
    return {
        "id": acif.cluster_id,
        "names": sorted({it.full_name for it in acif.items}),
        "main_keys": {mk[it.unique_id] for it in acif.items if it.unique_id in mk},
        "for": {e["name"] for it in acif.items for e in (it.for2020_codes or []) if e.get("name")},
        "unis": unis,
        "coinv": set().union(*(coinv_of[it.unique_id] for it in acif.items)),
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


def unlinked_pairs(parts, field, lifts, seen) -> tuple[int, int]:
    """(pairs with no evidence, pairs tested) among parts that have `field`; appends each
    non-sharing pair's best lift to `seen`."""
    have = [p for p in parts if p[field]]
    bad = tested = 0
    for p, q in combinations(have, 2):
        tested += 1
        if p[field] & q[field]:
            continue
        b = best_lift(p[field], q[field], lifts)
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


# ── year checks ──────────────────────────────────────────────────────────────

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


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    acifs, uf, report = build_acifs()
    n_start = len(acifs)
    x00c = importlib.import_module("src.00c_extract_propensities")
    crosswalk, hep_names = x00c._load_institution_name_crosswalk()
    P = PROCESSED_DATA
    for_lift = lift_table(pd.read_parquet(P / "for_name_pair_freq.parquet"), "name_a", "name_b",
                          pd.read_parquet(P / "for2020_group_rarity.parquet"), "name")
    uni_lift = lift_table(pd.read_parquet(P / "institution_pair_freq.parquet"), "institution_a",
                          "institution_b", pd.read_parquet(P / "institution_rarity.parquet"),
                          "institution_name")
    mk = main_keys()
    coinv_of = coinvestigators(acifs)
    single = single_org_grants()
    facts = {a.cluster_id: part_facts(a, mk, crosswalk, hep_names, coinv_of, single) for a in acifs}

    def merge_keys(a):
        return facts[a.cluster_id]["main_keys"]

    apart = [frozenset(p) for p in load_hand_distinct()]

    def keep_apart(group) -> bool:
        recs = {it.unique_id for a in group for it in a.items}
        return any(p <= recs for p in apart)

    comps = key_components(acifs, merge_keys)
    merged_acifs, mm = merge_by_key(acifs, UnionFind(dict(uf.parent)), merge_keys,
                                    check=lambda keys, group: "kept_apart" if keep_apart(group) else None)

    rows, seen_for, seen_uni = [], [], []
    for keys, group in comps:
        parts = [facts[a.cluster_id] for a in group]
        orcids = set().union(*(p["orcids"] for p in parts))
        status = "vetoed" if len(orcids) > 1 else "kept_apart" if keep_apart(group) else "merged"
        row = {"first_id": group[0].cluster_id, "status": status,
               "n_parts": len(parts), "n_records": sum(len(a.items) for a in group),
               "n_orcids": len(orcids), "names": sorted({n for p in parts for n in p["names"]}),
               "main_keys": sorted(keys), "parts": [p["id"] for p in parts]}
        if row["status"] == "merged":
            row["several_main_names"] = len(keys) > 1 and not any(p["main_keys"] >= set(keys) for p in parts)
            bad, tested = unlinked_pairs(parts, "for", for_lift, seen_for)
            row["for_pairs_unlinked"], row["for_pairs"] = bad, tested
            row["rare_for"] = bad > 0
            bad, tested = unlinked_pairs(parts, "unis", uni_lift, seen_uni)
            row["uni_pairs_unlinked"], row["uni_pairs"] = bad, tested
            row["rare_institutions"] = bad > 0
            row["coawardee_some_link"] = any(p["coinv"] & q["coinv"] for p, q in combinations(parts, 2))
            row["coawardee_all_linked"] = n_components(parts, lambda p, q: bool(p["coinv"] & q["coinv"])) == 1
            whole = year_problems([g for p in parts for g in p["grants"]])
            before = set().union(*(year_problems(p["grants"]) for p in parts))
            for k in ("interleaved_universities", "award_10y_before_decra", "two_decras",
                      "decra_after_ft_fl", "span_over_40y"):
                row[k] = k in whole and k not in before
            row["n_flags"] = sum(bool(row[f]) for f in FLAGS)
        rows.append(row)

    g = pd.DataFrame(rows)
    OUT.mkdir(parents=True, exist_ok=True)
    g.to_parquet(OUT / "trial_groups.parquet", index=False)
    text = render(g, n_start, len(merged_acifs), report, acifs, merged_acifs, mm, seen_for, seen_uni)
    (OUT / "trial_report.md").write_text(text, encoding="utf-8")
    print(text)
    print(f"Saved to {OUT}")


def _lift_dist(values) -> str:
    s = pd.Series(values)
    if s.empty:
        return "(none)"
    bins = pd.cut(s, [-0.01, 0, 0.25, 0.5, 1.0, 2.0, 1e9],
                  labels=["never together", "<0.25", "0.25-0.5", "0.5-1", "1-2", ">=2"])
    return ", ".join(f"{k}: {v:,}" for k, v in bins.value_counts().sort_index().items()) + f" (n={len(s):,})"


def render(g, n_start, n_end, report, acifs, merged_acifs, mm, seen_for, seen_uni) -> str:
    m = g[g.status == "merged"].copy()
    m["n_flags"] = m.n_flags.astype(int)
    v = g[g.status == "vetoed"]
    rec = Counter(len(a.items) for a in acifs)
    rec2 = Counter(len(a.items) for a in merged_acifs)
    vetoed_mm = sum(1 for x in mm if x["reason"] == "orcid_veto")
    sizes = [1, 2, 3, 5, 10, 20, 10**6]
    labels = ["2", "3", "4-5", "6-10", "11-20", "21+"]
    L = ["# Trial: simple name-based merge after the Scopus passes", "",
         "Rule: ACIFs sharing a record's main name key (first_name_canonical + family; middle, compound "
         "and initial-only keys don't link), chained; ORCID veto on (whole group). Checks are pair by pair "
         f"(no chaining); FOR and university evidence = shared, or a cross pair with lift >= {LIFT_MIN}. "
         "University evidence (and interleaving) uses single-organisation grants only.", "",
         "## Contraction", "",
         f"- Records: {report['n_seed']:,}",
         f"- ACIFs after the ARC ORCID merge / Scopus pass one / pass two / hand stage: "
         f"{report['n_arc_orcid']:,} / {report['n_scopus_pass_one']:,} / {report['n_scopus_pass_two']:,} / {n_start:,}",
         f"- Name groups of 2+ ACIFs: {len(g):,} ({int(g.n_parts.sum()):,} ACIFs)",
         f"- Merged: {len(m):,} groups, {int(m.n_parts.sum()):,} ACIFs -> {len(m):,}",
         f"- Vetoed (2+ ORCIDs in the group): {len(v):,} groups, {int(v.n_parts.sum()):,} ACIFs "
         f"(merge_by_key reports {vetoed_mm:,})",
         f"- Kept apart by a hand keep-apart pair: {int((g.status == 'kept_apart').sum()):,} groups, "
         f"{int(g.loc[g.status == 'kept_apart', 'n_parts'].sum()):,} ACIFs",
         f"- **ACIFs after the trial: {n_end:,}** (was {n_start:,}; {n_start - n_end:,} fewer)", "",
         "Merged group size (ACIFs joined):", ""]
    L += [f"- {k}: {c:,}" for k, c in pd.cut(m.n_parts, sizes, labels=labels).value_counts().sort_index().items()]
    L += ["", "Vetoed group size:", ""]
    L += [f"- {k}: {c:,}" for k, c in pd.cut(v.n_parts, sizes, labels=labels).value_counts().sort_index().items()]
    L += ["", "Records per ACIF (before -> after the trial):", "", "| records | before | after |", "|---|---|---|"]
    for k in sorted(set(rec) | set(rec2))[:12]:
        L.append(f"| {k} | {rec.get(k, 0):,} | {rec2.get(k, 0):,} |")
    L += [f"| largest | {max(rec):,} | {max(rec2):,} |", ""]
    L += ["## Checks on the merged groups", "", "| check | merged groups flagged | ACIFs in them |", "|---|---|---|"]
    for f in FLAGS:
        L.append(f"| {f} | {int(m[f].sum()):,} | {int(m.loc[m[f].astype(bool), 'n_parts'].sum()):,} |")
    L.append(f"| any check | {int((m.n_flags > 0).sum()):,} | {int(m.loc[m.n_flags > 0, 'n_parts'].sum()):,} |")
    L.append(f"| none | {int((m.n_flags == 0).sum()):,} | {int(m.loc[m.n_flags == 0, 'n_parts'].sum()):,} |")
    for f in INFO:
        L.append(f"| ({f}, information only) | {int(m[f].sum()):,} | {int(m.loc[m[f].astype(bool), 'n_parts'].sum()):,} |")
    clean = m[m.n_flags == 0]
    L += ["", "Co-awardees (evidence for the merge):", "",
          f"- some pair of parts shares a co-investigator: {int(m.coawardee_some_link.sum()):,} groups "
          f"({int(clean.coawardee_some_link.sum()):,} of them clean)",
          f"- shared co-investigators connect all the parts: {int(m.coawardee_all_linked.sum()):,} groups "
          f"({int(clean.coawardee_all_linked.sum()):,} of them clean)",
          f"- flagged groups with some shared co-investigator: {int(m.loc[m.n_flags > 0, 'coawardee_some_link'].sum()):,}",
          "", f"ACIFs after the trial if only clean groups merged: {n_start - int(clean.n_parts.sum()) + len(clean):,}",
          "", "Flags per merged group:", ""]
    L += [f"- {k}: {c:,}" for k, c in m.n_flags.value_counts().sort_index().items()]
    L += ["", "Clean (no flag) by group size:", ""]
    for lab, sub in m.groupby(pd.cut(m.n_parts, [1, 2, 3, 5, 10**6], labels=["2", "3", "4-5", "6+"]), observed=True):
        L.append(f"- {lab}: {int((sub.n_flags == 0).sum()):,} of {len(sub):,}")
    L += ["", "Best cross-pair lift for pairs of parts that share no FOR group:", "", "- " + _lift_dist(seen_for),
          "", "Same for universities:", "", "- " + _lift_dist(seen_uni), ""]
    for f in FLAGS + INFO:
        ex = m[m[f].astype(bool)].sort_values("n_parts").head(8)
        if len(ex):
            L += [f"## Examples: {f}", ""]
            for r in ex.itertuples():
                L.append(f"- {r.n_parts} ACIFs: {', '.join(r.names[:8])} -- {', '.join(r.parts[:6])}")
            L.append("")
    L += ["## Largest merged groups", ""]
    for r in m.sort_values("n_parts", ascending=False).head(15).itertuples():
        fl = [f for f in FLAGS + INFO if getattr(r, f)]
        L.append(f"- {r.n_parts} ACIFs: {', '.join(r.names[:6])} -- flags: {', '.join(fl) or 'none'}; "
                 f"FOR pairs unlinked {int(r.for_pairs_unlinked)}/{int(r.for_pairs)}, university pairs "
                 f"unlinked {int(r.uni_pairs_unlinked)}/{int(r.uni_pairs)}")
    L += ["", "## Largest vetoed groups", ""]
    for r in v.sort_values("n_parts", ascending=False).head(10).itertuples():
        L.append(f"- {r.n_parts} ACIFs, {r.n_orcids} ORCIDs: {', '.join(r.names[:10])}")
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    main()
