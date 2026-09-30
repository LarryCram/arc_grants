"""
analysis/16_scopus_lookup.py -- look ACIFs up in Scopus Author Search. Diagnostic only: nothing
in the ACIF build reads the output (see analysis/utils/scopus.py).

For each ACIF (the current build: load_items -> seed -> merge_by_orcid) one Author Search:
    OR over its full-given-name keys  AND  OR over AFFIL(name) of every ARC university on any
    of its grants (admin, announcement admin, other eligible and collaborating organisations).
Each profile found is recorded with its ORCID (if Scopus has one), and ARC / Scopus / ORCID are
cross-checked:
    ARC has an ORCID:   confirmed      -- a profile found carries the same ORCID
                        other_orcid    -- profiles carry ORCIDs, none the ARC one
                        no_scopus_orcid-- no profile found carries an ORCID
    ARC has no ORCID:   one_orcid      -- exactly one ORCID among the profiles found
                        several_orcids -- two or more
                        no_scopus_orcid
    no_profile          -- the search found nothing
Quota: one Author Search request per ACIF (cached by pybliometrics; a rerun spends nothing).

Outputs (PROCESSED_DATA/scopus/):
    acif_scopus_profiles.parquet   one row per (cluster_id, scopus_id)
    acif_scopus_summary.parquet    one row per ACIF searched, with the status above
    acif_scopus_lookup.md          counts and examples

Usage:
    .venv/bin/python analysis/16_scopus_lookup.py                   # seeded sample of 200 ACIFs
    .venv/bin/python analysis/16_scopus_lookup.py --sample 500 --seed 3
    .venv/bin/python analysis/16_scopus_lookup.py --cluster DE120101263_karen_marsh DP0208414_thomas_davis
    .venv/bin/python analysis/16_scopus_lookup.py --all             # every ACIF (~41K requests)
"""

import argparse
import random
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import PROCESSED_DATA
from analysis.utils.scopus import (acif_query, affiliation_names, grant_universities, init_scopus,
                                   load_university_map, search_keys)
from src.acif.build import UnionFind, build_stage_zero, compute_orcids, merge_by_orcid

OUT_DIR = PROCESSED_DATA / "scopus"


def status(arc_orcids: set[str], scopus_orcids: set[str], n_profiles: int) -> str:
    if n_profiles == 0:
        return "no_profile"
    if arc_orcids:
        if arc_orcids & scopus_orcids:
            return "confirmed"
        return "other_orcid" if scopus_orcids else "no_scopus_orcid"
    if len(scopus_orcids) == 1:
        return "one_orcid"
    return "several_orcids" if scopus_orcids else "no_scopus_orcid"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=200)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--cluster", nargs="*")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    from pybliometrics.scopus import AuthorSearch
    init_scopus()
    unis = load_university_map()
    affil = affiliation_names(unis)
    gunis = grant_universities()
    acifs, _ = merge_by_orcid(build_stage_zero(), UnionFind({}))
    for a in acifs:
        compute_orcids(a)
    if args.cluster:
        chosen = [a for a in acifs if a.cluster_id in set(args.cluster)]
    elif args.all:
        chosen = acifs
    else:
        chosen = random.Random(args.seed).sample(sorted(acifs, key=lambda a: a.cluster_id), args.sample)

    profiles, summary = [], []
    for i, a in enumerate(chosen, 1):
        keys = sorted({k for it in a.items for k in it.full_name_keys})
        grants = sorted({it.grant_code for it in a.items})
        heps = sorted(set().union(*[gunis.get(g, set()) for g in grants]))
        q = acif_query(keys, heps, affil)
        try:
            found = AuthorSearch(q).authors or []
            error = None
        except Exception as e:
            found, error = [], f"{type(e).__name__}: {str(e)[:100]}"
        arc_orcids = set(a.orcids)
        scopus_orcids = set()
        for p in found:
            orcid = (p.orcid or "").strip("[]") or None
            if orcid:
                scopus_orcids.add(orcid)
            profiles.append({
                "cluster_id": a.cluster_id, "scopus_id": p.eid.split("-")[-1], "orcid": orcid,
                "given_name": p.givenname, "surname": p.surname, "documents": p.documents,
                "current_affiliation": p.affiliation, "current_affiliation_id": p.affiliation_id,
                "country": p.country, "areas": p.areas, "orcid_matches_arc": bool(orcid and orcid in arc_orcids),
            })
        summary.append({
            "cluster_id": a.cluster_id, "n_grants": len(grants), "universities": heps,
            "search_keys": search_keys(keys), "arc_orcids": sorted(arc_orcids),
            "n_profiles": len(found), "scopus_orcids": sorted(scopus_orcids),
            "status": "error" if error else status(arc_orcids, scopus_orcids, len(found)),
            "error": error, "query": q,
        })
        if i % 100 == 0:
            print(f"  {i:,}/{len(chosen):,}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    s, p = pd.DataFrame(summary), pd.DataFrame(profiles)
    s.to_parquet(OUT_DIR / "acif_scopus_summary.parquet", index=False)
    p.to_parquet(OUT_DIR / "acif_scopus_profiles.parquet", index=False)
    (OUT_DIR / "acif_scopus_lookup.md").write_text(render(s, p), encoding="utf-8")
    print(render(s, p))
    print(f"\nSaved to {OUT_DIR}")


def render(s: pd.DataFrame, p: pd.DataFrame) -> str:
    lines = ["# ACIF Scopus lookup", "", f"ACIFs searched: {len(s):,}", ""]
    lines += ["| status | ACIFs |", "|---|---|"]
    lines += [f"| {k} | {v:,} |" for k, v in s.status.value_counts().items()]
    lines += ["", "Profiles found per ACIF:", ""]
    b = pd.cut(s.n_profiles, [-1, 0, 1, 5, 20, 10**9], labels=["0", "1", "2-5", "6-20", "21+"])
    lines += [f"- {k}: {v:,}" for k, v in b.value_counts().sort_index().items()]
    for st in ("other_orcid", "one_orcid", "several_orcids"):
        ex = s[s.status == st].head(10)
        if len(ex):
            lines += ["", f"## {st} (first {len(ex)})", ""]
            for r in ex.itertuples():
                lines.append(f"- {r.cluster_id}: ARC {r.arc_orcids or '-'} / Scopus {r.scopus_orcids} "
                             f"({r.n_profiles} profiles)")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
