"""
src/02b_shared_record_merges.py -- merge evidence from the linker (2026-10-21): ACIFs whose accepted
OpenAlex links share a record, merged when the set passes src/oax/shared_records.py's checks.

Reads the linker's accepted_links.parquet and the ARC build's acifs_arc / acif_arc_records; writes
processed/oax_link/shared_record_merges.parquet (one row per merge set: unique_ids of every record of
its ACIFs, cluster_ids, author_idxs), shared_record_left_out.parquet and shared_record_merges.md.
The ACIF build (src/acif/oax_merge.py, last stage of build_acifs()) applies the sets -- keyed on
record ids, so they hold across rebuilds -- under the ORCID veto, hand refusals and keep-apart pairs.
Then 01 -> 02 -> 02b again until 02b proposes no new merge.

Usage: .venv/bin/python src/02b_shared_record_merges.py
"""

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, OAX_AUTHORS, OAX_LINK_DIR, PROCESSED_DATA
from src.acif.hand import load_hand_distinct
from src.acif.name_merge import load_name_merge_inputs
from src.oax.shared_records import decide
from src.utils.names import HumanNameParser

OUT = OAX_LINK_DIR / "shared_record_merges.parquet"
LEFT = OAX_LINK_DIR / "shared_record_left_out.parquet"
REPORT = OAX_LINK_DIR / "shared_record_merges.md"


def carry_forward(sets: pd.DataFrame, acc: pd.DataFrame, rec: pd.DataFrame, prev_path=OUT) -> pd.DataFrame:
    """Add the previous run's sets that the build applied and the linker still supports: all their
    in-scope records now in one ACIF, which still has an accepted link to one of the set's records
    (without this a merged set shares no record any more, drops out, and the next build splits it
    again -- the cycle oscillated, 2026-10-21). Sets sharing records are joined into one."""
    if not Path(prev_path).exists():
        return sets
    prev = pd.read_parquet(prev_path)
    where = dict(zip(rec.unique_id, rec.cluster_id))
    links = acc.groupby("cluster_id").author_idx.apply(set).to_dict()
    keep = []
    for p in prev.itertuples():
        cs = {where[u] for u in p.unique_ids if u in where}
        if len(cs) == 1 and links.get(next(iter(cs)), set()) & set(p.author_idxs):
            keep.append({"set_id": f"carried.{p.set_id}", "cluster_ids": sorted(cs), "author_idxs": list(p.author_idxs),
                         "unique_ids": sorted(u for u in p.unique_ids if u in where), "carried": True})
    allsets = pd.concat([sets, pd.DataFrame(keep)], ignore_index=True) if keep else sets
    parent = list(range(len(allsets)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    first = {}
    for i, us in enumerate(allsets.unique_ids):
        for u in us:
            if u in first:
                parent[find(i)] = find(first[u])
            else:
                first[u] = i
    rows = []
    for g in sorted({find(i) for i in range(len(allsets))}):
        sub = allsets.iloc[[i for i in range(len(allsets)) if find(i) == g]]
        rows.append({"set_id": sub.set_id.iloc[0],
                     "cluster_ids": sorted({c for cs in sub.cluster_ids for c in cs}),
                     "author_idxs": sorted({int(x) for xs in sub.author_idxs for x in xs}),
                     "unique_ids": sorted({u for us in sub.unique_ids for u in us}),
                     "carried": bool(sub.carried.all())})
    return pd.DataFrame(rows)


def main():
    inp = load_name_merge_inputs()
    a = pd.read_parquet(ACIFS_ARC).set_index("cluster_id")
    a = a[~a.excluded]
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    acc = acc[acc.cluster_id.isin(a.index)].astype({"author_idx": "int64"})
    rec = pd.read_parquet(ACIF_ARC_RECORDS)
    rec = rec[rec.cluster_id.isin(a.index)]

    grants, givens, uids = defaultdict(list), defaultdict(set), defaultdict(list)
    for r in rec.itertuples():
        us = set()
        if r.grant_code in inp.single_org:
            orgs = list(r.admin_orgs) if r.admin_orgs is not None and len(r.admin_orgs) else [r.admin_org]
            us = {inp.crosswalk.get(o, o) for o in orgs if o} & inp.hep_names
        y = None if pd.isna(r.funding_commence_year) else int(r.funding_commence_year)
        grants[r.cluster_id].append((r.grant_code, y, frozenset(us)))
        uids[r.cluster_id].append(r.unique_id)
        k = inp.main_keys.get(r.unique_id)
        if k:
            givens[r.cluster_id].add(k.split("_", 1)[0])
    facts = {c: {"orcids": set(a.loc[c, "orcids"]), "for": {f["name"] for f in a.loc[c, "for2020_codes"]},
                 "grants": grants[c], "coinv": set(a.loc[c, "coawardee_acif_ids"])} for c in a.index}
    # keep-apart pairs (manual_confirmed_distinct.csv) are never proposed: the build would refuse the set
    # and the 01 -> 02 -> 02b cycle would never reach a fixed point (Gregory Hancock, 2026-10-23)
    where = dict(zip(rec.unique_id, rec.cluster_id))
    for x, y in load_hand_distinct():
        cx, cy = where.get(x), where.get(y)
        if cx and cy and cx != cy:
            facts[cx].setdefault("apart", set()).add(cy)
            facts[cy].setdefault("apart", set()).add(cx)

    want = sorted(set(acc.author_idx))
    con = duckdb.connect()
    con.register("want", pd.DataFrame({"author_idx": want}))
    prep = con.execute(f"""SELECT author_idx, full_name, full_name_keys FROM read_parquet('{PROCESSED_DATA}/openalex_authors_prep.parquet')
                           WHERE author_idx IN (SELECT author_idx FROM want)""").df()
    names = dict(zip(prep.author_idx, prep.full_name))
    keys = {i: list(k) for i, k in zip(prep.author_idx, prep.full_name_keys)}
    missing = [i for i in want if i not in names]
    if missing:   # records outside the HEP-context pool (stage 1 links them by ORCID)
        con.register("miss", pd.DataFrame({"author_idx": missing}))
        out = con.execute(f"""SELECT author_idx, display_name, display_name_alternatives FROM read_parquet('{OAX_AUTHORS}/*.parquet')
                              WHERE author_idx IN (SELECT author_idx FROM miss)""").df()
        p = HumanNameParser()
        for r in out.itertuples():
            ns = [r.display_name, *(list(r.display_name_alternatives) if r.display_name_alternatives is not None else [])]
            names[r.author_idx] = r.display_name
            keys[r.author_idx] = sorted({k for n in ns if isinstance(n, str) and n for k in p.parse(n).full_name_keys})

    sets, left = decide(acc[["cluster_id", "author_idx"]], facts, givens, names, keys)
    sets["unique_ids"] = [sorted(u for c in cs for u in uids[c]) for cs in sets.cluster_ids]
    sets["carried"] = False
    sets = carry_forward(sets, acc, rec)
    sets["names"] = [sorted({n for c in cs for n in a.loc[c, "full_names"]}) for cs in sets.cluster_ids]
    sets["record_names"] = [[names.get(i) for i in ids] for ids in sets.author_idxs]
    sets.to_parquet(OUT, index=False)
    left["name"] = [list(a.loc[c, "full_names"])[0] if c in a.index else "" for c in left.cluster_id]
    left.to_parquet(LEFT, index=False)

    n_in = int(sets.cluster_ids.map(len).sum())
    L = ["# Merges by shared OpenAlex record (02b)", "",
         f"- accepted links: {len(acc):,}; records accepted for 2+ ACIFs: {int((acc.author_idx.value_counts() > 1).sum()):,}",
         f"- merge sets: {len(sets):,} from {n_in:,} ACIFs (kept ACIFs {len(a):,} -> {len(a) - n_in + len(sets):,})",
         f"- ACIFs left out of their group: {len(left):,}", "", "| left out because | ACIFs |", "|---|---|"]
    for r, n in left.reason.value_counts().items():
        L.append(f"| {r} | {n:,} |")
    L += ["", "## Merge sets", ""]
    for s in sets.sort_values("cluster_ids", key=lambda x: -x.map(len)).itertuples():
        L.append(f"- {', '.join(s.names)} ({len(s.cluster_ids)} ACIFs; records {', '.join(f'A{i} {n}' for i, n in zip(s.author_idxs, s.record_names))})")
    L += ["", "## Left out", ""]
    for g, sub in left.groupby("group"):
        L.append(f"- group {g}: " + "; ".join(f"{r.cluster_id} ({r.name}): {r.reason}" for r in sub.itertuples()))
    REPORT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L[:12]))
    print(REPORT)


if __name__ == "__main__":
    main()
