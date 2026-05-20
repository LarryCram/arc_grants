"""
10b_split_fellowships.py  —  Layer 0.5

Split clusters that contain two or more fellowship records with overlapping
grant years.  A single investigator cannot hold two ARC fellowships
concurrently; if two fellowship records in the same cluster overlap in time,
they must belong to different people.

Split rule:
  1. Collect fellowship records (role_code in FELLOWSHIP_CODES).
  2. Look up each grant's (commence_year, commence_year + years_funded) window.
  3. If any two fellowship grants overlap → split:
       - One child cluster per distinct fellowship grant_code.
       - One residual child for all non-fellowship records (if any).
  4. Records whose dates are missing are treated conservatively: they do NOT
     trigger a split but are placed in the child cluster for their grant_code.

Split reason: OVERLAPPING_FELLOWSHIPS

Run AFTER 10_build_clusters.py, BEFORE 12_layer1_orcid.py.

Usage:
  python 10b_split_fellowships.py               # reads clusters_after_layer0.jsonl
  python 10b_split_fellowships.py --dry-run
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.pipeline.models import (
    Cluster, ClusterStatus, SplitReason, load_clusters, save_clusters,
)
from src.utils.io import setup_stdout_utf8


FELLOWSHIP_CODES = {
    "FT", "FL", "FF", "DE", "DECRA", "APD", "APF", "ARF",
    "QEII", "APDI", "IRF", "ARFI", "LE",
}


def _overlaps(s1: int, e1: int, s2: int, e2: int) -> bool:
    """True if [s1, e1) and [s2, e2) share at least one year."""
    return s1 < e2 and s2 < e1


def _try_split(
    cluster: Cluster,
    grant_dates: dict[str, tuple[int, int]],
    next_id: int,
) -> tuple[list[Cluster] | None, int]:
    """
    Returns (replacement_list, next_id) if a split is warranted, else (None, next_id).
    replacement_list = [parent_SPLIT, child1, child2, ..., residual?]
    """
    # Group records by fellowship grant (dedup by grant_code within fellowships)
    fellowship_by_grant: dict[str, list] = defaultdict(list)
    other_recs = []

    for r in cluster.records:
        if r.role_code in FELLOWSHIP_CODES:
            fellowship_by_grant[r.grant_code].append(r)
        else:
            other_recs.append(r)

    f_grants = list(fellowship_by_grant.keys())
    if len(f_grants) < 2:
        return None, next_id

    # Check for overlap between any pair of fellowship grants
    dated = [(gc, grant_dates[gc]) for gc in f_grants if gc in grant_dates]
    has_overlap = False
    for i in range(len(dated)):
        for j in range(i + 1, len(dated)):
            _, (s1, e1) = dated[i]
            _, (s2, e2) = dated[j]
            if _overlaps(s1, e1, s2, e2):
                has_overlap = True
                break
        if has_overlap:
            break

    if not has_overlap:
        return None, next_id

    # Build child clusters: one per fellowship grant
    n_children = len(f_grants) + (1 if other_recs else 0)
    child_ids = list(range(next_id, next_id + n_children))
    next_id += n_children
    sibling_ids = child_ids[:]

    children: list[Cluster] = []
    for i, gc in enumerate(f_grants):
        recs = fellowship_by_grant[gc]
        child = Cluster(
            cluster_id=child_ids[i],
            name_forms=cluster.name_forms,
            records=recs,
            orcids=sorted({r.orcid for r in recs if r.orcid}),
            institutions=sorted({h for r in recs for h in r.grant_heps}),
            for_2d=sorted({f for r in recs for f in r.for_2d}),
            co_awardee_cluster_ids=cluster.co_awardee_cluster_ids,
            parent_id=cluster.cluster_id,
            sibling_cluster_ids=[s for s in sibling_ids if s != child_ids[i]],
            split_reason=SplitReason.OVERLAPPING_FELLOWSHIPS.value,
        )
        children.append(child)

    if other_recs:
        residual = Cluster(
            cluster_id=child_ids[-1],
            name_forms=cluster.name_forms,
            records=other_recs,
            orcids=sorted({r.orcid for r in other_recs if r.orcid}),
            institutions=sorted({h for r in other_recs for h in r.grant_heps}),
            for_2d=sorted({f for r in other_recs for f in r.for_2d}),
            co_awardee_cluster_ids=cluster.co_awardee_cluster_ids,
            parent_id=cluster.cluster_id,
            sibling_cluster_ids=[c.cluster_id for c in children],
            split_reason=SplitReason.OVERLAPPING_FELLOWSHIPS.value,
        )
        children.append(residual)

    cluster.status = ClusterStatus.SPLIT.value
    return [cluster] + children, next_id


def main():
    setup_stdout_utf8()

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--input", default=str(PROCESSED_DATA / "clusters_after_layer0.jsonl"),
        help="Source cluster file (default: clusters_after_layer0.jsonl)",
    )
    args = parser.parse_args()

    # ── Load grant dates ──────────────────────────────────────────────────────
    print("Loading grant dates...")
    df_g = pd.read_parquet(
        PROCESSED_DATA / "grants.parquet",
        columns=["grant_code", "funding_commence_year", "years_funded"],
    )
    grant_dates: dict[str, tuple[int, int]] = {}
    for row in df_g.itertuples(index=False):
        if pd.notna(row.funding_commence_year) and pd.notna(row.years_funded):
            s = int(row.funding_commence_year)
            grant_dates[row.grant_code] = (s, s + int(row.years_funded))
    print(f"  Grants with dates: {len(grant_dates):,}")

    # ── Load clusters ─────────────────────────────────────────────────────────
    in_path = Path(args.input)
    print(f"Loading clusters from {in_path.name}...")
    clusters = load_clusters(in_path)
    targets = [c for c in clusters if c.status == ClusterStatus.UNRESOLVED.value]
    print(f"  Total:      {len(clusters):,}")
    print(f"  UNRESOLVED: {len(targets):,}")

    # ── Apply fellowship split ────────────────────────────────────────────────
    next_id = max(c.cluster_id for c in clusters) + 1
    n_split = 0
    n_children = 0
    split_log: list[dict] = []
    out_chunks: list[list[Cluster]] = []

    for cluster in clusters:
        if cluster.status != ClusterStatus.UNRESOLVED.value:
            out_chunks.append([cluster])
            continue

        replacement, next_id = _try_split(cluster, grant_dates, next_id)
        if replacement is None:
            out_chunks.append([cluster])
        else:
            n_split += 1
            active_children = [c for c in replacement if c.cluster_id != cluster.cluster_id]
            n_children += len(active_children)
            name = f"{cluster.name_forms[0].arc.first} {cluster.name_forms[0].arc.last}".strip()
            fellowship_children = [c for c in active_children if c.records and
                                   c.records[0].role_code in FELLOWSHIP_CODES]
            split_log.append({
                "name": name,
                "cluster_id": cluster.cluster_id,
                "n_fellowship_children": len(fellowship_children),
                "n_total_children": len(active_children),
                "child_ids": [c.cluster_id for c in active_children],
            })
            out_chunks.append(replacement)

    out_clusters = [c for chunk in out_chunks for c in chunk]

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"LAYER 0.5 — FELLOWSHIP SPLIT  {'(DRY RUN)' if args.dry_run else ''}")
    print(f"{'─'*60}")
    print(f"  Clusters split:          {n_split:>6,}")
    print(f"  New child clusters:      {n_children:>6,}")
    print(f"  Total clusters out:      {len(out_clusters):>6,}")

    if split_log:
        print(f"\n  {'Name':<30} {'Parent':>8} {'Fellows':>8} {'Children':>9}")
        print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*9}")
        for row in split_log[:40]:
            print(f"  {row['name']:<30} {row['cluster_id']:>8} "
                  f"{row['n_fellowship_children']:>8} {row['n_total_children']:>9}")
        if len(split_log) > 40:
            print(f"  ... and {len(split_log) - 40} more")

    # ── Save ─────────────────────────────────────────────────────────────────
    if not args.dry_run:
        out_path   = PROCESSED_DATA / "clusters.jsonl"
        checkpoint = PROCESSED_DATA / "clusters_after_layer0b.jsonl"
        save_clusters(out_clusters, out_path)
        save_clusters(out_clusters, checkpoint)
        print(f"\nSaved: {out_path}")
        print(f"       {checkpoint}")
    else:
        print("\n(Dry run — no files written)")


if __name__ == "__main__":
    main()
