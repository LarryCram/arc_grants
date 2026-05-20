"""
17d_resolve.py  —  Post-gate resolution of two tractable UNDECIDABLE patterns

Applies two rules to clusters that have oax_candidates but remain UNDECIDABLE:

  Rule A — ORCID direct match (tier 7, definitive)
    Cluster has an ARC ORCID that matches exactly one oax_candidate ORCID.

  Rule B — Same-name fragmentation (tier 8, high-confidence)
    All oax_candidates share the same display_name (OAX deduplication failure).
    Resolved by picking the candidate with the highest works_count.

Must be run AFTER 17b_parquet_oax.py (requires oax_candidates to be populated).

Usage:
  python 17d_resolve.py
  python 17d_resolve.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.pipeline.models import ClusterStatus, load_clusters, save_clusters
from src.utils.io import setup_stdout_utf8


def _apply_rule_a(cluster) -> dict | None:
    """ORCID direct match. Returns winning candidate dict or None."""
    if not cluster.orcids or not cluster.oax_candidates:
        return None
    arc_orcids = set(cluster.orcids)
    matches = [c for c in cluster.oax_candidates if c.get("orcid") in arc_orcids]
    if len(matches) == 1:
        return matches[0]
    return None


def _apply_rule_b(cluster) -> dict | None:
    """Same-name fragmentation: all candidates share display_name → highest works_count wins."""
    cands = cluster.oax_candidates
    if len(cands) < 2:
        return None
    names = {c["display_name"].lower() for c in cands}
    if len(names) != 1:
        return None
    return max(cands, key=lambda c: c.get("works_count") or 0)


def main():
    setup_stdout_utf8()

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    path = PROCESSED_DATA / "clusters.jsonl"
    print("Loading clusters...")
    clusters = load_clusters(path)
    undecidable = [c for c in clusters if c.status == ClusterStatus.UNDECIDABLE.value]
    with_cands  = [c for c in undecidable if c.oax_candidates]
    print(f"  Total:            {len(clusters):>7,}")
    print(f"  UNDECIDABLE:      {len(undecidable):>7,}")
    print(f"  With candidates:  {len(with_cands):>7,}")

    n_rule_a = n_rule_b = 0
    rows: list[dict] = []

    for cluster in with_cands:
        nf   = cluster.name_forms[0]
        name = f"{nf.arc.first} {nf.arc.last}".strip()

        # Rule A first — definitive
        winner = _apply_rule_a(cluster)
        if winner:
            rule = "A"
            tier = 7
            n_rule_a += 1
        else:
            winner = _apply_rule_b(cluster)
            if winner:
                rule = "B"
                tier = 8
                n_rule_b += 1

        if winner:
            second_works = None
            if rule == "B":
                others = [c for c in cluster.oax_candidates if c["oax_id"] != winner["oax_id"]]
                if others:
                    second_works = max(c.get("works_count") or 0 for c in others)

            rows.append({
                "cluster_id":    cluster.cluster_id,
                "arc_name":      name,
                "rule":          rule,
                "oax_id":        winner["oax_id"],
                "oax_name":      winner["display_name"],
                "orcid":         winner.get("orcid"),
                "works_count":   winner.get("works_count"),
                "second_works":  second_works,
                "cited_by_count": winner.get("cited_by_count"),
                "n_candidates":  len(cluster.oax_candidates),
            })

            if not args.dry_run:
                cluster.status = ClusterStatus.MATCHED.value
                cluster.oax_id = winner["oax_id"]
                cluster.tier   = tier

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"17d RESOLUTION SUMMARY  {'(DRY RUN)' if args.dry_run else ''}")
    print(f"{'─'*60}")
    print(f"  Rule A — ORCID direct match:   {n_rule_a:>5,}  (tier 7)")
    print(f"  Rule B — same-name by works:   {n_rule_b:>5,}  (tier 8)")
    print(f"  Total newly resolved:          {n_rule_a + n_rule_b:>5,}")
    print(f"  Still UNDECIDABLE after:       {len(undecidable) - n_rule_a - n_rule_b:>5,}")

    if rows:
        df = pd.DataFrame(rows)

        print(f"\nRule A resolutions:")
        rule_a_rows = df[df["rule"] == "A"]
        if len(rule_a_rows):
            for _, r in rule_a_rows.iterrows():
                print(f"  {r['arc_name']:<30} → {r['oax_id']}  orcid={r['orcid']}")
        else:
            print("  (none)")

        print(f"\nRule B resolutions (same-name fragment, top 20 by works_count):")
        rule_b_rows = df[df["rule"] == "B"].sort_values("works_count", ascending=False)
        print(f"  {'Name':<30} {'Winner works':>12} {'2nd works':>10} {'Frags':>6}  OAX ID")
        print(f"  {'-'*30} {'-'*12} {'-'*10} {'-'*6}  {'-'*30}")
        for _, r in rule_b_rows.head(20).iterrows():
            print(f"  {r['arc_name']:<30} {r['works_count'] or 0:>12,} "
                  f"{r['second_works'] or 0:>10,} {r['n_candidates']:>6}  {r['oax_id']}")
        if len(rule_b_rows) > 20:
            print(f"  ... and {len(rule_b_rows) - 20} more")

        out_path = PROCESSED_DATA / "layer5d_resolutions.csv"
        df.to_csv(out_path, index=False)
        print(f"\nResolutions written: {out_path}")

    if not args.dry_run:
        save_clusters(clusters, path)
        checkpoint = PROCESSED_DATA / "clusters_after_layer5d.jsonl"
        save_clusters(clusters, checkpoint)
        print(f"Saved: {path}")
        print(f"       {checkpoint}")


if __name__ == "__main__":
    main()
