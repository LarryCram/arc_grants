"""
17c_analysis.py  —  Post-Layer-5 diagnostic analysis

Answers four questions about the Layer 5 (parquet) output:

  1. Outcome distribution — matched / ambiguous / no-signal breakdown
  2. Cluster pathology — which clusters have too many HEPs × FoR codes
     and are likely multi-person clusters that need splitting
  3. Same-name fragmentation — ambiguous clusters where all candidates
     share a display name (OAX deduplication failures)
  4. ORCID utility — UNDECIDABLE clusters that have an ARC ORCID:
     does that ORCID appear in any OAX candidate?  If so, Layer 5 could
     resolve them with a direct ORCID match instead of the gate.

Usage:
  python 17c_analysis.py
  python 17c_analysis.py --csv layer5_candidates.csv  # override default path
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.pipeline.models import ClusterStatus, load_clusters
from src.utils.io import setup_stdout_utf8


def main():
    setup_stdout_utf8()

    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=str(PROCESSED_DATA / "layer5_candidates.csv"))
    args = parser.parse_args()

    print("Loading clusters...")
    clusters = load_clusters(PROCESSED_DATA / "clusters.jsonl")
    by_id = {c.cluster_id: c for c in clusters}

    undecidable = [c for c in clusters if c.status == ClusterStatus.UNDECIDABLE.value]
    matched_l5  = [c for c in clusters if c.status == ClusterStatus.MATCHED.value and c.tier == 6]
    print(f"  Total clusters:       {len(clusters):>7,}")
    print(f"  Tier-6 matched:       {len(matched_l5):>7,}")
    print(f"  Still UNDECIDABLE:    {len(undecidable):>7,}")

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"\nNo candidates CSV found at {csv_path} — run 17b_parquet_oax.py first.")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    print(f"\nCandidates CSV: {len(df):,} rows, {df['cluster_id'].nunique():,} clusters\n")

    # ── 1. Outcome distribution ───────────────────────────────────────────────
    print("═" * 60)
    print("1. OUTCOME DISTRIBUTION")
    print("═" * 60)
    outcome_counts = df.drop_duplicates("cluster_id")["outcome"].value_counts()
    for outcome, n in outcome_counts.items():
        print(f"  {outcome:<12} {n:>6,} clusters")

    # Clusters with zero candidates (not in CSV)
    processed_cids = set(df["cluster_id"])
    zero_results = [c for c in undecidable if c.cluster_id not in processed_cids and not c.oax_candidates]
    print(f"  {'zero results':<12} {len(zero_results):>6,} clusters  (no AU candidates found)")

    # ── 2. Cluster pathology ──────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("2. CLUSTER PATHOLOGY  (HEP count × FoR count → candidate load)")
    print("═" * 60)

    amb = df[df["outcome"] == "AMBIGUOUS"].drop_duplicates("cluster_id")[["cluster_id", "n_passing"]]
    amb = amb.copy()
    amb["n_heps"] = amb["cluster_id"].map(lambda cid: len(by_id[cid].institutions) if cid in by_id else 0)
    amb["n_for"]  = amb["cluster_id"].map(lambda cid: len(by_id[cid].for_2d)       if cid in by_id else 0)
    amb["hep_x_for"] = amb["n_heps"] * amb["n_for"]
    amb["arc_name"]  = amb["cluster_id"].map(
        lambda cid: f"{by_id[cid].name_forms[0].arc.first} {by_id[cid].name_forms[0].arc.last}"
        if cid in by_id else ""
    )

    # Pathological threshold: many HEPs OR very high candidate count
    pathological = amb[(amb["n_heps"] >= 5) | (amb["n_passing"] >= 20)].sort_values(
        "n_passing", ascending=False
    )
    print(f"\n  Ambiguous clusters with ≥5 HEPs or ≥20 passing candidates: {len(pathological):,}")
    print(f"  {'Name':<30} {'HEPs':>5} {'FoRs':>5} {'Passing':>8}")
    print(f"  {'-'*30} {'-'*5} {'-'*5} {'-'*8}")
    for _, row in pathological.head(20).iterrows():
        print(f"  {row['arc_name']:<30} {row['n_heps']:>5} {row['n_for']:>5} {row['n_passing']:>8}")
    if len(pathological) > 20:
        print(f"  ... and {len(pathological) - 20} more")

    # Distribution of n_passing for ambiguous
    print(f"\n  n_passing distribution (ambiguous clusters):")
    bins = [0, 2, 5, 10, 20, 50, 9999]
    labels = ["2", "3–5", "6–10", "11–20", "21–50", ">50"]
    amb["band"] = pd.cut(amb["n_passing"], bins=bins, labels=labels, right=True)
    band_counts = amb["band"].value_counts().sort_index()
    for band, n in band_counts.items():
        print(f"    {band:>6} passing:  {n:>5,} clusters")

    # ── 3. Same-name fragmentation ────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("3. SAME-NAME FRAGMENTATION  (ambiguous, all candidates share display name)")
    print("═" * 60)

    frags = df[(df["outcome"] == "AMBIGUOUS") & (df["same_name_frag"] == True)]
    frag_clusters = frags.drop_duplicates("cluster_id")
    print(f"\n  Fragmentation clusters: {len(frag_clusters):,}")
    print(f"  These could be resolved by picking highest works_count candidate.")

    # Show top fragmentation cases by works_count spread
    frag_stats = (
        frags.groupby("cluster_id")
        .agg(
            arc_name=("arc_name", "first"),
            n_candidates=("oax_id", "count"),
            max_works=("works_count", "max"),
            min_works=("works_count", "min"),
        )
        .reset_index()
        .sort_values("max_works", ascending=False)
    )
    print(f"\n  Top fragmentation cases (by highest candidate works_count):")
    print(f"  {'Name':<30} {'Frags':>6} {'MaxWorks':>9} {'MinWorks':>9}")
    print(f"  {'-'*30} {'-'*6} {'-'*9} {'-'*9}")
    for _, row in frag_stats.head(15).iterrows():
        print(f"  {row['arc_name']:<30} {row['n_candidates']:>6} {row['max_works']:>9,} {row['min_works']:>9,}")

    # ── 4. ORCID utility ──────────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("4. ORCID UTILITY  (can ARC ORCIDs resolve remaining UNDECIDABLE clusters?)")
    print("═" * 60)

    # Clusters with an ARC ORCID that are still UNDECIDABLE
    orcid_undecidable = [c for c in undecidable if c.orcids]
    print(f"\n  UNDECIDABLE clusters with ARC ORCID:  {len(orcid_undecidable):,}")

    # Of those, how many have oax_candidates with a matching ORCID?
    direct_hit = []
    near_miss  = []   # have ARC ORCID + have candidates, but no ORCID match
    for c in orcid_undecidable:
        arc_orcids = set(c.orcids)
        cand_orcids = {
            cand["orcid"] for cand in c.oax_candidates if cand.get("orcid")
        }
        if arc_orcids & cand_orcids:
            direct_hit.append(c)
        elif c.oax_candidates:
            near_miss.append(c)

    print(f"  With matching ORCID in oax_candidates: {len(direct_hit):,}  ← resolvable by ORCID match")
    print(f"  Candidates present but no ORCID match: {len(near_miss):,}  ← ORCID not in snapshot")
    print(f"  No candidates found at all:            {len(orcid_undecidable) - len(direct_hit) - len(near_miss):,}")

    if direct_hit:
        print(f"\n  Direct ORCID hits (sample — these should be auto-matched):")
        print(f"  {'Name':<30} {'ARC ORCID':<22} {'Matching OAX ID'}")
        print(f"  {'-'*30} {'-'*22} {'-'*30}")
        for c in direct_hit[:15]:
            arc_orcids = set(c.orcids)
            for cand in c.oax_candidates:
                if cand.get("orcid") in arc_orcids:
                    name = f"{c.name_forms[0].arc.first} {c.name_forms[0].arc.last}"
                    print(f"  {name:<30} {cand['orcid']:<22} {cand['oax_id']}")
                    break

    print(f"\n  Verdict: {'ORCID matching would resolve ' + str(len(direct_hit)) + ' additional clusters.' if direct_hit else 'No ORCID-resolvable clusters found in candidates.'}")

    print("\n" + "═" * 60)


if __name__ == "__main__":
    main()
