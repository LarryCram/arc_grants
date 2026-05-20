"""
16_absent_humanities.py  —  Humanities / creative arts ABSENT classification

Mark UNDECIDABLE clusters as ABSENT when every for_2d code on the cluster
is a humanities or creative arts code (Rule A: all-codes gate).

Rationale: OAX coverage is weak for humanities and creative arts practitioners.
A cluster whose entire research profile is within these fields is very unlikely
to have a meaningful OAX author record.

Humanities / creative arts FoR codes included:
  ANZSRC 2008: 19 (Creative Arts & Writing), 20 (Language & Culture),
               21 (History & Archaeology), 22 (Philosophy)
  ANZSRC 2020: 36 (Creative Arts & Writing), 43 (History/Heritage/Archaeology),
               47 (Language/Culture), 50 (Philosophy)

INPUT:  PROCESSED_DATA/clusters.jsonl
OUTPUT: PROCESSED_DATA/clusters.jsonl               (updated)
        PROCESSED_DATA/clusters_after_layer4.jsonl  (checkpoint)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.pipeline.models import Cluster, ClusterStatus, load_clusters, save_clusters
from src.utils.io import setup_stdout_utf8

HUMANITIES_CODES = {"19", "20", "21", "22", "36", "43", "47", "50"}


def _is_humanities_only(cluster: Cluster) -> bool:
    return bool(cluster.for_2d) and all(f in HUMANITIES_CODES for f in cluster.for_2d)


def main():
    setup_stdout_utf8()

    path = PROCESSED_DATA / "clusters.jsonl"
    print("Loading clusters...")
    clusters = load_clusters(path)
    undec = [c for c in clusters if c.status == ClusterStatus.UNDECIDABLE.value]
    print(f"  Total: {len(clusters):,}  |  UNDECIDABLE: {len(undec):,}")

    n_absent = 0
    for cluster in undec:
        if _is_humanities_only(cluster):
            cluster.status = ClusterStatus.ABSENT.value
            n_absent += 1

    from collections import defaultdict
    status_counts: dict[str, int] = defaultdict(int)
    for c in clusters:
        status_counts[c.status] += 1

    print(f"\n  Marked ABSENT (humanities/CA only): {n_absent:,}")
    print(f"\n  Final cluster store status:")
    for status in ("MATCHED", "ABSENT", "UNDECIDABLE", "SPLIT"):
        n = status_counts.get(status, 0)
        if n:
            print(f"    {status:<14} {n:>7,}")

    save_clusters(clusters, path)
    checkpoint = PROCESSED_DATA / "clusters_after_layer4.jsonl"
    save_clusters(clusters, checkpoint)
    print(f"\nSaved:")
    print(f"  {path}")
    print(f"  {checkpoint}")


if __name__ == "__main__":
    main()
