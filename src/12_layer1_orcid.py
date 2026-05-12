"""
12_layer1_orcid.py  —  Layer 1

Match clusters to OAX authors using ORCID from the local Feb 2026 snapshot.

Single-ORCID clusters: look up the ORCID, score, decide.
Multi-ORCID clusters: look up all ORCIDs; if >= 2 resolve to *different*
OAX authors, split the cluster (SplitReason.ORCID_BOTH_IN_OAX) and match
each ORCID-anchored child. Records with no ORCID or an unresolved ORCID
form a residual child cluster (UNRESOLVED, falls through to Layer 2).

Scoring (config/scoring.py):
  ORCID resolves in OAX:  90 pts
  Full first name match: +55 pts  →  145 accepted
  Initial match only:    +20 pts  →  110 accepted
  Name incompatible:     + 0 pts  →   90 UNRESOLVED (bad ORCID)

Accept when score >= ACCEPT_THRESHOLD and >= MIN_INDICATORS.

ORCID not found in local snapshot: cluster left UNRESOLVED for Layer 2.

INPUT:  PROCESSED_DATA/clusters.jsonl
        OAX_AUTHORS/*.parquet
OUTPUT: PROCESSED_DATA/clusters.jsonl              (updated in place)
        PROCESSED_DATA/clusters_after_layer1.jsonl (checkpoint)
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.scoring import (
    ACCEPT_THRESHOLD, FULL_FIRST, INITIAL_ONLY, MIN_INDICATORS, ORCID,
)
from config.settings import OAX_AUTHORS, PROCESSED_DATA
from src.pipeline.models import (
    Cluster, ClusterStatus, SplitReason, load_clusters, save_clusters,
)
from src.utils.io import setup_stdout_utf8
from src.utils.names import strip_diacriticals, strip_parens


# ── Name helpers ──────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return re.sub(r"[^a-z ]", "", strip_diacriticals(s).lower()).strip()


def _oax_firsts(display_name: str, alternatives: list[str]) -> list[str]:
    """Normalised first-name tokens from all OAX name strings for this author."""
    firsts = []
    for name in [display_name] + (alternatives or []):
        if not name:
            continue
        n = HumanName(strip_diacriticals(strip_parens(name)))
        f = _norm(n.first)
        if f:
            firsts.append(f)
    return firsts


def _name_score(cluster: Cluster, oax_firsts_list: list[str]) -> tuple[int, int]:
    """
    Return (name_points, n_name_indicators) for the name component of the score.
    Full first = 55 (FULL_FIRST), initial compatible = 20 (INITIAL_ONLY), else 0.
    """
    arc_firsts = [nf.norm_np.first for nf in cluster.name_forms if nf.norm_np.first]

    if not oax_firsts_list or not arc_firsts:
        # One side has no first name — cannot rule out, treat as initial
        return INITIAL_ONLY, 1

    # Check full first match
    for af in arc_firsts:
        for of in oax_firsts_list:
            if len(af) > 1 and len(of) > 1 and af == of:
                return FULL_FIRST, 1

    # Check initial compatibility
    for af in arc_firsts:
        for of in oax_firsts_list:
            ta = re.findall(r"[a-z]+", af)
            tb = re.findall(r"[a-z]+", of)
            if not ta or not tb:
                return INITIAL_ONLY, 1
            for x in ta:
                for y in tb:
                    if x == y:
                        return INITIAL_ONLY, 1
                    if len(x) == 1 and y.startswith(x):
                        return INITIAL_ONLY, 1
                    if len(y) == 1 and x.startswith(y):
                        return INITIAL_ONLY, 1

    return 0, 0   # incompatible


# ── OAX lookup ────────────────────────────────────────────────────────────────

OaxRecord = dict  # keys: oax_id, display_name, display_name_alternatives


def _fetch_oax_by_orcids(orcids: list[str]) -> dict[str, OaxRecord]:
    """
    Query OAX parquet for the given bare ORCIDs.
    Returns dict: bare_orcid → {oax_id, display_name, display_name_alternatives}.
    OAX stores ORCIDs as 'https://orcid.org/XXXX-...'; we normalise to bare form.
    """
    if not orcids:
        return {}
    glob = str(OAX_AUTHORS / "*.parquet")
    placeholders = ", ".join(f"'{o}'" for o in orcids)
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT
            regexp_replace(orcid, 'https://orcid.org/', '') AS orcid_bare,
            ids.openalex                                     AS oax_id,
            display_name,
            display_name_alternatives
        FROM read_parquet('{glob}')
        WHERE regexp_replace(orcid, 'https://orcid.org/', '') IN ({placeholders})
          AND ids.openalex IS NOT NULL
    """).fetchall()
    result = {}
    for orcid_bare, oax_id, disp, alts in rows:
        raw_alts = list(alts) if alts is not None else []
        result[orcid_bare] = {
            "oax_id":                   oax_id,
            "display_name":             disp,
            "display_name_alternatives": raw_alts,
        }
    return result


# ── Matching ──────────────────────────────────────────────────────────────────

def _try_match(cluster: Cluster, oax: OaxRecord) -> tuple[int, int]:
    """Return (total_score, n_indicators) for cluster vs OAX author."""
    firsts = _oax_firsts(oax["display_name"], oax["display_name_alternatives"])
    name_pts, name_ind = _name_score(cluster, firsts)
    total = ORCID + name_pts
    n_ind = 1 + name_ind          # 1 for ORCID itself
    return total, n_ind


def _accept(score: int, n_indicators: int) -> bool:
    return score >= ACCEPT_THRESHOLD and n_indicators >= MIN_INDICATORS


# ── Split helpers ─────────────────────────────────────────────────────────────

def _split_cluster(
    parent: Cluster,
    orcid_to_oax: dict[str, OaxRecord],
    next_id: int,
) -> tuple[list[Cluster], int]:
    """
    Split a multi-ORCID cluster whose ORCIDs resolve to different OAX authors.
    Returns (new_clusters, updated_next_id).
    new_clusters includes the mutated parent (status=SPLIT) + all children.
    """
    # Group records by their ORCID
    by_orcid: dict[str | None, list] = defaultdict(list)
    for rec in parent.records:
        key = rec.orcid if (rec.orcid and rec.orcid in orcid_to_oax) else None
        by_orcid[key].append(rec)

    resolved_orcids = [o for o in parent.orcids if o in orcid_to_oax]
    child_ids = list(range(next_id, next_id + len(resolved_orcids) + 1))
    next_id += len(resolved_orcids) + 1

    children: list[Cluster] = []
    residual_records = by_orcid.get(None, [])

    for i, orcid in enumerate(resolved_orcids):
        oax = orcid_to_oax[orcid]
        recs = by_orcid.get(orcid, [])
        if not recs:
            continue
        child = Cluster(
            cluster_id=child_ids[i],
            name_forms=parent.name_forms,
            records=recs,
            orcids=[orcid],
            institutions=sorted({h for r in recs for h in r.grant_heps}),
            for_2d=sorted({f for r in recs for f in r.for_2d}),
            co_awardee_cluster_ids=parent.co_awardee_cluster_ids,
            parent_id=parent.cluster_id,
            sibling_cluster_ids=[cid for cid in child_ids if cid != child_ids[i]],
            split_reason=SplitReason.ORCID_BOTH_IN_OAX.value,
        )
        score, n_ind = _try_match(child, oax)
        if _accept(score, n_ind):
            child.status = ClusterStatus.MATCHED.value
            child.oax_id = oax["oax_id"]
            child.tier   = 1
        children.append(child)

    # Residual child (no ORCID or ORCID not in OAX) — left UNRESOLVED
    if residual_records:
        residual = Cluster(
            cluster_id=child_ids[-1],
            name_forms=parent.name_forms,
            records=residual_records,
            orcids=[],
            institutions=sorted({h for r in residual_records for h in r.grant_heps}),
            for_2d=sorted({f for r in residual_records for f in r.for_2d}),
            co_awardee_cluster_ids=parent.co_awardee_cluster_ids,
            parent_id=parent.cluster_id,
            sibling_cluster_ids=[c.cluster_id for c in children],
            split_reason=SplitReason.ORCID_BOTH_IN_OAX.value,
        )
        children.append(residual)

    parent.status = ClusterStatus.SPLIT.value
    return [parent] + children, next_id


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()

    path = PROCESSED_DATA / "clusters.jsonl"
    print(f"Loading clusters from {path}...")
    clusters = load_clusters(path)
    print(f"  Loaded: {len(clusters):,}")

    # ── Collect all ORCIDs to look up ────────────────────────────────────────
    needed: set[str] = set()
    for c in clusters:
        if c.status == ClusterStatus.UNRESOLVED.value and c.orcids:
            needed.update(c.orcids)

    print(f"Looking up {len(needed):,} ORCIDs in OAX snapshot...")
    oax_by_orcid = _fetch_oax_by_orcids(list(needed))
    print(f"  Found in OAX: {len(oax_by_orcid):,}")

    # ── Process clusters ──────────────────────────────────────────────────────
    next_id    = max(c.cluster_id for c in clusters) + 1
    new_chunks: list[list[Cluster]] = []   # replacement lists (parent ± children)

    n_matched = n_unresolved = n_bad_orcid = n_split = 0

    for cluster in clusters:
        if cluster.status != ClusterStatus.UNRESOLVED.value or not cluster.orcids:
            new_chunks.append([cluster])
            continue

        if len(cluster.orcids) == 1:
            orcid = cluster.orcids[0]
            oax   = oax_by_orcid.get(orcid)
            if oax is None:
                # ORCID not in local snapshot — leave UNRESOLVED for Layer 2
                n_unresolved += 1
                new_chunks.append([cluster])
                continue
            score, n_ind = _try_match(cluster, oax)
            if _accept(score, n_ind):
                cluster.status = ClusterStatus.MATCHED.value
                cluster.oax_id = oax["oax_id"]
                cluster.tier   = 1
                n_matched += 1
            else:
                # ORCID found but name incompatible — leave UNRESOLVED
                n_bad_orcid += 1
            new_chunks.append([cluster])

        else:
            # Multi-ORCID: split only if >= 2 resolve to different OAX authors
            resolved = {o: oax_by_orcid[o] for o in cluster.orcids if o in oax_by_orcid}
            distinct_oax = {r["oax_id"] for r in resolved.values()}
            if len(distinct_oax) >= 2:
                replacement, next_id = _split_cluster(cluster, resolved, next_id)
                n_split += 1
                n_matched += sum(
                    1 for c in replacement
                    if c.status == ClusterStatus.MATCHED.value
                )
                new_chunks.append(replacement)
            else:
                # All ORCIDs → same OAX author (or only one resolved): treat as single
                if resolved:
                    oax = next(iter(resolved.values()))
                    score, n_ind = _try_match(cluster, oax)
                    if _accept(score, n_ind):
                        cluster.status = ClusterStatus.MATCHED.value
                        cluster.oax_id = oax["oax_id"]
                        cluster.tier   = 1
                        n_matched += 1
                    else:
                        n_bad_orcid += 1
                else:
                    n_unresolved += 1
                new_chunks.append([cluster])

    # Flatten
    out_clusters = [c for chunk in new_chunks for c in chunk]

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"LAYER 1 SUMMARY")
    print(f"{'─'*60}")
    print(f"  Matched (tier 1):          {n_matched:>7,}")
    print(f"  Split (multi-ORCID):       {n_split:>7,}")
    print(f"  ORCID not in snapshot:     {n_unresolved:>7,}")
    print(f"  Name mismatch (bad ORCID): {n_bad_orcid:>7,}")
    print(f"  Total clusters out:        {len(out_clusters):>7,}")

    # ── Save ─────────────────────────────────────────────────────────────────
    save_clusters(out_clusters, path)
    checkpoint = PROCESSED_DATA / "clusters_after_layer1.jsonl"
    save_clusters(out_clusters, checkpoint)
    print(f"\nSaved:")
    print(f"  {path}")
    print(f"  {checkpoint}")


if __name__ == "__main__":
    main()
