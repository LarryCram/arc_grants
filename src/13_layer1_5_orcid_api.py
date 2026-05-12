"""
13_layer1_5_orcid_api.py  —  Layer 1.5

For UNRESOLVED clusters with no ORCID in ARC data, query the ORCID public
API by name to find a candidate ORCID iD, then verify via the OAX local
parquet snapshot.

Query strategy (per cluster):
  1. family-name:{norm_family} AND given-names:{norm_first}
  2. If result count > 1 and cluster has institution evidence, re-query
     adding affiliation-org-name:{institution_name}

A cluster advances to MATCHED (tier=2) only when:
  - Exactly one ORCID iD returned from the name (or name+institution) query
  - That ORCID resolves in the OAX local snapshot
  - Score: ORCID (90) + name indicator >= ACCEPT_THRESHOLD with >= MIN_INDICATORS

Clusters with zero results, ambiguous results, or OAX lookup failures
are left UNRESOLVED for Layer 2.

Rate: 0.1 s between ORCID API calls (polite anonymous access, ~10 req/s).

INPUT:  PROCESSED_DATA/clusters.jsonl
        PROCESSED_DATA/institution_concordance.parquet
        OAX_AUTHORS/*.parquet
OUTPUT: PROCESSED_DATA/clusters.jsonl                (updated)
        PROCESSED_DATA/clusters_after_layer1_5.jsonl (checkpoint)
"""

import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.scoring import (
    ACCEPT_THRESHOLD, FULL_FIRST, INITIAL_ONLY, MIN_INDICATORS,
    ORCID as ORCID_PTS,
)
from config.settings import OAX_AUTHORS, PROCESSED_DATA
from src.pipeline.models import Cluster, ClusterStatus, load_clusters, save_clusters
from src.utils.io import setup_stdout_utf8
from src.utils.names import strip_diacriticals, strip_parens


ORCID_API = "https://pub.orcid.org/v3.0/search/"
API_DELAY  = 0.1   # seconds between ORCID API calls


# ── Name helpers ──────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return re.sub(r"[^a-z ]", "", strip_diacriticals(s).lower()).strip()


def _oax_firsts(display_name: str, alternatives: list[str]) -> list[str]:
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
    arc_firsts = [nf.norm_np.first for nf in cluster.name_forms if nf.norm_np.first]
    if not oax_firsts_list or not arc_firsts:
        return INITIAL_ONLY, 1
    for af in arc_firsts:
        for of in oax_firsts_list:
            if len(af) > 1 and len(of) > 1 and af == of:
                return FULL_FIRST, 1
    for af in arc_firsts:
        for of in oax_firsts_list:
            ta = re.findall(r"[a-z]+", af)
            tb = re.findall(r"[a-z]+", of)
            if not ta or not tb:
                return INITIAL_ONLY, 1
            for x in ta:
                for y in tb:
                    if x == y:                         return INITIAL_ONLY, 1
                    if len(x) == 1 and y.startswith(x): return INITIAL_ONLY, 1
                    if len(y) == 1 and x.startswith(y): return INITIAL_ONLY, 1
    return 0, 0


def _accept(score: int, n_indicators: int) -> bool:
    return score >= ACCEPT_THRESHOLD and n_indicators >= MIN_INDICATORS


# ── ORCID public API ──────────────────────────────────────────────────────────

def _orcid_search(
    family: str, first: str, affiliation: str | None = None
) -> tuple[int, list[str]]:
    """Return (num_found, [bare_orcid_id, ...]) from the ORCID public API."""
    q = f"family-name:{family}"
    if first:
        q += f" AND given-names:{first}"
    if affiliation:
        q += f" AND affiliation-org-name:{affiliation}"
    url = ORCID_API + "?" + urllib.parse.urlencode({"q": q, "rows": 10})
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception:
        return 0, []
    num_found = data.get("num-found", 0)
    results   = data.get("result") or []
    paths = [
        r["orcid-identifier"]["path"]
        for r in results
        if "orcid-identifier" in r
    ]
    return num_found, paths


# ── OAX batch lookup ──────────────────────────────────────────────────────────

def _fetch_oax_by_orcids(orcids: list[str]) -> dict[str, dict]:
    if not orcids:
        return {}
    glob         = str(OAX_AUTHORS / "*.parquet")
    placeholders = ", ".join(f"'{o}'" for o in orcids)
    rows = duckdb.connect().execute(f"""
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
        result[orcid_bare] = {
            "oax_id":                    oax_id,
            "display_name":              disp,
            "display_name_alternatives": list(alts) if alts is not None else [],
        }
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()

    path = PROCESSED_DATA / "clusters.jsonl"
    print(f"Loading clusters from {path}...")
    clusters = load_clusters(path)
    targets  = [
        c for c in clusters
        if c.status == ClusterStatus.UNRESOLVED.value and not c.orcids
    ]
    print(f"  Total clusters: {len(clusters):,}  |  No-ORCID UNRESOLVED: {len(targets):,}")

    # hep_code → first known arc_admin_org name (for ORCID affiliation query)
    df_conc = pd.read_parquet(
        PROCESSED_DATA / "institution_concordance.parquet",
        columns=["arc_admin_org", "hep_code"],
    )
    hep_to_name: dict[str, str] = {
        hep: grp["arc_admin_org"].iloc[0]
        for hep, grp in df_conc[df_conc["hep_code"].notna()].groupby("hep_code")
    }

    # ── Phase 1: ORCID API queries ────────────────────────────────────────────
    print(f"\nQuerying ORCID API...")
    candidate: dict[int, str] = {}   # cluster_id → bare ORCID iD
    n_zero = n_unique = n_narrowed = n_ambiguous = 0

    for i, cluster in enumerate(targets, 1):
        if i % 500 == 0:
            print(f"  {i:,} / {len(targets):,}  (candidates so far: {len(candidate):,})")

        nf     = cluster.name_forms[0].norm_np
        family = nf.last
        first  = nf.first
        if not family:
            n_zero += 1
            continue

        time.sleep(API_DELAY)
        num_found, paths = _orcid_search(family, first)

        if num_found == 0:
            n_zero += 1
        elif num_found == 1:
            candidate[cluster.cluster_id] = paths[0]
            n_unique += 1
        elif cluster.institutions:
            inst_name = hep_to_name.get(cluster.institutions[0], cluster.institutions[0])
            time.sleep(API_DELAY)
            num2, paths2 = _orcid_search(family, first, inst_name)
            if num2 == 1:
                candidate[cluster.cluster_id] = paths2[0]
                n_narrowed += 1
            else:
                n_ambiguous += 1
        else:
            n_ambiguous += 1

    print(f"\n  Zero results:              {n_zero:>7,}")
    print(f"  Unique (first query):      {n_unique:>7,}")
    print(f"  Unique (after narrowing):  {n_narrowed:>7,}")
    print(f"  Ambiguous / unresolvable:  {n_ambiguous:>7,}")
    print(f"  Candidate ORCIDs found:    {len(candidate):>7,}")

    # ── Phase 2: Batch OAX lookup ─────────────────────────────────────────────
    print(f"\nLooking up {len(candidate):,} ORCIDs in OAX snapshot...")
    oax_by_orcid = _fetch_oax_by_orcids(list(set(candidate.values())))
    print(f"  Found in OAX: {len(oax_by_orcid):,}")

    # ── Phase 3: Score and update ─────────────────────────────────────────────
    cluster_index = {c.cluster_id: c for c in clusters}
    n_matched = n_bad = 0

    for cid, orcid in candidate.items():
        oax = oax_by_orcid.get(orcid)
        if oax is None:
            continue
        cluster    = cluster_index[cid]
        firsts     = _oax_firsts(oax["display_name"], oax["display_name_alternatives"])
        name_pts, name_ind = _name_score(cluster, firsts)
        score = ORCID_PTS + name_pts
        n_ind = 1 + name_ind
        if _accept(score, n_ind):
            cluster.status = ClusterStatus.MATCHED.value
            cluster.oax_id = oax["oax_id"]
            cluster.tier   = 2
            n_matched += 1
        else:
            n_bad += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    remaining = sum(
        1 for c in clusters if c.status == ClusterStatus.UNRESOLVED.value
    )
    print(f"\n{'─'*60}")
    print(f"LAYER 1.5 SUMMARY")
    print(f"{'─'*60}")
    print(f"  Matched (tier 2):           {n_matched:>7,}")
    print(f"  ORCID found, name mismatch: {n_bad:>7,}")
    print(f"  Still UNRESOLVED:           {remaining:>7,}")

    # ── Save ──────────────────────────────────────────────────────────────────
    save_clusters(clusters, path)
    checkpoint = PROCESSED_DATA / "clusters_after_layer1_5.jsonl"
    save_clusters(clusters, checkpoint)
    print(f"\nSaved:")
    print(f"  {path}")
    print(f"  {checkpoint}")


if __name__ == "__main__":
    main()
