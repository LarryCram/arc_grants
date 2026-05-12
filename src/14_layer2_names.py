"""
14_layer2_names.py  —  Layer 2

Match remaining UNRESOLVED clusters to OAX authors via the name index.

Algorithm:
  1. Collect all normalised family tokens from UNRESOLVED clusters.
  2. Batch-query oax_name_index.parquet for all tokens → candidate pool per cluster.
  3. Filter each candidate by first_names_compatible; note if pool is unique (1 candidate).
  4. Batch-query OAX partition parquets for institution and topic data on all candidates.
  5. Score each candidate; accept if exactly one reaches the threshold.
  6. API supplement: for zero-hit clusters query the OAX API by name (polite rate limit).

Scoring (config/scoring.py) — no ORCID available in this layer:
  Full first name match:   55
  Initial match only:      20
  Institution match:       45   (HEP code in OAX ever-affiliations)
  FoR field overlap >= 1:  15   (cluster FoR → OAX topic field name)
  FoR field overlap >= 3:  25   (replaces >= 1 score)
  Unique in pool:          30   (only one candidate passes name gate)

Accept: score >= 100, >= 2 indicators, exactly one candidate accepted.

INPUT:  PROCESSED_DATA/clusters.jsonl
        PROCESSED_DATA/oax_name_index.parquet
        PROCESSED_DATA/institution_concordance.parquet
        DATA_ROOT/for_oax_concordance.csv
        OAX_AUTHORS/*.parquet
OUTPUT: PROCESSED_DATA/clusters.jsonl              (updated)
        PROCESSED_DATA/clusters_after_layer2.jsonl (checkpoint)
"""

import json
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.scoring import (
    ACCEPT_THRESHOLD, FOR_ONE_FIELD, FOR_THREE_FIELDS,
    FULL_FIRST, INITIAL_ONLY, INSTITUTION, MIN_INDICATORS, UNIQUE_IN_POOL,
)
from config.settings import FOR_OAX_CSV, OAX_AUTHORS, PROCESSED_DATA
from src.pipeline.models import Cluster, ClusterStatus, load_clusters, save_clusters
from src.utils.io import setup_stdout_utf8
from src.utils.names import strip_diacriticals, strip_parens

API_DELAY = 0.05   # seconds between OAX API calls


# ── Name helpers ──────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return re.sub(r"[^a-z ]", "", strip_diacriticals(s).lower()).strip()


def _first_names_compatible(a: str, b: str) -> bool:
    ta = re.findall(r"[a-z]+", a)
    tb = re.findall(r"[a-z]+", b)
    if not ta or not tb:
        return True
    for x in ta:
        for y in tb:
            if x == y:                         return True
            if len(x) == 1 and y.startswith(x): return True
            if len(y) == 1 and x.startswith(y): return True
    return False


def _cluster_family_tokens(cluster: Cluster) -> set[str]:
    tokens = set()
    for nf in cluster.name_forms:
        if nf.norm_np.last:
            tokens.add(nf.norm_np.last)
    return tokens


def _cluster_arc_firsts(cluster: Cluster) -> list[str]:
    return [nf.norm_np.first for nf in cluster.name_forms if nf.norm_np.first]


# ── Scoring helpers ───────────────────────────────────────────────────────────

def _name_indicator(arc_firsts: list[str], oax_firsts: set[str]) -> tuple[int, int]:
    """Return (name_points, n_name_indicators)."""
    if not oax_firsts or not arc_firsts:
        return INITIAL_ONLY, 1
    for af in arc_firsts:
        for of in oax_firsts:
            if len(af) > 1 and len(of) > 1 and af == of:
                return FULL_FIRST, 1
    for af in arc_firsts:
        for of in oax_firsts:
            if _first_names_compatible(af, of):
                return INITIAL_ONLY, 1
    return 0, 0


def _institution_indicator(
    cluster_heps: list[str],
    oax_aff_ids: list[str],
    inst_id_to_hep: dict[str, str],
) -> tuple[int, int]:
    oax_heps = {inst_id_to_hep[i] for i in oax_aff_ids if i in inst_id_to_hep}
    if oax_heps & set(cluster_heps):
        return INSTITUTION, 1
    return 0, 0


def _for_indicator(
    cluster_for2d: list[str],
    oax_field_names: list[str],
    for2d_to_fields: dict[str, set[str]],
) -> tuple[int, int]:
    expected = set()
    for code in cluster_for2d:
        expected.update(for2d_to_fields.get(code, set()))
    if not expected:
        return 0, 0
    overlap = sum(1 for f in oax_field_names if f in expected)
    if overlap >= 3:
        return FOR_THREE_FIELDS, 1
    if overlap >= 1:
        return FOR_ONE_FIELD, 1
    return 0, 0


def _score_candidate(
    cluster: Cluster,
    arc_firsts: list[str],
    oax_firsts: set[str],
    oax_aff_ids: list[str],
    oax_field_names: list[str],
    unique_in_pool: bool,
    inst_id_to_hep: dict[str, str],
    for2d_to_fields: dict[str, set[str]],
) -> tuple[int, int]:
    """Return (total_score, n_indicators)."""
    name_pts,  name_ind  = _name_indicator(arc_firsts, oax_firsts)
    inst_pts,  inst_ind  = _institution_indicator(cluster.institutions, oax_aff_ids, inst_id_to_hep)
    for_pts,   for_ind   = _for_indicator(cluster.for_2d, oax_field_names, for2d_to_fields)
    uniq_pts  = UNIQUE_IN_POOL if unique_in_pool else 0
    uniq_ind  = 1 if unique_in_pool else 0
    total = name_pts + inst_pts + for_pts + uniq_pts
    n_ind = name_ind + inst_ind + for_ind + uniq_ind
    return total, n_ind


def _accept(score: int, n_ind: int) -> bool:
    return score >= ACCEPT_THRESHOLD and n_ind >= MIN_INDICATORS


# ── OAX API supplement ────────────────────────────────────────────────────────

_oax_email = ""   # set in main()


def _oax_api_candidates(display_name: str) -> list[str]:
    """Return up to 5 OAX author IDs matching display_name from the OAX API."""
    params = urllib.parse.urlencode({
        "search":   display_name,
        "mailto":   _oax_email,
        "per_page": 5,
        "select":   "id",
    })
    url = f"https://api.openalex.org/authors?{params}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return [r["id"] for r in data.get("results", []) if "id" in r]
    except Exception:
        return []


# ── Batch OAX data fetch ──────────────────────────────────────────────────────

def _fetch_oax_data(oax_ids: list[str]) -> dict[str, dict]:
    """Batch-fetch affiliations and topic field names for a list of OAX author IDs."""
    if not oax_ids:
        return {}
    glob = str(OAX_AUTHORS / "*.parquet")
    con  = duckdb.connect()
    con.execute(
        "CREATE TEMP TABLE _tids AS SELECT unnest(?::VARCHAR[]) AS oax_id",
        [oax_ids],
    )
    rows = con.execute(f"""
        SELECT
            ids.openalex                                              AS oax_id,
            list_transform(affiliations, x -> x.institution.id)      AS aff_ids,
            list_transform(topics,       x -> x.field.display_name)  AS field_names
        FROM read_parquet('{glob}')
        WHERE ids.openalex IN (SELECT oax_id FROM _tids)
          AND ids.openalex IS NOT NULL
    """).fetchall()
    result = {}
    for oax_id, aff_ids, field_names in rows:
        result[oax_id] = {
            "aff_ids":    list(aff_ids)    if aff_ids    is not None else [],
            "field_names": list(field_names) if field_names is not None else [],
        }
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global _oax_email
    setup_stdout_utf8()

    # ── Load clusters ─────────────────────────────────────────────────────────
    path = PROCESSED_DATA / "clusters.jsonl"
    print(f"Loading clusters...")
    clusters  = load_clusters(path)
    targets   = [c for c in clusters if c.status == ClusterStatus.UNRESOLVED.value]
    print(f"  Total: {len(clusters):,}  |  UNRESOLVED: {len(targets):,}")

    # ── Load concordances ─────────────────────────────────────────────────────
    df_conc = pd.read_parquet(
        PROCESSED_DATA / "institution_concordance.parquet",
        columns=["institution_id", "hep_code"],
    )
    inst_id_to_hep: dict[str, str] = {
        row.institution_id: row.hep_code
        for row in df_conc[df_conc["institution_id"].notna() & df_conc["hep_code"].notna()].itertuples()
    }

    df_for = pd.read_csv(FOR_OAX_CSV, sep=";")
    df_for["for_2d"] = df_for["for_2digit"].apply(lambda x: f"{int(x):02d}")
    for2d_to_fields: dict[str, set[str]] = (
        df_for.groupby("for_2d")["oax_field"].apply(set).to_dict()
    )

    from config.settings import OPENALEX_EMAIL
    _oax_email = OPENALEX_EMAIL

    # ── Phase 1: Collect family tokens ───────────────────────────────────────
    all_tokens: set[str] = set()
    for c in targets:
        all_tokens.update(_cluster_family_tokens(c))
    print(f"  Unique family tokens to look up: {len(all_tokens):,}")

    # ── Phase 2: Batch name index query ──────────────────────────────────────
    index_path = str(PROCESSED_DATA / "oax_name_index.parquet")
    con = duckdb.connect()
    con.execute(
        "CREATE TEMP TABLE _tokens AS SELECT unnest(?::VARCHAR[]) AS tok",
        [list(all_tokens)],
    )
    print("Querying name index...")
    idx_rows = con.execute(f"""
        SELECT norm_family_token, oax_id, name_form, is_initial_form
        FROM read_parquet('{index_path}')
        WHERE norm_family_token IN (SELECT tok FROM _tokens)
    """).fetchall()
    print(f"  Name index rows returned: {len(idx_rows):,}")

    # Build token → {oax_id → set[first_name]}
    token_to_candidates: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for tok, oax_id, name_form, is_init in idx_rows:
        parts = name_form.split(" ", 1)
        first = parts[0] if len(parts) == 2 else ""
        token_to_candidates[tok][oax_id].add(first)

    # ── Phase 3: Filter by first_names_compatible, identify unique pool ───────
    print("Filtering candidates by name compatibility...")
    # cluster_id → set[oax_id] passing name gate
    cluster_candidates: dict[int, set[str]] = {}
    zero_hit_clusters: list[Cluster] = []

    for cluster in targets:
        tokens   = _cluster_family_tokens(cluster)
        arc_firsts = _cluster_arc_firsts(cluster)
        pool: set[str] = set()
        for tok in tokens:
            for oax_id, oax_firsts in token_to_candidates.get(tok, {}).items():
                if any(_first_names_compatible(af, of) for af in arc_firsts for of in oax_firsts) \
                        or not arc_firsts or not oax_firsts:
                    pool.add(oax_id)
        if pool:
            cluster_candidates[cluster.cluster_id] = pool
        else:
            zero_hit_clusters.append(cluster)

    unique_oax_ids = {oid for pool in cluster_candidates.values() for oid in pool}
    print(f"  Clusters with candidates:  {len(cluster_candidates):,}")
    print(f"  Zero-hit clusters:         {len(zero_hit_clusters):,}")
    print(f"  Unique candidate OAX IDs:  {len(unique_oax_ids):,}")

    # ── Phase 4: Batch OAX data fetch ────────────────────────────────────────
    print("Fetching OAX author data for candidates...")
    oax_data = _fetch_oax_data(list(unique_oax_ids))
    print(f"  Authors fetched: {len(oax_data):,}")

    # ── Phase 5: Score and accept ─────────────────────────────────────────────
    print("Scoring candidates...")
    cluster_index = {c.cluster_id: c for c in clusters}
    n_matched = n_ambiguous = n_unresolved = 0

    for cluster in targets:
        pool = cluster_candidates.get(cluster.cluster_id)
        if not pool:
            continue
        arc_firsts  = _cluster_arc_firsts(cluster)
        unique      = len(pool) == 1
        accepted: list[tuple[str, int]] = []   # (oax_id, score)

        for oax_id in pool:
            data = oax_data.get(oax_id)
            if data is None:
                continue
            # Collect OAX first names for this candidate from the index
            oax_firsts: set[str] = set()
            for tok in _cluster_family_tokens(cluster):
                oax_firsts.update(token_to_candidates.get(tok, {}).get(oax_id, set()))

            score, n_ind = _score_candidate(
                cluster, arc_firsts, oax_firsts,
                data["aff_ids"], data["field_names"],
                unique, inst_id_to_hep, for2d_to_fields,
            )
            if _accept(score, n_ind):
                accepted.append((oax_id, score))

        if len(accepted) == 1:
            oax_id, _ = accepted[0]
            cluster.status = ClusterStatus.MATCHED.value
            cluster.oax_id = oax_id
            cluster.tier   = 3
            n_matched += 1
        elif len(accepted) > 1:
            n_ambiguous += 1
        else:
            n_unresolved += 1

    print(f"\n{'─'*60}")
    print(f"LAYER 2 — NAME INDEX PASS")
    print(f"{'─'*60}")
    print(f"  Matched (tier 3):          {n_matched:>7,}")
    print(f"  Ambiguous (multiple):      {n_ambiguous:>7,}")
    print(f"  Unresolved (no accept):    {n_unresolved:>7,}")
    print(f"  Zero-hit (no candidates):  {len(zero_hit_clusters):>7,}")

    # Save after main pass so API supplement is optional
    save_clusters(clusters, path)
    print(f"\n  (Checkpoint saved — safe to interrupt before API supplement)")

    # ── Phase 6: API supplement for zero-hit clusters ─────────────────────────
    if not zero_hit_clusters:
        print("\nNo zero-hit clusters — skipping API supplement.")
    else:
        print(f"\nAPI supplement: querying OAX for {len(zero_hit_clusters):,} zero-hit clusters...")
        api_candidates: dict[int, list[str]] = {}

        for i, cluster in enumerate(zero_hit_clusters, 1):
            if i % 200 == 0:
                print(f"  {i:,} / {len(zero_hit_clusters):,}")
            nf   = cluster.name_forms[0]
            name = f"{nf.arc.first} {nf.arc.last}".strip()
            time.sleep(API_DELAY)
            ids  = _oax_api_candidates(name)
            if ids:
                api_candidates[cluster.cluster_id] = ids

        # Fetch OAX data for new candidates
        new_ids = {oid for ids in api_candidates.values() for oid in ids}
        new_ids -= unique_oax_ids   # skip already fetched
        if new_ids:
            new_data = _fetch_oax_data(list(new_ids))
            oax_data.update(new_data)

        n_api_matched = n_api_ambig = 0
        for cluster in zero_hit_clusters:
            ids = api_candidates.get(cluster.cluster_id, [])
            if not ids:
                continue
            arc_firsts = _cluster_arc_firsts(cluster)
            accepted: list[tuple[str, int]] = []
            for oax_id in ids:
                data = oax_data.get(oax_id)
                if data is None:
                    continue
                # No name index firsts available — derive from OAX API name
                oax_firsts: set[str] = set()
                score, n_ind = _score_candidate(
                    cluster, arc_firsts, oax_firsts,
                    data["aff_ids"], data["field_names"],
                    len(ids) == 1, inst_id_to_hep, for2d_to_fields,
                )
                if _accept(score, n_ind):
                    accepted.append((oax_id, score))
            if len(accepted) == 1:
                oax_id, _ = accepted[0]
                cluster.status = ClusterStatus.MATCHED.value
                cluster.oax_id = oax_id
                cluster.tier   = 4
                n_api_matched += 1
            elif len(accepted) > 1:
                n_api_ambig += 1

        n_matched += n_api_matched
        print(f"\n{'─'*60}")
        print(f"LAYER 2 — API SUPPLEMENT")
        print(f"{'─'*60}")
        print(f"  Matched (tier 4):          {n_api_matched:>7,}")
        print(f"  Ambiguous (multiple):      {n_api_ambig:>7,}")

    # ── Final summary ─────────────────────────────────────────────────────────
    remaining = sum(1 for c in clusters if c.status == ClusterStatus.UNRESOLVED.value)
    print(f"\n{'─'*60}")
    print(f"LAYER 2 TOTAL")
    print(f"{'─'*60}")
    print(f"  Matched this layer:        {n_matched:>7,}")
    print(f"  Still UNRESOLVED:          {remaining:>7,}")

    save_clusters(clusters, path)
    checkpoint = PROCESSED_DATA / "clusters_after_layer2.jsonl"
    save_clusters(clusters, checkpoint)
    print(f"\nSaved:")
    print(f"  {path}")
    print(f"  {checkpoint}")


if __name__ == "__main__":
    main()
