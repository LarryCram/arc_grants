"""
15_layer3_for.py  —  Layer 3

For clusters still UNRESOLVED after Layer 2, apply co-awardee evidence to
narrow the candidate pool and add scoring signal. At the end, all remaining
UNRESOLVED clusters are marked UNDECIDABLE.

Algorithm:
  1. Re-build candidate pools from oax_name_index.parquet (same as Layer 2).
  2. For clusters with ≥1 matched co-awardee:
     a. Compute the union of co-awardees' HEP codes and OAX topic fields.
     b. Filter the candidate pool to those sharing ≥1 HEP OR ≥1 field with any
        matched co-awardee; fall back to the full pool if no candidate survives.
     c. Recompute unique_in_pool against the narrowed pool.
  3. Score every candidate in the (narrowed) pool with the full indicator set
     plus CO_AWARDEE_INST / CO_AWARDEE_FIELD signals.
  4. Accept if exactly one candidate passes (score >= 100, >= 2 indicators).
  5. Mark all remaining UNRESOLVED as UNDECIDABLE.

Co-awardee scoring additions (config/scoring.py):
  CO_AWARDEE_INST  = 25   candidate shares ≥1 HEP with a matched co-awardee
  CO_AWARDEE_FIELD = 15   candidate shares ≥1 OAX topic field with matched co-awardee

INPUT:  PROCESSED_DATA/clusters.jsonl
        PROCESSED_DATA/oax_name_index.parquet
        PROCESSED_DATA/institution_concordance.parquet
        DATA_ROOT/for_oax_concordance.csv
        OAX_AUTHORS/*.parquet
OUTPUT: PROCESSED_DATA/clusters.jsonl              (updated)
        PROCESSED_DATA/clusters_after_layer3.jsonl (checkpoint)
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.scoring import (
    ACCEPT_THRESHOLD, CO_AWARDEE_FIELD, CO_AWARDEE_INST,
    FOR_ONE_FIELD, FOR_THREE_FIELDS,
    FULL_FIRST, INITIAL_ONLY, INSTITUTION, MIN_INDICATORS, UNIQUE_IN_POOL,
)
from config.settings import FOR_OAX_CSV, OAX_AUTHORS, PROCESSED_DATA
from src.pipeline.models import Cluster, ClusterStatus, load_clusters, save_clusters
from src.utils.io import setup_stdout_utf8
from src.utils.names import strip_diacriticals, strip_parens


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
            if x == y:                           return True
            if len(x) == 1 and y.startswith(x): return True
            if len(y) == 1 and x.startswith(y): return True
    return False


def _cluster_family_tokens(cluster: Cluster) -> set[str]:
    return {nf.norm_np.last for nf in cluster.name_forms if nf.norm_np.last}


def _cluster_arc_firsts(cluster: Cluster) -> list[str]:
    return [nf.norm_np.first for nf in cluster.name_forms if nf.norm_np.first]


# ── Scoring helpers ───────────────────────────────────────────────────────────

def _name_indicator(arc_firsts: list[str], oax_firsts: set[str]) -> tuple[int, int]:
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
    expected: set[str] = set()
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
    ca_heps: set[str],
    ca_fields: set[str],
    inst_id_to_hep: dict[str, str],
    for2d_to_fields: dict[str, set[str]],
) -> tuple[int, int]:
    name_pts, name_ind = _name_indicator(arc_firsts, oax_firsts)
    inst_pts, inst_ind = _institution_indicator(cluster.institutions, oax_aff_ids, inst_id_to_hep)
    for_pts,  for_ind  = _for_indicator(cluster.for_2d, oax_field_names, for2d_to_fields)
    uniq_pts = UNIQUE_IN_POOL if unique_in_pool else 0
    uniq_ind = 1 if unique_in_pool else 0

    # Co-awardee signal
    cand_heps   = {inst_id_to_hep[a] for a in oax_aff_ids   if a in inst_id_to_hep}
    cand_fields = set(oax_field_names)
    ca_inst_pts  = CO_AWARDEE_INST  if (ca_heps   and cand_heps   & ca_heps)   else 0
    ca_fld_pts   = CO_AWARDEE_FIELD if (ca_fields and cand_fields & ca_fields) else 0
    ca_inst_ind  = 1 if ca_inst_pts  else 0
    ca_fld_ind   = 1 if ca_fld_pts   else 0

    total = name_pts + inst_pts + for_pts + uniq_pts + ca_inst_pts + ca_fld_pts
    n_ind = name_ind + inst_ind + for_ind + uniq_ind + ca_inst_ind + ca_fld_ind
    return total, n_ind


def _accept(score: int, n_ind: int) -> bool:
    return score >= ACCEPT_THRESHOLD and n_ind >= MIN_INDICATORS


# ── Batch OAX data fetch ──────────────────────────────────────────────────────

_OAX_QUERY = """
    SELECT
        ids.openalex                                              AS oax_id,
        list_transform(affiliations, x -> x.institution.id)      AS aff_ids,
        list_transform(topics,       x -> x.field.display_name)  AS field_names
    FROM read_parquet('{source}')
    WHERE ids.openalex IN (SELECT oax_id FROM _tids)
      AND ids.openalex IS NOT NULL
"""


def _query_partition(source: str, oax_ids: list[str]) -> list[tuple] | None:
    """Run the fetch query against source (glob or single file). Returns None on error."""
    con = duckdb.connect()
    con.execute(
        "CREATE TEMP TABLE _tids AS SELECT unnest(?::VARCHAR[]) AS oax_id",
        [oax_ids],
    )
    try:
        return con.execute(_OAX_QUERY.format(source=source)).fetchall()
    except duckdb.InvalidInputException:
        return None


def _fetch_oax_data(oax_ids: list[str]) -> dict[str, dict]:
    import glob as glob_module

    if not oax_ids:
        return {}
    glob_pat = str(OAX_AUTHORS / "*.parquet")
    rows = _query_partition(glob_pat, oax_ids)
    if rows is None:
        # Bad UTF-8 in one partition — retry per file, skipping offenders
        print("  Warning: batch fetch failed (bad encoding); retrying per partition...")
        rows = []
        for fpath in sorted(glob_module.glob(glob_pat)):
            result = _query_partition(fpath, oax_ids)
            if result is None:
                print(f"  Warning: skipping {Path(fpath).name} (invalid encoding)")
            else:
                rows.extend(result)

    result = {}
    for oax_id, aff_ids, field_names in rows:
        result[oax_id] = {
            "aff_ids":     list(aff_ids)     if aff_ids     is not None else [],
            "field_names": list(field_names) if field_names is not None else [],
        }
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()

    # ── Load clusters ─────────────────────────────────────────────────────────
    path = PROCESSED_DATA / "clusters.jsonl"
    print("Loading clusters...")
    clusters = load_clusters(path)
    targets  = [c for c in clusters if c.status == ClusterStatus.UNRESOLVED.value]
    print(f"  Total: {len(clusters):,}  |  UNRESOLVED: {len(targets):,}")

    # Index of matched clusters: cluster_id → oax_id
    matched_index: dict[int, str] = {
        c.cluster_id: c.oax_id
        for c in clusters
        if c.status == ClusterStatus.MATCHED.value and c.oax_id
    }
    n_with_co = sum(
        1 for c in targets
        if any(cid in matched_index for cid in c.co_awardee_cluster_ids)
    )
    print(f"  UNRESOLVED with ≥1 matched co-awardee: {n_with_co:,}")

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

    # ── Phase 1: Collect family tokens ───────────────────────────────────────
    all_tokens: set[str] = set()
    for c in targets:
        all_tokens.update(_cluster_family_tokens(c))
    print(f"  Unique family tokens: {len(all_tokens):,}")

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
    print(f"  Name index rows: {len(idx_rows):,}")

    # token → {oax_id → set[first_name]}
    token_to_candidates: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for tok, oax_id, name_form, _ in idx_rows:
        parts = name_form.split(" ", 1)
        first = parts[0] if len(parts) == 2 else ""
        token_to_candidates[tok][oax_id].add(first)

    # ── Phase 3: Filter by first_names_compatible ────────────────────────────
    print("Filtering candidates by name compatibility...")
    cluster_candidates: dict[int, set[str]] = {}
    for cluster in targets:
        tokens     = _cluster_family_tokens(cluster)
        arc_firsts = _cluster_arc_firsts(cluster)
        pool: set[str] = set()
        for tok in tokens:
            for oax_id, oax_firsts in token_to_candidates.get(tok, {}).items():
                if (not arc_firsts or not oax_firsts
                        or any(
                            _first_names_compatible(af, of)
                            for af in arc_firsts for of in oax_firsts
                        )):
                    pool.add(oax_id)
        if pool:
            cluster_candidates[cluster.cluster_id] = pool

    unique_oax_ids = {oid for pool in cluster_candidates.values() for oid in pool}
    print(f"  Clusters with candidates: {len(cluster_candidates):,}")
    print(f"  Unique candidate OAX IDs: {len(unique_oax_ids):,}")

    # ── Phase 4: Batch OAX data fetch (candidates + co-awardees) ────────────
    co_awardee_oax_ids: set[str] = set()
    for cluster in targets:
        for cid in cluster.co_awardee_cluster_ids:
            oid = matched_index.get(cid)
            if oid:
                co_awardee_oax_ids.add(oid)

    all_needed = list(unique_oax_ids | co_awardee_oax_ids)
    print(f"Fetching OAX data for {len(all_needed):,} authors ({len(co_awardee_oax_ids):,} co-awardees)...")
    oax_data = _fetch_oax_data(all_needed)
    print(f"  Authors fetched: {len(oax_data):,}")

    # ── Phase 5: Score with co-awardee signal ────────────────────────────────
    print("Scoring candidates...")
    cluster_index = {c.cluster_id: c for c in clusters}
    n_matched = n_ambiguous = n_no_accept = n_no_pool = 0
    n_narrowed_pools = 0

    for cluster in targets:
        pool = cluster_candidates.get(cluster.cluster_id)
        if not pool:
            n_no_pool += 1
            continue

        arc_firsts = _cluster_arc_firsts(cluster)

        # Build co-awardee profile union for this cluster
        ca_heps:   set[str] = set()
        ca_fields: set[str] = set()
        for cid in cluster.co_awardee_cluster_ids:
            oid = matched_index.get(cid)
            if oid:
                d = oax_data.get(oid)
                if d:
                    for aid in d["aff_ids"]:
                        h = inst_id_to_hep.get(aid)
                        if h:
                            ca_heps.add(h)
                    ca_fields.update(d["field_names"])

        # Narrow pool to co-awardee-consistent candidates
        if ca_heps or ca_fields:
            consistent: set[str] = set()
            for oid in pool:
                d = oax_data.get(oid)
                if d is None:
                    continue
                cand_heps   = {inst_id_to_hep[a] for a in d["aff_ids"] if a in inst_id_to_hep}
                cand_fields = set(d["field_names"])
                if (cand_heps & ca_heps) or (cand_fields & ca_fields):
                    consistent.add(oid)
            if consistent:
                if len(consistent) < len(pool):
                    n_narrowed_pools += 1
                pool = consistent

        unique   = len(pool) == 1
        accepted: list[tuple[str, int]] = []

        for oax_id in pool:
            d = oax_data.get(oax_id)
            if d is None:
                continue
            oax_firsts: set[str] = set()
            for tok in _cluster_family_tokens(cluster):
                oax_firsts.update(token_to_candidates.get(tok, {}).get(oax_id, set()))

            score, n_ind = _score_candidate(
                cluster, arc_firsts, oax_firsts,
                d["aff_ids"], d["field_names"],
                unique, ca_heps, ca_fields,
                inst_id_to_hep, for2d_to_fields,
            )
            if _accept(score, n_ind):
                accepted.append((oax_id, score))

        if len(accepted) == 1:
            oax_id, _ = accepted[0]
            cluster.status = ClusterStatus.MATCHED.value
            cluster.oax_id = oax_id
            cluster.tier   = 5
            n_matched += 1
        elif len(accepted) > 1:
            n_ambiguous += 1
        else:
            n_no_accept += 1

    # ── Mark remaining UNRESOLVED as UNDECIDABLE ──────────────────────────────
    n_undecidable = 0
    for cluster in clusters:
        if cluster.status == ClusterStatus.UNRESOLVED.value:
            cluster.status = ClusterStatus.UNDECIDABLE.value
            n_undecidable += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    status_counts: dict[str, int] = defaultdict(int)
    for c in clusters:
        status_counts[c.status] += 1

    print(f"\n{'─'*60}")
    print(f"LAYER 3 SUMMARY")
    print(f"{'─'*60}")
    print(f"  Pools narrowed by co-awardee:  {n_narrowed_pools:>7,}")
    print(f"  Matched (tier 5):              {n_matched:>7,}")
    print(f"  Ambiguous (multiple accept):   {n_ambiguous:>7,}")
    print(f"  No accept:                     {n_no_accept:>7,}")
    print(f"  No candidate pool:             {n_no_pool:>7,}")
    print(f"  Marked UNDECIDABLE:            {n_undecidable:>7,}")
    print(f"\n  Final cluster store status:")
    for status in ("MATCHED", "ABSENT", "UNDECIDABLE", "SPLIT", "UNRESOLVED"):
        n = status_counts.get(status, 0)
        if n:
            print(f"    {status:<14} {n:>7,}")

    # ── Save ──────────────────────────────────────────────────────────────────
    save_clusters(clusters, path)
    checkpoint = PROCESSED_DATA / "clusters_after_layer3.jsonl"
    save_clusters(clusters, checkpoint)
    print(f"\nSaved:")
    print(f"  {path}")
    print(f"  {checkpoint}")


if __name__ == "__main__":
    main()
