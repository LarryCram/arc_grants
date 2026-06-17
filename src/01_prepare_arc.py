"""
src/01_prepare_arc.py

Prepare ARC investigators for Splink, then deduplicate grant rows into person clusters.

Phase 1 – Prep: parse and normalise ARC name/institution/FOR fields
    → arc_investigators_prep.parquet

Phase 2 – Dedupe: Splink dedupe_only on the prep output
    → arc_persons.parquet  (62k grant rows → ~22,819 persons)
"""

import sys
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, ADMIN_ORGS_CSV, GRANT_SUMMARIES_CSV, DISKCACHE_DIR
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import make_expanded_for_tokens, name_part_tokens, strip_diacriticals, for_name_tokens

import diskcache
from src.utils.lookup_for_topic import ForTopicLookup
from src.utils.cluster_checks import load_for_divisions, division_mismatch, is_case_a, is_suspicious, first_names_compatible

MANUAL_SPLITS_CSV        = Path(__file__).resolve().parents[1] / "config" / "manual_splits.csv"
MANUAL_ORCIDS_CSV        = Path(__file__).resolve().parents[1] / "config" / "manual_orcids.csv"
MANUAL_MERGES_CSV        = Path(__file__).resolve().parents[1] / "config" / "manual_merges.csv"
ENRICHMENT_BLOCKLIST_CSV = Path(__file__).resolve().parents[1] / "config" / "enrichment_blocklist.csv"
CLUSTER_THRESHOLD = 0.9
RARE_NAME_TF      = 2e-6   # OAX full_name_key TF below this → rare name (tier 2 vs 3)


# ── FOR code normalisation (2008 → 2020) ─────────────────────────────────────

def upgrade_for_code(lu: ForTopicLookup, code: str | None) -> str | None:
    """Upgrade a 2008 FOR group code to its 2020 equivalent; 2020 codes pass through.
    Returns None for unmappable codes (SQL COALESCE keeps the original in that case).
    """
    if not code:
        return None
    return lu.upgrade_for_code(code)


def upgrade_for_name(lu: ForTopicLookup, code: str | None, name: str | None) -> str | None:
    """Return the official ANZSRC 2020 group name when a 2008 code is upgraded,
    or the original name when already 2020 or no mapping exists.
    """
    if not code:
        return name
    code20 = lu.upgrade_for_code(code)
    if code20 is None or code20 == code:
        return name
    sf_row = lu.group_to_subfield(code20)
    return sf_row["group_name"] if sf_row else name


# ── Phase 1 helpers ───────────────────────────────────────────────────────────

def _initials(toks: list[str]) -> list[str]:
    return [t[0] for t in toks if t]


def arc_name_arrays(first_name: str, family_name: str) -> dict:
    full = f"{first_name or ''} {family_name or ''}".strip()
    hn = HumanName(full)

    if not hn.last and hn.first:
        hn.last = hn.first

    f_toks = name_part_tokens(hn.first) + name_part_tokens(hn.middle)
    fam_norm = strip_diacriticals(hn.last).lower().strip() if hn.last else ""

    first_names = list(set(f_toks + _initials(f_toks)))
    family_names = [fam_norm] if fam_norm else []

    if not f_toks and fam_norm:
        first_names.append(fam_norm[0])

    return {"first_names": list(set(first_names)), "family_names": family_names}


# ── Phase 2 helpers ───────────────────────────────────────────────────────────


def _load_all(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    print(f"  {len(df)} grant rows, {df['unique_id'].nunique()} unique IDs")
    return df


def _first_initial(toks) -> str | None:
    if toks is None or len(toks) == 0:
        return None
    full = [t for t in toks if len(t) > 1]
    return full[0][0] if full else toks[0][0]


def _first_name_canonical(full_toks, initials) -> str | None:
    if full_toks is not None and len(full_toks) > 0:
        return max(full_toks, key=len)
    if initials is not None and len(initials) > 0:
        return initials[0]
    return None


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["inst_arr"] = df["institution_oax_id"].apply(
        lambda x: [x] if (pd.notna(x) and x) else []
    )
    df["family_name_main"] = df["family_names"].apply(
        lambda x: max(x, key=len) if (x is not None and len(x) > 0) else None
    )
    df["first_initial"] = df["first_names"].apply(_first_initial)
    df["for_name_tokens"] = df["for_name_tokens"].apply(
        lambda x: list(x) if x is not None else []
    )
    df["first_name_full"] = df["first_name_full"].apply(
        lambda x: list(x) if x is not None else []
    )
    df["first_initials"] = df["first_initials"].apply(
        lambda x: list(x) if x is not None else []
    )
    df["first_name_canonical"] = df.apply(
        lambda r: _first_name_canonical(r["first_name_full"], r["first_initials"]),
        axis=1,
    )
    df["full_name_key"] = df.apply(
        lambda r: f"{r['first_name_canonical']}_{r['family_name_main']}"
        if (pd.notna(r["first_name_canonical"]) and pd.notna(r["family_name_main"]))
        else None,
        axis=1,
    )
    return df[[
        "unique_id", "full_name",
        "first_name_canonical", "full_name_key",
        "family_name_main", "first_initial",
        "orcid", "inst_arr", "for_name_tokens",
    ]]


def _norm_full(name: str) -> str:
    return re.sub(r'[^a-z ]', '', name.lower()).strip()


def _split_multi_name_clusters(
    df_cluster_ids: pd.DataFrame,
    df_prep: pd.DataFrame,
    orcid_enrichment: pd.DataFrame | None = None,
    history: dict | None = None,
) -> pd.DataFrame:
    """
    For clusters containing 2+ genuinely distinct full name forms, use three
    signals to detect mis-merged different people and split them:

      1. Co-investigator sets  — build a defaultdict(set) keyed on normalised
         full name; each set contains every other investigator who appeared on
         any grant bearing that name form.  Disjoint sets = no shared network.

      2. FOR field sets — union of for_names across all grants for each name
         form.  Disjoint sets = different research areas.

      3. ORCID enrichment cache — if two name forms resolve to different ORCIDs
         the split is definitive regardless of the other signals.

    Split rule:
      - Different ORCIDs                                → definitive split
      - Disjoint co-investigator sets AND disjoint FOR  → definitive split

    Abbreviated / common name forms (first token ≤ 2 chars, e.g. "Chun Li")
    are not used to drive splits.  After full-name forms are split into
    sub-clusters, each abbreviated grant is assigned to the sub-cluster whose
    FOR set overlaps most with that grant's FOR.  If no overlap → own cluster.
    """
    # Load raw investigators filtered to in-scope schemes and roles only
    raw_inv_path = PROCESSED_DATA / "investigators_raw.parquet"
    raw_inv = pd.read_parquet(raw_inv_path, columns=["grant_code", "first_name", "family_name", "role_code"])
    raw_inv = raw_inv[
        raw_inv["grant_code"].str[:2].isin(KEEP_SCHEMES) &
        raw_inv["role_code"].isin(KEEP_ROLES)
    ]
    raw_inv["coinv_name"] = (raw_inv["first_name"] + " " + raw_inv["family_name"]).str.strip()

    # Build ORCID enrichment lookup: (norm_full_name) → orcid
    enriched_orcid: dict[str, str] = {}
    if orcid_enrichment is not None and len(orcid_enrichment):
        for _, er in orcid_enrichment.iterrows():
            if pd.notna(er.get("orcid")):
                key = _norm_full(f"{er['first_name']} {er['family_name']}")
                enriched_orcid[key] = er["orcid"]

    merged = df_prep[["unique_id", "full_name", "for_names", "orcid"]].merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])
    merged["norm_name"] = merged["full_name"].apply(_norm_full)
    # Grant code is the prefix of unique_id before the last underscore block
    merged["grant_code"] = merged["unique_id"].apply(lambda u: u.rsplit("_", 1)[0])

    out = []
    n_splits = 0

    for cid, grp in merged.groupby("cluster_id"):
        # If all records share one ORCID, name variance is data-quality noise — never split
        cluster_orcids = grp["orcid"].dropna().unique()
        if len(cluster_orcids) == 1:
            out.append(grp[["unique_id", "cluster_id"]])
            continue

        # Distinct normalised full name forms that have a full first name (>2 chars)
        def _first_tok(norm):
            parts = norm.split()
            return parts[0] if parts else ""

        full_forms = {n for n in grp["norm_name"].unique() if len(_first_tok(n)) > 2}
        abbrev_rows = grp[grp["norm_name"].apply(lambda n: len(_first_tok(n)) <= 2)]

        if len(full_forms) <= 1:
            out.append(grp[["unique_id", "cluster_id"]])
            continue

        # ── Build co-investigator and FOR sets per name form ──────────────────
        coinv: dict[str, set] = defaultdict(set)
        for_sets: dict[str, set] = defaultdict(set)

        for norm_name in full_forms:
            form_rows = grp[grp["norm_name"] == norm_name]
            for _, row in form_rows.iterrows():
                gc = row["grant_code"]
                # co-investigators on this grant (everyone else)
                others = raw_inv[
                    (raw_inv["grant_code"] == gc) &
                    (raw_inv["coinv_name"] != row["full_name"])
                ]["coinv_name"]
                coinv[norm_name].update(others)
                # FOR fields for this grant row
                fors = row["for_names"]
                if fors is not None:
                    for_sets[norm_name].update(fors)

        # ── Check pairwise for split signals ─────────────────────────────────
        form_list = sorted(full_forms)
        split_pairs: set[tuple] = set()

        for i, a in enumerate(form_list):
            for b in form_list[i + 1:]:
                oa = enriched_orcid.get(a)
                ob = enriched_orcid.get(b)
                different_orcid = oa and ob and oa != ob
                disjoint_coinv  = len(coinv[a] & coinv[b]) == 0
                disjoint_for    = len(for_sets[a] & for_sets[b]) == 0
                if different_orcid or (disjoint_coinv and disjoint_for):
                    split_pairs.add((a, b))

        if not split_pairs:
            out.append(grp[["unique_id", "cluster_id"]])
            continue

        # ── Assign each full-name form to a sub-cluster ───────────────────────
        # Use union-find to group compatible name forms together
        parent = {f: f for f in form_list}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        # Forms NOT in split_pairs are compatible → union them
        for i, a in enumerate(form_list):
            for b in form_list[i + 1:]:
                if (a, b) not in split_pairs:
                    union(a, b)

        root_to_forms: dict[str, list] = defaultdict(list)
        for f in form_list:
            root_to_forms[find(f)].append(f)

        if len(root_to_forms) <= 1:
            out.append(grp[["unique_id", "cluster_id"]])
            continue

        n_splits += 1
        print(f"  Multi-name split: {cid}")
        for root, forms in root_to_forms.items():
            print(f"    sub-cluster: {forms}")

        # Build sub-cluster FOR sets (union across forms in each sub-cluster)
        sub_for: dict[str, set] = {}
        for root, forms in root_to_forms.items():
            sub_for[root] = set().union(*(for_sets[f] for f in forms))

        # Assign full-name rows
        norm_to_root = {f: find(f) for f in form_list}
        sub = grp.copy()

        def get_root(row):
            nn = row["norm_name"]
            if nn in norm_to_root:
                return norm_to_root[nn]
            # Abbreviated form: assign by FOR overlap with sub-clusters
            fors = set(row["for_names"]) if row["for_names"] is not None else set()
            if fors:
                scores = {root: len(fors & sf) for root, sf in sub_for.items()}
                best_root, best_score = max(scores.items(), key=lambda x: x[1])
                if best_score > 0:
                    return best_root
            return None  # no FOR overlap → singleton

        sub["_root_key"] = sub.apply(get_root, axis=1)
        root_canonical: dict[str, str] = {
            root: root_grp["unique_id"].min()
            for root, root_grp in sub[sub["_root_key"].notna()].groupby("_root_key")
        }
        sub["cluster_id"] = sub.apply(
            lambda r: root_canonical[r["_root_key"]] if r["_root_key"] is not None else r["unique_id"],
            axis=1,
        )
        sub = sub.drop(columns=["_root_key"])
        if history is not None:
            parent_hist = history.pop(cid, [{"event": "splink_cluster"}])
            for new_cid, new_grp in sub.groupby("cluster_id"):
                history[new_cid] = parent_hist + [{
                    "event": "name_split",
                    "split_from": cid,
                    "name_forms": sorted(new_grp["norm_name"].unique().tolist()),
                }]
        out.append(sub[["unique_id", "cluster_id"]])

    print(f"  Multi-name split: {n_splits} cluster(s) split")
    return pd.concat(out, ignore_index=True)


def _merge_by_orcid(
    df_cluster_ids: pd.DataFrame, df_raw: pd.DataFrame,
    history: dict | None = None,
) -> pd.DataFrame:
    """
    Deterministically merge clusters linked by the same ORCID.

    For each ORCID that appears in 2+ Splink clusters: verify that all
    member records share a compatible family name, then collapse those
    clusters to a single canonical id (orcid_<orcid>).  If the same ORCID
    spans incompatible family names the group is left unchanged and printed
    as a data-quality warning.
    """
    merged = df_raw[["unique_id", "orcid", "family_names"]].merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])
    merged["family_name_main"] = merged["family_names"].apply(
        lambda x: max(x, key=len) if (x is not None and len(x) > 0) else None
    )

    with_orcid = merged[merged["orcid"].notna()].copy()
    orcid_span = with_orcid.groupby("orcid")["cluster_id"].nunique()
    multi_orcids = orcid_span[orcid_span > 1].index

    if len(multi_orcids) == 0:
        print("  ORCID merge: no cross-cluster ORCIDs")
        return df_cluster_ids

    remapping: dict[str, str] = {}
    n_merged = n_conflict = 0

    for orcid in sorted(multi_orcids):
        rows = with_orcid[with_orcid["orcid"] == orcid]
        fam_names = set(rows["family_name_main"].dropna())
        if len(fam_names) > 1:
            print(f"  NOTE ORCID {orcid} spans family names {fam_names} — merging on ORCID authority")
            n_conflict += 1
        clusters = sorted(rows["cluster_id"].unique())
        canonical = min(clusters)
        non_canonical = [c for c in clusters if c != canonical]
        for cid in clusters:
            remapping[cid] = canonical
        if history is not None:
            history.setdefault(canonical, [{"event": "splink_cluster"}]).append({
                "event": "orcid_merge",
                "merged_from": non_canonical,
                "orcid": orcid,
            })
            for c in non_canonical:
                history.pop(c, None)
        n_merged += 1

    if not remapping:
        return df_cluster_ids

    out = df_cluster_ids.copy()
    out["cluster_id"] = out["cluster_id"].map(lambda c: remapping.get(c, c))
    print(f"  ORCID merge: {n_merged} ORCID(s) merged across clusters, {n_conflict} conflict(s) skipped")
    return out


def _split_orcid_conflicts(
    df_cluster_ids: pd.DataFrame, df_original: pd.DataFrame,
    history: dict | None = None,
) -> pd.DataFrame:
    merged = df_original[["unique_id", "orcid"]].merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])

    out = []
    for cid, grp in merged.groupby("cluster_id"):
        distinct_orcids = grp["orcid"].dropna().unique()
        if len(distinct_orcids) <= 1:
            out.append(grp[["unique_id", "cluster_id"]])
        else:
            sub = grp.copy()
            sub["cluster_id"] = sub["unique_id"]  # no-ORCID records become singletons
            for orcid_val in distinct_orcids:
                mask = sub["orcid"] == orcid_val
                canonical = sub.loc[mask, "unique_id"].min()
                sub.loc[mask, "cluster_id"] = canonical
            out.append(sub[["unique_id", "cluster_id"]])
            if history is not None:
                parent_hist = history.pop(cid, [{"event": "splink_cluster"}])
                for new_cid, new_grp in sub.groupby("cluster_id"):
                    orcid_val = new_grp["orcid"].dropna().iloc[0] if new_grp["orcid"].notna().any() else None
                    history[new_cid] = parent_hist + [{
                        "event": "orcid_conflict_split",
                        "split_from": cid,
                        "orcid": orcid_val,
                    }]

    return pd.concat(out, ignore_index=True)


def _aggregate_clusters(df_original: pd.DataFrame, df_cluster_ids: pd.DataFrame) -> pd.DataFrame:
    merged = df_original.merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])

    def union_lists(series):
        out = set()
        for lst in series:
            if lst is not None:
                out.update(lst)
        return sorted(out)

    def distinct_nonempty(series):
        return sorted({v for v in series if pd.notna(v) and v})

    return (
        merged.groupby("cluster_id")
        .agg(
            full_names=("full_name", lambda s: sorted(set(s))),
            first_names=("first_names", union_lists),
            family_names=("family_names", union_lists),
            orcids=("orcid", distinct_nonempty),
            inst_arr=("inst_arr", union_lists),
            for_names=("for_names", union_lists),
            for_codes=("for_codes", union_lists),
            grant_ids=("unique_id", list),
            n_grants=("unique_id", "count"),
        )
        .reset_index()
    )


def _load_inst_names() -> dict[str, str]:
    lookup = {}
    if not ADMIN_ORGS_CSV.exists():
        return lookup
    with open(ADMIN_ORGS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            iid = (row.get("institution_id") or "").strip()
            name = (row.get("organisationName") or "").strip()
            if iid and name:
                lookup[iid] = name
    return lookup


def _inst_label(inst_id: str, lookup: dict) -> str:
    return lookup.get(inst_id, inst_id.split("/")[-1] if inst_id else "?")




def _apply_manual_merges(persons: pd.DataFrame, cluster_history: dict) -> pd.DataFrame:
    """Merge cluster pairs listed in config/manual_merges.csv.

    Applied unconditionally — no initial-compatibility check.
    cluster_keep survives; cluster_drop is absorbed into it.
    """
    if not MANUAL_MERGES_CSV.exists():
        return persons

    merges: list[tuple[str, str]] = []
    with open(MANUAL_MERGES_CSV, newline="") as f:
        for row in csv.DictReader(f):
            keep = row["cluster_keep"].strip()
            drop = row["cluster_drop"].strip()
            if keep and drop:
                merges.append((keep, drop))

    if not merges:
        return persons

    remapping = {drop: keep for keep, drop in merges}

    def _union(series_of_lists):
        result = set()
        for lst in series_of_lists:
            result.update(lst)
        return sorted(result)

    out = persons.copy()
    out["cluster_id"] = out["cluster_id"].map(lambda c: remapping.get(c, c))

    vc = out["cluster_id"].value_counts()
    multi = set(vc[vc > 1].index)

    unchanged = out[~out["cluster_id"].isin(multi)].copy()
    merged_parts = []

    for canonical, grp in out[out["cluster_id"].isin(multi)].groupby("cluster_id"):
        best_idx = grp["n_grants"].idxmax()
        absorbed = [drop for drop, keep in remapping.items() if keep == canonical]
        merged_parts.append({
            "cluster_id":    canonical,
            "full_names":    _union(grp["full_names"]),
            "first_names":   _union(grp["first_names"]),
            "family_names":  _union(grp["family_names"]),
            "orcids":        _union(grp["orcids"]),
            "inst_arr":      _union(grp["inst_arr"]),
            "for_names":     _union(grp["for_names"]),
            "for_codes":     _union(grp["for_codes"]),
            "grant_ids":     [g for row in grp["grant_ids"] for g in row],
            "n_grants":      int(grp["n_grants"].sum()),
            "full_name_key": grp.loc[best_idx, "full_name_key"],
        })
        cluster_history.setdefault(canonical, []).append({
            "event": "manual_merge",
            "merged_from": absorbed,
        })
        for cid in absorbed:
            cluster_history.pop(cid, None)

    print(f"  Manual merges: {len(merges)} pair(s)")
    return pd.concat([unchanged, pd.DataFrame(merged_parts)], ignore_index=True)


def _merge_same_grant_coinvestigators(
    persons: pd.DataFrame,
    cluster_history: dict,
) -> pd.DataFrame:
    """Auto-merge same-blocking-key clusters on the same single-org grant.

    If a grant's only eligible organisations are one university (n_eligible_orgs == 1),
    two clusters sharing the same (family_name_main, first_initial) on that grant
    are the same person — they are literally co-investigators at the same institution
    on the same project.

    Skips pairs where the clusters already carry distinct non-empty ORCIDs.
    """
    grants = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")
    if "n_eligible_orgs" not in grants.columns:
        print("  Same-grant auto-merges: skipped (n_eligible_orgs not in grants_flat — re-run 00)")
        return persons

    single_org = set(grants.loc[grants["n_eligible_orgs"] == 1, "grant_code"])

    # Blocking key per cluster from current persons state
    out = persons.copy()
    out["_fam"] = out["family_names"].apply(lambda x: max(x, key=len) if x else None)
    out["_ini"] = out["first_names"].apply(_first_initial)

    # grant_ids column stores unique_ids like "DP0208022_WilliamSmyth";
    # grant_code = first "_"-separated token
    exploded = (
        out[["cluster_id", "_fam", "_ini", "orcids", "grant_ids"]]
        .explode("grant_ids")
        .rename(columns={"grant_ids": "unique_id"})
        .dropna(subset=["unique_id", "_fam", "_ini"])
    )
    exploded["grant_code"] = exploded["unique_id"].str.split("_").str[0]
    exploded = exploded[exploded["grant_code"].isin(single_org)]

    out = out.drop(columns=["_fam", "_ini"])

    if exploded.empty:
        print("  Same-grant auto-merges: 0 (no single-org grants with same-name clusters)")
        return out

    groups = (
        exploded.groupby(["grant_code", "_fam", "_ini"])["cluster_id"]
        .agg(lambda s: sorted(s.unique()))
        .reset_index()
        .rename(columns={"cluster_id": "clusters"})
    )
    groups = groups[groups["clusters"].apply(len) >= 2]

    if groups.empty:
        print("  Same-grant auto-merges: 0")
        return out

    orcid_idx = persons.set_index("cluster_id")["orcids"].to_dict()

    parent: dict[str, str] = {}

    def find(x: str) -> str:
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while x != root:
            parent[x], x = root, parent.get(x, x)
        return root

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        keep, drop = (ra, rb) if ra < rb else (rb, ra)
        parent[drop] = keep

    n_skipped = 0
    for _, row in groups.iterrows():
        clusters = row["clusters"]
        non_empty = [set(orcid_idx.get(c, [])) for c in clusters if orcid_idx.get(c)]
        if len(non_empty) >= 2:
            union_all = set.union(*non_empty)
            shared    = set.intersection(*non_empty)
            if len(union_all) > len(shared):
                n_skipped += 1
                continue
        for c in clusters[1:]:
            union(clusters[0], c)

    all_ids  = {c for row in groups["clusters"] for c in row}
    remapping = {c: find(c) for c in all_ids if find(c) != c}

    if not remapping:
        print(f"  Same-grant auto-merges: 0 ({n_skipped} skipped — ORCID conflict)")
        return out

    def _union_lists(series_of_lists):
        result = set()
        for lst in series_of_lists:
            result.update(lst)
        return sorted(result)

    out["cluster_id"] = out["cluster_id"].map(lambda c: remapping.get(c, c))
    vc    = out["cluster_id"].value_counts()
    multi = set(vc[vc > 1].index)

    unchanged    = out[~out["cluster_id"].isin(multi)].copy()
    merged_parts = []

    for canonical, grp in out[out["cluster_id"].isin(multi)].groupby("cluster_id"):
        best_idx = grp["n_grants"].idxmax()
        absorbed = [drop for drop, keep in remapping.items() if keep == canonical]
        merged_parts.append({
            "cluster_id":    canonical,
            "full_names":    _union_lists(grp["full_names"]),
            "first_names":   _union_lists(grp["first_names"]),
            "family_names":  _union_lists(grp["family_names"]),
            "orcids":        _union_lists(grp["orcids"]),
            "inst_arr":      _union_lists(grp["inst_arr"]),
            "for_names":     _union_lists(grp["for_names"]),
            "for_codes":     _union_lists(grp["for_codes"]),
            "grant_ids":     [g for r in grp["grant_ids"] for g in r],
            "n_grants":      int(grp["n_grants"].sum()),
            "full_name_key": grp.loc[best_idx, "full_name_key"],
        })
        cluster_history.setdefault(canonical, []).append({
            "event":       "same_grant_merge",
            "merged_from": absorbed,
        })
        for cid in absorbed:
            cluster_history.pop(cid, None)

    n = len(remapping)
    skip_note = f" ({n_skipped} skipped — ORCID conflict)" if n_skipped else ""
    print(f"  Same-grant auto-merges: {n} absorbed → {len(merged_parts)} canonical(s){skip_note}")
    return pd.concat([unchanged, pd.DataFrame(merged_parts)], ignore_index=True)


def _compute_reliability_tier(persons: pd.DataFrame, tf_lookup: dict) -> pd.DataFrame:
    """Add reliability_tier column classifying each cluster's identity confidence.

    1a  HAS_ORCID, source = ARC data
    1b  HAS_ORCID, source = ORCID enrichment (00b)
    1c  HAS_ORCID, source = manual_orcids.csv
    2   NO_ORCID, multi-grant, rare name  (tf < RARE_NAME_TF)
    3   NO_ORCID, multi-grant, common name
    4   NO_ORCID, singleton, no gap_candidates (isolated)
    4u  NO_ORCID, singleton, has gap_candidates (unresolved collision)
    """
    def _tier(row):
        if row["orcid_status"] in ("HAS_ORCID", "MULTI_ORCID"):
            hist = row["cluster_history"]
            if isinstance(hist, str):
                hist = json.loads(hist)
            events = {e.get("event") for e in (hist or [])}
            if "manual_orcid" in events:
                return "1c"
            if "enriched_orcid" in events:
                return "1b"
            return "1a"
        # NO_ORCID
        if row["n_grants"] >= 2:
            fnk = row.get("full_name_key")
            tf = tf_lookup.get(fnk, 1.0) if fnk else 1.0
            return "2" if tf < RARE_NAME_TF else "3"
        # singleton
        cands = row.get("gap_candidates")
        return "4u" if (cands is not None and len(cands) > 0) else "4"

    out = persons.copy()
    out["reliability_tier"] = out.apply(_tier, axis=1)
    counts = out["reliability_tier"].value_counts().sort_index()
    print(f"  Reliability tiers: {dict(counts)}")
    return out


def _compute_gap_candidates(
    persons: pd.DataFrame, div_map: dict, adj: set
) -> pd.DataFrame:
    """For each cluster, record gap_candidates: other cluster_ids sharing
    (family_name_main, first_initial) that cannot be ruled out as the same person.

    Incompatibility tests (any one → keep separate):
      - name:    first_names sets are mutually incompatible (cascade: full name / initial)
      - FOR:     combined for_names span disconnected major divisions
      - ORCID:   both have ORCIDs and they are disjoint
    """
    def _fnm(family_names):
        return max(family_names, key=len) if family_names else None

    p = persons[["cluster_id", "first_names", "family_names", "for_names", "orcids"]].copy()
    p["_fnm"] = p["family_names"].apply(_fnm)
    p = p.dropna(subset=["_fnm"])

    idx = p.set_index("cluster_id")
    gap: dict[str, list[str]] = {cid: [] for cid in persons["cluster_id"]}
    n_compat = n_incompat = 0

    for fnm, grp in p.groupby("_fnm"):
        if len(grp) < 2:
            continue
        cids = list(grp["cluster_id"])
        for i, c1 in enumerate(cids):
            for c2 in cids[i + 1:]:
                r1, r2 = idx.loc[c1], idx.loc[c2]
                fn1, fn2 = list(r1["first_names"]), list(r2["first_names"])
                name_incompat = (
                    not first_names_compatible(fn1, fn2) or
                    not first_names_compatible(fn2, fn1)
                )
                div_incompat = division_mismatch(
                    list(r1["for_names"]) + list(r2["for_names"]), div_map, adj
                )
                orcid_incompat = (
                    len(r1["orcids"]) > 0 and len(r2["orcids"]) > 0
                    and not set(r1["orcids"]) & set(r2["orcids"])
                )
                if name_incompat or div_incompat or orcid_incompat:
                    n_incompat += 1
                else:
                    gap[c1].append(c2)
                    gap[c2].append(c1)
                    n_compat += 1

    print(f"  Gap 1: {n_compat} compatible pairs, {n_incompat} incompatible pairs")
    out = persons.copy()
    out["gap_candidates"] = out["cluster_id"].map(gap)
    return out


def _diagnostic_report(
    persons: pd.DataFrame,
    inst_lookup: dict,
    div_map: dict,
    adj: set,
    tf_lookup: dict,
) -> None:
    multi = persons[persons["n_grants"] > 1].sort_values("n_grants", ascending=False)
    singletons = (persons["n_grants"] == 1).sum()
    print(f"\n  Clusters: {len(persons)}  ({singletons} singletons, {len(multi)} multi-grant)")

    one_inst   = multi[multi["inst_arr"].apply(len) == 1]
    multi_inst = multi[multi["inst_arr"].apply(len) > 1]
    no_inst    = multi[multi["inst_arr"].apply(len) == 0]
    print(f"    1 institution:   {len(one_inst):>4}")
    print(f"    2+ institutions: {len(multi_inst):>4}")
    print(f"    0 institutions:  {len(no_inst):>4}")

    suspect = multi[multi.apply(
        lambda r: is_suspicious(r, div_map, adj, tf_lookup), axis=1
    )]
    case_a = suspect[suspect["full_names"].apply(is_case_a)]
    case_b = suspect[~suspect["full_names"].apply(is_case_a)]

    auto_committed = len(multi) - len(suspect)
    print(f"\n  Multi-grant auto-committed (ORCID / rare name / compatible divisions): {auto_committed}")
    print(f"  Needs review (common name, no ORCID, cross-division FOR): {len(suspect)}")
    print(f"    Case A — single name: {len(case_a)}")
    print(f"    Case B — multiple names: {len(case_b)}")

    def _show(df, label, n=20):
        print(f"\n  {label} (top {n} by grant count):")
        for _, row in df.head(n).iterrows():
            names  = "; ".join(row["full_names"][:4])
            orcids = ", ".join(row["orcids"]) if row["orcids"] else "—"
            insts  = "; ".join(_inst_label(i, inst_lookup) for i in row["inst_arr"][:4])
            divs   = sorted({div_map.get(fn, "?") for fn in row["for_names"] if fn})
            fors   = "; ".join(row["for_names"]) if row["for_names"] else "—"
            print(f"    n={row['n_grants']:>3}  [{orcids}]  {names}")
            print(f"           insts: {insts}")
            print(f"           FOR:   {fors}")
            print(f"           divs:  {' '.join(divs)}")

    _show(case_a, "Case A — single name, needs review")
    _show(case_b, "Case B — multiple names, needs review")


def _export_manual_splits_template(
    persons: pd.DataFrame,
    inst_lookup: dict,
    div_map: dict,
    adj: set,
    tf_lookup: dict,
) -> None:
    multi_inst = persons[
        (persons["n_grants"] > 1)
        & (persons["inst_arr"].apply(len) > 1)
        & (persons["full_names"].apply(is_case_a))
        & persons.apply(lambda r: is_suspicious(r, div_map, adj, tf_lookup), axis=1)
    ].sort_values("n_grants", ascending=False)

    existing: dict[str, dict] = {}
    if MANUAL_SPLITS_CSV.exists():
        with open(MANUAL_SPLITS_CSV, newline="") as f:
            for row in csv.DictReader(f):
                existing[row["cluster_id"]] = row

    rows = []
    for _, p in multi_inst.iterrows():
        cid = p["cluster_id"]
        prev = existing.get(cid, {})
        rows.append({
            "cluster_id":               cid,
            "n_grants":                 p["n_grants"],
            "names":                    "; ".join(p["full_names"]),
            "orcids":                   "; ".join(p["orcids"]) if p["orcids"] else "",
            "institutions":             "; ".join(_inst_label(i, inst_lookup) for i in p["inst_arr"]),
            "for_names":                "; ".join(p["for_names"]),
            "grant_ids":                "; ".join(p["grant_ids"]),
            "confirmed_different_people": prev.get("confirmed_different_people", ""),
            "notes":                    prev.get("notes", ""),
        })

    with open(MANUAL_SPLITS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  Case A template written → {MANUAL_SPLITS_CSV}")
    coded = sum(1 for r in rows if r["confirmed_different_people"].strip().lower() == "true")
    print(f"  {coded} of {len(rows)} clusters hand-coded as 'confirmed_different_people'")


def _apply_manual_splits(
    df_cluster_ids: pd.DataFrame, df_original: pd.DataFrame,
    history: dict | None = None,
) -> pd.DataFrame:
    if not MANUAL_SPLITS_CSV.exists():
        return df_cluster_ids

    split_clusters: set[str] = set()
    with open(MANUAL_SPLITS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("confirmed_different_people", "").strip().lower() == "true":
                split_clusters.add(row["cluster_id"])

    if not split_clusters:
        return df_cluster_ids

    print(f"  Applying manual splits for {len(split_clusters)} cluster(s)...")

    merged = df_original[["unique_id", "institution_oax_id"]].merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])

    out = []
    for cid, grp in merged.groupby("cluster_id"):
        if cid not in split_clusters:
            out.append(grp[["unique_id", "cluster_id"]])
            continue
        sub = grp.copy()
        inst_canonical: dict[str, str] = {
            inst_key: inst_grp["unique_id"].min()
            for inst_key, inst_grp in sub.groupby("institution_oax_id", dropna=True)
            if inst_key
        }
        sub["cluster_id"] = sub.apply(
            lambda r: inst_canonical.get(r["institution_oax_id"], r["unique_id"])
            if (pd.notna(r["institution_oax_id"]) and r["institution_oax_id"])
            else r["unique_id"],
            axis=1,
        )
        if history is not None:
            parent_hist = history.pop(cid, [{"event": "splink_cluster"}])
            for new_cid, new_grp in sub.groupby("cluster_id"):
                inst = new_grp["institution_oax_id"].dropna().iloc[0] if new_grp["institution_oax_id"].notna().any() else None
                history[new_cid] = parent_hist + [{
                    "event": "manual_split",
                    "split_from": cid,
                    "institution": inst,
                }]
        out.append(sub[["unique_id", "cluster_id"]])

    return pd.concat(out, ignore_index=True)


def _apply_enriched_orcids(
    persons: pd.DataFrame,
    df_cluster_ids: pd.DataFrame,
    cluster_history: dict,
) -> pd.DataFrame:
    """Promote high/au_match ORCIDs from orcid_enrichment.parquet into cluster orcids field.

    Only promotes when exactly 1 distinct ORCID is found across all enriched name forms
    in a cluster, and the cluster currently has no ORCID.
    """
    enrichment_path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if not enrichment_path.exists():
        return persons

    enrichment = pd.read_parquet(enrichment_path)
    enrichment = enrichment[
        enrichment["confidence"].isin(["high", "au_match"]) & enrichment["orcid"].notna()
    ]
    if len(enrichment) == 0:
        return persons

    # Load blocklist: (cluster_id, orcid) pairs that must not be promoted
    blocklist: set[tuple[str, str]] = set()
    if ENRICHMENT_BLOCKLIST_CSV.exists():
        with open(ENRICHMENT_BLOCKLIST_CSV, newline="") as f:
            for row in csv.DictReader(f):
                cid, orcid = row["cluster_id"].strip(), row["orcid"].strip()
                if cid and orcid:
                    blocklist.add((cid, orcid))
        if blocklist:
            print(f"  Enrichment blocklist: {len(blocklist)} blocked (cluster, orcid) pair(s)")

    inv = pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet")
    inv = inv[inv["role_code"].isin(KEEP_ROLES) & inv["grant_code"].str[:2].isin(KEEP_SCHEMES)]

    joined = (
        enrichment[["first_name", "family_name", "orcid", "confidence"]]
        .merge(inv[["unique_id", "first_name", "family_name"]], on=["first_name", "family_name"], how="inner")
        .merge(df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="inner")
    )

    by_cluster = (
        joined.groupby("cluster_id")
        .agg(distinct_orcids=("orcid", lambda s: sorted(s.dropna().unique())),
             confidence=("confidence", "first"))
        .reset_index()
    )
    promotable = by_cluster[by_cluster["distinct_orcids"].apply(len) == 1]

    out = persons.copy()
    n_promoted = 0
    for _, row in promotable.iterrows():
        cid, orcid, conf = row["cluster_id"], row["distinct_orcids"][0], row["confidence"]
        if (cid, orcid) in blocklist:
            continue
        mask = (out["cluster_id"] == cid) & (out["orcids"].apply(len) == 0)
        if not mask.any():
            continue
        out.loc[mask, "orcids"] = out.loc[mask, "orcids"].apply(
            lambda existing: sorted(set(list(existing) + [orcid]))
        )
        cluster_history.setdefault(cid, []).append({
            "event": "enriched_orcid", "orcid": orcid, "confidence": conf,
        })
        n_promoted += 1

    print(f"  Promoted {n_promoted} enriched ORCID(s) to clusters (high/au_match)")
    return out


def _merge_persons_by_orcid(persons: pd.DataFrame, cluster_history: dict) -> pd.DataFrame:
    """Merge clusters sharing the same ORCID after enrichment/manual additions.

    _merge_by_orcid handles ARC-data ORCIDs at the row level before aggregation.
    This handles cases where enrichment assigned the same ORCID to two already-
    aggregated clusters that never appeared together in the Splink input.
    Skips merges where first-name initials are incompatible (signals wrong enrichment hit).
    """
    orcid_map: dict[str, list[str]] = {}
    for _, row in persons[persons["orcids"].apply(len) > 0].iterrows():
        for orcid in row["orcids"]:
            orcid_map.setdefault(orcid, []).append(row["cluster_id"])

    conflicts = {o: sorted(cids) for o, cids in orcid_map.items() if len(cids) > 1}
    if not conflicts:
        return persons

    idx = persons.set_index("cluster_id")

    remapping: dict[str, str] = {}
    for orcid, cids in conflicts.items():
        canonical = min(cids)
        absorbed = [c for c in cids if c != canonical]
        can_initials = {x for x in idx.loc[canonical, "first_names"] if len(x) == 1}
        skip = False
        for cid in absorbed:
            abs_initials = {x for x in idx.loc[cid, "first_names"] if len(x) == 1}
            if can_initials and abs_initials and not (can_initials & abs_initials):
                print(f"  SKIP post-enrichment merge [{orcid}]: first-initial mismatch")
                print(f"    {canonical}: initials={sorted(can_initials)}")
                print(f"    {cid}:        initials={sorted(abs_initials)}")
                skip = True
                break
        if not skip:
            for cid in absorbed:
                remapping[cid] = canonical

    if not remapping:
        return persons

    out = persons.copy()
    out["cluster_id"] = out["cluster_id"].map(lambda c: remapping.get(c, c))

    vc = out["cluster_id"].value_counts()
    multi = set(vc[vc > 1].index)

    def _union(series_of_lists):
        result = set()
        for lst in series_of_lists:
            result.update(lst)
        return sorted(result)

    unchanged = out[~out["cluster_id"].isin(multi)].copy()

    merged_parts = []
    for canonical, grp in out[out["cluster_id"].isin(multi)].groupby("cluster_id"):
        best_idx = grp["n_grants"].idxmax()
        merged_parts.append({
            "cluster_id":   canonical,
            "full_names":   _union(grp["full_names"]),
            "first_names":  _union(grp["first_names"]),
            "family_names": _union(grp["family_names"]),
            "orcids":       _union(grp["orcids"]),
            "inst_arr":     _union(grp["inst_arr"]),
            "for_names":    _union(grp["for_names"]),
            "for_codes":    _union(grp["for_codes"]),
            "grant_ids":    [g for row in grp["grant_ids"] for g in row],
            "n_grants":     int(grp["n_grants"].sum()),
            "full_name_key": grp.loc[best_idx, "full_name_key"],
        })
        absorbed_here = [c for c, can in remapping.items() if can == canonical]
        cluster_history.setdefault(canonical, []).append({
            "event": "post_enrichment_merge",
            "merged_from": absorbed_here,
        })
        for cid in absorbed_here:
            cluster_history.pop(cid, None)

    print(f"  Post-enrichment merge: absorbed {len(remapping)} cluster(s)")
    return pd.concat([unchanged, pd.DataFrame(merged_parts)], ignore_index=True)


def _apply_manual_orcids(persons: pd.DataFrame, cluster_history: dict) -> pd.DataFrame:
    """Inject verified ORCIDs for clusters where ARC data has none (config/manual_orcids.csv)."""
    if not MANUAL_ORCIDS_CSV.exists():
        return persons
    overrides: dict[str, str] = {}
    with open(MANUAL_ORCIDS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            cid, orcid = row["cluster_id"].strip(), row["orcid"].strip()
            if cid and orcid:
                overrides[cid] = orcid
    if not overrides:
        return persons
    out = persons.copy()
    for cid, orcid in overrides.items():
        mask = out["cluster_id"] == cid
        if not mask.any():
            print(f"  WARNING manual_orcids: cluster {cid} not found")
            continue
        out.loc[mask, "orcids"] = out.loc[mask, "orcids"].apply(lambda _: [orcid])
        cluster_history.setdefault(cid, []).append({"event": "manual_orcid", "orcid": orcid})
        print(f"  Manual ORCID: {cid} → {orcid}")
    print(f"  Applied {len(overrides)} manual ORCID override(s)")
    return out


def _promote_low_by_for(
    persons: pd.DataFrame,
    df_cluster_ids: pd.DataFrame,
    cluster_history: dict,
) -> pd.DataFrame:
    """Promote low-confidence enrichment entries when FOR matching uniquely picks one AU candidate.

    For each no-ORCID cluster whose name appears in `low` enrichment rows, compares each
    AU candidate's ORCID-derived ERA FOR (from for_cache) against the cluster's ARC for_names.
    Promotes the single candidate with the highest non-zero token overlap.
    """
    enrichment_path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if not enrichment_path.exists():
        return persons

    enrichment = pd.read_parquet(enrichment_path)
    enrichment = enrichment[
        (enrichment["confidence"] == "low")
        & enrichment["au_candidates"].notna()
        & (enrichment["au_candidates"] != "[]")
    ]
    if len(enrichment) == 0:
        return persons

    inv = pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet")
    inv = inv[inv["role_code"].isin(KEEP_ROLES) & inv["grant_code"].str[:2].isin(KEEP_SCHEMES)]

    joined = (
        enrichment[["first_name", "family_name", "au_candidates"]]
        .merge(inv[["unique_id", "first_name", "family_name"]], on=["first_name", "family_name"], how="inner")
        .merge(df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="inner")
    )

    def _union_candidates(series):
        seen: set[str] = set()
        result = []
        for raw in series:
            for c in (json.loads(raw) if isinstance(raw, str) else raw):
                oid = c["orcid"]
                if oid and oid not in seen:
                    seen.add(oid)
                    result.append(c)
        return result

    by_cluster = (
        joined.groupby("cluster_id")
        .agg(candidates=("au_candidates", _union_candidates))
        .reset_index()
    )

    persons_no_orcid = set(persons.loc[persons["orcids"].apply(len) == 0, "cluster_id"])
    by_cluster = by_cluster[by_cluster["cluster_id"].isin(persons_no_orcid)]

    arc_for_toks = persons.set_index("cluster_id")["for_names"].apply(
        lambda names: {tok for name in (names or []) for tok in for_name_tokens(name)}
    )

    out = persons.copy()
    n_promoted = 0

    with diskcache.Cache(str(DISKCACHE_DIR / "orcid_for")) as for_cache:
        for _, row in by_cluster.iterrows():
            cid = row["cluster_id"]
            candidates = row["candidates"]
            if len(candidates) < 2:
                continue
            arc_toks = arc_for_toks.get(cid, set())
            if not arc_toks:
                continue

            scores = []
            for c in candidates:
                oid = c["orcid"]
                orcid_for = for_cache.get(oid, [])
                orcid_toks = {tok for e in orcid_for for tok in for_name_tokens(e["name"])}
                scores.append((oid, len(arc_toks & orcid_toks)))

            max_score = max(s for _, s in scores)
            if max_score == 0:
                continue
            winners = [oid for oid, s in scores if s == max_score]
            if len(winners) != 1:
                continue

            winner = winners[0]
            mask = (out["cluster_id"] == cid) & (out["orcids"].apply(len) == 0)
            if not mask.any():
                continue
            out.loc[mask, "orcids"] = out.loc[mask, "orcids"].apply(
                lambda existing: sorted(set(list(existing) + [winner]))
            )
            cluster_history.setdefault(cid, []).append({
                "event": "enriched_orcid", "orcid": winner,
                "confidence": "low_for_disambiguated", "for_score": max_score,
            })
            n_promoted += 1

    print(f"  Promoted {n_promoted} low-confidence ORCID(s) via ERA FOR matching")
    return out


def _enrich_orcid_for(persons: pd.DataFrame) -> pd.DataFrame:
    """Add orcid_for_codes column — ERA FOR codes derived from each cluster's ORCID works.

    Reads for_cache (populated by 00b_enrich_orcid.py). Merges codes across all ORCIDs
    in a cluster, summing counts. Clusters with no ORCID or no cached works get [].
    """
    with diskcache.Cache(str(DISKCACHE_DIR / "orcid_for")) as for_cache:
        def _get_for(orcids):
            merged: dict[str, dict] = {}
            for oid in (orcids or []):
                if not oid:
                    continue
                for entry in for_cache.get(oid, []):
                    code = entry["code"]
                    if code not in merged:
                        merged[code] = {"code": code, "name": entry["name"], "count": 0}
                    merged[code]["count"] += entry["count"]
            return sorted(merged.values(), key=lambda x: -x["count"])

        out = persons.copy()
        out["orcid_for_codes"] = out["orcids"].apply(_get_for)

    n_with_for = (out["orcid_for_codes"].apply(len) > 0).sum()
    print(f"  orcid_for_codes: {n_with_for} clusters with ≥1 ERA FOR code")
    return out


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    con = duckdb.connect()

    # ── Phase 1: Prepare ARC investigators ───────────────────────────────────

    print("=== Phase 1: ARC prep ===")
    con.create_function("arc_names", arc_name_arrays,
                        ['VARCHAR', 'VARCHAR'],
                        'STRUCT(first_names VARCHAR[], family_names VARCHAR[])')

    concordance_csv = Path(__file__).resolve().parents[1] / "config" / "for_concordance.csv"
    expanded_for_tokens = make_expanded_for_tokens(str(concordance_csv))
    con.create_function("for_tokens", expanded_for_tokens,
                        ['VARCHAR'],
                        'VARCHAR[]')

    _lu = ForTopicLookup()
    con.create_function("upgrade_for_code",
                        lambda code: upgrade_for_code(_lu, code),
                        ['VARCHAR'], 'VARCHAR',
                        null_handling='special')
    con.create_function("upgrade_for_name",
                        lambda code, name: upgrade_for_name(_lu, code, name),
                        ['VARCHAR', 'VARCHAR'], 'VARCHAR',
                        null_handling='special')

    out_arc = PROCESSED_DATA / "arc_investigators_prep.parquet"

    roles_sql   = ", ".join(f"'{r}'" for r in KEEP_ROLES)
    schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)

    print(f"  Saving to {out_arc}...")
    con.execute(f"""
        COPY (
            WITH arc_raw AS (
                SELECT
                    i.unique_id,
                    i.grant_code as grant_id,
                    i.first_name,
                    i.family_name,
                    g.admin_org as AdminOrg,
                    i.role_code as role,
                    i.orcid,
                    upgrade_for_name(
                        regexp_extract(s.primary_field_of_research, '^\\d{{4}}'),
                        g.primary_for_name
                    ) as for_name,
                    COALESCE(
                        upgrade_for_code(regexp_extract(s.primary_field_of_research, '^\\d{{4}}')),
                        regexp_extract(s.primary_field_of_research, '^\\d{{4}}')
                    ) as for_code
                FROM '{PROCESSED_DATA}/investigators_raw.parquet' i
                LEFT JOIN '{PROCESSED_DATA}/grants_flat.parquet' g
                    ON i.grant_code = g.grant_code
                LEFT JOIN read_csv_auto('{GRANT_SUMMARIES_CSV}') s
                    ON i.grant_code = s.grant_id
                WHERE i.role_code IN ({roles_sql})
                  AND substring(i.grant_code, 1, 2) IN ({schemes_sql})
            )
            SELECT
                a.unique_id,
                CONCAT_WS(' ', a.first_name, a.family_name) AS full_name,
                arc_names(a.first_name, a.family_name).first_names AS first_names,
                list_filter(arc_names(a.first_name, a.family_name).first_names, x -> len(x) = 1)  AS first_initials,
                list_filter(arc_names(a.first_name, a.family_name).first_names, x -> len(x) > 1)  AS first_name_full,
                arc_names(a.first_name, a.family_name).family_names AS family_names,
                a.orcid,
                o.institution_id as institution_oax_id,
                [a.for_name] as for_names,
                list_filter([a.for_code], x -> x != '') as for_codes,
                for_tokens(a.for_name) as for_name_tokens,
                'AU' as country_code
            FROM arc_raw a
            LEFT JOIN read_csv_auto('{ADMIN_ORGS_CSV}') o
                ON a.AdminOrg = o.organisationName_alias
        ) TO '{out_arc}' (FORMAT PARQUET)
    """)
    print("  ARC prep complete.")

    # ── Phase 2: Deduplicate ARC persons ─────────────────────────────────────

    print("\n=== Phase 2: ARC dedupe ===")
    arc_path = PROCESSED_DATA / "arc_investigators_prep.parquet"
    out_path  = PROCESSED_DATA / "arc_persons.parquet"

    print("[1/5] Loading full ARC grant rows...")
    df_raw = _load_all(con, arc_path)

    print("[2/5] Preparing Splink input columns...")
    df = _prep(df_raw)
    print(f"  first_initial coverage: "
          f"{df['first_initial'].notna().sum()} / {len(df)} rows")

    settings = SettingsCreator(
        unique_id_column_name="unique_id",
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("family_name_main", "first_initial"),
            "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        ],
        comparisons=[
            cl.CustomComparison(
                output_column_name="first_name_canonical",
                comparison_description="First name: exact / initial-match / full-mismatch",
                comparison_levels=[
                    {
                        "sql_condition": "first_name_canonical_l IS NULL OR first_name_canonical_r IS NULL",
                        "label_for_charts": "null",
                        "is_null_level": True,
                    },
                    {
                        "sql_condition": "first_name_canonical_l = first_name_canonical_r",
                        "label_for_charts": "Exact match",
                        "tf_adjustment_column": "first_name_canonical",
                        "tf_adjustment_weight": 1.0,
                    },
                    {
                        "sql_condition": (
                            "(length(first_name_canonical_l) = 1"
                            " AND length(first_name_canonical_r) > 1"
                            " AND first_name_canonical_l = substr(first_name_canonical_r, 1, 1))"
                            " OR"
                            " (length(first_name_canonical_r) = 1"
                            " AND length(first_name_canonical_l) > 1"
                            " AND first_name_canonical_r = substr(first_name_canonical_l, 1, 1))"
                        ),
                        "label_for_charts": "Initial matches full name",
                    },
                    {
                        "sql_condition": (
                            "length(first_name_canonical_l) > 1"
                            " AND length(first_name_canonical_r) > 1"
                            " AND first_name_canonical_l != first_name_canonical_r"
                        ),
                        "label_for_charts": "Full name mismatch",
                        "m_probability": 0.02,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other",
                    },
                ],
            ),
            cl.CustomComparison(
                output_column_name="family_name_main",
                comparison_levels=[
                    cll.NullLevel("family_name_main"),
                    cll.ExactMatchLevel("family_name_main").configure(
                        tf_adjustment_column="family_name_main",
                        tf_adjustment_weight=1.0,
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.CustomComparison(
                output_column_name="full_name_key",
                comparison_levels=[
                    cll.NullLevel("full_name_key"),
                    cll.ExactMatchLevel("full_name_key").configure(
                        tf_adjustment_column="full_name_key",
                        tf_adjustment_weight=1.0,
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.ExactMatch("orcid").configure(
                m_probabilities=[0.85, 0.15]
            ),
            cl.ArrayIntersectAtSizes("inst_arr", [1]),
            cl.ArrayIntersectAtSizes("for_name_tokens", [2, 1]).configure(
                m_probabilities=[0.35, 0.45, 0.20]
            ),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker(df, settings, db_api=db_api)

    print("  Registering OAX name frequency tables...")
    for fname, col in [
        ("oax_tf_family_name.parquet",  "family_name_main"),
        ("oax_tf_first_name.parquet",   "first_name_canonical"),
        ("oax_tf_full_name.parquet",    "full_name_key"),
    ]:
        tf = pd.read_parquet(PROCESSED_DATA / fname)
        linker.table_management.register_term_frequency_lookup(tf, col)

    print("[3/5] Training EM model...")
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("family_name_main")],
        recall=0.8,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        fix_u_probabilities=True,
    )

    print("[4/5] Predicting matches and clustering...")
    df_pred = linker.inference.predict(threshold_match_probability=0.5)
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_pred, threshold_match_probability=CLUSTER_THRESHOLD
    )
    df_cluster_ids = df_clusters.as_pandas_dataframe()

    cluster_history: dict[str, list] = {
        cid: [{"event": "splink_cluster"}]
        for cid in df_cluster_ids["cluster_id"].unique()
    }

    print("  Applying ORCID deterministic merge...")
    df_cluster_ids = _merge_by_orcid(df_cluster_ids, df_raw, cluster_history)
    print("  Applying ORCID conflict split...")
    df_cluster_ids = _split_orcid_conflicts(df_cluster_ids, df_raw, cluster_history)
    print("  Applying multi-name cluster split...")
    orcid_enrichment = None
    enrichment_path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if enrichment_path.exists():
        orcid_enrichment = pd.read_parquet(enrichment_path)
    df_cluster_ids = _split_multi_name_clusters(df_cluster_ids, df_raw, orcid_enrichment, cluster_history)
    print("  Applying manual splits (config/manual_splits.csv)...")
    df_cluster_ids = _apply_manual_splits(df_cluster_ids, df_raw, cluster_history)

    n_clusters = df_cluster_ids["cluster_id"].nunique()
    n_records  = len(df_cluster_ids)
    print(f"  {n_clusters} clusters from {n_records} records "
          f"({n_records/n_clusters:.2f} grants/person avg)")

    print("[5/5] Aggregating to person-level records...")
    df_raw["inst_arr"] = df_raw["institution_oax_id"].apply(
        lambda x: [x] if (pd.notna(x) and x) else []
    )
    df_raw["for_names"] = df_raw["for_names"].apply(
        lambda x: list(x) if x is not None else []
    )
    df_raw["for_codes"] = df_raw["for_codes"].apply(
        lambda x: list(x) if x is not None else []
    )
    persons = _aggregate_clusters(df_raw, df_cluster_ids)

    # Add full_name_key (mode per cluster) — needed by is_suspicious for rare-name check
    fnk = (
        df[["unique_id", "full_name_key"]]
        .merge(df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left")
    )
    fnk["cluster_id"] = fnk["cluster_id"].fillna(fnk["unique_id"])
    fnk_mode = (
        fnk.dropna(subset=["full_name_key"])
        .groupby("cluster_id")["full_name_key"]
        .agg(lambda s: s.mode().iloc[0] if len(s) else None)
        .reset_index()
    )
    persons = persons.merge(fnk_mode, on="cluster_id", how="left")

    div_map, adj = load_for_divisions()
    tf_df = pd.read_parquet(PROCESSED_DATA / "oax_tf_full_name.parquet")
    tf_lookup = dict(zip(tf_df["full_name_key"], tf_df["tf_full_name_key"]))

    print("  Promoting enriched ORCIDs (orcid_enrichment.parquet)...")
    persons = _apply_enriched_orcids(persons, df_cluster_ids, cluster_history)
    print("  Promoting low-confidence ORCIDs via ERA FOR matching...")
    persons = _promote_low_by_for(persons, df_cluster_ids, cluster_history)
    print("  Applying manual ORCID overrides (config/manual_orcids.csv)...")
    persons = _apply_manual_orcids(persons, cluster_history)
    print("  Merging clusters with shared post-enrichment ORCIDs...")
    persons = _merge_persons_by_orcid(persons, cluster_history)
    print("  Applying manual merges (config/manual_merges.csv)...")
    persons = _apply_manual_merges(persons, cluster_history)
    print("  Auto-merging same-name clusters on single-org grants...")
    persons = _merge_same_grant_coinvestigators(persons, cluster_history)

    # orcid_status: ORCID enrichment signal (used by 00b to target NO_ORCID RESOLVED clusters)
    persons["orcid_status"] = persons["orcids"].apply(
        lambda x: "HAS_ORCID" if len(x) == 1 else ("MULTI_ORCID" if len(x) > 1 else "NO_ORCID")
    )
    # resolution_status: is this cluster definitively one person?
    persons["resolution_status"] = persons.apply(
        lambda r: "UNRESOLVED" if is_suspicious(r, div_map, adj, tf_lookup) else "RESOLVED",
        axis=1,
    )
    persons.loc[persons["orcid_status"] == "MULTI_ORCID", "resolution_status"] = "UNRESOLVED"

    persons["cluster_history"] = persons["cluster_id"].map(
        lambda cid: json.dumps(cluster_history.get(cid, [{"event": "splink_cluster"}]))
    )

    print("  Enriching clusters with ERA FOR codes from ORCID works...")
    persons = _enrich_orcid_for(persons)

    print("  Computing Gap 1 candidates (same blocking key, no incompatibility)...")
    persons = _compute_gap_candidates(persons, div_map, adj)

    print("  Computing reliability tiers...")
    persons = _compute_reliability_tier(persons, tf_lookup)

    persons.to_parquet(out_path, index=False)
    print(f"  Saved {len(persons)} person records → {out_path}")

    map_path = PROCESSED_DATA / "arc_grant_cluster_map.parquet"
    gmap = (
        persons[["cluster_id", "grant_ids"]]
        .explode("grant_ids")
        .rename(columns={"grant_ids": "unique_id"})
        [["unique_id", "cluster_id"]]
    )
    gmap.to_parquet(map_path, index=False)
    print(f"  Saved grant→cluster map → {map_path}")

    inst_lookup = _load_inst_names()
    _diagnostic_report(persons, inst_lookup, div_map, adj, tf_lookup)
    _export_manual_splits_template(persons, inst_lookup, div_map, adj, tf_lookup)


if __name__ == "__main__":
    main()
