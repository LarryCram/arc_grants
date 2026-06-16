"""
src/01a_diagnose.py

Quality diagnostics for arc_persons.parquet — the output of 01_prepare_arc.py.

Tests three propositions:
  (A) No false positives  — no two distinct people merged into one cluster
  (B) No false negatives  — no one person split across two clusters
  (C) Every ARC person is resolved and covered

Run:
    .venv/bin/python src/01a_diagnose.py
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.cluster_checks import load_for_divisions, is_suspicious

PASS = "✓"
FAIL = "✗"
INFO = "·"

CO_INV_MIN_SHARED = 1       # B3: minimum shared co-investigators to report
SCHEMES_OF_INTEREST = ["FF", "FL", "FT", "DE", "DP", "LP", "DI"]


def _header(label: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print(f"{'─' * 60}")


def _result(label: str, value: int, expect_zero: bool = True) -> None:
    icon = (PASS if value == 0 else FAIL) if expect_zero else INFO
    print(f"  {icon}  {label}: {value}")


def _show_clusters(df: pd.DataFrame, n: int = 10) -> None:
    for _, r in df.head(n).iterrows():
        names  = "; ".join(r["full_names"][:4])
        orcids = ", ".join(r["orcids"]) if list(r["orcids"]) else "—"
        print(f"       {r['cluster_id']}  n={r['n_grants']}  [{orcids}]  {names}")


# ── load data ──────────────────────────────────────────────────────────────────

def _load() -> tuple:
    persons = pd.read_parquet(PROCESSED_DATA / "arc_persons.parquet")

    gmap = pd.read_parquet(PROCESSED_DATA / "arc_grant_cluster_map.parquet")

    inv = pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet")
    inv_f = inv[
        inv["role_code"].isin(KEEP_ROLES)
        & inv["grant_code"].str[:2].isin(KEEP_SCHEMES)
    ][["unique_id", "grant_code", "orcid"]].copy()

    prep = pd.read_parquet(PROCESSED_DATA / "arc_investigators_prep.parquet")[
        ["unique_id", "family_names", "first_initials"]
    ].copy()

    tf_df = pd.read_parquet(PROCESSED_DATA / "oax_tf_full_name.parquet")
    tf_lookup = dict(zip(tf_df["full_name_key"], tf_df["tf_full_name_key"]))

    div_map, adj = load_for_divisions()

    return persons, gmap, inv_f, prep, tf_lookup, div_map, adj


# ── A: false positives ─────────────────────────────────────────────────────────

def check_A(persons, gmap, inv_f, prep, tf_lookup, div_map, adj) -> int:
    _header("[A] FALSE POSITIVES — distinct people merged into one cluster")
    failures = 0

    # A1: MULTI_ORCID
    multi = persons[persons["orcid_status"] == "MULTI_ORCID"]
    _result("A1  MULTI_ORCID clusters", len(multi))
    if len(multi):
        failures += len(multi)
        _show_clusters(multi)

    # A2: UNRESOLVED
    unres = persons[persons["resolution_status"] == "UNRESOLVED"]
    _result("A2  UNRESOLVED clusters", len(unres))
    if len(unres):
        failures += len(unres)
        _show_clusters(unres)

    # A3: is_suspicious across ALL clusters (must agree with A2)
    suspect = persons[persons.apply(
        lambda r: is_suspicious(r, div_map, adj, tf_lookup), axis=1
    )]
    icon = FAIL if len(suspect) else PASS
    # MULTI_ORCID clusters are UNRESOLVED (A1) but not is_suspicious (has ORCIDs → bails)
    print(f"  {icon}  A3  is_suspicious (all clusters, excl MULTI_ORCID): {len(suspect)}")
    if len(suspect):
        _show_clusters(suspect)
    failures += len(suspect)

    # A4: NO_ORCID clusters with divergent family names (exact mismatch)
    no_orc = persons[persons["orcid_status"] == "NO_ORCID"].copy()
    no_orc["n_family_forms"] = no_orc["family_names"].apply(lambda x: len(set(x)))
    div_fam = no_orc[no_orc["n_family_forms"] > 1].sort_values("n_family_forms", ascending=False)
    _result("A4  NO_ORCID clusters with divergent family names", len(div_fam), expect_zero=False)
    for _, r in div_fam.head(10).iterrows():
        fams = sorted(set(r["family_names"]))
        print(f"       {r['cluster_id']}  n={r['n_grants']}  family_names={fams}")

    # A5: within-grant same-cluster pairs
    inv_clustered = inv_f.merge(gmap, on="unique_id", how="inner")
    # group by (grant_code, cluster_id) — find grants with 2+ rows in same cluster
    by_gc = inv_clustered.groupby(["grant_code", "cluster_id"])
    hard_fails = []
    info_cases = []
    for (grant, cluster), grp in by_gc:
        if len(grp) <= 1:
            continue
        orcids = grp["orcid"].dropna().unique()
        if len(orcids) > 1:
            hard_fails.append({"grant": grant, "cluster": cluster, "orcids": list(orcids)})
        elif len(orcids) == 0:
            info_cases.append({"grant": grant, "cluster": cluster, "records": list(grp["unique_id"])})
        # same single ORCID → expected name-change variant, skip

    _result("A5  within-grant same-cluster, conflicting ORCIDs (hard)", len(hard_fails))
    for c in hard_fails[:10]:
        print(f"       grant={c['grant']}  cluster={c['cluster']}  orcids={c['orcids']}")
    failures += len(hard_fails)

    print(f"  {INFO}  A5  within-grant same-cluster, no ORCID (informational): {len(info_cases)}")
    for c in info_cases[:10]:
        print(f"       grant={c['grant']}  cluster={c['cluster']}  records={c['records']}")

    return failures


# ── B: false negatives ─────────────────────────────────────────────────────────

def check_B(persons, gmap, inv_f, prep) -> int:
    _header("[B] FALSE NEGATIVES — one person split across two clusters")
    failures = 0

    # B1: same ORCID in 2+ clusters
    orcid_rows = (
        persons[persons["orcids"].apply(len) > 0][["cluster_id", "orcids"]]
        .explode("orcids")
        .rename(columns={"orcids": "orcid"})
    )
    dup_orcids = (
        orcid_rows.groupby("orcid")["cluster_id"]
        .nunique()
        .reset_index()
        .query("cluster_id > 1")
    )
    _result("B1  ORCID appearing in 2+ clusters", len(dup_orcids))
    if len(dup_orcids):
        failures += len(dup_orcids)
        for _, r in dup_orcids.head(10).iterrows():
            clusters = orcid_rows[orcid_rows["orcid"] == r["orcid"]]["cluster_id"].tolist()
            print(f"       orcid={r['orcid']}  clusters={clusters}")

    # B2: same full_name_key in 2+ clusters
    fnk = persons.dropna(subset=["full_name_key"])
    dup_fnk = (
        fnk.groupby("full_name_key")["cluster_id"]
        .nunique()
        .reset_index()
        .query("cluster_id > 1")
        .sort_values("cluster_id", ascending=False)
    )
    _result("B2  full_name_key in 2+ clusters (informational)", len(dup_fnk), expect_zero=False)
    if len(dup_fnk):
        for _, r in dup_fnk.head(10).iterrows():
            clusters = fnk[fnk["full_name_key"] == r["full_name_key"]]["cluster_id"].tolist()
            print(f"       fnk={r['full_name_key']}  clusters={clusters}")

    # B3: same blocking key + co-investigator overlap
    # Derive blocking key per unique_id from prep
    prep2 = prep.copy()
    prep2["family_name_main"] = prep2["family_names"].apply(
        lambda x: x[0] if len(x) > 0 else None
    )
    prep2["first_initial"] = prep2["first_initials"].apply(
        lambda x: x[0] if len(x) > 0 else None
    )
    prep2 = prep2[["unique_id", "family_name_main", "first_initial"]].dropna()

    # Per cluster: take mode of (family_name_main, first_initial)
    cluster_keys = (
        prep2.merge(gmap, on="unique_id", how="inner")
        .groupby("cluster_id")
        .agg(
            family_name_main=("family_name_main", lambda s: s.mode().iloc[0] if len(s) else None),
            first_initial=("first_initial", lambda s: s.mode().iloc[0] if len(s) else None),
        )
        .reset_index()
        .dropna(subset=["family_name_main", "first_initial"])
    )

    # Build co-investigator sets per cluster
    inv_clustered = inv_f.merge(gmap, on="unique_id", how="inner")
    grant_cluster_sets = (
        inv_clustered.groupby("grant_code")["cluster_id"]
        .apply(set)
        .to_dict()
    )
    co_inv: dict[str, set] = {cid: set() for cid in persons["cluster_id"]}
    for cls_set in grant_cluster_sets.values():
        for cid in cls_set:
            if cid in co_inv:
                co_inv[cid] |= cls_set - {cid}

    # Find pairs sharing blocking key
    b3_hits = []
    for (fam, init), grp in cluster_keys.groupby(["family_name_main", "first_initial"]):
        if len(grp) < 2:
            continue
        clusters = list(grp["cluster_id"])
        for i, c1 in enumerate(clusters):
            for c2 in clusters[i + 1:]:
                s1, s2 = co_inv.get(c1, set()), co_inv.get(c2, set())
                shared = s1 & s2
                if len(shared) >= CO_INV_MIN_SHARED:
                    union = s1 | s2
                    jaccard = len(shared) / len(union) if union else 0.0
                    b3_hits.append({
                        "key": f"{fam} {init}",
                        "c1": c1, "c2": c2,
                        "shared": len(shared), "jaccard": round(jaccard, 3),
                    })

    b3_hits.sort(key=lambda x: -x["jaccard"])
    print(f"  {INFO}  B3  same blocking key + shared co-investigators (informational): {len(b3_hits)}")
    for h in b3_hits[:10]:
        print(f"       key={h['key']}  jaccard={h['jaccard']}  shared={h['shared']}")
        print(f"           {h['c1']}")
        print(f"           {h['c2']}")

    return failures


# ── C: coverage ────────────────────────────────────────────────────────────────

def check_C(persons, gmap, inv_f) -> int:
    _header("[C] COVERAGE — every ARC person clustered and resolved")
    failures = 0

    # C1: every unique_id in scope maps to a cluster
    inv_ids  = set(inv_f["unique_id"])
    gmap_ids = set(gmap["unique_id"])
    missing  = inv_ids - gmap_ids
    _result("C1  investigators_raw unique_ids with no cluster", len(missing))
    if missing:
        failures += len(missing)
        for uid in sorted(missing)[:10]:
            print(f"       {uid}")

    # C2: UNRESOLVED (mirrors A2 — stated here for coverage framing)
    unres = (persons["resolution_status"] == "UNRESOLVED").sum()
    _result("C2  UNRESOLVED clusters", unres)
    failures += unres

    # C3: per-scheme distinct person counts
    exploded = (
        persons[["cluster_id", "grant_ids"]]
        .explode("grant_ids")
        .assign(scheme=lambda df: df["grant_ids"].str[:2])
    )
    scheme_counts = (
        exploded[exploded["scheme"].isin(SCHEMES_OF_INTEREST)]
        .groupby("scheme")["cluster_id"]
        .nunique()
        .reindex(SCHEMES_OF_INTEREST, fill_value=0)
    )
    print(f"\n  {INFO}  C3  Distinct persons per scheme:")
    for scheme, count in scheme_counts.items():
        print(f"       {scheme}: {count}")

    # C4: orcid_status distribution
    dist = persons["orcid_status"].value_counts()
    print(f"\n  {INFO}  C4  orcid_status distribution:")
    for status, count in dist.items():
        pct = 100 * count / len(persons)
        print(f"       {status}: {count}  ({pct:.1f}%)")

    return failures


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=== 01a: arc_persons quality diagnostics ===")
    print(f"    Loading from {PROCESSED_DATA}")

    persons, gmap, inv_f, prep, tf_lookup, div_map, adj = _load()
    print(f"    {len(persons)} clusters  |  {len(gmap)} grant→cluster mappings")

    fa = check_A(persons, gmap, inv_f, prep, tf_lookup, div_map, adj)
    fb = check_B(persons, gmap, inv_f, prep)
    fc = check_C(persons, gmap, inv_f)

    total = fa + fb + fc
    _header("SUMMARY")
    print(f"  Hard failures: {total}")
    print(f"  A (false positives): {fa}  |  B (false negatives): {fb}  |  C (coverage): {fc}")
    if total == 0:
        print(f"  {PASS}  All hard checks passed")
    else:
        print(f"  {FAIL}  {total} hard failure(s) — investigate before proceeding to 03")


if __name__ == "__main__":
    main()
