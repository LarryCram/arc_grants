"""
src/01a_diagnose.py

Quality diagnostics for awards_cif_arc_only.parquet — the output of 01_prepare_arc.py.

Tests three propositions:
  (A) No false positives  — no two distinct people merged into one cluster
  (B) No false negatives  — no one person split across two clusters
  (C) Every ARC person is resolved and covered

Run:
    .venv/bin/python src/01a_diagnose.py
"""

import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.utils.cluster_checks import is_suspicious_for2020
from src.utils.awards_cif import (
    load_award_cif_items,
    AwardCIFItem,
    _load_confirmed_not_suspicious,
    cluster_detail_data,
    cluster_detail_text,
    sample_4u_clusters,
)

PASS = "✓"
FAIL = "✗"
INFO = "·"

CO_INV_MIN_SHARED = 1       # B3: minimum shared co-investigators to report
SCHEMES_OF_INTEREST = ["FF", "FL", "FT", "DE", "DP", "LP"]


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


# ── structured, importable result (2026-08-24) ──────────────────────────────────
# Everything below carries the SAME hard-failure logic the console report above already runs --
# no new detection, just a structured result alongside the print()s, so callers other than this
# script's own main() (e.g. a Dossier build step) can ask "is this one cluster_id sound?" without
# re-scanning the whole population or re-parsing console output.

@dataclass
class HardCheckResult:
    """Population-wide hard-failure verdict -- the same checks main()'s console report sums
    into "Hard failures", but carrying WHICH cluster_ids are implicated per check, not just a
    total. Computed once per persons/gmap snapshot (run_diagnostics()); verify_cluster() does
    O(1) set lookups against this rather than re-scanning the population per call."""
    n_failures: int = 0
    known_cluster_ids: set[str] = field(default_factory=set)           # every cluster_id in this persons snapshot
    multi_orcid: set[str] = field(default_factory=set)                 # A1
    unresolved: set[str] = field(default_factory=set)                  # A2
    suspicious_for2020: set[str] = field(default_factory=set)          # A3
    within_grant_orcid_conflict: set[str] = field(default_factory=set) # A5 hard
    orcid_in_multiple_clusters: set[str] = field(default_factory=set)  # B1
    missing_unique_ids: set[str] = field(default_factory=set)          # C1 -- unique_ids, not cluster_ids


@dataclass(frozen=True)
class ClusterVerdict:
    cluster_id: str
    sound: bool
    reasons: list[str] = field(default_factory=list)  # e.g. ["A1_MULTI_ORCID", "B1_ORCID_IN_MULTIPLE_CLUSTERS"]


def verify_cluster(cluster_id: str, result: HardCheckResult) -> ClusterVerdict:
    """Per-cluster verification: is this cluster_id 'sound' per the same hard checks main()'s
    console report already runs? Not MULTI_ORCID, resolution_status==RESOLVED (i.e. not
    UNRESOLVED / not is_suspicious_for2020), not implicated in a within-grant ORCID conflict or
    a B1 cross-cluster ORCID collision, and actually present in this persons snapshot at all --
    absence itself is a real signal of drift (e.g. a cluster_id from a stale downstream file
    that a later 01_prepare_arc.py run renamed or split away in the current ARC-only checkpoint).
    This is the piece a Dossier build step calls per-person before rendering anything."""
    reasons = []
    if cluster_id not in result.known_cluster_ids:
        reasons.append("NOT_FOUND_IN_ARC_ONLY")
    if cluster_id in result.multi_orcid:
        reasons.append("A1_MULTI_ORCID")
    if cluster_id in result.unresolved:
        reasons.append("A2_UNRESOLVED")
    if cluster_id in result.suspicious_for2020:
        reasons.append("A3_SUSPICIOUS_FOR2020")
    if cluster_id in result.within_grant_orcid_conflict:
        reasons.append("A5_WITHIN_GRANT_ORCID_CONFLICT")
    if cluster_id in result.orcid_in_multiple_clusters:
        reasons.append("B1_ORCID_IN_MULTIPLE_CLUSTERS")
    return ClusterVerdict(cluster_id=cluster_id, sound=not reasons, reasons=reasons)


# ── load data ──────────────────────────────────────────────────────────────────

def _load() -> tuple:
    # AwardsCIF is the sole core (2026-08-21 consolidation) -- arc_persons.parquet and
    # arc_investigators_prep.parquet are retired; both of this function's former direct-parquet
    # reads of those two files are replaced by load_award_cif_items() and awards_cif_arc_only.
    # parquet respectively. Reads the ARC-only checkpoint deliberately, not the OAX-enriched
    # awards_cif.parquet -- every check in this file (A/B/C below) operates purely on ARC-
    # internal fields (orcid_status, family_names, for2020_codes, full_name_key, grant/co-
    # investigator structure), never oax_candidates, so this file can run immediately after
    # 01_prepare_arc.py without waiting on 03/03b, and can't be misled by a stale OAX
    # enrichment the way reading awards_cif.parquet could.
    persons = pd.read_parquet(PROCESSED_DATA / "awards_cif_arc_only.parquet")

    gmap = pd.read_parquet(PROCESSED_DATA / "arc_grant_cluster_map.parquet")

    # arc_investigators_prep.parquet's `first_initials` was an array (list_filter(...,
    # len(x)==1)); every real consumer below only ever took its first element as a scalar --
    # exactly what AwardCIFItem.first_initial already is, so no array wrapping is needed here.
    items, _corrections, _orcid_corrections = load_award_cif_items()
    prep = pd.DataFrame([
        {"unique_id": it.unique_id, "family_names": it.family_names, "first_initials": [it.first_initial] if it.first_initial else []}
        for it in items
    ])

    # inv_f: derived from load_award_cif_items()'s own returned items, NOT an independent
    # re-filter of investigators_raw.parquet. 2026-08-21 fix -- the old version reimplemented
    # role_code/grant_code scope filtering by hand here, which silently drifted from
    # load_award_cif_items()'s actual scope (that function ALSO drops non-HEP-admin_org records,
    # a filter this file's own hand-rolled version never applied) -- C1 was wrongly flagging 186
    # correctly-excluded CSIRO-administered (non-HEP) DECRA records as a coverage gap. Deriving
    # from the same items list load_award_cif_items() already returns makes drift structurally
    # impossible: there is exactly one place scope is decided, and every check in this file reads
    # from it.
    inv_f = pd.DataFrame([
        {"unique_id": it.unique_id, "grant_code": it.grant_code, "orcid": it.orcid}
        for it in items
    ])

    tf_df = pd.read_parquet(PROCESSED_DATA / "oax_tf_full_name.parquet")
    tf_lookup = dict(zip(tf_df["full_name_key"], tf_df["tf_full_name_key"]))

    grants = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")

    return persons, gmap, inv_f, prep, tf_lookup, items, grants


# ── A: false positives ─────────────────────────────────────────────────────────

def check_A(persons, gmap, inv_f, prep, tf_lookup, result: "HardCheckResult", verbose: bool = True) -> int:
    if verbose:
        _header("[A] FALSE POSITIVES — distinct people merged into one cluster")
    failures = 0

    # A1: MULTI_ORCID
    multi = persons[persons["orcid_status"] == "MULTI_ORCID"]
    result.multi_orcid = set(multi["cluster_id"])
    if verbose:
        _result("A1  MULTI_ORCID clusters", len(multi))
    if len(multi):
        failures += len(multi)
        if verbose:
            _show_clusters(multi)

    # A2: UNRESOLVED
    unres = persons[persons["resolution_status"] == "UNRESOLVED"]
    result.unresolved = set(unres["cluster_id"])
    if verbose:
        _result("A2  UNRESOLVED clusters", len(unres))
    if len(unres):
        failures += len(unres)
        if verbose:
            _show_clusters(unres)

    # A3: is_suspicious_for2020 across ALL clusters (must agree with A2) -- same
    # manual_confirmed_not_suspicious.csv override compute_reliability() applies, or a
    # human-reviewed cluster would show here as a phantom A2/A3 disagreement forever.
    confirmed_not_suspicious = _load_confirmed_not_suspicious()
    suspect = persons[persons.apply(
        lambda r: is_suspicious_for2020(r["full_name_key"], r["for2020_codes"], tf_lookup, r["n_grants"])
        and r["cluster_id"] not in confirmed_not_suspicious,
        axis=1,
    )]
    result.suspicious_for2020 = set(suspect["cluster_id"])
    if verbose:
        icon = FAIL if len(suspect) else PASS
        # MULTI_ORCID clusters are UNRESOLVED (A1) but not is_suspicious (has ORCIDs → bails)
        print(f"  {icon}  A3  is_suspicious_for2020 (all clusters, excl MULTI_ORCID): {len(suspect)}")
        if len(suspect):
            _show_clusters(suspect)
    failures += len(suspect)

    # A4: NO_ORCID clusters with divergent family names (exact mismatch) -- informational only
    if verbose:
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

    result.within_grant_orcid_conflict = {c["cluster"] for c in hard_fails}
    if verbose:
        _result("A5  within-grant same-cluster, conflicting ORCIDs (hard)", len(hard_fails))
        for c in hard_fails[:10]:
            print(f"       grant={c['grant']}  cluster={c['cluster']}  orcids={c['orcids']}")
    failures += len(hard_fails)

    if verbose:
        print(f"  {INFO}  A5  within-grant same-cluster, no ORCID (informational): {len(info_cases)}")
        for c in info_cases[:10]:
            print(f"       grant={c['grant']}  cluster={c['cluster']}  records={c['records']}")

    return failures


# ── B: false negatives ─────────────────────────────────────────────────────────

def check_B(persons, gmap, inv_f, prep, result: "HardCheckResult", verbose: bool = True) -> int:
    if verbose:
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
    # From the FULL orcid_rows/dup_orcids frames, not the head(10) console-preview slice below --
    # the structured result must carry every implicated cluster_id, not just the first 10 printed.
    result.orcid_in_multiple_clusters = set(
        orcid_rows[orcid_rows["orcid"].isin(dup_orcids["orcid"])]["cluster_id"]
    )
    if verbose:
        _result("B1  ORCID appearing in 2+ clusters", len(dup_orcids))
    if len(dup_orcids):
        failures += len(dup_orcids)
        if verbose:
            for _, r in dup_orcids.head(10).iterrows():
                clusters = orcid_rows[orcid_rows["orcid"] == r["orcid"]]["cluster_id"].tolist()
                print(f"       orcid={r['orcid']}  clusters={clusters}")

    # B2: same full_name_key in 2+ clusters -- informational only
    if verbose:
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

    # B3: same blocking key + co-investigator overlap -- informational only, and moderately
    # expensive (O(n^2) within each blocking-key group), so skip entirely when verbose=False
    # (e.g. a Dossier build step's per-cluster verify_cluster() call never needs this).
    if verbose:
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

def check_C(persons, gmap, inv_f, result: "HardCheckResult", verbose: bool = True) -> int:
    if verbose:
        _header("[C] COVERAGE — every ARC person clustered and resolved")
    failures = 0

    # C1: every unique_id in scope maps to a cluster
    inv_ids  = set(inv_f["unique_id"])
    gmap_ids = set(gmap["unique_id"])
    missing  = inv_ids - gmap_ids
    result.missing_unique_ids = set(missing)
    if verbose:
        _result("C1  investigators_raw unique_ids with no cluster", len(missing))
    if missing:
        failures += len(missing)
        if verbose:
            for uid in sorted(missing)[:10]:
                print(f"       {uid}")

    # C2: UNRESOLVED (mirrors A2 — stated here for coverage framing)
    unres = (persons["resolution_status"] == "UNRESOLVED").sum()
    if verbose:
        _result("C2  UNRESOLVED clusters", unres)
    failures += unres

    if verbose:
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


# ── run_diagnostics: the importable entry point ─────────────────────────────────

def run_diagnostics(
    persons=None, gmap=None, inv_f=None, prep=None, tf_lookup=None, items=None, grants=None,
    verbose: bool = True,
) -> HardCheckResult:
    """Runs the full A/B/C hard-failure battery once, returns a structured HardCheckResult.
    Loads via _load() if persons is None (real usage, e.g. `main()` or a Dossier build step);
    tests/other callers may pass already-loaded frames or hand-built fixtures instead."""
    if persons is None:
        persons, gmap, inv_f, prep, tf_lookup, items, grants = _load()
    result = HardCheckResult(known_cluster_ids=set(persons["cluster_id"]))
    fa = check_A(persons, gmap, inv_f, prep, tf_lookup, result, verbose=verbose)
    fb = check_B(persons, gmap, inv_f, prep, result, verbose=verbose)
    fc = check_C(persons, gmap, inv_f, result, verbose=verbose)
    result.n_failures = fa + fb + fc
    if verbose:
        _header("SUMMARY")
        print(f"  Hard failures: {result.n_failures}")
        print(f"  A (false positives): {fa}  |  B (false negatives): {fb}  |  C (coverage): {fc}")
        if result.n_failures == 0:
            print(f"  {PASS}  All hard checks passed")
        else:
            print(f"  {FAIL}  {result.n_failures} hard failure(s) — investigate before proceeding to 03")
    return result


def render_4u_sample_report(n: int = 30, seed: int = 42, persons=None, gmap=None, items=None, grants=None) -> str:
    """A representative, human-readable sample of the reliability_tier=='4u' population
    (NO_ORCID, single-grant, has >=1 gap_candidate -- compute_gap_candidates()'s permissive
    upper bound, not a confirmed-problem count). Purely descriptive: shows each sampled
    cluster's own detail plus its gap_candidates' own detail side by side, for a human (or
    Claude, reading it) to characterize real-under-merge-risk vs incidental-surname-overlap.
    Computes no verdict itself -- no new candidate-scoring/merge logic, deliberately out of
    scope (see CLAUDE.md's "no standalone merge() operator" note).

    Accepts already-loaded frames (persons/gmap/items/grants) to avoid a redundant _load() when
    a caller -- e.g. main() -- has already loaded them; loads fresh only if persons is None."""
    if persons is None:
        persons, gmap, inv_f, prep, tf_lookup, items, grants = _load()
    pool = persons[persons["reliability_tier"] == "4u"]
    sample_ids = sample_4u_clusters(pool, n=n, seed=seed)
    gap_lookup = dict(zip(persons["cluster_id"], persons["gap_candidates"]))
    lines = [
        "# 4u sample review — under-merge risk sizing", "",
        f"Population: {len(pool)} clusters at reliability_tier=='4u'. "
        f"Sample: {len(sample_ids)} (seed={seed}). Each entry below shows the sampled cluster's "
        f"own grants, then its gap_candidates' own grants nested underneath, for side-by-side "
        f"comparison. Informational only -- sizing a future decision, not deciding or fixing anything.",
        "",
    ]
    for cid in sample_ids:
        d = cluster_detail_data(cid, gmap, items, grants, gap_candidate_ids=list(gap_lookup.get(cid, [])))
        lines.append(cluster_detail_text(d))
        lines.append("")
    return "\n".join(lines)


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    persons, gmap, inv_f, prep, tf_lookup, items, grants = _load()

    if len(sys.argv) > 1 and sys.argv[1] == "--detail":
        for cid in sys.argv[2:]:
            print(cluster_detail_text(cluster_detail_data(cid, gmap, items, grants)))
            print()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--json":
        import json
        data = [cluster_detail_data(cid, gmap, items, grants) for cid in sys.argv[2:]]
        print(json.dumps(data, indent=2))
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--sample-4u":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        seed = int(sys.argv[3]) if len(sys.argv) > 3 else 42
        print(render_4u_sample_report(n=n, seed=seed, persons=persons, gmap=gmap, items=items, grants=grants))
        return

    print("=== 01a: arc_persons quality diagnostics ===")
    print(f"    Loading from {PROCESSED_DATA}")
    print(f"    {len(persons)} clusters  |  {len(gmap)} grant→cluster mappings")

    result = run_diagnostics(persons, gmap, inv_f, prep, tf_lookup, items, grants)
    sys.exit(1 if result.n_failures else 0)


if __name__ == "__main__":
    main()
