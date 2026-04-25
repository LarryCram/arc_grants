"""
06_assign_layer2_matches.py

Layer 2 match assignment: convert the candidate pairs table into resolved
matches and a Layer 3 residual.

ACCEPTANCE TIERS (precision over recall):

    Tier 1  exactly 1 candidate with first_score == 1.0
                Unique full given-name match; initial-match candidates ignored
                at this tier.

    Tier 2  0 candidates with first_score == 1.0,
            exactly 1 candidate with first_score == 0.8
                Unique initial match with no competing full match.

    Tier 3  multiple candidates at the best first_score level (> 0),
            but exactly one of those has institution_match == True
                Institution resolves the remaining ambiguity.

    All other persons → layer3_residual.parquet.

INPUT:
    PROCESSED_DATA/layer2_name_candidates.parquet
    PROCESSED_DATA/layer1_residual.parquet

OUTPUT:
    PROCESSED_DATA/layer2_matches.parquet
        arc_first_name, arc_family_name, arc_orcid, arc_sub_case,
        arc_admin_orgs, arc_oax_inst_ids, n_arc_grants,
        oax_id, oax_display_name, oax_orcid,
        first_score, institution_match, tier

    PROCESSED_DATA/layer3_residual.parquet
        Same schema as layer1_residual.parquet, sub_case:
            'ambiguous'  — candidates exist but could not be resolved
            'no_match'   — no OAX family-name match at all
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.utils.io import setup_stdout_utf8


def main():
    setup_stdout_utf8()

    df_cands = pd.read_parquet(PROCESSED_DATA / "layer2_name_candidates.parquet")
    df_res   = pd.read_parquet(PROCESSED_DATA / "layer1_residual.parquet")

    print(f"Candidate pairs:            {len(df_cands):>8,}")

    # ── Per-person signal counts ──────────────────────────────────────────────
    # Count full matches (1.0) and initial matches (0.8) separately per person.
    grp = df_cands.groupby(["arc_first_name", "arc_family_name"], dropna=False)

    per_person = grp.apply(lambda g: pd.Series({
        "n_full":    (g["first_score"] == 1.0).sum(),
        "n_initial": (g["first_score"] == 0.8).sum(),
        "n_inst_full":    ((g["first_score"] == 1.0) & g["institution_match"]).sum(),
        "n_inst_initial": ((g["first_score"] == 0.8) & g["institution_match"]).sum(),
    }), include_groups=False).reset_index()

    df_cands = df_cands.merge(per_person,
                              on=["arc_first_name", "arc_family_name"], how="left")

    resolved_names: set = set()

    # ── Tier 1: unique full-name match ────────────────────────────────────────
    tier1 = df_cands[
        (df_cands["first_score"] == 1.0) &
        (df_cands["n_full"] == 1)
    ].copy()
    tier1["tier"] = 1
    resolved_names.update(zip(tier1["arc_first_name"], tier1["arc_family_name"]))

    # ── Tier 2: unique initial match, no full match competing ─────────────────
    tier2 = df_cands[
        ~df_cands.apply(lambda r: (r["arc_first_name"], r["arc_family_name"]) in resolved_names, axis=1) &
        (df_cands["first_score"] == 0.8) &
        (df_cands["n_full"] == 0) &
        (df_cands["n_initial"] == 1)
    ].copy()
    tier2["tier"] = 2
    resolved_names.update(zip(tier2["arc_first_name"], tier2["arc_family_name"]))

    # ── Tier 3: institution resolves ambiguity at the best score level ─────────
    # Among unresolved persons with first_score > 0 candidates, keep the one
    # institution-confirmed candidate — but only when exactly 1 is confirmed
    # at the best available first_score level.
    t3_pool = df_cands[
        ~df_cands.apply(lambda r: (r["arc_first_name"], r["arc_family_name"]) in resolved_names, axis=1) &
        (df_cands["first_score"] > 0) &
        (df_cands["institution_match"] == True)
    ].copy()

    # For each person in the pool, determine the best first_score and count
    # institution-confirmed candidates at that level.
    def best_inst_candidate(g):
        best_fs = g["first_score"].max()
        best_rows = g[g["first_score"] == best_fs]
        inst_rows = best_rows[best_rows["institution_match"] == True]
        if len(inst_rows) == 1:
            return inst_rows.iloc[[0]]
        return pd.DataFrame()

    tier3_parts = []
    for name_key, g in t3_pool.groupby(["arc_first_name", "arc_family_name"], dropna=False):
        result = best_inst_candidate(g)
        if len(result):
            tier3_parts.append(result)

    if tier3_parts:
        tier3 = pd.concat(tier3_parts, ignore_index=True)
        tier3["tier"] = 3
        resolved_names.update(zip(tier3["arc_first_name"], tier3["arc_family_name"]))
    else:
        tier3 = pd.DataFrame(columns=df_cands.columns.tolist() + ["tier"])

    # ── Combine resolved matches ──────────────────────────────────────────────
    keep_cols = [
        "arc_first_name", "arc_family_name", "arc_orcid", "arc_sub_case",
        "arc_admin_orgs", "arc_oax_inst_ids", "n_arc_grants",
        "oax_id", "oax_display_name", "oax_orcid",
        "first_score", "institution_match", "tier",
    ]
    df_matches = pd.concat(
        [tier1[keep_cols], tier2[keep_cols], tier3[keep_cols]],
        ignore_index=True,
    )

    # ── Layer 3 residual ──────────────────────────────────────────────────────
    candidate_names = set(zip(df_cands["arc_first_name"], df_cands["arc_family_name"]))

    df_res["_name_key"] = list(zip(df_res["first_name"], df_res["family_name"]))

    def assign_sub_case(name_key):
        if name_key in resolved_names:
            return None
        if name_key in candidate_names:
            return "ambiguous"
        return "no_match"

    df_res["sub_case"] = df_res["_name_key"].apply(assign_sub_case)
    df_layer3 = (
        df_res[df_res["sub_case"].notna()]
        .drop(columns=["_name_key"])
        .reset_index(drop=True)
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    l3_amb   = df_layer3[df_layer3["sub_case"] == "ambiguous"].groupby(["first_name","family_name"], dropna=False).ngroups
    l3_none  = df_layer3[df_layer3["sub_case"] == "no_match"].groupby(["first_name","family_name"], dropna=False).ngroups

    print(f"\n{'─'*54}")
    print(f"LAYER 2 ASSIGNMENT SUMMARY")
    print(f"{'─'*54}")
    print(f"  Tier 1 — unique full-name match:     {len(tier1):>6,}")
    print(f"  Tier 2 — unique initial match:       {len(tier2):>6,}")
    print(f"  Tier 3 — inst-confirmed from short:  {len(tier3):>6,}")
    print(f"  Total Layer 2 resolved:              {len(df_matches):>6,}")
    print(f"\n  Layer 3 unique persons:              {l3_amb+l3_none:>6,}")
    print(f"    ambiguous (has candidates):        {l3_amb:>6,}")
    print(f"    no_match  (no family-name hit):    {l3_none:>6,}")

    # ── Save ──────────────────────────────────────────────────────────────────
    matches_path = PROCESSED_DATA / "layer2_matches.parquet"
    layer3_path  = PROCESSED_DATA / "layer3_residual.parquet"

    df_matches.to_parquet(matches_path, index=False)
    df_layer3.to_parquet(layer3_path,   index=False)

    print(f"\nSaved:")
    print(f"  {matches_path}  ({len(df_matches):,} rows)")
    print(f"  {layer3_path}  ({len(df_layer3):,} rows)")


if __name__ == "__main__":
    main()
