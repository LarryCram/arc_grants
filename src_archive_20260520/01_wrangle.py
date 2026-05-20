"""
01_wrangle.py

Join grant_summaries enrichment onto grants_flat, then filter to in-scope
schemes and role codes as defined in config/scope.py.

INPUT:
    PROCESSED_DATA/grants_flat.parquet
    PROCESSED_DATA/investigators_raw.parquet
    PROCESSED_DATA/for_codes.parquet
    DATA_ROOT/raw/grant_summaries.csv

OUTPUT:
    PROCESSED_DATA/grants.parquet        -- enriched + filtered grants
    PROCESSED_DATA/investigators.parquet -- in-scope role codes, in-scope grants only
    PROCESSED_DATA/for_codes.parquet     -- filtered to in-scope grants
"""

import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, GRANT_SUMMARIES_CSV
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.io import setup_stdout_utf8

# Columns to pull from grant_summaries and their target names
SUMMARY_COLS = {
    "grant_id":                         "grant_code",
    "program":                          "program",
    "submissionYear":                   "submission_year",
    "roundNumber":                      "round_number",
    "schemeRound":                      "scheme_round",
    "lead_investigator":                "lead_investigator",
    "anticipated_end_date":             "anticipated_end_date",
    "national_interest_test_statement": "nit_statement",
}


def main():
    setup_stdout_utf8()

    # ── Load Phase 0 outputs ─────────────────────────────────────────────────
    df_grants = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")
    df_inv    = pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet")
    df_for    = pd.read_parquet(PROCESSED_DATA / "for_codes.parquet")

    print("Phase 0 inputs:")
    print(f"  grants_flat:         {len(df_grants):>8,}")
    print(f"  investigators_raw:   {len(df_inv):>8,}")
    print(f"  for_codes:           {len(df_for):>8,}")

    # ── Join grant_summaries enrichment ──────────────────────────────────────
    summaries = pd.read_csv(
        GRANT_SUMMARIES_CSV,
        usecols=list(SUMMARY_COLS.keys()),
        dtype=str,
    ).rename(columns=SUMMARY_COLS)
    df_grants = df_grants.merge(summaries, on="grant_code", how="left")

    # ── Derive scheme_code from grant_code prefix ────────────────────────────
    df_grants["scheme_code"] = df_grants["grant_code"].str[:2].str.strip()

    # ── Filter to in-scope schemes ───────────────────────────────────────────
    n_before = len(df_grants)
    df_grants = df_grants[df_grants["scheme_code"].isin(KEEP_SCHEMES)].copy()
    print(f"\nAfter scheme filter: {len(df_grants):>7,}  (dropped {n_before - len(df_grants):,})")
    print(f"  Scheme breakdown:")
    for scheme, cnt in df_grants["scheme_code"].value_counts().items():
        print(f"    {scheme}  {cnt:>6,}")

    # ── Filter investigators: in-scope role + in-scope grant ─────────────────
    in_scope_grants = set(df_grants["grant_code"])

    n_before = len(df_inv)
    df_inv = df_inv[
        df_inv["role_code"].isin(KEEP_ROLES) &
        df_inv["grant_code"].isin(in_scope_grants)
    ].copy()
    print(f"\nAfter investigator filter: {len(df_inv):>6,}  (dropped {n_before - len(df_inv):,})")
    print(f"  Role breakdown:")
    for role, cnt in df_inv["role_code"].value_counts().items():
        print(f"    {role:<12} {cnt:>6,}")

    # ── Filter FOR codes to in-scope grants ──────────────────────────────────
    n_before = len(df_for)
    df_for = df_for[df_for["grant_code"].isin(in_scope_grants)]
    print(f"\nAfter FOR filter:  {len(df_for):>8,}  (dropped {n_before - len(df_for):,})")

    # ── Save ─────────────────────────────────────────────────────────────────
    grants_path = PROCESSED_DATA / "grants.parquet"
    inv_path    = PROCESSED_DATA / "investigators.parquet"
    for_path    = PROCESSED_DATA / "for_codes_wrangled.parquet"

    df_grants.to_parquet(grants_path, index=False)
    df_inv.to_parquet(inv_path,    index=False)
    df_for.to_parquet(for_path,    index=False)

    print(f"\nSaved:")
    print(f"  {grants_path}")
    print(f"  {inv_path}")
    print(f"  {for_path}")


if __name__ == "__main__":
    main()
