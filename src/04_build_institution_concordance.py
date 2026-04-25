"""
04_build_institution_concordance.py

Build the ARC admin_org → OAX institution_id concordance used in Layer 2
name matching as a confirmation signal.

INPUT:
    DATA_ROOT/admin_orgs.csv          -- read-only seed file (do not modify)
        organisationName_alias  raw ARC org name (appears in ARC data)
        organisationName        canonical form
        HEP                     'y' if Higher Education Provider
        state                   state code
        institution_arc         ARC canonical name for mapped institutions
        institution_name        OAX display name
        institution_id          OAX institution ID (https://openalex.org/IXXXXXXX)

    PROCESSED_DATA/grants.parquet     -- to validate coverage

OUTPUT:
    PROCESSED_DATA/institution_concordance.parquet
        arc_admin_org     ARC admin_org string as it appears in grants.parquet
        institution_id    OAX institution ID (null if unmapped)
        institution_name  OAX display name (null if unmapped)

MAPPING LOGIC:
    Direct rows  — institution_arc is set → arc_admin_org = institution_arc
    Alias rows   — institution_id is null → resolve via organisationName
                   to a direct row, carrying through institution_id
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import ADMIN_ORGS_CSV, PROCESSED_DATA
from src.utils.io import setup_stdout_utf8


def main():
    setup_stdout_utf8()

    # ── Load seed file (read-only) ────────────────────────────────────────────
    df_seed = pd.read_csv(ADMIN_ORGS_CSV, dtype=str).fillna("")
    print(f"Seed rows loaded:           {len(df_seed):>6,}")

    # ── Direct mappings: institution_arc → institution_id ────────────────────
    direct = (
        df_seed[df_seed["institution_id"] != ""]
        [["institution_arc", "institution_id", "institution_name"]]
        .drop_duplicates("institution_arc")
        .rename(columns={"institution_arc": "arc_admin_org"})
    )
    print(f"Direct mappings:            {len(direct):>6,}")

    # ── Alias mappings: organisationName_alias → organisationName → id ───────
    # Aliases are rows where institution_id is blank.
    # Resolve by joining organisationName → direct.arc_admin_org.
    aliases = df_seed[df_seed["institution_id"] == ""][
        ["organisationName_alias", "organisationName"]
    ].rename(columns={"organisationName_alias": "arc_admin_org"})

    aliases = aliases.merge(
        direct.rename(columns={"arc_admin_org": "organisationName"}),
        on="organisationName",
        how="left",
    ).drop(columns=["organisationName"])

    n_alias_resolved   = aliases["institution_id"].notna().sum()
    n_alias_unresolved = aliases["institution_id"].isna().sum()
    print(f"Aliases resolved:           {n_alias_resolved:>6,}")
    print(f"Aliases unresolved:         {n_alias_unresolved:>6,}")

    if n_alias_unresolved > 0:
        print("\n  Unresolved aliases (no OAX institution_id):")
        for _, row in aliases[aliases["institution_id"].isna()].iterrows():
            print(f"    {row['arc_admin_org']}")

    # ── Combine — one row per (arc_admin_org, institution_id) pair ───────────
    # An ARC admin org may map to multiple OAX institution IDs (e.g. a university
    # with a separately-indexed campus).
    concordance = (
        pd.concat([direct, aliases], ignore_index=True)
        .drop_duplicates(["arc_admin_org", "institution_id"])
        .sort_values(["arc_admin_org", "institution_id"])
        .reset_index(drop=True)
    )

    # ── Supplementary mappings not yet in seed file ──────────────────────────
    # Added here rather than editing the read-only seed.
    # One row per (arc_admin_org, institution_id) — multiple rows per ARC org
    # where OAX has split the institution into sub-entities.
    supplementary = pd.DataFrame([
        {
            # OAX stores the main UNSW entity as "UNSW Sydney"
            "arc_admin_org":    "The University of New South Wales",
            "institution_id":   "https://openalex.org/I31746571",
            "institution_name": "UNSW Sydney",
        },
        {
            # OAX also has "UNSW Canberra" (formerly ADFA) as a separate entity;
            # ARC records both under the same admin org.
            "arc_admin_org":    "The University of New South Wales",
            "institution_id":   "https://openalex.org/I4394709116",
            "institution_name": "UNSW Canberra",
        },
        {
            # OAX: "University of Newcastle Australia" (AU) — distinct from
            # Newcastle University (GB, I84884186)
            "arc_admin_org":    "The University of Newcastle",
            "institution_id":   "https://openalex.org/I78757542",
            "institution_name": "University of Newcastle Australia",
        },
    ])

    concordance = (
        pd.concat([concordance, supplementary], ignore_index=True)
        .drop_duplicates(["arc_admin_org", "institution_id"])
        .sort_values(["arc_admin_org", "institution_id"])
        .reset_index(drop=True)
    )
    print(f"Supplementary rows added:   {len(supplementary):>6,}")

    # ── Validate against grants.parquet ──────────────────────────────────────
    df_grants = pd.read_parquet(PROCESSED_DATA / "grants.parquet",
                                columns=["grant_code", "admin_org"])
    df_grants = df_grants[df_grants["admin_org"] != ""]

    grant_orgs = df_grants["admin_org"].value_counts().rename("n_grants").reset_index()
    grant_orgs.columns = ["arc_admin_org", "n_grants"]

    # Collapse to one row per arc_admin_org for coverage counting
    # (an org mapped to 2 OAX IDs should count as 1 mapped org, not 2)
    concordance_orgs = (
        concordance[concordance["institution_id"].notna()]
        .groupby("arc_admin_org")["institution_id"]
        .count()
        .reset_index()
        .rename(columns={"institution_id": "n_oax_ids"})
    )
    merged = grant_orgs.merge(concordance_orgs, on="arc_admin_org", how="left")
    n_orgs_total    = len(merged)
    n_orgs_mapped   = merged["n_oax_ids"].notna().sum()
    n_orgs_unmapped = merged["n_oax_ids"].isna().sum()
    grants_mapped   = merged.loc[merged["n_oax_ids"].notna(), "n_grants"].sum()
    grants_total    = merged["n_grants"].sum()

    print(f"\n── Validation against grants.parquet ───────────────────")
    print(f"  Unique admin_orgs in grants:   {n_orgs_total:>5,}")
    print(f"  Mapped to OAX institution:     {n_orgs_mapped:>5,}  ({100*n_orgs_mapped/n_orgs_total:.1f}%)")
    print(f"  Unmapped:                      {n_orgs_unmapped:>5,}  ({100*n_orgs_unmapped/n_orgs_total:.1f}%)")
    print(f"  Grants covered by mapping:     {grants_mapped:>5,}  ({100*grants_mapped/grants_total:.1f}%)")
    print(f"  Grants uncovered:              {grants_total - grants_mapped:>5,}  ({100*(grants_total-grants_mapped)/grants_total:.1f}%)")

    if n_orgs_unmapped > 0:
        print(f"\n  Unmapped admin_orgs (with grant counts):")
        unmapped = merged[merged["n_oax_ids"].isna()].sort_values("n_grants", ascending=False)
        for _, row in unmapped.iterrows():
            print(f"    {row['n_grants']:>4}  {row['arc_admin_org']}")

    # ── Save ──────────────────────────────────────────────────────────────────
    out_path = PROCESSED_DATA / "institution_concordance.parquet"
    concordance.to_parquet(out_path, index=False)
    print(f"\nSaved: {out_path}  ({len(concordance):,} rows)")


if __name__ == "__main__":
    main()
