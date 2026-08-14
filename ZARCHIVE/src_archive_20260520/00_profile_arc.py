"""
00_profile_arc.py

PURPOSE:
    Parse and profile the raw ARC grants CSV.
    Each row contains a JSON blob in the 'single_grant' column.
    This script extracts, flattens, and profiles the data without
    modifying the source file.

INPUT:
    DATA_ROOT/raw/raw_json.csv

OUTPUT:
    DATA_ROOT/processed/grants_flat.parquet      -- Flattened grant records
    DATA_ROOT/processed/investigators_raw.parquet -- Extracted investigator records
    DATA_ROOT/processed/for_codes.parquet        -- Field of research assignments
    OUTPUT_ROOT/profiles/grant_profile.txt       -- Human readable summary

DECISIONS ENCODED HERE:
    - investigators-at-announcement used as primary (investigators-current often empty)
    - ORCIDs trimmed of whitespace on extraction
    - FOR type retained to distinguish FOR08 vs FOR20 (pre/post 2018)
    - Partner Investigators (PI) retained but flagged separately
    - Both administering-organisation and announcement-administering-organisation retained
"""

import json
import sys
import pandas as pd
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import ARC_GRANTS_CSV, PROFILES_OUT, PROCESSED_DATA
from src.utils.paths import ensure_dirs
from src.utils.io import setup_stdout_utf8


def safe_str(val) -> str:
    # handles explicit JSON null (None) as well as missing keys
    return (val or "").strip()


# ── Parsing ──────────────────────────────────────────────────────────────────

def parse_row(row_json: str, row_index: int) -> dict | None:
    """
    Parse a single JSON blob from the single_grant column.
    Returns None and logs if parsing fails.
    """
    try:
        obj = json.loads(row_json)
        return obj.get("data", {}).get("attributes", {})
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"  WARNING: Row {row_index} failed to parse: {e}")
        return None


def extract_investigators(attrs: dict, grant_code: str) -> list[dict]:
    """
    Extract investigators from a grant's attributes dict.
    Prefers investigators-at-announcement; falls back to investigators-current.
    """
    investigators = attrs.get("investigators-at-announcement", [])
    source = "announcement"
    if not investigators:
        investigators = attrs.get("investigators-current", [])
        source = "current"

    records = []
    for inv in investigators:
        orcid_raw = inv.get("orcidIdentifier") or ""
        orcid_clean = orcid_raw.strip() or None

        records.append({
            "grant_code":    grant_code,
            "title":         safe_str(inv.get("title")),
            "first_name":    safe_str(inv.get("firstName")),
            "family_name":   safe_str(inv.get("familyName")),
            "role_code":     safe_str(inv.get("roleCode")),
            "role_name":     safe_str(inv.get("roleName")),
            "is_fellowship": inv.get("isFellowship", False),
            "orcid":         orcid_clean,
            "inv_source":    source,
        })
    return records


def extract_for_codes(attrs: dict, grant_code: str) -> list[dict]:
    """Extract field of research codes, retaining FOR type (FOR08 vs FOR20)."""
    records = []
    for for_item in attrs.get("field-of-research", []):
        records.append({
            "grant_code":   grant_code,
            "for_code":     safe_str(for_item.get("code")),
            "for_name":     safe_str(for_item.get("name")),
            "for_type":     safe_str(for_item.get("type")),
            "is_primary":   for_item.get("isPrimary", False),
        })
    return records


def extract_grant_flat(attrs: dict, grant_code: str) -> dict:
    """Extract flat grant-level fields."""
    return {
        "grant_code":           grant_code,
        "scheme_name":          safe_str(attrs.get("scheme-name")),
        "grant_status":         safe_str(attrs.get("grant-status")),
        "funding_commence_year":attrs.get("funding-commencement-year"),
        "years_funded":         attrs.get("years-funded"),
        "funding_announced":    attrs.get("funding-at-announcement"),
        "funding_current":      attrs.get("funding-current"),
        "admin_org":            safe_str(attrs.get("administering-organisation") or
                                         attrs.get("announcement-administering-organisation")),
        "grant_summary":        safe_str(attrs.get("grant-summary")),
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()
    ensure_dirs()

    print(f"Reading: {ARC_GRANTS_CSV}")
    df_raw = pd.read_csv(ARC_GRANTS_CSV, dtype=str)
    print(f"  Rows in CSV: {len(df_raw)}")

    # Normalise column names to lowercase stripped
    df_raw.columns = [c.strip().lower() for c in df_raw.columns]

    if "single_grant" not in df_raw.columns:
        print(f"ERROR: 'single_grant' column not found. Columns: {list(df_raw.columns)}")
        sys.exit(1)

    # ── Parse all rows ───────────────────────────────────────────────────────
    grants_flat     = []
    investigators   = []
    for_codes       = []
    parse_failures  = []

    for idx, row in df_raw.iterrows():
        attrs = parse_row(row["single_grant"], idx)
        if attrs is None:
            parse_failures.append(idx)
            continue

        grant_code = attrs.get("code", f"UNKNOWN_{idx}")

        grants_flat.append(extract_grant_flat(attrs, grant_code))
        investigators.extend(extract_investigators(attrs, grant_code))
        for_codes.extend(extract_for_codes(attrs, grant_code))

    df_grants = pd.DataFrame(grants_flat)
    df_inv    = pd.DataFrame(investigators)
    df_for    = pd.DataFrame(for_codes)

    # ── Save Parquet outputs ─────────────────────────────────────────────────
    grants_path = PROCESSED_DATA / "grants_flat.parquet"
    inv_path    = PROCESSED_DATA / "investigators_raw.parquet"
    for_path    = PROCESSED_DATA / "for_codes.parquet"

    df_grants.to_parquet(grants_path, index=False)
    df_inv.to_parquet(inv_path,    index=False)
    df_for.to_parquet(for_path,    index=False)

    print(f"\n  Saved: {grants_path}")
    print(f"  Saved: {inv_path}")
    print(f"  Saved: {for_path}")

    # ── Profile ──────────────────────────────────────────────────────────────
    profile_lines = []
    p = profile_lines.append  # shorthand

    p("=" * 60)
    p("ARC GRANTS DATA PROFILE")
    p("=" * 60)

    p(f"\n── Source ──────────────────────────────────────────")
    p(f"  CSV rows:              {len(df_raw):>8,}")
    p(f"  Parse failures:        {len(parse_failures):>8,}")
    p(f"  Grants parsed:         {len(df_grants):>8,}")

    p(f"\n── Grants ──────────────────────────────────────────")
    p(f"  Year range:            {df_grants.funding_commence_year.min()} "
      f"– {df_grants.funding_commence_year.max()}")
    p(f"  Unique schemes:        {df_grants.scheme_name.nunique():>8,}")
    p(f"  Missing admin org:     {df_grants.admin_org.eq('').sum():>8,}")
    p(f"  Null funding amount:   {df_grants.funding_announced.isna().sum():>8,}")

    p(f"\n  Grant status counts:")
    for status, cnt in df_grants.grant_status.value_counts().items():
        p(f"    {status:<40} {cnt:>6,}")

    p(f"\n  Grants per year (sample):")
    year_counts = df_grants.funding_commence_year.value_counts().sort_index()
    for year, cnt in year_counts.items():
        p(f"    {year}  {cnt:>5,}")

    p(f"\n── Investigators (raw) ─────────────────────────────")
    p(f"  Total investigator rows:     {len(df_inv):>8,}")
    p(f"  Unique family names:         {df_inv.family_name.nunique():>8,}")
    p(f"  Unique first names:          {df_inv.first_name.nunique():>8,}")
    p(f"  Unique name combinations:    "
      f"{df_inv[['first_name','family_name']].drop_duplicates().shape[0]:>8,}")

    p(f"\n  Role code distribution:")
    for role, cnt in df_inv.role_code.value_counts().items():
        p(f"    {role:<10} {cnt:>8,}")

    p(f"\n  ORCID coverage:")
    p(f"    Has ORCID:             {df_inv.orcid.notna().sum():>8,}  "
      f"({100*df_inv.orcid.notna().mean():.1f}%)")
    p(f"    No ORCID:              {df_inv.orcid.isna().sum():>8,}  "
      f"({100*df_inv.orcid.isna().mean():.1f}%)")

    p(f"\n  Investigators sourced from 'current' (not announcement):")
    p(f"    {df_inv[df_inv.inv_source=='current'].grant_code.nunique():>8,} grants")

    p(f"\n  Grants per investigator (by family+first name):")
    grants_per_inv = df_inv.groupby(
        ["family_name", "first_name"])["grant_code"].nunique()
    p(f"    1 grant:               "
      f"{(grants_per_inv == 1).sum():>8,}")
    p(f"    2–5 grants:            "
      f"{((grants_per_inv >= 2) & (grants_per_inv <= 5)).sum():>8,}")
    p(f"    6–10 grants:           "
      f"{((grants_per_inv >= 6) & (grants_per_inv <= 10)).sum():>8,}")
    p(f"    >10 grants:            "
      f"{(grants_per_inv > 10).sum():>8,}")
    p(f"    Max grants one person: {grants_per_inv.max():>8,}")

    p(f"\n── Field of Research ───────────────────────────────")
    p(f"  Total FOR assignments:   {len(df_for):>8,}")
    p(f"  FOR08 assignments:       {df_for[df_for.for_type=='FOR08'].shape[0]:>8,}")
    p(f"  FOR20 assignments:       {df_for[df_for.for_type=='FOR20'].shape[0]:>8,}")
    p(f"  Grants with no FOR:      "
      f"{len(df_grants) - df_for.grant_code.nunique():>8,}")

    p(f"\n── Data Quality Flags ──────────────────────────────")
    # Names with only initials
    initial_only = df_inv[df_inv.first_name.str.match(r'^[A-Z]\.?$', na=False)]
    p(f"  Initial-only first names:    {len(initial_only):>8,}")

    # Empty names
    p(f"  Empty family names:          "
      f"{df_inv.family_name.eq('').sum():>8,}")
    p(f"  Empty first names:           "
      f"{df_inv.first_name.eq('').sum():>8,}")

    # Malformed ORCIDs (should be 19 chars: 0000-0000-0000-0000)
    has_orcid = df_inv[df_inv.orcid.notna()]
    bad_orcid = has_orcid[~has_orcid.orcid.str.match(
        r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$', na=False)]
    p(f"  Malformed ORCIDs:            {len(bad_orcid):>8,}")
    if len(bad_orcid) > 0:
        p(f"  Sample malformed:")
        for val in bad_orcid.orcid.head(5):
            p(f"    '{val}'")

    # Encoding issues in grant summaries
    mojibake = df_grants[df_grants.grant_summary.str.contains(
        'â€', na=False, regex=False)]
    p(f"  Grants with encoding issues: {len(mojibake):>8,}")

    if parse_failures:
        p(f"\n  Parse failure row indices: {parse_failures}")

    p("\n" + "=" * 60)

    # ── Write and print profile ──────────────────────────────────────────────
    profile_text = "\n".join(profile_lines)
    profile_path = PROFILES_OUT / "grant_profile.txt"
    profile_path.write_text(profile_text, encoding="utf-8")

    print("\n" + profile_text)
    print(f"\nProfile saved to: {profile_path}")


if __name__ == "__main__":
    main()