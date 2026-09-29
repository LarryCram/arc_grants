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
    DATA_ROOT/processed/grants_flat.parquet      -- Flattened grant records (enriched with primary_field_of_research)
    DATA_ROOT/processed/investigators_raw.parquet -- Extracted investigator records
    DATA_ROOT/processed/arc_names.parquet        -- NameParser() output per unique_id: the ONLY
                                                    parsed ARC names; nothing else parses one
    DATA_ROOT/processed/arc_name_renames.parquet -- announcement/current forms merged because
                                                    their parsed full_name_keys overlap
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
from dataclasses import asdict
import pandas as pd
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import ARC_GRANTS_CSV, GRANT_SUMMARIES_CSV, PROFILES_OUT, PROCESSED_DATA
from src.utils.paths import ensure_dirs
from src.utils.io import setup_stdout_utf8
from src.utils.names import HumanNameParser, ParsedName

_NAME_PARSER = HumanNameParser()


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


def _parse_investigator_list(inv_list: list, grant_code: str) -> dict[str, list[tuple[dict, ParsedName]]]:
    """unique_id -> [(raw investigator dict, ParsedName), ...] for one snapshot list, in list
    order. Every name is parsed here, once, by NameParser(); the id's name part is the parser's
    full_name_key (full_name_key_raw when the ASCII key is empty, per ParsedName's own contract)."""
    out: dict[str, list] = {}
    for inv in inv_list:
        first_name = safe_str(inv.get("firstName"))
        family_name = safe_str(inv.get("familyName"))
        parsed = _NAME_PARSER.parse((first_name, family_name))
        key = parsed.full_name_key or parsed.full_name_key_raw
        if key is None:
            raise ValueError(f"{grant_code}: NameParser returned no key for {first_name!r} {family_name!r}")
        out.setdefault(f"{grant_code}_{key}", []).append((inv, parsed))
    return out


def _display_name(inv: dict) -> str:
    return f"{safe_str(inv.get('firstName'))} {safe_str(inv.get('familyName'))}".strip()


def extract_investigators(attrs: dict, grant_code: str) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Extract investigators from a grant's attributes dict -- the only place an ARC name is parsed.
    Returns (investigator_rows, arc_name_rows, rename_rows).

    Unions investigators-at-announcement and investigators-current. Every name is parsed once by
    NameParser(); the id is grant_code + "_" + the parser's full_name_key. An announcement name and
    a current name are the same investigator when their parsed names agree:
      - same id, or
      - their full_name_keys sets overlap (e.g. DE120101452 "Mahmuda Akhtar" / "M. Shumi Akhtar"
        share m_akhtar), one-to-one only -- an announcement name overlapping 2+ current names (or
        the reverse) is left unmerged, never guessed. The pair merges into the current id and is
        reported in rename_rows.
    Anything else is an addition or deletion between snapshots. (2026-09-29: replaces a "one
    dropped + one added = rename" rule, first in 00c_extract_propensities.py's award_rename_map,
    that ignored the names and merged different people, e.g. "Jessica Hyles" / "Ben Trevaskis".)

    arc_name_rows: one row per id -- the parser's fields for the display form (current if present),
    full_name_keys unioned over every raw form merged into the id, the raw name_forms, which
    snapshot(s) the id appeared in (in_announcement / in_current), and renamed_from (the
    announcement id merged in, if any).

    title/role_code/role_name/is_fellowship/first_name/family_name prefer the CURRENT record (2026-09-29 fix,
    confirmed via real cases -- e.g. FT100100761/FT100100627, both an identical person/ORCID
    whose role_code was corrected from a generic "CI" at announcement to the properly-specific
    "FT"/is_fellowship=True at current -- an ARC administrative correction/refinement over time,
    not two genuine facts to reconcile), falling back to the announcement record's own value
    only when current genuinely lacks one (an announcement-only investigator who never appears
    in the current snapshot at all). is_fellowship in particular was checked directly and found
    to be entirely DERIVED from role_code -- 0 real cases exist where role_code matches but
    is_fellowship still differs -- so no separate handling was needed for it beyond following
    role_code's own precedence.

    ORCID keeps its own separate, unchanged precedence: whichever source is processed first
    (announcement) wins, backfilling from current only if announcement's own orcid is empty --
    a real ORCID essentially never legitimately differs between snapshots (confirmed directly:
    1 conflict in 78,571 real matched pairs), so this precedence rarely matters in practice and
    wasn't part of what changed here.
    """
    ann = _parse_investigator_list(attrs.get("investigators-at-announcement") or [], grant_code)
    curr = _parse_investigator_list(attrs.get("investigators-current") or [], grant_code)

    def _keys(entries):
        return {k for _, parsed in entries for k in parsed.full_name_keys}

    only_ann, only_curr = set(ann) - set(curr), set(curr) - set(ann)
    overlaps = {a: {c for c in only_curr if _keys(ann[a]) & _keys(curr[c])} for a in only_ann}
    renamed: dict[str, str] = {}
    for a, cs in overlaps.items():
        if len(cs) == 1:
            c = next(iter(cs))
            if sum(c in other for other in overlaps.values()) == 1:
                renamed[a] = c

    seen: dict[str, dict] = {}
    forms: dict[str, dict[str, list]] = {}  # unique_id -> {source: [(inv, ParsedName), ...]}

    def _process(by_id, source):
        for raw_id, entries in by_id.items():
            unique_id = renamed.get(raw_id, raw_id)
            forms.setdefault(unique_id, {"announcement": [], "current": []})[source].extend(entries)
            inv = entries[0][0]
            orcid_clean = (inv.get("orcidIdentifier") or "").strip() or None
            if unique_id not in seen:
                seen[unique_id] = {
                    "unique_id":     unique_id,
                    "grant_code":    grant_code,
                    "title":         safe_str(inv.get("title")),
                    "first_name":    safe_str(inv.get("firstName")),
                    "family_name":   safe_str(inv.get("familyName")),
                    "role_code":     safe_str(inv.get("roleCode")),
                    "role_name":     safe_str(inv.get("roleName")),
                    "is_fellowship": inv.get("isFellowship", False),
                    "orcid":         orcid_clean,
                    "inv_source":    source,
                }
                continue
            # Present in both lists (or merged as a rename): ORCID keeps announcement-first
            # precedence, backfilled from current; everything else prefers current.
            row = seen[unique_id]
            if orcid_clean and not row["orcid"]:
                row["orcid"] = orcid_clean
            if source == "current":
                row["title"] = safe_str(inv.get("title"))
                row["first_name"] = safe_str(inv.get("firstName"))
                row["family_name"] = safe_str(inv.get("familyName"))
                row["role_code"] = safe_str(inv.get("roleCode"))
                row["role_name"] = safe_str(inv.get("roleName"))
                row["is_fellowship"] = inv.get("isFellowship", False)
                row["inv_source"] = source

    _process(ann, "announcement")
    _process(curr, "current")

    announcement_id_for = {c: a for a, c in renamed.items()}
    name_rows = []
    for unique_id, by_source in forms.items():
        all_forms = by_source["current"] + by_source["announcement"]
        display = all_forms[0][1]
        name_row = {k: (list(v) if isinstance(v, tuple) else v) for k, v in asdict(display).items()}
        name_row["full_name_keys"] = list(dict.fromkeys(k for _, p in all_forms for k in p.full_name_keys))
        name_rows.append({
            "unique_id": unique_id,
            "grant_code": grant_code,
            "in_announcement": bool(by_source["announcement"]),
            "in_current": bool(by_source["current"]),
            "renamed_from": announcement_id_for.get(unique_id),
            "name_forms": list(dict.fromkeys(_display_name(inv) for inv, _ in all_forms)),
            **name_row,
        })

    rename_rows = [
        {
            "grant_code": grant_code,
            "announcement_unique_id": a,
            "current_unique_id": c,
            "announcement_name": _display_name(ann[a][0][0]),
            "current_name": _display_name(curr[c][0][0]),
            "shared_keys": sorted(_keys(ann[a]) & _keys(curr[c])),
        }
        for a, c in renamed.items()
    ]
    return list(seen.values()), name_rows, rename_rows


# Removed extract_for_codes as we are now using primary_field_of_research from grant_summaries


def extract_grant_flat(attrs: dict, grant_code: str) -> dict:
    """Extract flat grant-level fields.

    eligible_roles / eligible_names (2026-08-16): ARC's own organisations-at-announcement list
    carries 7 distinct roleName values, not just the 2 originally handled here. Investigated all
    7 directly against real in-scope (KEEP_SCHEMES) grants before finalizing this set:
      - Administering Organisation (30,551, exactly 1/grant) -- include
      - Other Eligible Organisation (5,036) -- include
      - Collaborating Organisation (701) -- include (added 2026-08-16): real examples are genuine
        Australian universities (Melbourne, Monash, Macquarie, ACU, Griffith), same character as
        Other Eligible Organisation
      - Partner Organisation (15,250) -- excluded (user-directed): formally a weaker/different
        relationship to the grant than the funded team itself
      - Host Organisation (809, mostly on FT/Future Fellowship grants) -- excluded: real examples
        overwhelmingly foreign universities or industry (Cambridge, Lund, Caltech, Boeing, Dyson),
        rarely an Australian HEP; represents where a fellow is hosted, not part of the funded team
      - Other Organisation (8,126) and bare Other (889) -- excluded: real examples overwhelmingly
        foreign universities or non-HEP bodies (Auckland, Illinois, UCL, museums, private companies)
    `eligible_names` (the original 2-role set) was previously computed and then discarded -- only
    its count survived into grants_flat.parquet (n_eligible_orgs). n_eligible_orgs is load-bearing
    downstream (01_prepare_arc.py/awards_cif.py's _merge_same_grant_coinvestigators,
    04_resolve_links.py's institution-overlap check all treat n_eligible_orgs==1 as "single-org
    grant" and rely on that exact 2-role definition) -- so it keeps its original 2-role scope
    unchanged here. The new eligible_orgs column below is a *separate*, deliberately wider 3-role
    set (adds Collaborating Organisation) for HEP-code resolution -- see
    src/utils/awards_cif.py's HEP-code aggregation, which consumes it. Do not fold eligible_orgs's
    role set back into n_eligible_orgs's -- that would silently change which grants count as
    "single-org" for the merge/disambiguation logic above.
    """
    orgs = attrs.get("organisations-at-announcement", []) or []
    n_eligible_roles = {"Administering Organisation", "Other Eligible Organisation"}
    n_eligible_names = {o["organisationName"] for o in orgs
                        if o.get("roleName") in n_eligible_roles and o.get("organisationName")}
    eligible_orgs_roles = n_eligible_roles | {"Collaborating Organisation"}
    eligible_orgs_names = {o["organisationName"] for o in orgs
                           if o.get("roleName") in eligible_orgs_roles and o.get("organisationName")}
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
        # 2026-08-25: the module docstring above has claimed "both retained" since before this
        # field existed -- admin_org itself only ever kept ONE value (current, falling back to
        # announcement only when current is missing), silently discarding the announcement-time
        # value whenever both are present and differ. Confirmed real at scale: 13.03% of grants
        # the pipeline treats as "single institution" via n_eligible_orgs==1 actually have a
        # DIFFERENT admin org between snapshots (e.g. DP110100989: Wollongong at announcement,
        # Australian Catholic University current, same investigators throughout). Persisted as
        # its own explicit field, not folded anonymously into eligible_orgs below -- keeping the
        # announcement-vs-current distinction visible is itself valuable evidence (the specific
        # transfer story), not just set membership.
        "announcement_admin_org": safe_str(attrs.get("announcement-administering-organisation")),
        "grant_summary":        safe_str(attrs.get("grant-summary")),
        "n_eligible_orgs":      len(n_eligible_names),
        "eligible_orgs":        sorted(eligible_orgs_names),
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
    arc_names       = []
    renames         = []
    parse_failures  = []

    for idx, row in df_raw.iterrows():
        attrs = parse_row(row["single_grant"], idx)
        if attrs is None:
            parse_failures.append(idx)
            continue

        grant_code = attrs.get("code", f"UNKNOWN_{idx}")

        grants_flat.append(extract_grant_flat(attrs, grant_code))
        inv_rows, name_rows, rename_rows = extract_investigators(attrs, grant_code)
        investigators.extend(inv_rows)
        arc_names.extend(name_rows)
        renames.extend(rename_rows)

    df_grants  = pd.DataFrame(grants_flat)
    df_inv     = pd.DataFrame(investigators)
    df_names   = pd.DataFrame(arc_names)
    df_renames = pd.DataFrame(renames)

    # ── Enrich grants with primary_field_of_research from summaries ──────────
    print(f"\nEnriching grants with {GRANT_SUMMARIES_CSV}")
    summaries = pd.read_csv(GRANT_SUMMARIES_CSV, usecols=['grant_id', 'primary_field_of_research'])
    summaries = summaries.rename(columns={'grant_id': 'grant_code'})
    
    # Strip the leading 4-digit code and hyphen (e.g., '4605 - Data Management' -> 'Data Management')
    summaries['primary_for_name'] = summaries['primary_field_of_research'].str.replace(r'^[0-9]+\s*-\s*', '', regex=True)
    
    # Merge onto df_grants
    df_grants = df_grants.merge(summaries[['grant_code', 'primary_for_name']], on='grant_code', how='left')

    # ── Save Parquet outputs ─────────────────────────────────────────────────
    grants_path  = PROCESSED_DATA / "grants_flat.parquet"
    inv_path     = PROCESSED_DATA / "investigators_raw.parquet"
    names_path   = PROCESSED_DATA / "arc_names.parquet"
    renames_path = PROCESSED_DATA / "arc_name_renames.parquet"

    df_grants.to_parquet(grants_path, index=False)
    df_inv.to_parquet(inv_path, index=False)
    df_names.to_parquet(names_path, index=False)
    df_renames.to_parquet(renames_path, index=False)

    print(f"\n  Saved: {grants_path}")
    print(f"  Saved: {inv_path}")
    print(f"  Saved: {names_path}")
    print(f"  Saved: {renames_path}")

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

    # FOR section removed from profile since we rely on the primary_for_name now.

    p(f"\n── Announcement vs current (parsed names, arc_names.parquet) ──")
    both = df_names.in_announcement & df_names.in_current
    p(f"  Same name in both:           {both.sum():>8,}")
    p(f"  Only at announcement:        {(df_names.in_announcement & ~df_names.in_current).sum():>8,}  (deletions)")
    p(f"  Only in current:             {(~df_names.in_announcement & df_names.in_current).sum():>8,}  (additions)")
    p(f"    (both includes {len(df_renames):,} renames -- different forms whose parsed keys overlap)")
    p(f"  Full rename list: {renames_path}")
    for r in df_renames.head(20).itertuples():
        p(f"    {r.grant_code:<14} {r.announcement_name!r} -> {r.current_name!r}  shared={list(r.shared_keys)}")

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