"""
03_match_layer1_orcid.py

Layer 1 disambiguation: ORCID matching in two passes.

Pass 1 — Join ARC ORCIDs against OAX AU-affiliated authors only.
          Matches validated with name similarity.

Pass 2 — The small residual (ARC ORCIDs not found in AU) is looked up in the
          full 106M global OAX table by ORCID. Name similarity (including all
          display_name_alternatives) is then computed for each match.

INPUT:
    PROCESSED_DATA/investigators.parquet
    OAX_AUTHORS/*.parquet   (~106M rows)

OUTPUT:
    PROCESSED_DATA/layer1_orcid_matches.parquet
        All ORCID matches (pass 1 + pass 2 combined). One row per unique ARC ORCID.
        Columns: arc_orcid, arc_first_name, arc_family_name, n_arc_grants,
                 oax_id, oax_display_name, au_in_oax,
                 name_sim, name_ok   (both passes; checked against all display_name_alternatives)

    PROCESSED_DATA/layer1_residual.parquet
        investigators.parquet rows not matched in either pass, with sub_case:
            'not_in_oax'  — ARC has ORCID, not found in OAX at all
            'no_orcid'    — ARC investigator has no ORCID

NAME SIMILARITY (pass 1 only):
    Token Jaccard on lowercase alpha tokens, with half-credit for initial
    matches (e.g. 'J' matches 'John'). Checked against display_name and all
    display_name_alternatives.
    name_ok = True if best score >= NAME_SIM_THRESHOLD (0.3)
    name_ok=False matches are retained but flagged for review.
"""

import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROCESSED_DATA
from src.utils.io import setup_stdout_utf8

NAME_SIM_THRESHOLD = 0.3
OAX_GLOB = str(OAX_AUTHORS / "*.parquet")


# ── Name similarity ───────────────────────────────────────────────────────────

def _tokens(name: str) -> list[str]:
    return re.findall(r"[a-z]+", name.lower())


def _sim_pair(a: list[str], b: list[str]) -> float:
    if not a or not b:
        return 0.0
    sa, sb = set(a), set(b)
    score = 0.0
    for t in sa:
        if t in sb:
            score += 1.0
        elif len(t) == 1 and any(s.startswith(t) for s in sb):
            score += 0.5
        elif len(t) > 1 and t[0] in sb:
            score += 0.5
    return min(score / len(sa | sb), 1.0)


def best_name_sim(arc_first: str, arc_family: str, oax_names: list[str]) -> float:
    arc_tok = _tokens(f"{arc_first} {arc_family}")
    return max((_sim_pair(arc_tok, _tokens(n)) for n in oax_names), default=0.0)


# ── Pass 2: global ORCID lookup for AU misses ─────────────────────────────────

def lookup_unmatched_globally(
    df_unmatched: pd.DataFrame,
    con: duckdb.DuckDBPyConnection,
    oax_glob: str,
) -> pd.DataFrame:
    """
    Look up ARC ORCIDs that were not found in the OAX AU subset against the
    full global OAX table (ORCID match only, no name filtering).

    Returns one row per ARC ORCID found globally.
    """
    print(f"\nPass 2: global ORCID lookup for {len(df_unmatched):,} AU misses...")
    con.register("unmatched_tbl", df_unmatched)

    df = con.execute(f"""
        SELECT
            u.orcid          AS arc_orcid,
            u.arc_first_name,
            u.arc_family_name,
            u.n_arc_grants,
            o.id             AS oax_id,
            o.display_name   AS oax_display_name,
            o.display_name_alternatives AS oax_alts
        FROM unmatched_tbl u
        JOIN (
            SELECT
                id,
                display_name,
                display_name_alternatives,
                regexp_replace(orcid, '^https://orcid\\.org/', '') AS orcid
            FROM read_parquet('{oax_glob}')
            WHERE orcid IS NOT NULL AND orcid != ''
        ) o ON u.orcid = o.orcid
    """).df()

    print(f"  Found globally:           {len(df):>8,}")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()
    con = duckdb.connect()

    # ── Load ARC investigators ────────────────────────────────────────────────
    inv_path = PROCESSED_DATA / "investigators.parquet"
    df_inv = pd.read_parquet(inv_path)
    print(f"ARC investigators loaded:   {len(df_inv):>8,}")

    n_with_orcid    = df_inv["orcid"].notna().sum()
    n_without_orcid = df_inv["orcid"].isna().sum()
    print(f"  Has ORCID:                {n_with_orcid:>8,}  ({100*n_with_orcid/len(df_inv):.1f}%)")
    print(f"  No ORCID:                 {n_without_orcid:>8,}  ({100*n_without_orcid/len(df_inv):.1f}%)")

    # Unique ARC persons with ORCID
    df_arc_orcid = (
        df_inv[df_inv["orcid"].notna()]
        .groupby("orcid", as_index=False)
        .agg(
            arc_first_name=("first_name",  "first"),
            arc_family_name=("family_name", "first"),
            n_arc_grants=("grant_code",    "nunique"),
        )
    )
    print(f"\nUnique ARC ORCIDs:          {len(df_arc_orcid):>8,}")

    # ── Pass 1: ORCID join against OAX AU authors ────────────────────────────
    print(f"\nPass 1: ORCID join against OAX AU authors...")
    con.register("arc_orcid_tbl", df_arc_orcid)

    df_matched = con.execute(f"""
        SELECT
            a.orcid                          AS arc_orcid,
            a.arc_first_name,
            a.arc_family_name,
            a.n_arc_grants,
            o.id                             AS oax_id,
            o.display_name                   AS oax_display_name,
            o.display_name_alternatives      AS oax_alts
        FROM arc_orcid_tbl a
        JOIN (
            SELECT
                id,
                display_name,
                display_name_alternatives,
                regexp_replace(orcid, '^https://orcid\\.org/', '') AS orcid
            FROM read_parquet('{OAX_GLOB}')
            WHERE orcid IS NOT NULL AND orcid != ''
              AND list_contains(
                  list_transform(last_known_institutions, x -> x.country_code), 'AU'
              )
        ) o ON a.orcid = o.orcid
    """).df()

    print(f"  AU matches found:         {len(df_matched):>8,}")

    # Name similarity validation
    def compute_sim(row) -> float:
        alts = row["oax_alts"] if row["oax_alts"] is not None else []
        return best_name_sim(row["arc_first_name"], row["arc_family_name"],
                             [row["oax_display_name"]] + list(alts))

    print("  Computing name similarity...")
    df_matched["name_sim"]   = df_matched.apply(compute_sim, axis=1).round(3)
    df_matched["name_ok"]    = df_matched["name_sim"] >= NAME_SIM_THRESHOLD
    df_matched["au_in_oax"]  = True
    df_matched = df_matched.drop(columns=["oax_alts"])

    n_name_ok      = df_matched["name_ok"].sum()
    n_name_suspect = (~df_matched["name_ok"]).sum()
    print(f"  name_ok (>= {NAME_SIM_THRESHOLD}):        {n_name_ok:>8,}  ({100*n_name_ok/len(df_matched):.1f}%)")
    print(f"  name suspect (< {NAME_SIM_THRESHOLD}):    {n_name_suspect:>8,}  ({100*n_name_suspect/len(df_matched):.1f}%)")

    if n_name_suspect > 0:
        print("\n  Suspicious matches (sample):")
        cols = ["arc_first_name", "arc_family_name", "arc_orcid", "oax_display_name", "name_sim"]
        print(df_matched[~df_matched["name_ok"]][cols].head(10).to_string(index=False))

    # ── Pass 2: global lookup for AU misses ──────────────────────────────────
    matched_pass1 = set(df_matched["arc_orcid"])
    df_au_misses  = df_arc_orcid[~df_arc_orcid["orcid"].isin(matched_pass1)].copy()

    df_global = lookup_unmatched_globally(df_au_misses, con, OAX_GLOB)

    # ── Name similarity for Pass 2 ────────────────────────────────────────────
    if len(df_global) > 0:
        print("  Computing name similarity for Pass 2 matches...")
        def compute_sim_p2(row) -> float:
            alts = row["oax_alts"] if row["oax_alts"] is not None else []
            return best_name_sim(row["arc_first_name"], row["arc_family_name"],
                                 [row["oax_display_name"]] + list(alts))
        df_global["name_sim"] = df_global.apply(compute_sim_p2, axis=1).round(3)
        df_global["name_ok"]  = df_global["name_sim"] >= NAME_SIM_THRESHOLD

        n2_ok      = df_global["name_ok"].sum()
        n2_suspect = (~df_global["name_ok"]).sum()
        print(f"  name_ok (>= {NAME_SIM_THRESHOLD}):        {n2_ok:>8,}")
        print(f"  name suspect (< {NAME_SIM_THRESHOLD}):    {n2_suspect:>8,}")
        if n2_suspect > 0:
            cols = ["arc_first_name", "arc_family_name", "arc_orcid", "oax_display_name", "name_sim"]
            print("\n  Suspicious global matches (sample):")
            print(df_global[~df_global["name_ok"]][cols].head(10).to_string(index=False))

    df_global = df_global.drop(columns=["oax_alts"])

    # ── Combine passes ────────────────────────────────────────────────────────
    df_global["au_in_oax"] = False
    df_all_matched = pd.concat([df_matched, df_global], ignore_index=True)

    matched_pass2  = set(df_global["arc_orcid"]) if len(df_global) > 0 else set()
    all_matched    = matched_pass1 | matched_pass2

    def sub_case(row) -> str:
        if pd.isna(row["orcid"]):
            return "no_orcid"
        if row["orcid"] not in all_matched:
            return "not_in_oax"
        return "matched"

    df_inv["sub_case"] = df_inv.apply(sub_case, axis=1)
    df_residual = df_inv[df_inv["sub_case"] != "matched"].copy()

    # ── Coverage summary ──────────────────────────────────────────────────────
    n_p1_rows       = df_inv["orcid"].isin(matched_pass1).sum()
    n_p2_rows       = df_inv["orcid"].isin(matched_pass2).sum()
    n_not_oax_rows  = (df_inv["sub_case"] == "not_in_oax").sum()
    n_no_orcid_rows = (df_inv["sub_case"] == "no_orcid").sum()
    n_unique_inv    = df_inv.groupby(["family_name", "first_name"]).ngroups

    print(f"\n{'─'*50}")
    print(f"LAYER 1 COVERAGE SUMMARY")
    print(f"{'─'*50}")
    print(f"  Pass 1  AU ORCID match (rows):   {n_p1_rows:>7,}  ({100*n_p1_rows/len(df_inv):.1f}%)")
    print(f"  Pass 2  global, not AU (rows):   {n_p2_rows:>7,}  ({100*n_p2_rows/len(df_inv):.1f}%)")
    print(f"  ORCID not in OAX at all (rows):  {n_not_oax_rows:>7,}  ({100*n_not_oax_rows/len(df_inv):.1f}%)")
    print(f"  No ORCID (rows):                 {n_no_orcid_rows:>7,}  ({100*n_no_orcid_rows/len(df_inv):.1f}%)")
    print(f"\n  Unique persons total (approx):   {n_unique_inv:>7,}")
    print(f"  Unique ARC ORCIDs — pass 1:      {len(matched_pass1):>7,}")
    print(f"  Unique ARC ORCIDs — pass 2:      {len(matched_pass2):>7,}")
    print(f"  Unique ARC ORCIDs — not in OAX:  {len(df_au_misses) - len(matched_pass2):>7,}")
    print(f"  Residual rows for Layer 2/3:     {len(df_residual):>7,}")

    # ── Save ──────────────────────────────────────────────────────────────────
    matches_path  = PROCESSED_DATA / "layer1_orcid_matches.parquet"
    residual_path = PROCESSED_DATA / "layer1_residual.parquet"

    df_all_matched.to_parquet(matches_path,   index=False)
    df_residual.to_parquet(residual_path,     index=False)

    print(f"\nSaved:")
    print(f"  {matches_path}  ({len(df_all_matched):,} rows)")
    print(f"  {residual_path}  ({len(df_residual):,} rows)")


if __name__ == "__main__":
    main()
