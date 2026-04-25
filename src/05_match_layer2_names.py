"""
05_match_layer2_names.py

Layer 2 disambiguation: name matching against OAX AU-affiliated authors.

Applies to investigators not resolved in Layer 1 (no_orcid + not_in_oax).
Searches OAX AU authors by name, using institution as a confirmation signal.

MATCHING RULES (precision over recall):
    - Family name must match exactly after diacritical stripping and normalisation
    - Given name, full (>1 char, no trailing period): require a full token match
      in the OAX name — no credit if OAX only has the initial
    - Given name, initial (1 char or ends in period): accept OAX full name starting
      with that initial
    - Diacriticals stripped from both sides in this first pass; revisit after
      inspecting misses
    - Threshold: name_sim >= NAME_SIM_THRESHOLD (0.5)

OUTPUT:
    One row per (ARC person × OAX candidate) pair above threshold.
    Multiple rows for the same ARC person = multiple OAX candidates (ambiguous).

    PROCESSED_DATA/layer2_name_candidates.parquet
        arc_first_name, arc_family_name, arc_orcid, arc_sub_case,
        arc_admin_orgs      (pipe-separated list of all admin_orgs this person appeared on)
        arc_oax_inst_ids    (pipe-separated OAX institution IDs for those admin_orgs)
        oax_id, oax_display_name, oax_orcid
        name_sim, institution_match, n_candidates

INPUT:
    PROCESSED_DATA/layer1_residual.parquet
    PROCESSED_DATA/grants.parquet
    PROCESSED_DATA/institution_concordance.parquet
    OAX_AUTHORS/*.parquet
"""

import re
import sys
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROCESSED_DATA
from src.utils.io import setup_stdout_utf8

NAME_SIM_THRESHOLD = 0.5
OAX_GLOB = str(OAX_AUTHORS / "*.parquet")


# ── Name normalisation ────────────────────────────────────────────────────────

def _strip_diacriticals(s: str) -> str:
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def _tokens(name: str) -> list[str]:
    return re.findall(r"[a-z]+", _strip_diacriticals(name).lower())


def _is_initial(first_name: str) -> bool:
    return bool(re.match(r"^[A-Za-z]\.?$", first_name.strip()))


def name_sim(arc_first: str, arc_family: str, oax_names: list[str]) -> float:
    """
    Score name similarity between an ARC investigator and a list of OAX name
    variants (display_name + display_name_alternatives).

    Direction-aware initial handling:
        ARC full name → no credit if OAX only has the initial
        ARC initial   → credit for OAX full name starting with that initial
    """
    arc_initial = _is_initial(arc_first)
    arc_given   = _tokens(arc_first)
    arc_family_ = _tokens(arc_family)
    arc_all     = arc_given + arc_family_

    arc_all_set = set(arc_all)
    best = 0.0
    for oax_name in oax_names:
        oax_tok = _tokens(oax_name)
        if not oax_tok:
            continue

        score = 0.0
        for t in arc_all_set:
            if t in oax_tok:
                score += 1.0
            elif arc_initial and t in arc_given and len(t) == 1:
                # ARC initial → credit for OAX full name starting with it
                if any(s.startswith(t) for s in oax_tok):
                    score += 0.8
            # ARC full name token not found in OAX → no partial credit

        sim = score / len(arc_all_set | set(oax_tok))
        best = max(best, sim)

    return round(best, 3)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()
    con = duckdb.connect()

    # ── Load inputs ───────────────────────────────────────────────────────────
    df_res  = pd.read_parquet(PROCESSED_DATA / "layer1_residual.parquet")
    df_grts = pd.read_parquet(PROCESSED_DATA / "grants.parquet",
                              columns=["grant_code", "admin_org"])
    df_conc = pd.read_parquet(PROCESSED_DATA / "institution_concordance.parquet")

    print(f"Residual investigators:     {len(df_res):>8,}")
    print(f"  no_orcid:                 {(df_res['sub_case']=='no_orcid').sum():>8,}")
    print(f"  not_in_oax:               {(df_res['sub_case']=='not_in_oax').sum():>8,}")

    # ── Build grant → OAX institution IDs map ─────────────────────────────────
    # Concordance is (arc_admin_org, institution_id) — one ARC org may have multiple OAX IDs
    inst_map = (
        df_grts.merge(df_conc[["arc_admin_org","institution_id"]],
                      left_on="admin_org", right_on="arc_admin_org", how="left")
        .groupby("grant_code")["institution_id"]
        .apply(lambda x: sorted(set(v for v in x if pd.notna(v))))
        .reset_index()
        .rename(columns={"institution_id": "oax_inst_ids"})
    )

    # ── Deduplicate residual to unique persons ────────────────────────────────
    # Collect all admin_orgs and OAX institution IDs seen for each person.
    # Key: (first_name, family_name, orcid) — orcid may be null.
    df_res_inst = (
        df_res
        .merge(df_grts[["grant_code", "admin_org"]], on="grant_code", how="left")
        .merge(inst_map, on="grant_code", how="left")
    )

    persons = (
        df_res_inst.groupby(["first_name", "family_name", "sub_case"], dropna=False)
        .agg(
            arc_orcid=("orcid", "first"),
            arc_admin_orgs=("admin_org", lambda x: "|".join(sorted(set(str(v) for v in x if pd.notna(v))))),
            arc_oax_inst_ids=("oax_inst_ids", lambda x: sorted(set(
                iid for lst in x if isinstance(lst, list) for iid in lst
            ))),
            n_arc_grants=("grant_code", "nunique"),
        )
        .reset_index()
    )
    print(f"\nUnique persons to match:    {len(persons):>8,}")
    print(f"  with initial-only first:  {persons['first_name'].apply(_is_initial).sum():>8,}")

    # ── Load OAX AU authors ───────────────────────────────────────────────────
    print(f"\nLoading OAX AU authors...")
    df_oax = con.execute(f"""
        SELECT
            id,
            display_name,
            display_name_alternatives,
            regexp_replace(orcid, '^https://orcid\\.org/', '') AS orcid,
            list_transform(
                list_filter(last_known_institutions, x -> x.country_code = 'AU'),
                x -> x.id
            ) AS au_inst_ids
        FROM read_parquet('{OAX_GLOB}')
        WHERE list_contains(
            list_transform(last_known_institutions, x -> x.country_code), 'AU'
        )
    """).df()
    print(f"  OAX AU authors:           {len(df_oax):>8,}")

    # ── Build family-name index on OAX ───────────────────────────────────────
    def last_norm_token(name: str) -> str:
        toks = _tokens(name)
        return toks[-1] if toks else ""

    df_oax["_fam_idx"] = df_oax["display_name"].apply(last_norm_token)
    fam_index = df_oax.groupby("_fam_idx").indices

    # ── Match persons against OAX ─────────────────────────────────────────────
    print(f"Matching persons by name...")
    records = []

    for _, arc in persons.iterrows():
        fam_toks = _tokens(arc["family_name"])
        if not fam_toks:
            continue
        lookup = max(fam_toks, key=len)

        idxs = fam_index.get(lookup, [])
        if not len(idxs):
            continue

        arc_inst_set = set(arc["arc_oax_inst_ids"])

        for oax in df_oax.iloc[idxs].itertuples(index=False):
            alts  = list(oax.display_name_alternatives) if oax.display_name_alternatives is not None else []
            sim   = name_sim(arc["first_name"], arc["family_name"],
                             [oax.display_name] + alts)
            if sim < NAME_SIM_THRESHOLD:
                continue

            oax_inst_ids  = oax.au_inst_ids if oax.au_inst_ids is not None else []
            inst_match    = bool(arc_inst_set & set(oax_inst_ids))

            records.append({
                "arc_first_name":    arc["first_name"],
                "arc_family_name":   arc["family_name"],
                "arc_orcid":         arc["arc_orcid"],
                "arc_sub_case":      arc["sub_case"],
                "arc_admin_orgs":    arc["arc_admin_orgs"],
                "arc_oax_inst_ids":  "|".join(sorted(arc_inst_set)),
                "n_arc_grants":      arc["n_arc_grants"],
                "oax_id":            oax.id,
                "oax_display_name":  oax.display_name,
                "oax_orcid":         oax.orcid if oax.orcid else None,
                "name_sim":          sim,
                "institution_match": inst_match,
            })

    df_cands = pd.DataFrame(records)
    print(f"  Candidate pairs found:    {len(df_cands):>8,}")

    if len(df_cands) == 0:
        print("No matches found.")
        return

    # ── Add n_candidates per ARC person ──────────────────────────────────────
    n_cands = (
        df_cands.groupby(["arc_first_name", "arc_family_name", "arc_orcid"],
                         dropna=False)["oax_id"]
        .transform("count")
    )
    df_cands["n_candidates"] = n_cands

    # ── Summary ───────────────────────────────────────────────────────────────
    n_persons_matched = df_cands.groupby(
        ["arc_first_name", "arc_family_name", "arc_orcid"], dropna=False
    ).ngroups
    n_unambiguous   = (df_cands["n_candidates"] == 1).sum()
    n_inst_confirms = df_cands["institution_match"].sum()

    print(f"\n{'─'*50}")
    print(f"LAYER 2 COVERAGE SUMMARY")
    print(f"{'─'*50}")
    print(f"  Unique persons matched:          {n_persons_matched:>7,}  of {len(persons):,}")
    print(f"  Unmatched persons:               {len(persons)-n_persons_matched:>7,}")
    print(f"  Candidate pairs total:           {len(df_cands):>7,}")
    print(f"  Unambiguous (n_candidates=1):    {df_cands[df_cands['n_candidates']==1]['arc_first_name'].nunique():>7,}")
    print(f"  Institution match confirms:      {n_inst_confirms:>7,}  ({100*n_inst_confirms/len(df_cands):.1f}% of pairs)")

    print(f"\n  n_candidates distribution:")
    for n, cnt in df_cands.groupby("n_candidates")["arc_first_name"].count().items():
        persons_at_n = df_cands[df_cands["n_candidates"]==n].groupby(
            ["arc_first_name","arc_family_name"]).ngroups
        print(f"    {n:>3} candidates:  {persons_at_n:>6,} persons")

    # ── Save ──────────────────────────────────────────────────────────────────
    out_path = PROCESSED_DATA / "layer2_name_candidates.parquet"
    df_cands.to_parquet(out_path, index=False)
    print(f"\nSaved: {out_path}  ({len(df_cands):,} rows)")


if __name__ == "__main__":
    main()
