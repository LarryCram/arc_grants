"""
src/00a_analyse_arc.py

Diagnostic stats on ARC investigator name disambiguation.
Establishes the scale of the problem before any solution is designed.

Inputs:
    processed/investigators_raw.parquet
    processed/grants_flat.parquet
    processed/arc_persons.parquet   (current Splink output, for comparison)
    config/grant_summaries.csv      (for FOR division codes)

Output:
    stdout + profiles/arc_name_analysis.txt
"""

import sys
import re
from pathlib import Path
from collections import defaultdict

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, PROFILES_OUT, GRANT_SUMMARIES_CSV
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import strip_diacriticals, name_part_tokens
from src.utils.io import setup_stdout_utf8


# ── Name normalisation (same logic as 01_prepare_arc.py) ─────────────────────

def _norm_family(s: str) -> str:
    return strip_diacriticals(s or "").lower().strip()

def _first_initial(s: str) -> str | None:
    toks = name_part_tokens(s or "")
    return toks[0][0] if toks else None

def _full_first(s: str) -> str | None:
    """Longest full token (≥4 chars) from the first name field, or None."""
    toks = name_part_tokens(s or "")
    full = [t for t in toks if len(t) >= 4]
    return max(full, key=len) if full else None

def _make_name_key(family: str, first: str) -> str | None:
    f = _norm_family(family)
    i = _first_initial(first)
    return f"{f}_{i}" if (f and i) else None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _orcid_status(series: pd.Series) -> str:
    u = series.dropna().unique()
    if len(u) == 0:  return "none"
    if len(u) == 1:  return "single"
    return "conflict"

def _fmt(n: int, total: int) -> str:
    return f"{n:>7,}  ({100*n/total:.1f}%)"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()
    lines: list[str] = []
    p = lines.append

    # ── Load & filter ─────────────────────────────────────────────────────────
    inv    = pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet")
    grants = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")
    summaries = pd.read_csv(
        GRANT_SUMMARIES_CSV,
        usecols=["grant_id", "primary_field_of_research"],
    )
    summaries["for_div"] = summaries["primary_field_of_research"].str.extract(r"^(\d{2})")

    inv = inv[inv["role_code"].isin(KEEP_ROLES)]
    inv = inv[inv["grant_code"].str[:2].isin(KEEP_SCHEMES)]
    inv = inv.merge(grants[["grant_code", "admin_org"]], on="grant_code", how="left")
    inv = inv.merge(
        summaries[["grant_id", "for_div"]].rename(columns={"grant_id": "grant_code"}),
        on="grant_code", how="left",
    )

    inv["name_key"]  = inv.apply(lambda r: _make_name_key(r.family_name, r.first_name), axis=1)
    inv["full_first"] = inv["first_name"].apply(_full_first)
    inv = inv[inv["name_key"].notna()].copy()

    total_rows = len(inv)

    # ── Name-group aggregation ────────────────────────────────────────────────
    grp = (
        inv.groupby("name_key")
        .agg(
            n              = ("unique_id",   "count"),
            orcid_status   = ("orcid",       _orcid_status),
            n_orcids       = ("orcid",       lambda s: s.dropna().nunique()),
            orcids         = ("orcid",       lambda s: sorted(s.dropna().unique())),
            n_inst         = ("admin_org",   "nunique"),
            institutions   = ("admin_org",   lambda s: sorted(s.dropna().unique())),
            n_for_div      = ("for_div",     "nunique"),
            for_divs       = ("for_div",     lambda s: sorted(s.dropna().unique())),
            full_firsts    = ("full_first",  lambda s: sorted(set(s.dropna()))),
            all_first_names= ("first_name",  lambda s: sorted(set(s))),
            grant_codes    = ("grant_code",  lambda s: sorted(set(s))),
        )
        .reset_index()
    )
    grp["n_full_firsts"] = grp["full_firsts"].apply(len)
    total_grps = len(grp)

    # ── Section 1: Scale ──────────────────────────────────────────────────────
    p("=" * 70)
    p("ARC INVESTIGATOR NAME DISAMBIGUATION — PRE-SPLINK ANALYSIS")
    p("=" * 70)

    p(f"\n── Input (after role/scheme filter) ─────────────────────────────────")
    p(f"  Investigator rows:          {total_rows:>8,}")
    p(f"  Unique name keys:           {total_grps:>8,}")
    p(f"  Avg rows per name group:    {total_rows/total_grps:>11.2f}")

    p(f"\n── Name group size distribution ─────────────────────────────────────")
    for label, mask in [
        ("Singleton (1 row)",   grp.n == 1),
        ("Small   (2–5 rows)",  (grp.n >= 2) & (grp.n <= 5)),
        ("Medium  (6–20 rows)", (grp.n >= 6) & (grp.n <= 20)),
        ("Large   (>20 rows)",  grp.n > 20),
    ]:
        p(f"  {label}:   {_fmt(mask.sum(), total_grps)}")
    p(f"  Max group size:             {grp.n.max():>8,}  ({grp.loc[grp.n.idxmax(), 'name_key']})")

    # ── Section 2: ORCID ──────────────────────────────────────────────────────
    p(f"\n── ORCID status (per name group) ────────────────────────────────────")
    for status in ["none", "single", "conflict"]:
        mask = grp.orcid_status == status
        p(f"  {status:<10}: {_fmt(mask.sum(), total_grps)}")

    multi = grp[grp.n > 1]
    p(f"\n── Multi-row groups only (n>1): {len(multi):,} ────────────────────")
    p(f"  ORCID conflict (≥2 distinct ORCIDs):       {(multi.orcid_status=='conflict').sum():>6,}")
    p(f"  Single ORCID (consistent across rows):     {(multi.orcid_status=='single').sum():>6,}")
    p(f"  No ORCID at all:                           {(multi.orcid_status=='none').sum():>6,}")

    # ── Section 3: Institution & FOR field spread ─────────────────────────────
    p(f"\n── Within multi-row groups: institution & FOR spread ────────────────")
    p(f"  1 institution:                             {(multi.n_inst==1).sum():>6,}")
    p(f"  2+ institutions:                           {(multi.n_inst>1).sum():>6,}")
    p(f"    of which: same FOR division:             {((multi.n_inst>1)&(multi.n_for_div==1)).sum():>6,}")
    p(f"    of which: different FOR divisions:       {((multi.n_inst>1)&(multi.n_for_div>1)).sum():>6,}")

    # ── Section 4: First-name conflicts ───────────────────────────────────────
    p(f"\n── First-name conflicts (distinct full first names ≥4 chars) ────────")
    p(f"  Groups with 1 full first name:             {(grp.n_full_firsts==1).sum():>6,}")
    p(f"  Groups with 2+ full first names:           {(grp.n_full_firsts>1).sum():>6,}")
    p(f"  Groups with no full first name (initials): {(grp.n_full_firsts==0).sum():>6,}")

    # ── Section 5: Classification ─────────────────────────────────────────────
    p(f"\n── Rough pre-Splink classification ──────────────────────────────────")

    singleton       = grp.n == 1
    orcid_split     = (grp.n > 1) & (grp.orcid_status == "conflict")
    orcid_ok        = (grp.n > 1) & (grp.orcid_status == "single")
    name_conflict   = (grp.n > 1) & (grp.n_full_firsts > 1)
    multi_inst_div  = (grp.n > 1) & (grp.n_inst > 1) & (grp.n_for_div > 1) & (grp.orcid_status == "none")
    same_inst       = (grp.n > 1) & (grp.n_inst == 1) & (grp.orcid_status == "none")

    p(f"  Singleton → trivially one person:          {singleton.sum():>6,}")
    p(f"  ORCID conflict → Splink will split:        {orcid_split.sum():>6,}")
    p(f"  Single ORCID → very likely one person:     {orcid_ok.sum():>6,}")
    p(f"  Same institution, no ORCID → probably ok:  {same_inst.sum():>6,}")
    p(f"  Different names in group (Case B):         {name_conflict.sum():>6,}  ← Splink should split")
    p(f"  No ORCID + multi-inst + multi-div:         {multi_inst_div.sum():>6,}  ← ambiguous before Splink")

    # ── Section 6: Post-Splink comparison ────────────────────────────────────
    persons_path = PROCESSED_DATA / "arc_persons.parquet"
    if persons_path.exists():
        persons = pd.read_parquet(persons_path)
        p(f"\n── Post-Splink arc_persons.parquet ──────────────────────────────────")
        p(f"  Clusters (persons):                        {len(persons):>6,}")
        p(f"  Multi-grant clusters:                      {(persons.n_grants>1).sum():>6,}")
        p(f"  Clusters with ORCID:                       {persons.orcids.apply(len).gt(0).sum():>6,}")
        p(f"  Clusters with 2+ institutions:             {persons.inst_arr.apply(len).gt(1).sum():>6,}")

        # Case B: clusters with 2+ distinct full first names
        def _distinct_full_firsts(full_names):
            firsts = set()
            for name in full_names:
                parts = name.strip().split()
                if parts:
                    tok = _full_first(parts[0])
                    if tok:
                        firsts.add(tok)
            return len(firsts)

        persons["n_full_firsts"] = persons["full_names"].apply(_distinct_full_firsts)
        case_b = persons[
            (persons.n_grants > 1)
            & (persons.n_full_firsts > 1)
            & (persons.orcids.apply(len) == 0)
        ]
        p(f"\n  Case B (different full first names merged, no ORCID): {len(case_b)}")
        p(f"  → These are most likely mis-merges")
        if len(case_b):
            p(f"\n  Top Case B clusters by grant count:")
            for _, row in case_b.sort_values("n_grants", ascending=False).head(20).iterrows():
                firsts = sorted({
                    _full_first(nm.strip().split()[0])
                    for nm in row.full_names
                    if nm.strip() and _full_first(nm.strip().split()[0])
                })
                insts = "; ".join(row.inst_arr[:3]) if len(row.inst_arr) else "—"
                p(f"    n={row.n_grants:>3}  [{row.cluster_id}]  names: {'; '.join(row.full_names[:4])}")
                p(f"           distinct firsts: {', '.join(firsts)}   insts: {insts}")

        # Case A: suspicious — single name, no ORCID, multi-institution
        # Use full_names to check if all start with same first-name token
        def _single_first_token(full_names):
            firsts = set()
            for name in full_names:
                parts = name.strip().split()
                if parts:
                    firsts.add(parts[0].lower())
            return len(firsts) <= 1

        persons["is_case_a_name"] = persons["full_names"].apply(_single_first_token)
        case_a = persons[
            (persons.n_grants > 1)
            & (persons.inst_arr.apply(len) > 1)
            & (persons.orcids.apply(len) == 0)
            & persons.is_case_a_name
        ]
        p(f"\n  Case A (same name, no ORCID, multi-institution): {len(case_a)}")
        p(f"  → Mostly career moves; some may be different people")
        if len(case_a):
            p(f"\n  Top Case A clusters (sorted by grant count):")
            for _, row in case_a.sort_values("n_grants", ascending=False).head(20).iterrows():
                insts = "; ".join(row.inst_arr[:4]) if len(row.inst_arr) else "—"
                fors  = "; ".join(row.for_names[:3]) if len(row.for_names) else "—"
                p(f"    n={row.n_grants:>3}  {'; '.join(row.full_names[:3])}")
                p(f"           insts: {insts}")
                p(f"           FOR:   {fors}")

    # ── Print and save ────────────────────────────────────────────────────────
    report = "\n".join(lines)
    print(report)
    out = PROFILES_OUT / "arc_name_analysis.txt"
    out.write_text(report, encoding="utf-8")
    print(f"\nSaved → {out}")


if __name__ == "__main__":
    main()
