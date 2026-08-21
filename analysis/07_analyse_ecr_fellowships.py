"""
Early-career fellowship (DECRA/APD/APDI) cohort analysis — supports the ECR "prompt paper".

Scope note (2026-08-08): the early-career cohort is DECRA + APD + APDI only. APF ("Australian
Professorial Fellowship") is deliberately excluded — despite the similar-looking code, ARC's own
role_name confirms it's a senior fellowship, not postdoctoral (see CLAUDE.md "Early-Career
Fellowship Cohort Review"). ARF/QEII/ARFI/IRF are not part of this cohort either.

Functions:
  build_cohort()            — DECRA/APD/APDI persons linked to an OpenAlex author, one row each:
                               cluster_id, name, oax_id, award_year, for_codes, for_names
  load_panel_map()          — data_persisted/for_panels_2020.csv -> code->panel lookup (group-level
                               overrides checked before division-level fallback)
  add_for_division_panel()  — annotate a cohort frame with for_division (FOR2020 division label)
                               and panel (BSB/EIC/HCA/MPCE/SBE) columns
  year_crosstab()           — Year x {division|panel} table with both margins (row + column totals);
                               single_years=True sub-samples exact 5-year-spaced years instead of
                               summing across bins
  project_scaled_cohort()   — proportional what-if: redistribute a target total N across the
                               current field/panel mix for a given anchor year

Usage:
  python analysis/07_analyse_ecr_fellowships.py
    -> writes summary table + division/panel crosstabs (single-year and 5-year-bin) to
       OUTPUT_ROOT/analysis/ecr_fellowships/
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd
from research_classification import Resolver

from config.settings import PROCESSED_DATA, OUTPUT_ROOT
from config.scope import KEEP_SCHEMES
from src.utils.for_resolve import upgrade_for_code

ECR_ROLES = ("DECRA", "APD", "APDI")  # APF excluded -- see module docstring

ANALYSIS_OUT = OUTPUT_ROOT / "analysis" / "ecr_fellowships"
PANEL_MAP_CSV = Path(__file__).parent.parent / "data_persisted" / "for_panels_2020.csv"

ARC_PERSONS  = str(PROCESSED_DATA / "awards_cif.parquet")
INV_RAW      = str(PROCESSED_DATA / "investigators_raw.parquet")
GRANT_MAP    = str(PROCESSED_DATA / "arc_grant_cluster_map.parquet")
GRANTS_FLAT  = str(PROCESSED_DATA / "grants_flat.parquet")
OAX_RESOLVED = str(PROCESSED_DATA / "arc_oax_resolved.parquet")


def build_cohort(con: duckdb.DuckDBPyConnection, roles=ECR_ROLES, linked_only=True) -> pd.DataFrame:
    """One row per early-career person: cluster_id, name, oax_id, award_year, for_codes, for_names.

    award_year is the earliest funding_commence_year among the person's grants in `roles`.
    linked_only=True restricts to persons with a resolved OpenAlex author (arc_oax_resolved.parquet).
    """
    roles_sql   = ", ".join(f"'{r}'" for r in roles)
    schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)

    df = con.execute(f"""
        WITH inv AS (
            SELECT DISTINCT i.unique_id, i.grant_code
            FROM read_parquet('{INV_RAW}') i
            WHERE i.role_code IN ({roles_sql}) AND substring(i.grant_code,1,2) IN ({schemes_sql})
        ),
        mapped AS (
            SELECT DISTINCT m.cluster_id, inv.grant_code
            FROM inv JOIN read_parquet('{GRANT_MAP}') m ON m.unique_id = inv.unique_id
        ),
        person_year AS (
            SELECT m.cluster_id, MIN(g.funding_commence_year) AS award_year
            FROM mapped m LEFT JOIN read_parquet('{GRANTS_FLAT}') g ON m.grant_code = g.grant_code
            GROUP BY 1
        )
        SELECT p.cluster_id, p.full_names, p.for_names, p.for2020_codes,
               py.award_year, l.oax_id
        FROM read_parquet('{ARC_PERSONS}') p
        JOIN person_year py ON p.cluster_id = py.cluster_id
        LEFT JOIN read_parquet('{OAX_RESOLVED}') l ON p.cluster_id = l.arc_id
    """).fetchdf()

    if linked_only:
        df = df[df["oax_id"].notna()].copy()

    df["name"] = df["full_names"].apply(lambda x: x[0] if len(x) else "")
    df["for_field"] = df["for_names"].apply(lambda x: " / ".join(x))
    # for2020_codes is the full per-grant FOR2020 code list (every code ARC recorded, not just
    # the primary one -- see CLAUDE.md "Scope and FOR-code fixes"); arc_persons.parquet's own
    # plain `for_codes` column is a stale, primary-only field that this function used to read
    # instead, silently dropping every secondary code (found 2026-08-18, Amanda Macdonald's
    # dossier showing only 4702 when her ACIF has both 4702 and 4703). Already ordered
    # primary-first by awards_cif.py/01_prepare_arc.py, so no re-sorting needed here.
    df["for_codes"] = df["for2020_codes"].apply(lambda entries: [e["code"] for e in entries])
    df["award_year"] = df["award_year"].astype("Int64")
    return df[["cluster_id", "name", "oax_id", "award_year", "for_field", "for_codes"]]


def load_panel_map(csv_path: Path = PANEL_MAP_CSV) -> tuple[dict, dict]:
    """Return (group_panel, div_panel) dicts from data_persisted/for_panels_2020.csv."""
    panels = pd.read_csv(csv_path, dtype=str)
    group_panel = dict(zip(panels[panels["level"] == "group"]["code"], panels[panels["level"] == "group"]["panel"]))
    div_panel   = dict(zip(panels[panels["level"] == "division"]["code"], panels[panels["level"] == "division"]["panel"]))
    return group_panel, div_panel


def _panel_for_code(code20: str, group_panel: dict, div_panel: dict) -> str:
    if not code20:
        return "(unmapped)"
    if code20 in group_panel:
        return group_panel[code20]
    if code20[:2] in div_panel:
        return div_panel[code20[:2]]
    return "(unmapped)"


def add_for_division_panel(df: pd.DataFrame, resolver: Resolver | None = None) -> pd.DataFrame:
    """Annotate df (must have a for_codes column, list-of-str per row) with for_division and panel.

    Uses the *first* FOR2020-resolvable code per person as the primary field for these two columns
    (for_field, kept separately by build_cohort, retains the full multi-code text).
    """
    resolver = resolver or Resolver()
    div_names = dict(zip(
        resolver._con.execute("SELECT code, label FROM for_2020 WHERE level='division'").fetchdf()["code"],
        resolver._con.execute("SELECT code, label FROM for_2020 WHERE level='division'").fetchdf()["label"],
    ))
    group_panel, div_panel = load_panel_map()

    def _primary_20(codes):
        # codes is a list (fresh from build_cohort) or a numpy array (round-tripped via CSV) --
        # both iterate the same way, no parsing needed either way.
        for c in codes:
            c20 = upgrade_for_code(str(c))
            if c20:
                return c20
        return None

    df = df.copy()
    primary_code20 = df["for_codes"].apply(_primary_20)
    df["for_division"] = primary_code20.apply(lambda c: div_names.get(c[:2], f"div {c[:2]}") if c else "(unknown)")
    df["panel"] = primary_code20.apply(lambda c: _panel_for_code(c, group_panel, div_panel) if c else "(unmapped)")
    return df


def year_crosstab(df: pd.DataFrame, group_col: str, single_years: bool = False, anchor: int = 5) -> pd.DataFrame:
    """Year x group_col table with both margins (row Total + column Total).

    single_years=True: only rows where award_year % anchor == 0, counted individually
                        (e.g. exactly 2005, 2010, 2015... not summed across a window).
    single_years=False: award_year binned into `anchor`-year windows and summed.
    """
    d = df.dropna(subset=["award_year"]).copy()
    if single_years:
        d = d[d["award_year"] % anchor == 0]
        d["year_col"] = d["award_year"].astype("Int64").astype(str)
    else:
        d["year_col"] = (d["award_year"] // anchor * anchor).astype("Int64").astype(str) + "s"

    ct = pd.crosstab(d[group_col], d["year_col"], margins=True, margins_name="Total")
    year_cols = sorted(c for c in ct.columns if c != "Total")
    ct = ct[year_cols + ["Total"]]
    return ct.sort_values("Total", ascending=False)


def project_scaled_cohort(df: pd.DataFrame, group_col: str, anchor_year: int, target_total: int) -> pd.DataFrame:
    """What-if: redistribute target_total proportionally across the anchor_year's group_col mix.

    Purely proportional (assumes the expansion preserves the current field/panel balance) --
    a policy-driven expansion could easily target specific groups disproportionately instead;
    this is a naive baseline for comparison, not a forecast.
    """
    actual = df[df["award_year"] == anchor_year][group_col].value_counts().rename(f"actual_{anchor_year}")
    proj = (actual / actual.sum() * target_total).round(0).astype(int).rename(f"proportional_{target_total}")
    out = pd.concat([actual, proj, (proj - actual).rename("delta")], axis=1)
    return out.sort_values(f"actual_{anchor_year}", ascending=False)


def main():
    print("=== 07_analyse_ecr_fellowships ===")
    ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    print("[1/3] Building linked ECR cohort (DECRA/APD/APDI)...")
    cohort = build_cohort(con)
    print(f"  {len(cohort)} linked persons")

    print("[2/3] Annotating FOR division + panel...")
    cohort = add_for_division_panel(cohort)
    out_summary = ANALYSIS_OUT / "ecr_linked_summary.csv"
    cohort.drop(columns=["for_codes"]).to_csv(out_summary, index=False)
    print(f"  -> {out_summary}")

    print("[3/3] Building year crosstabs...")
    for group_col in ("for_division", "panel"):
        for single_years, tag in [(False, "5yrbin"), (True, "singleyear")]:
            ct = year_crosstab(cohort, group_col, single_years=single_years)
            out = ANALYSIS_OUT / f"ecr_{tag}_{group_col}_crosstab.csv"
            ct.to_csv(out)
            print(f"  -> {out}")

    print("\nDone.")


if __name__ == "__main__":
    main()
