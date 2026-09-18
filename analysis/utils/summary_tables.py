"""
analysis/utils/summary_tables.py

Population-level summary tables for publication -- distinct from analysis/utils/acif_report.py
(one ACIF at a time). Reads only results.db's `arc` table (one row per ACIF x grant, already
deduped -- see analysis/utils/results_db.py), same "results.db is the sole source" discipline
as the per-ACIF report.

CI/F terminology (2026-09-18, direct instruction -- established project usage, not invented
here): "CI/F" = the whole in-scope ACIF population, Chief Investigator OR Fellow -- literally
what "ACIF" (ARC Chief Investigator/Fellow) stands for. `is_fellowship` (investigators_raw.parquet's
own field, carried through onto `arc`) is THE flag for whether a record is a Fellow -- not an
inferred role_code list (direct correction, 2026-09-18: an earlier draft of this file classified
"CI" via `role_code = 'CI'` and fellowship tiers via role_code lists with no `is_fellowship` gate
at all; fixed so `is_fellowship`/`NOT is_fellowship` is the first, authoritative test everywhere
a CI/Fellow distinction is made, with role_code only used to sub-classify WITHIN the already-
`is_fellowship`-gated population into tiers). Every row in `arc` is scoped to config/scope.py's
KEEP_ROLES, which is exactly {CI} union {every fellowship role_code}, so CI/F, CI-only, and
Fellow-only are the natural three-way split of this population, not three independent
categories that happen to overlap by coincidence.

Fellowship tier mapping (early/middle/senior) is only confidently derivable from this project's
own already-established documentation for a subset of the 13 KEEP_ROLES fellowship codes:
  - Early:  DECRA, APD, APDI      (config/scope.py's own ECR_ROLES)
  - Middle: FT                     (CLAUDE.md repeatedly calls Future Fellowship "mid-career")
  - Senior: FL, FF, APF            (Laureate/Federation/Professorial -- all senior per CLAUDE.md)
The remaining fellowship codes (ARF, QEII, CI-DORA, DAATSIA, IRF, ARFI) have no equivalent
documented tier anywhere in this project -- rather than guess, they're kept in their own
"Other fellowship (tier not classified)" bucket, not forced into early/middle/senior.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb
import pandas as pd
from tabulate import tabulate

_TIER_CASE = """
    CASE
        WHEN NOT is_fellowship THEN 'Chief Investigator (non-fellowship)'
        WHEN role_code IN ('DECRA', 'APD', 'APDI') THEN 'Early-career fellowship'
        WHEN role_code = 'FT' THEN 'Mid-career fellowship'
        WHEN role_code IN ('FL', 'FF', 'APF') THEN 'Senior fellowship'
        ELSE 'Other fellowship (tier not classified)'
    END
"""

_TIER_ORDER = [
    "Early-career fellowship",
    "Mid-career fellowship",
    "Senior fellowship",
    "Other fellowship (tier not classified)",
    "Chief Investigator (non-fellowship)",
]


def _table(df: pd.DataFrame) -> str:
    if df.empty:
        return "(none)"
    return tabulate(df, headers="keys", tablefmt="github", showindex=False)


def _counts_by(con: duckdb.DuckDBPyConnection, group_expr: str, label: str) -> pd.DataFrame:
    return con.execute(f"""
        SELECT {group_expr} AS "{label}", COUNT(*) AS awards, COUNT(DISTINCT cluster_id) AS persons
        FROM arc
        GROUP BY {group_expr}
        ORDER BY awards DESC
    """).fetchdf()


def build_summary_report(con: duckdb.DuckDBPyConnection) -> str:
    """Full markdown summary-tables report, built entirely from results.db's `arc` table."""
    total_awards, total_persons = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT cluster_id) FROM arc"
    ).fetchone()

    by_scheme = _counts_by(
        con, "regexp_extract(grant_code, '^[A-Z]+')", "Scheme code"
    )
    by_scheme = by_scheme.sort_values("Scheme code")

    by_tier_raw = con.execute(f"""
        SELECT {_TIER_CASE} AS tier, COUNT(*) AS awards, COUNT(DISTINCT cluster_id) AS persons
        FROM arc
        GROUP BY tier
    """).fetchdf()
    by_tier = (
        by_tier_raw.set_index("tier").reindex(_TIER_ORDER).reset_index()
        .rename(columns={"tier": "Fellowship tier"})
    )

    # CI/F: the whole population (every row is CI or a fellowship role by construction of
    # KEEP_ROLES) -- ci_only/f_only/both are the distinct-person three-way split; at the raw
    # award-record level a record is always exactly one or the other (no overlap possible),
    # so "both" only has meaning for persons, not awards.
    n_ci_awards, n_ci_persons = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT cluster_id) FROM arc WHERE NOT is_fellowship"
    ).fetchone()
    n_f_awards, n_f_persons = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT cluster_id) FROM arc WHERE is_fellowship"
    ).fetchone()
    n_both_persons = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT cluster_id FROM arc WHERE NOT is_fellowship
            INTERSECT
            SELECT cluster_id FROM arc WHERE is_fellowship
        )
    """).fetchone()[0]
    n_ci_only_persons = n_ci_persons - n_both_persons
    n_f_only_persons = n_f_persons - n_both_persons

    lines = [
        "# ACIF population summary",
        "",
        "For eventual publication -- population-level counts only, no per-person detail. "
        "\"Awards\" = one investigator-role record on one grant (an ACIF holding 3 grants "
        "contributes 3 awards); \"Persons\" = distinct ACIF (deduped, cross-grant identity).",
        "",
        "## Total",
        "",
        _table(pd.DataFrame({
            "Metric": ["Total awards", "Total persons (distinct ACIF)"],
            "Count": [total_awards, total_persons],
        })),
        "",
        "## By scheme code",
        "",
        _table(by_scheme.rename(columns={"awards": "Awards", "persons": "Persons (distinct)"})),
        "",
        "## By fellowship tier",
        "",
        "Early = DECRA/APD/APDI, Middle = FT, Senior = FL/FF/APF -- see module docstring for why "
        "the remaining fellowship codes (ARF, QEII, CI-DORA, DAATSIA, IRF, ARFI) are kept "
        "unclassified rather than guessed into a tier.",
        "",
        _table(by_tier.rename(columns={"awards": "Awards", "persons": "Persons (distinct)"})),
        "",
        "## CI / Fellow",
        "",
        "\"CI/F\" = the whole ACIF population (Chief Investigator OR Fellow) -- every award in "
        "scope is one or the other by construction, so CI/F's own award/person counts equal the "
        "Total row above. CI-only/Fellow-only/Both are the distinct-person three-way split; a "
        "single award record is always exactly CI or Fellow, never both, so \"Both\" only applies "
        "at the person level.",
        "",
        _table(pd.DataFrame({
            "Category": ["CI/F (total)", "CI", "Fellow", "-- CI only (persons)", "-- Fellow only (persons)", "-- Both CI and Fellow (persons)"],
            "Awards": [total_awards, n_ci_awards, n_f_awards, "", "", ""],
            "Persons (distinct)": [total_persons, n_ci_persons, n_f_persons, n_ci_only_persons, n_f_only_persons, n_both_persons],
        })),
        "",
    ]

    return "\n".join(lines)
