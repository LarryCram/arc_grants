"""
analysis/utils/summary_tables.py

Population-level summary tables for publication -- distinct from analysis/utils/acif_report.py
(one ACIF at a time). Reads only results.db's `arc` table (one row per ACIF x grant, already
deduped) and `title` table (one row per ACIF, carries reliability_tier) -- see
analysis/utils/results_db.py -- same "results.db is the sole source" discipline as the
per-ACIF report.

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

Fellowship tier mapping (early/mid/senior) is sourced directly from config/scope.py's own
FELLOWSHIP_TIER -- the single canonical classification, user-confirmed 2026-09-18, covering
every one of KEEP_ROLES' 12 fellowship codes (nothing left unclassified):
  - Early:  DECRA, APD, APDI, CI-DORA, IRF, DAATSIA
  - Mid:    FT, QEII, ARF, ARFI
  - Senior: FF, FL, APF
Built FROM config/scope.py, not duplicated by hand here -- the same drift this project has
repeatedly found and fixed elsewhere (e.g. 01a_diagnose.py's own stale SCHEMES_OF_INTEREST
copy) is exactly what sourcing from the one canonical dict avoids.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb
import pandas as pd
from tabulate import tabulate

from config.scope import FELLOWSHIP_TIER


def _build_tier_case() -> str:
    """SQL CASE built from config.scope.FELLOWSHIP_TIER -- every fellowship role_code in
    KEEP_ROLES is covered by construction, so the ELSE branch is purely defensive (fires only
    if a future role_code is added to KEEP_ROLES without an accompanying FELLOWSHIP_TIER entry,
    not a gap that exists today)."""
    lines = ["CASE", "    WHEN NOT is_fellowship THEN 'Chief Investigator (non-fellowship)'"]
    for tier in ("Early-career", "Mid-career", "Senior"):
        codes = sorted(c for c, t in FELLOWSHIP_TIER.items() if t == tier)
        code_list = ", ".join(f"'{c}'" for c in codes)
        lines.append(f"    WHEN role_code IN ({code_list}) THEN '{tier} fellowship'")
    lines.append("    ELSE 'Other fellowship (tier not classified)'")
    lines.append("END")
    return "\n".join(lines)


_TIER_CASE = _build_tier_case()

_TIER_ORDER = [
    "Early-career fellowship",
    "Mid-career fellowship",
    "Senior fellowship",
    "Other fellowship (tier not classified)",
    "Chief Investigator (non-fellowship)",
]

# reliability_tier is a per-ACIF (per-cluster_id) field, not a per-award one -- it grades how
# strong the evidence is that a given ACIF's own ARC-side deduplication (Splink clustering +
# ORCID-based merge/split rules + manually-verified overrides, see CLAUDE.md) really is one
# real person. 1a = strongest (own ORCID, always consistent); 4/4u = weakest. Canonical order
# per CLAUDE.md's own documentation of the tier ladder.
_RELIABILITY_TIER_ORDER = ["1a", "1b", "1c", "2", "3", "4", "4u"]


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

    by_reliability_raw = con.execute("""
        SELECT t.reliability_tier AS tier, COUNT(*) AS awards, COUNT(DISTINCT a.cluster_id) AS persons
        FROM arc a
        JOIN title t ON t.cluster_id = a.cluster_id
        GROUP BY t.reliability_tier
    """).fetchdf()
    by_reliability = (
        by_reliability_raw.set_index("tier").reindex(_RELIABILITY_TIER_ORDER)
        .fillna(0).astype(int).reset_index()
        .rename(columns={"tier": "Reliability tier"})
    )

    by_tier_raw = con.execute(f"""
        SELECT {_TIER_CASE} AS tier, COUNT(*) AS awards, COUNT(DISTINCT cluster_id) AS persons
        FROM arc
        GROUP BY tier
    """).fetchdf()
    # reindex can introduce a row with no matching data (e.g. "Other fellowship" now that every
    # KEEP_ROLES fellowship code has a real tier) -- fillna(0) keeps the table showing a clean
    # 0 rather than tabulate's default "nan" rendering.
    by_tier = (
        by_tier_raw.set_index("tier").reindex(_TIER_ORDER).fillna(0).astype(int).reset_index()
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
        "## By reliability tier",
        "",
        "reliability_tier is a per-ACIF measure of how well-evidenced that ACIF's own identity "
        "is (ARC-internal deduplication: Splink clustering + ORCID-based merge/split rules + "
        "manually-verified overrides) -- NOT a measure of the ARC-OAX OpenAlex link. "
        "1a = strongest (own ORCID, always consistent) down to 4/4u = weakest.",
        "",
        _table(by_reliability.rename(columns={"awards": "Awards", "persons": "Persons (distinct)"})),
        "",
        "## By scheme code",
        "",
        _table(by_scheme.rename(columns={"awards": "Awards", "persons": "Persons (distinct)"})),
        "",
        "## By fellowship tier",
        "",
        "Early = DECRA/APD/APDI/CI-DORA/IRF/DAATSIA, Mid = FT/QEII/ARF/ARFI, Senior = FF/FL/APF "
        "-- config/scope.py's FELLOWSHIP_TIER, user-confirmed 2026-09-18.",
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
