"""
analysis/utils/dossier_build.py

`build_dossier()`: constructs a Dossier from currently-persisted tables only. Research-mode
partial version -- most of the plan's upstream tables (outlet columns, own_institution_metrics,
coauthor_track_record_at_award, work_topic_diversity, Uzzi novelty) don't exist yet, so those
fields are left at their empty/None defaults rather than faked. Fields populated now: identity,
award_contexts (grant_id/scheme/award_year/career_age_at_award, no coauthor data yet), works
(field/type/title/year/citations only -- no outlet/institution), annual_series.

Reuses analysis/07_analyse_ecr_fellowships.py's build_cohort()/add_for_division_panel() for
cohort membership and panel classification rather than re-deriving that logic here.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
import pandas as pd

from config.settings import PROCESSED_DATA, OUTPUT_ROOT
from config.scope import KEEP_SCHEMES
from analysis.utils.dossier import AwardContext, Dossier, Work, YearRecord
from analysis.utils.dedup import create_deduped_works, count_exclusions
from importlib import import_module

_ecr = import_module("analysis.07_analyse_ecr_fellowships")

ARC_PERSONS = str(PROCESSED_DATA / "arc_persons.parquet")
GRANT_MAP = str(PROCESSED_DATA / "arc_grant_cluster_map.parquet")
GRANTS_FLAT = str(PROCESSED_DATA / "grants_flat.parquet")
INV_RAW = str(PROCESSED_DATA / "investigators_raw.parquet")
OEUVRES = str(OUTPUT_ROOT / "analysis" / "oeuvres.parquet")
ANNUAL_METRICS = str(OUTPUT_ROOT / "analysis" / "annual_metrics.parquet")


def _fetch_ecr_roles(cluster_id: str, con: duckdb.DuckDBPyConnection) -> list[str]:
    roles_sql = ", ".join(f"'{r}'" for r in _ecr.ECR_ROLES)
    rows = con.execute(f"""
        SELECT DISTINCT i.role_code
        FROM read_parquet('{GRANT_MAP}') m
        JOIN read_parquet('{INV_RAW}') i ON m.unique_id = i.unique_id
        WHERE m.cluster_id = ? AND i.role_code IN ({roles_sql})
        ORDER BY 1
    """, [cluster_id]).fetchall()
    return [r[0] for r in rows]


def _fetch_award_contexts(cluster_id: str, con: duckdb.DuckDBPyConnection, first_pub_year: int | None) -> list[AwardContext]:
    roles_sql = ", ".join(f"'{r}'" for r in _ecr.ECR_ROLES)
    rows = con.execute(f"""
        SELECT DISTINCT g.grant_code, g.scheme_name, CAST(g.funding_commence_year AS INTEGER) AS award_year
        FROM read_parquet('{GRANT_MAP}') m
        JOIN read_parquet('{INV_RAW}') i ON m.unique_id = i.unique_id
        JOIN read_parquet('{GRANTS_FLAT}') g ON i.grant_code = g.grant_code
        WHERE m.cluster_id = ? AND i.role_code IN ({roles_sql})
        ORDER BY award_year
    """, [cluster_id]).fetchall()
    return [
        AwardContext(
            grant_id=grant_code,
            scheme=scheme_name,
            award_year=award_year,
            career_age_at_award=(award_year - first_pub_year) if (first_pub_year and award_year) else None,
        )
        for grant_code, scheme_name, award_year in rows
    ]


def _fetch_works(cluster_id: str, con: duckdb.DuckDBPyConnection) -> tuple[list[Work], dict[str, int]]:
    safe_id = cluster_id.replace("'", "''")  # arc_ids are pipeline-controlled, but guard anyway
    person_oeuvres = f"(SELECT * FROM read_parquet('{OEUVRES}') WHERE arc_id = '{safe_id}' AND is_primary_author_id = TRUE)"

    count_exclusions(con, person_oeuvres, out_table="_dossier_exclusions")
    excluded = dict(con.execute("SELECT reason, n FROM _dossier_exclusions").fetchall())

    create_deduped_works(con, person_oeuvres, out_table="_dossier_deduped_works")
    rows = con.execute("""
        SELECT work_idx, publication_year, cited_by_count, type, title, field_name, subfield_name, domain_name
        FROM _dossier_deduped_works
        ORDER BY publication_year
    """).fetchall()
    works = [
        Work(
            work_idx=r[0], publication_year=r[1], cited_by_count=r[2], type=r[3], title=r[4],
            field_name=r[5], subfield_name=r[6], domain_name=r[7],
        )
        for r in rows
    ]
    return works, excluded


def _fetch_annual_series(cluster_id: str, con: duckdb.DuckDBPyConnection) -> tuple[list[YearRecord], int | None]:
    rows = con.execute(f"""
        SELECT year, first_pub_year, n_pubs, n_citations_snapshot, h_index, n_works_cumul,
               total_citations_cumul, top_field, n_highly_cited
        FROM read_parquet('{ANNUAL_METRICS}')
        WHERE arc_id = ?
        ORDER BY year
    """, [cluster_id]).fetchall()
    if not rows:
        return [], None
    first_pub_year = rows[0][1]
    series = [
        YearRecord(
            year=r[0], n_pubs=r[2], n_citations_snapshot=r[3], h_index=r[4],
            n_works_cumul=r[5], total_citations_cumul=r[6], top_field=r[7], n_highly_cited=r[8],
        )
        for r in rows
    ]
    return series, first_pub_year


def build_dossier(cohort_row: pd.Series, con: duckdb.DuckDBPyConnection) -> Dossier:
    """cohort_row: one row from build_cohort()/add_for_division_panel()'s output frame
    (must have cluster_id, name, oax_id, for_codes, for_division, panel)."""
    cluster_id = cohort_row["cluster_id"]

    person = con.execute(
        f"SELECT full_names FROM read_parquet('{ARC_PERSONS}') WHERE cluster_id = ?", [cluster_id]
    ).fetchone()
    full_names = list(person[0]) if person else [cohort_row["name"]]
    preferred_name = full_names[0]
    name_variants = full_names[1:]

    annual_series, first_pub_year = _fetch_annual_series(cluster_id, con)
    works, excluded_work_counts = _fetch_works(cluster_id, con)
    award_contexts = _fetch_award_contexts(cluster_id, con, first_pub_year)
    ecr_roles = _fetch_ecr_roles(cluster_id, con)

    return Dossier(
        arc_id=cluster_id,
        preferred_name=preferred_name,
        name_variants=name_variants,
        for_codes=list(cohort_row["for_codes"]),
        for_division=cohort_row.get("for_division"),
        panel=cohort_row.get("panel"),
        oax_id=cohort_row["oax_id"],
        ecr_roles=ecr_roles,
        works=works,
        annual_series=annual_series,
        first_pub_year=first_pub_year,
        excluded_work_counts=excluded_work_counts,
        award_contexts=award_contexts,
    )
