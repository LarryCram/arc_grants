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

from config.settings import PROCESSED_DATA, OUTPUT_ROOT, OPENALEX_DIR, ADMIN_ORGS_CSV
from config.scope import KEEP_SCHEMES
from analysis.utils.dossier import AwardContext, Dossier, PileDiagnostic, Work, YearRecord
from analysis.utils.dedup import create_deduped_works, count_exclusions
from src.utils.oeuvre_build import AUTH_GLOB, STAGE3_SURVIVORS
from src.utils.work_piling import PILING_RESULTS
from src.utils.cluster_checks import for2020_all_fields, for2020_all_subfields
from importlib import import_module

_ecr = import_module("analysis.07_analyse_ecr_fellowships")

ARC_PERSONS = str(PROCESSED_DATA / "arc_persons.parquet")
GRANT_MAP = str(PROCESSED_DATA / "arc_grant_cluster_map.parquet")
GRANTS_FLAT = str(PROCESSED_DATA / "grants_flat.parquet")
INV_RAW = str(PROCESSED_DATA / "investigators_raw.parquet")
OEUVRES = str(OUTPUT_ROOT / "analysis" / "oeuvres.parquet")
ANNUAL_METRICS = str(OUTPUT_ROOT / "analysis" / "annual_metrics.parquet")
AWARDS_CIF = str(PROCESSED_DATA / "awards_cif.parquet")

_admin_orgs = pd.read_csv(ADMIN_ORGS_CSV)
HEP_CODE_TO_NAME: dict[str, str] = dict(
    _admin_orgs[_admin_orgs["hep_code"].notna()]
    .drop_duplicates("hep_code")[["hep_code", "institution_name"]]
    .itertuples(index=False, name=None)
)


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
    """Every grant this cluster holds any role on -- not scoped to ECR_ROLES (2026-08-18
    correction: an ACIF is a whole person, not one award episode, so filtering the award list
    to ECR-role grants only silently dropped real grants -- e.g. Andrew Burrow's DP0985878,
    where his role is CI -- from his own record. An ECR-specific view should filter on
    award_year/career_age_at_award instead of hiding role_code='CI' grants outright."""
    rows = con.execute(f"""
        SELECT DISTINCT g.grant_code, g.scheme_name, i.role_code,
               CAST(g.funding_commence_year AS INTEGER) AS award_year
        FROM read_parquet('{GRANT_MAP}') m
        JOIN read_parquet('{INV_RAW}') i ON m.unique_id = i.unique_id
        JOIN read_parquet('{GRANTS_FLAT}') g ON i.grant_code = g.grant_code
        WHERE m.cluster_id = ?
        ORDER BY award_year
    """, [cluster_id]).fetchall()

    out = []
    for grant_code, scheme_name, role_code, award_year in rows:
        others = con.execute(f"""
            SELECT DISTINCT i.first_name, i.family_name, i.role_code
            FROM read_parquet('{INV_RAW}') i
            WHERE i.grant_code = ? AND i.unique_id NOT IN (
                SELECT unique_id FROM read_parquet('{GRANT_MAP}') WHERE cluster_id = ?
            )
            ORDER BY 1, 2
        """, [grant_code, cluster_id]).fetchall()
        other_investigators = [f"{fn} {ln} ({rc})" for fn, ln, rc in others]
        out.append(AwardContext(
            grant_id=grant_code,
            scheme=scheme_name,
            role_code=role_code,
            award_year=award_year,
            career_age_at_award=(award_year - first_pub_year) if (first_pub_year and award_year) else None,
            other_investigators=other_investigators,
        ))
    return out


def _fetch_acif_fields_subfields(cluster_id: str, con: duckdb.DuckDBPyConnection) -> tuple[list[str], list[str]]:
    """This ACIF's own declared FOR2020 codes (awards_cif.parquet), resolved to OAX field/
    subfield sets -- what piling's channel_piles() checks each pile against. Empty ([], []) if
    the ACIF isn't in awards_cif.parquet (e.g. excluded) or has no resolvable FOR codes."""
    row = con.execute(
        f"SELECT for2020_codes FROM read_parquet('{AWARDS_CIF}') WHERE cluster_id = ?", [cluster_id]
    ).fetchone()
    if not row or row[0] is None:
        return [], []
    codes = [dict(c) for c in row[0]]
    return sorted(for2020_all_fields(codes)), sorted(for2020_all_subfields(codes))


def _fetch_acif_institutions(cluster_id: str, con: duckdb.DuckDBPyConnection) -> list[str]:
    """This ACIF's own HEP institutions (awards_cif.parquet's hep_codes -- union across every
    grant's eligible_orgs, not just the administering org), resolved to full names via
    admin_orgs.csv where available -- "Name (CODE)", falling back to the bare code if a code
    has no name mapping. What piling's hep_match is checked against."""
    row = con.execute(
        f"SELECT hep_codes FROM read_parquet('{AWARDS_CIF}') WHERE cluster_id = ?", [cluster_id]
    ).fetchone()
    if not row or row[0] is None:
        return []
    return [
        f"{HEP_CODE_TO_NAME[code]} ({code})" if code in HEP_CODE_TO_NAME else code
        for code in sorted(row[0])
    ]


def _fetch_piling_diagnostics(cluster_id: str, con: duckdb.DuckDBPyConnection) -> list[PileDiagnostic]:
    """Per-pile corroboration diagnostics from oeuvre_piling_results.parquet (src/utils/
    work_piling.py) -- excludes noise (pile_id=-1). Empty list if this ACIF hasn't been piled
    yet (not the same as "piled and found nothing" -- piling coverage is still being
    backfilled across the population), or has fewer than 2 candidate works (not evaluated)."""
    piles = con.execute(f"""
        SELECT pile_id, count(*) AS n_works,
               any_value(orcid_match) AS orcid_match, any_value(hep_match) AS hep_match,
               any_value(field_match) AS field_match, any_value(subfield_match) AS subfield_match,
               any_value(confirmed) AS confirmed
        FROM read_parquet('{PILING_RESULTS}')
        WHERE cluster_id = ? AND pile_id != -1
        GROUP BY pile_id
        ORDER BY n_works DESC
    """, [cluster_id]).fetchdf()
    if piles.empty:
        return []

    work_pile = con.execute(f"""
        SELECT pile_id, work_idx FROM read_parquet('{PILING_RESULTS}')
        WHERE cluster_id = ? AND pile_id != -1
    """, [cluster_id]).fetchdf()

    surv = con.execute(f"""
        SELECT work_idx, field_names, subfield_names, source_author_idxs
        FROM read_parquet('{STAGE3_SURVIVORS}')
        WHERE cluster_id = ?
    """, [cluster_id]).fetchdf().set_index("work_idx")

    own_idxs: set[int] = set()
    for lst in surv["source_author_idxs"]:
        if lst is not None:
            own_idxs.update(int(x) for x in lst)

    work_list = [int(w) for w in work_pile["work_idx"].tolist()]
    coa = pd.DataFrame(columns=["work_idx", "display_name"])
    if work_list:
        wsql = ",".join(str(w) for w in work_list)
        excl_sql = ",".join(str(x) for x in own_idxs) or "-1"
        coa = con.execute(f"""
            SELECT au.work_idx, a.display_name
            FROM read_parquet('{AUTH_GLOB}') au
            JOIN read_parquet('{OPENALEX_DIR}/authors/*.parquet') a ON a.author_idx = au.author_idx
            WHERE au.work_idx IN ({wsql}) AND au.author_idx NOT IN ({excl_sql})
        """).fetchdf()

    out = []
    for row in piles.itertuples():
        pw = work_pile[work_pile["pile_id"] == row.pile_id]["work_idx"].tolist()
        pfields, psub = set(), set()
        for w in pw:
            if w in surv.index:
                fn, sn = surv.loc[w, "field_names"], surv.loc[w, "subfield_names"]
                if fn is not None:
                    pfields.update(list(fn))
                if sn is not None:
                    psub.update(list(sn))
        coauthor_names = sorted(coa[coa["work_idx"].isin(pw)]["display_name"].dropna().unique().tolist())
        out.append(PileDiagnostic(
            pile_id=int(row.pile_id), n_works=int(row.n_works),
            orcid_match=row.orcid_match, hep_match=row.hep_match,
            field_match=row.field_match, subfield_match=row.subfield_match,
            confirmed=row.confirmed,
            pile_fields=sorted(pfields), pile_subfields=sorted(psub),
            coauthor_names=coauthor_names,
        ))
    return out


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
    acif_fields, acif_subfields = _fetch_acif_fields_subfields(cluster_id, con)
    acif_institutions = _fetch_acif_institutions(cluster_id, con)
    piles = _fetch_piling_diagnostics(cluster_id, con)

    return Dossier(
        arc_id=cluster_id,
        preferred_name=preferred_name,
        name_variants=name_variants,
        for_codes=list(cohort_row["for_codes"]),
        for_division=cohort_row.get("for_division"),
        panel=cohort_row.get("panel"),
        oax_id=cohort_row["oax_id"],
        ecr_roles=ecr_roles,
        acif_fields=acif_fields,
        acif_subfields=acif_subfields,
        acif_institutions=acif_institutions,
        piles=piles,
        works=works,
        annual_series=annual_series,
        first_pub_year=first_pub_year,
        excluded_work_counts=excluded_work_counts,
        award_contexts=award_contexts,
    )
