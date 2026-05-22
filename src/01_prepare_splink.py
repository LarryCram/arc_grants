"""
src/data_prep.py

Prepares ARC and OpenAlex datasets for Splink linkage.
Uses DuckDB Python UDFs to process names (unidecode, tokenization) directly 
in the database to avoid memory overflow.
"""

import sys
from pathlib import Path
import duckdb
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROCESSED_DATA, ADMIN_ORGS_CSV, GRANT_SUMMARIES_CSV
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import make_expanded_for_tokens, name_part_tokens


def _initials(toks: list[str]) -> list[str]:
    return [t[0] for t in toks if t]


def arc_name_arrays(first_name: str, family_name: str) -> dict:
    full = f"{first_name or ''} {family_name or ''}".strip()
    hn = HumanName(full)

    # Mononym: nameparser puts single tokens in first, not last
    if not hn.last and hn.first:
        hn.last = hn.first

    f_toks = name_part_tokens(hn.first) + name_part_tokens(hn.middle)
    fam_toks = name_part_tokens(hn.last)

    first_names = list(set(f_toks + _initials(f_toks)))
    family_names = list(set(fam_toks))

    # Mononym fallback: only when there are no given-name tokens (e.g. "Cher").
    # Do NOT fire for normal names — adding the family-name initial to first_names
    # would pollute matching (e.g. "obrien"[0] = "o" falsely appearing as a given name).
    if not f_toks:
        for fam in family_names:
            if fam:
                first_names.append(fam[0])

    return {"first_names": list(set(first_names)), "family_names": family_names}


def oax_name_arrays(display_name: str, alts: list[str]) -> dict:
    all_names = [display_name] if display_name else []
    if alts:
        all_names.extend(alts)

    first_toks: set[str] = set()
    family_toks: set[str] = set()

    for n in all_names:
        if not n:
            continue
        hn = HumanName(n)

        if not hn.last and hn.first:
            hn.last = hn.first

        f_t = name_part_tokens(hn.first) + name_part_tokens(hn.middle)
        fam_t = name_part_tokens(hn.last)

        for ft in f_t:
            first_toks.add(ft)
            first_toks.add(ft[0])
        family_toks.update(fam_t)

    # Mononym fallback: only when no given-name tokens were found across all names
    if not first_toks:
        for fam in family_toks:
            if fam:
                first_toks.add(fam[0])

    return {"first_names": list(first_toks), "family_names": list(family_toks)}

def main():
    print("Initializing DuckDB and registering UDFs...")
    con = duckdb.connect()
    
    con.create_function("arc_names", arc_name_arrays, 
                        ['VARCHAR', 'VARCHAR'], 
                        'STRUCT(first_names VARCHAR[], family_names VARCHAR[])')
    
    con.create_function("oax_names", oax_name_arrays,
                        ['VARCHAR', 'VARCHAR[]'],
                        'STRUCT(first_names VARCHAR[], family_names VARCHAR[])')

    concordance_csv = Path(__file__).resolve().parents[1] / "config" / "for_concordance.csv"
    expanded_for_tokens = make_expanded_for_tokens(str(concordance_csv))
    con.create_function("for_tokens", expanded_for_tokens,
                        ['VARCHAR'],
                        'VARCHAR[]')

    out_arc = PROCESSED_DATA / "arc_investigators_prep.parquet"
    out_oax = PROCESSED_DATA / "openalex_authors_prep.parquet"

    roles_sql = ", ".join(f"'{r}'" for r in KEEP_ROLES)
    schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)

    print(f"\n[1/2] Processing ARC Investigators (saving to {out_arc})...")
    
    con.execute(f"""
        COPY (
            WITH arc_raw AS (
                SELECT
                    i.unique_id,
                    i.grant_code as grant_id,
                    i.first_name,
                    i.family_name,
                    g.admin_org as AdminOrg,
                    i.role_code as role,
                    i.orcid,
                    g.primary_for_name as for_name,
                    regexp_extract(s.primary_field_of_research, '^\\d{{4}}') as for_code
                FROM '{PROCESSED_DATA}/investigators_raw.parquet' i
                LEFT JOIN '{PROCESSED_DATA}/grants_flat.parquet' g
                    ON i.grant_code = g.grant_code
                LEFT JOIN read_csv_auto('{GRANT_SUMMARIES_CSV}') s
                    ON i.grant_code = s.grant_id
                WHERE i.role_code IN ({roles_sql})
                  AND substring(i.grant_code, 1, 2) IN ({schemes_sql})
            )
            SELECT
                a.unique_id,
                CONCAT_WS(' ', a.first_name, a.family_name) AS full_name,
                arc_names(a.first_name, a.family_name).first_names AS first_names,
                list_filter(arc_names(a.first_name, a.family_name).first_names, x -> len(x) = 1)  AS first_initials,
                list_filter(arc_names(a.first_name, a.family_name).first_names, x -> len(x) > 1)  AS first_name_full,
                arc_names(a.first_name, a.family_name).family_names AS family_names,
                a.orcid,
                o.institution_id as institution_oax_id,
                [a.for_name] as for_names,
                list_filter([a.for_code], x -> x != '') as for_codes,
                for_tokens(a.for_name) as for_name_tokens,
                'AU' as country_code
            FROM arc_raw a
            INNER JOIN read_csv_auto('{ADMIN_ORGS_CSV}') o
                ON a.AdminOrg = o.organisationName_alias
        ) TO '{out_arc}' (FORMAT PARQUET)
    """)

    print(f"\n[2/2] Processing OpenAlex Authors (saving to {out_oax})...")
    
    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT 
                    id AS unique_id,
                    display_name AS full_name,
                    oax_names(display_name, display_name_alternatives) AS parsed,
                    replace(orcid, 'https://orcid.org/', '') AS orcid,
                    list_distinct(list_transform(
                        list_filter(affiliations, x -> x.institution.country_code = 'AU'),
                        x -> x.institution.id
                    )) AS inst_ids,
                    list_transform(topics, x -> x.display_name) as topic_names,
                    list_transform(topics, x -> x.subfield.display_name) as subfield_names
                FROM read_parquet('{OAX_AUTHORS}/*.parquet')
                WHERE list_contains(
                    list_transform(affiliations, x -> x.institution.country_code), 'AU'
                )
            )
            SELECT
                unique_id,
                full_name,
                parsed.first_names AS first_names,
                list_filter(parsed.first_names, x -> len(x) = 1)  AS first_initials,
                list_filter(parsed.first_names, x -> len(x) > 1)  AS first_name_full,
                parsed.family_names AS family_names,
                orcid,
                inst_ids,
                topic_names,
                subfield_names,
                'AU' as country_code
            FROM base
        ) TO '{out_oax}' (FORMAT PARQUET)
    """)
    print("Data preparation complete!")

if __name__ == "__main__":
    main()
