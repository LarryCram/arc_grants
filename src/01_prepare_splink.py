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
from src.utils.names import make_expanded_for_tokens, name_part_tokens, strip_diacriticals


def _initials(toks: list[str]) -> list[str]:
    return [t[0] for t in toks if t]


def arc_name_arrays(first_name: str, family_name: str) -> dict:
    full = f"{first_name or ''} {family_name or ''}".strip()
    hn = HumanName(full)

    # Mononym: nameparser puts single tokens in first, not last
    if not hn.last and hn.first:
        hn.last = hn.first

    f_toks = name_part_tokens(hn.first) + name_part_tokens(hn.middle)
    fam_norm = strip_diacriticals(hn.last).lower().strip() if hn.last else ""

    first_names = list(set(f_toks + _initials(f_toks)))
    family_names = [fam_norm] if fam_norm else []

    if not f_toks and fam_norm:
        first_names.append(fam_norm[0])

    return {"first_names": list(set(first_names)), "family_names": family_names}


def oax_name_arrays(display_name: str, alts: list[str]) -> dict:
    first_toks: set[str] = set()
    family_toks: set[str] = set()

    def _parse_name(n: str) -> None:
        if not n:
            return
        hn = HumanName(n)
        if not hn.last and hn.first:
            hn.last = hn.first
        for ft in name_part_tokens(hn.first) + name_part_tokens(hn.middle):
            first_toks.add(ft)
            first_toks.add(ft[0])
        fam_norm = strip_diacriticals(hn.last).lower().strip() if hn.last else ""
        if fam_norm:
            family_toks.add(fam_norm)

    # Primary: display_name is always "First Last" and curated by OAX.
    _parse_name(display_name)

    # Fallback: only use alternatives when display_name yields no family name.
    # Alternatives are contaminated with co-author names (OAX disambiguation errors)
    # and include reversed "Last, First" / "Last First" forms; avoid unless necessary.
    if not family_toks and alts:
        for alt in alts:
            _parse_name(alt)

    # Mononym fallback: when no given-name tokens exist, seed with family initial.
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

    print(f"\n[3/3] Building OAX term-frequency tables...")
    import pandas as pd

    df_oax = pd.read_parquet(out_oax)
    n = len(df_oax)

    def _max_by_len(lst):
        if lst is None or len(lst) == 0:
            return None
        return max(lst, key=len)

    def _canonical(row):
        full = row["first_name_full"]
        inits = row["first_initials"]
        if full is not None and len(full) > 0:
            return max(full, key=len)
        if inits is not None and len(inits) > 0:
            return inits[0]
        return None

    df_oax["family_name_main"]     = df_oax["family_names"].apply(_max_by_len)
    df_oax["first_name_canonical"] = df_oax.apply(_canonical, axis=1)
    df_oax["full_name_key"] = df_oax.apply(
        lambda r: f"{r['first_name_canonical']}_{r['family_name_main']}"
        if r["first_name_canonical"] is not None and r["family_name_main"] is not None
        else None,
        axis=1,
    )

    for col, fname in [
        ("family_name_main",     "oax_tf_family_name.parquet"),
        ("first_name_canonical", "oax_tf_first_name.parquet"),
        ("full_name_key",        "oax_tf_full_name.parquet"),
    ]:
        counts = df_oax[col].dropna().value_counts()
        tf = counts.reset_index()
        tf.columns = [col, f"tf_{col}"]
        tf[f"tf_{col}"] = tf[f"tf_{col}"] / n
        tf.to_parquet(PROCESSED_DATA / fname, index=False)
        print(f"  {fname}: {len(tf):,} unique values")

    print("Data preparation complete!")

if __name__ == "__main__":
    main()
