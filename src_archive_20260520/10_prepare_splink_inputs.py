"""
src/10_prepare_splink_inputs.py

Prepares ARC and OpenAlex datasets for Splink.
Uses DuckDB Python UDFs to process names (unidecode, tokenization) directly 
in the database to avoid pandas memory overflow on the 64GB machine.
Incorporates `display_name_alternatives` from OpenAlex.
"""

import sys
import re
import unicodedata
from pathlib import Path
import duckdb
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROCESSED_DATA

OAX_GLOB = f"{OAX_AUTHORS}.parquet"

# ── Name Normalisation Logic ──────────────────────────────────────────────────

_EXOTIC_HYPHENS = re.compile(r"[­‐‑‒–—―−－]")

def _strip_diacriticals(s: str) -> str:
    if not s:
        return ""
    s = _EXOTIC_HYPHENS.sub("-", s)
    s = s.replace("ı", "i").replace("İ", "I")
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")

def _tokens(name: str) -> list[str]:
    if not name:
        return []
    return re.findall(r"[a-z]+", _strip_diacriticals(name).lower())

def _generate_initials(toks: list[str]) -> list[str]:
    """Generate 1-letter initials for each token to allow initial-matching."""
    return [t[0] for t in toks if len(t) > 0]

# ── UDFs for ARC Investigators ───────────────────────────────────────────────

def arc_name_arrays(first_name: str, family_name: str) -> dict:
    """Parses ARC names with nameparser and returns token arrays including initials."""
    full = f"{first_name or ''} {family_name or ''}".strip()
    hn = HumanName(full)
    
    f_toks = _tokens(hn.first) + _tokens(hn.middle)
    fam_toks = _tokens(hn.last)
    
    first_names = list(set(f_toks + _generate_initials(f_toks)))
    family_names = list(set(fam_toks))
    
    return {"first_names": first_names, "family_names": family_names}

# ── UDFs for OpenAlex Authors ────────────────────────────────────────────────

def oax_name_arrays(display_name: str, alts: list[str]) -> dict:
    """Processes display name and alternatives into aggregated token arrays using nameparser."""
    all_names = []
    if display_name:
        all_names.append(display_name)
    if alts:
        all_names.extend(alts)
        
    first_toks = set()
    family_toks = set()
    
    for n in all_names:
        if not n:
            continue
        hn = HumanName(n)
        
        f_t = _tokens(hn.first) + _tokens(hn.middle)
        fam_t = _tokens(hn.last)
        
        for ft in f_t:
            first_toks.add(ft)
            first_toks.add(ft[0]) # Add initial
            
        for fam in fam_t:
            family_toks.add(fam)
            
    # If a name only had 1 token, we put it in family. 
    # Just in case, add its initial to first names to be safe.
    for fam in family_toks:
        if fam:
            first_toks.add(fam[0])
            
    return {"first_names": list(first_toks), "family_names": list(family_toks)}

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Initializing DuckDB and registering UDFs...")
    con = duckdb.connect()
    
    # Register UDFs. 
    # We return a STRUCT so we can extract both arrays easily in SQL.
    con.create_function("arc_names", arc_name_arrays, 
                        ['VARCHAR', 'VARCHAR'], 
                        'STRUCT(first_names VARCHAR[], family_names VARCHAR[])')
    
    con.create_function("oax_names", oax_name_arrays, 
                        ['VARCHAR', 'VARCHAR[]'], 
                        'STRUCT(first_names VARCHAR[], family_names VARCHAR[])')

    out_arc = PROCESSED_DATA / "splink_arc_input.parquet"
    out_oax = PROCESSED_DATA / "splink_oax_input.parquet"

    print("\n[1/2] Processing ARC Investigators...")
    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT 
                    grant_code || '_' || orcid AS unique_id,
                    first_name AS raw_first_name,
                    family_name AS raw_family_name,
                    arc_names(first_name, family_name) AS parsed,
                    -- Extract mapped OAX inst IDs from a future joined view or keep empty for now
                    -- For now, we will extract them from the institution_concordance if needed, 
                    -- but to keep it simple, we load what we have.
                    []::VARCHAR[] AS inst_ids 
                FROM read_parquet('{PROCESSED_DATA}/investigators.parquet')
            )
            SELECT 
                unique_id,
                parsed.first_names AS first_names,
                parsed.family_names AS family_names,
                inst_ids
            FROM base
        ) TO '{out_arc}' (FORMAT PARQUET)
    """)
    print(f"Saved to: {out_arc}")

    print("\n[2/2] Processing OpenAlex Authors...")
    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT 
                    id AS unique_id,
                    display_name,
                    display_name_alternatives,
                    oax_names(display_name, display_name_alternatives) AS parsed,
                    list_distinct(list_transform(
                        list_filter(affiliations, x -> x.institution.country_code = 'AU'),
                        x -> x.institution.id
                    )) AS inst_ids
                FROM read_parquet('{OAX_GLOB}')
                WHERE list_contains(
                    list_transform(affiliations, x -> x.institution.country_code), 'AU'
                )
            )
            SELECT 
                unique_id,
                parsed.first_names AS first_names,
                parsed.family_names AS family_names,
                inst_ids
            FROM base
        ) TO '{out_oax}' (FORMAT PARQUET)
    """)
    print(f"Saved to: {out_oax}")
    
    print("\nData preparation complete! Both datasets now have matching schemas:")
    print("- unique_id (VARCHAR)")
    print("- first_names (VARCHAR[])")
    print("- family_names (VARCHAR[])")
    print("- inst_ids (VARCHAR[])")

if __name__ == "__main__":
    main()
