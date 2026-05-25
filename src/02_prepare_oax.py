"""
src/02_prepare_oax.py

Prepare OpenAlex HEP-context authors for Splink linkage.

Phase 1 – author_hep: group authorships_hep by author, aggregating institution
    IDs (from HEP authorships), field distribution with fractions (from HEP
    works), and name/ORCID/topic data (from authors entity).
    → author_hep.parquet

Phase 2 – Splink prep: parse names via oax_name_arrays UDF.
    → openalex_authors_prep.parquet

Phase 3 – TF tables for Splink term-frequency adjustment.
    → oax_tf_family_name.parquet
    → oax_tf_first_name.parquet
    → oax_tf_full_name.parquet
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROCESSED_DATA
from src.utils.names import name_part_tokens, strip_diacriticals

PROC = PROCESSED_DATA


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

    _parse_name(display_name)

    # Fallback: only use alternatives when display_name yields no family name.
    # Alternatives are contaminated with co-author names (OAX disambiguation errors).
    if not family_toks and alts:
        for alt in alts:
            _parse_name(alt)

    if not first_toks:
        for fam in family_toks:
            if fam:
                first_toks.add(fam[0])

    return {"first_names": list(first_toks), "family_names": list(family_toks)}


def main():
    con = duckdb.connect()

    auth_hep  = PROC / "authorships_hep.parquet"
    works_hep = PROC / "works_hep.parquet"
    out_hep   = PROC / "author_hep.parquet"
    out_oax   = PROC / "openalex_authors_prep.parquet"

    # ── Phase 1: Build author_hep ─────────────────────────────────────────────

    print(f"[1/3] Building author_hep ({out_hep})...")
    con.execute(f"""
        COPY (
            WITH
              -- one row per (work, field) — deduplicated so a work with
              -- multiple topics in the same field counts once per field
              work_fields AS (
                SELECT DISTINCT work_idx, field_name
                FROM '{works_hep}'
              ),
              -- field distribution per author across their HEP-context works
              field_counts AS (
                SELECT
                  a.author_idx,
                  count(DISTINCT a.work_idx)               AS works_count,
                  unnest(map_entries(histogram(wf.field_name))) AS entry
                FROM '{auth_hep}' a
                JOIN work_fields wf USING (work_idx)
                GROUP BY a.author_idx
              ),
              field_fracs AS (
                SELECT
                  author_idx,
                  works_count,
                  entry.key                                                             AS field,
                  entry.value                                                           AS field_count,
                  round(entry.value::DOUBLE / SUM(entry.value) OVER
                        (PARTITION BY author_idx), 4)                                  AS field_fraction
                FROM field_counts
              ),
              sorted_fields AS (
                SELECT
                  author_idx,
                  works_count,
                  list({{'field': field, 'count': field_count, 'fraction': field_fraction}}
                       ORDER BY field_fraction DESC) AS sorted_fields
                FROM field_fracs
                GROUP BY author_idx, works_count
              ),
              -- institution IDs per author from HEP authorships → full OAX URLs
              inst_agg AS (
                SELECT
                  author_idx,
                  list_distinct(list_transform(
                    list(institution_idx),
                    x -> 'https://openalex.org/I' || x::VARCHAR
                  )) AS inst_ids
                FROM '{auth_hep}'
                WHERE institution_idx IS NOT NULL
                GROUP BY author_idx
              )
            SELECT
              'https://openalex.org/A' || sf.author_idx::VARCHAR  AS unique_id,
              au.display_name                                       AS full_name,
              au.display_name_alternatives,
              replace(au.orcid, 'https://orcid.org/', '')          AS orcid,
              COALESCE(ia.inst_ids, [])                            AS inst_ids,
              list_transform(au.topics, x -> x.display_name)       AS topic_names,
              list_transform(au.topics, x -> x.subfield.display_name) AS subfield_names,
              sf.works_count,
              sf.sorted_fields
            FROM sorted_fields sf
            LEFT JOIN inst_agg ia USING (author_idx)
            JOIN read_parquet('{OAX_AUTHORS}/*.parquet') au
              ON au.id[23:]::BIGINT = sf.author_idx
        ) TO '{out_hep}' (FORMAT PARQUET)
    """)
    n_hep = con.execute(f"SELECT count(*) FROM '{out_hep}'").fetchone()[0]
    print(f"  {n_hep:,} authors → {out_hep}")

    # ── Phase 2: Splink prep (name parsing) ───────────────────────────────────

    print(f"[2/3] Parsing names → {out_oax}...")
    con.create_function("oax_names", oax_name_arrays,
                        ['VARCHAR', 'VARCHAR[]'],
                        'STRUCT(first_names VARCHAR[], family_names VARCHAR[])')

    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT
                    unique_id,
                    full_name,
                    oax_names(full_name, display_name_alternatives) AS parsed,
                    orcid,
                    inst_ids,
                    topic_names,
                    subfield_names,
                    works_count,
                    sorted_fields
                FROM '{out_hep}'
            )
            SELECT
                unique_id,
                full_name,
                parsed.first_names                                          AS first_names,
                list_filter(parsed.first_names, x -> len(x) = 1)           AS first_initials,
                list_filter(parsed.first_names, x -> len(x) > 1)           AS first_name_full,
                parsed.family_names                                         AS family_names,
                orcid,
                inst_ids,
                topic_names,
                subfield_names,
                works_count,
                sorted_fields
            FROM base
        ) TO '{out_oax}' (FORMAT PARQUET)
    """)

    # ── Phase 3: TF tables ────────────────────────────────────────────────────

    print("[3/3] Building term-frequency tables...")
    df = pd.read_parquet(out_oax)
    n = len(df)

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

    df["family_name_main"]     = df["family_names"].apply(_max_by_len)
    df["first_name_canonical"] = df.apply(_canonical, axis=1)
    df["full_name_key"] = df.apply(
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
        counts = df[col].dropna().value_counts()
        tf = counts.reset_index()
        tf.columns = [col, f"tf_{col}"]
        tf[f"tf_{col}"] = tf[f"tf_{col}"] / n
        tf.to_parquet(PROCESSED_DATA / fname, index=False)
        print(f"  {fname}: {len(tf):,} unique values")

    print("OAX prep complete.")


if __name__ == "__main__":
    main()
