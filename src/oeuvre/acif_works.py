"""
The ACIF works set (2026-10-07, user: one step from the linker's accepted links straight to one
table, replacing the separate records / authorships / works steps): one row per (ACIF, work) for
every work of every OpenAlex author (`author_idx`) accepted for the ACIF by linker stage 1
(processed/oax_link/orcid_links.parquet, status accept_*).

An `author_idx` is only an index to a set of works: its works may or may not be the person's, so
every work is a candidate and later steps sort it into "the person's" / "not the person's". Once an
author is linked, ALL its authorships count (OpenAlex's full compact authorships table, not only
works with an Australian co-author). No decisions are made here: the table holds data only.

Per row:
  cluster_id, work_idx
  authorships   list of {author_idx, link_status, printed_name, institutions} -- one per ACIF
                author on the work (almost always one); printed_name is the author name as printed
                on that paper; institutions is a list of {institution_idx, name, country}
  work metadata doi, title, publication_year, type, authors_count, cited_by_count, is_retracted,
                is_paratext, source_id (the journal/book/repository)
  work_authors / work_authors_with_institution
                distinct authors on the whole work, and how many of them have an institution
                (all authorships of the work, not only the ACIF's)
  fields        fields / subfields with their share of the work's summed topic scores; dominant_field
                / dominant_subfield only when one holds MORE than DOMINANT_SHARE (a single top topic
                is not used: a work usually has three nearly equal topics, median top share 0.34)
OpenAlex's authorships table holds some exact duplicate rows; they are dropped.
"""

from __future__ import annotations

import duckdb
import pandas as pd

from config.settings import DUCKDB_TMP_DIR, OAX_LINK_DIR, OPENALEX_COMPACT_DIR

AUTHORSHIPS = OPENALEX_COMPACT_DIR / "authorships"
WORKS = OPENALEX_COMPACT_DIR / "works"
TOPICS = OPENALEX_COMPACT_DIR / "work_topics"
LINKS = OAX_LINK_DIR / "orcid_links.parquet"
DOMINANT_SHARE = 0.5


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{DUCKDB_TMP_DIR}'")
    return con


def accepted_links(links: pd.DataFrame) -> pd.DataFrame:
    """(cluster_id, author_idx, link_status) for the accepted links."""
    a = links.loc[links.status.str.startswith("accept"), ["cluster_id", "author_idx", "status"]]
    return a.rename(columns={"status": "link_status"}).astype({"author_idx": "int64"}).reset_index(drop=True)


def build_acif_works(links: pd.DataFrame, out_path, con=None, authorships=AUTHORSHIPS, works=WORKS,
                     topics=TOPICS) -> int:
    """Write the ACIF works set for the accepted links in `links` to `out_path`; returns rows."""
    con = con or connect()
    con.register("links", accepted_links(links))
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE au AS
        SELECT DISTINCT l.cluster_id, l.link_status, a.author_idx, a.work_idx, a.author_name,
               a.institution_idx, a.institution_name, a.country_code
        FROM read_parquet('{authorships}/*.parquet') a JOIN links l USING (author_idx)""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE per_author AS
        SELECT cluster_id, work_idx, author_idx, any_value(link_status) AS link_status,
               min(author_name) AS printed_name,
               list({'institution_idx': institution_idx, 'name': institution_name, 'country': country_code}
                    ORDER BY institution_idx) FILTER (WHERE institution_idx IS NOT NULL) AS institutions
        FROM au GROUP BY cluster_id, work_idx, author_idx""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE per_work AS
        SELECT cluster_id, work_idx,
               list({'author_idx': author_idx, 'link_status': link_status, 'printed_name': printed_name,
                     'institutions': coalesce(institutions, [])} ORDER BY author_idx) AS authorships
        FROM per_author GROUP BY cluster_id, work_idx""")
    con.execute("CREATE OR REPLACE TEMP TABLE want AS SELECT DISTINCT work_idx FROM per_work")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE work_authors AS
        SELECT work_idx, count(DISTINCT author_idx) AS work_authors,
               count(DISTINCT author_idx) FILTER (WHERE institution_idx IS NOT NULL) AS work_authors_with_institution
        FROM read_parquet('{authorships}/*.parquet') JOIN want USING (work_idx) GROUP BY 1""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE t AS
        SELECT t.work_idx, t.score, t.subfield_name, t.field_name
        FROM read_parquet('{topics}/*.parquet') t JOIN want USING (work_idx)""")
    for level in ("field", "subfield"):
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {level}s AS
            WITH s AS (SELECT work_idx, {level}_name AS name, sum(score) AS w FROM t GROUP BY 1, 2),
                 sh AS (SELECT work_idx, name, w / sum(w) OVER (PARTITION BY work_idx) AS share FROM s)
            SELECT work_idx,
                   list({{'name': name, 'share': round(share, 3)}} ORDER BY share DESC, name) AS {level}s,
                   arg_max(name, share) AS top, max(share) AS top_share
            FROM sh GROUP BY work_idx""")
    con.execute(f"""
        COPY (
            SELECT p.cluster_id, p.work_idx, p.authorships,
                   w.doi, w.title, w.publication_year, w.type, w.authors_count, w.cited_by_count,
                   w.is_retracted, w.is_paratext, w.source_id,
                   wa.work_authors, wa.work_authors_with_institution,
                   f.fields,
                   CASE WHEN f.top_share > {DOMINANT_SHARE} THEN f.top END AS dominant_field,
                   f.top_share AS dominant_field_share,
                   sf.subfields,
                   CASE WHEN sf.top_share > {DOMINANT_SHARE} THEN sf.top END AS dominant_subfield,
                   sf.top_share AS dominant_subfield_share
            FROM per_work p
            LEFT JOIN read_parquet('{works}/*.parquet') w USING (work_idx)
            LEFT JOIN work_authors wa USING (work_idx)
            LEFT JOIN fields f USING (work_idx)
            LEFT JOIN subfields sf USING (work_idx)
            ORDER BY p.cluster_id, p.work_idx
        ) TO '{out_path}' (FORMAT parquet)""")
    return con.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[0]
