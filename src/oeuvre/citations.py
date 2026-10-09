"""
Step 6 of the oeuvre extractor (2026-10-09, user: a dossier time-line of works per year and cites per
year): citations each work received in each year, from OpenAlex's reference lists
(compact/references: citer_idx, cited_list) and the citing work's publication year.

Output acif_work_citations.parquet: one row per (work_idx, year) with `citations`, for every distinct
work in acif_works_single.parquet (whatever its step-5 decision; a person's yearly citations are the
sum over the works accepted for them). Citations by a work with no publication year are dropped.
Only the version of record's work_idx is counted (citations to other versions of the same work are
not added).
"""

from __future__ import annotations

from config.settings import OPENALEX_COMPACT_DIR

REFERENCES = OPENALEX_COMPACT_DIR / "references"
WORKS = OPENALEX_COMPACT_DIR / "works"


def work_citations_by_year(con, single_path, out_path, references=REFERENCES, works=WORKS) -> int:
    """Write (work_idx, year, citations) for the works in `single_path`; returns rows written."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE wc_want AS SELECT DISTINCT work_idx FROM read_parquet('{single_path}')")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE wc_pairs AS
        SELECT r.citer_idx, r.c AS work_idx
        FROM (SELECT citer_idx, unnest(cited_list) AS c FROM read_parquet('{references}/*.parquet')) r
        JOIN wc_want w ON w.work_idx = r.c""")
    con.execute(f"""COPY (
        SELECT p.work_idx, w.publication_year AS year, count(DISTINCT p.citer_idx) AS citations
        FROM wc_pairs p JOIN read_parquet('{works}/*.parquet') w ON w.work_idx = p.citer_idx
        WHERE w.publication_year IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 2
    ) TO '{out_path}' (FORMAT parquet)""")
    return con.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[0]
