"""
Step 2 of the oeuvre extractor (2026-10-07, user): drop works that are not research outputs or
whose OpenAlex record looks corrupt. Each (ACIF, work) row of acif_works.parquet gets the first
drop reason that applies, in this order:

  paratext        is_paratext (front matter, tables of contents, ...)
  retracted       is_retracted
  type            type not in KEEP_TYPES (datasets, letters, editorials, errata, peer reviews, ...)
  no_inst_no_doi  no author on the whole work has an institution AND the work has no DOI -- a sign
                  of a corrupt works record (half of such works have no DOI, against 5% of the
                  rest); some real older books and papers fall here too, accepted: they can be
                  rebuilt from the works and authorships tables if needed

Kept rows go to acif_works_kept.parquet (all columns); dropped rows to work_drops.parquet
(cluster_id, work_idx, type, publication_year, drop_reason).
"""

from __future__ import annotations

KEEP_TYPES = ("article", "preprint", "book", "book-chapter", "review", "report", "dissertation")
REASONS = ("paratext", "retracted", "type", "no_inst_no_doi")

DROP_REASON_SQL = f"""
    CASE WHEN is_paratext THEN 'paratext'
         WHEN is_retracted THEN 'retracted'
         WHEN type IS NULL OR type NOT IN {KEEP_TYPES} THEN 'type'
         WHEN coalesce(work_authors_with_institution, 0) = 0 AND doi IS NULL THEN 'no_inst_no_doi'
    END"""


def filter_works(con, in_path, kept_path, drops_path) -> tuple[int, int]:
    """Split the ACIF works at `in_path` into kept and dropped rows; returns (kept, dropped)."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fw AS
                    SELECT *, {DROP_REASON_SQL} AS drop_reason FROM read_parquet('{in_path}')""")
    con.execute(f"""COPY (SELECT * EXCLUDE (drop_reason) FROM fw WHERE drop_reason IS NULL
                          ORDER BY cluster_id, work_idx) TO '{kept_path}' (FORMAT parquet)""")
    con.execute(f"""COPY (SELECT cluster_id, work_idx, type, publication_year, drop_reason FROM fw
                          WHERE drop_reason IS NOT NULL ORDER BY cluster_id, work_idx)
                    TO '{drops_path}' (FORMAT parquet)""")
    return tuple(con.execute("SELECT count(*) FILTER (WHERE drop_reason IS NULL), "
                             "count(*) FILTER (WHERE drop_reason IS NOT NULL) FROM fw").fetchone())
