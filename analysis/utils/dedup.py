"""
analysis/utils/dedup.py

Shared title-based deduplication for oeuvre-shaped tables (arc_id, work_idx,
publication_year, cited_by_count, type, field_name, subfield_name, domain_name, title).
Extracted from 03_annual_metrics.py's inline `deduped_works` CTE, which previously only
existed there -- oeuvres.parquet itself (and anything reading it directly, e.g.
analysis/utils/dossier_build.py) had no dedup applied, so the same real paper's preprint
and published versions counted as two separate works. Both callers now share this one
implementation instead of risking two copies drifting apart.

Title matching uses analysis/utils/title_norm.py's strict normalisation (case/whitespace/
hyphen-insensitive, HTML-entity and LaTeX-symbol aware, but preserves all other punctuation
and content so genuinely different titles never collapse -- see that module's docstring for
the verified merge/non-merge test cases). Titles <20 chars are not deduplicated at all (too
indistinctive on their own) -- known gap: exact-duplicate short "titles" that are really
metadata artifacts (e.g. a bare filename like "1234567.pdf" appearing 2-3 times for the same
person) currently don't collapse either, since they fall under the same length exemption;
not yet addressed, tracked separately.

Same-title works collapse to one row per (arc_id, title): publication_year becomes the
earliest year any version was public (preprint -> final); cited_by_count becomes the SUM
across all versions, not just the surviving row's own count -- a citing paper cites either the
preprint or the published version, essentially never both, so each version's citations are
genuine and additive, and picking only one version's count silently drops real citations
(confirmed empirically: e.g. one real pair was article=25/preprint=9, article-only would have
undercounted by 26%). Which row's *other* fields (work_idx, type, field_name) survive is still
decided by type priority (article > review > book-chapter > book > preprint > other), then
lowest work_idx -- that part is about picking the most authoritative record's metadata,
unrelated to the citation-sum question.

Before any of the above, rows are filtered by analysis/utils/exclusions.py's exclude_reason()
-- our own work-inclusion scope (disallowed types, filename-artifact titles), not an OpenAlex
flag. count_exclusions() runs the same check without filtering, so per-ECR exclusion counts
can be reported rather than just silently dropped.
"""

import duckdb

from analysis.utils import title_norm, exclusions

MIN_PUB_YEAR = 1950
MAX_PUB_YEAR = 2026


def create_deduped_works(
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    out_table: str = "deduped_works",
    min_year: int = MIN_PUB_YEAR,
    max_year: int = MAX_PUB_YEAR,
) -> None:
    """Creates/replaces `out_table` in `con` from `source_sql` (a table name, a
    read_parquet(...) call, or any SQL expression yielding arc_id, work_idx,
    publication_year, cited_by_count, type, field_name, subfield_name, domain_name, title,
    doi)."""
    title_norm.register(con)
    exclusions.register(con)
    con.execute(f"""
    CREATE OR REPLACE TABLE {out_table} AS
    WITH base AS (
        SELECT DISTINCT
            arc_id, work_idx, publication_year, cited_by_count,
            type, field_name, subfield_name, domain_name, title,
            CASE WHEN title IS NOT NULL AND length(title) >= 20
                 THEN norm_title(title)
                 ELSE CAST(work_idx AS VARCHAR)
            END AS norm_title_key
        FROM {source_sql}
        WHERE publication_year BETWEEN {min_year} AND {max_year}
          AND exclude_reason(title, type, doi) IS NULL
    ),
    ranked AS (
        SELECT *,
            -- earliest year across all versions of this title = when paper was first public
            MIN(publication_year) OVER (PARTITION BY arc_id, norm_title_key) AS first_pub_year_for_title,
            -- sum, not pick-one -- each version's citations are real and additive
            SUM(cited_by_count) OVER (PARTITION BY arc_id, norm_title_key) AS summed_cited_by_count,
            ROW_NUMBER() OVER (
                PARTITION BY arc_id, norm_title_key
                ORDER BY
                    CASE type
                        WHEN 'article'      THEN 1
                        WHEN 'review'       THEN 2
                        WHEN 'book-chapter' THEN 3
                        WHEN 'book'         THEN 4
                        WHEN 'preprint'     THEN 5
                        ELSE                     6
                    END ASC,
                    work_idx ASC
            ) AS title_rn
        FROM base
    )
    SELECT arc_id, work_idx,
           first_pub_year_for_title AS publication_year,
           summed_cited_by_count AS cited_by_count,
           type, field_name, subfield_name, domain_name, title
    FROM ranked WHERE title_rn = 1
    """)


def count_exclusions(
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    out_table: str = "work_exclusion_counts",
) -> None:
    """Creates/replaces `out_table`: arc_id, reason, n -- one row per (arc_id, reason) that
    had >=1 excluded work, for per-ECR reporting. Unfiltered scan of `source_sql` (same
    columns as create_deduped_works), independent of any year range."""
    exclusions.register(con)
    con.execute(f"""
    CREATE OR REPLACE TABLE {out_table} AS
    SELECT arc_id, exclude_reason(title, type, doi) AS reason, COUNT(*) AS n
    FROM {source_sql}
    WHERE exclude_reason(title, type, doi) IS NOT NULL
    GROUP BY arc_id, reason
    """)
