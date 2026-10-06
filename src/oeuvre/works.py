"""
Step 3: metadata of every work the ACIFs' author records reach, from OpenAlex's compact works
table, with the work's fields and subfields from its topics (compact/work_topics).

Fields (user, 2026-10-06): a single top topic is not useful -- a work usually has three topics
with nearly equal scores (median top-topic share 0.34). Topic scores are summed per field and
per subfield instead: `fields` / `subfields` list every field/subfield with its share of the
work's topic weight, and `dominant_field` / `dominant_subfield` is set only when one holds MORE
than DOMINANT_SHARE of it (two of three topics agree; an even split is a tie); otherwise it is null -- a work spread over
three fields (e.g. Mathematics / Engineering / Computer Science at 0.34 each) has no dominant
field. Checked on samples before adopting; 43% of works sit in one field, about a quarter spread
over three.
"""

from __future__ import annotations

import pandas as pd

from config.settings import OPENALEX_COMPACT_DIR

WORKS = OPENALEX_COMPACT_DIR / "works"
TOPICS = OPENALEX_COMPACT_DIR / "work_topics"
DOMINANT_SHARE = 0.5


def pull_works(con, authorships_path, out_path, works=WORKS, topics=TOPICS) -> int:
    """Write one row per distinct work in `authorships_path` to `out_path`; returns rows."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE want AS SELECT DISTINCT work_idx FROM read_parquet('{authorships_path}')")
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
            SELECT w.work_idx, w.doi, w.title, w.publication_year, w.type, w.authors_count,
                   w.cited_by_count, w.is_retracted, w.is_paratext, w.source_id,
                   f.fields,
                   CASE WHEN f.top_share > {DOMINANT_SHARE} THEN f.top END AS dominant_field,
                   f.top_share AS dominant_field_share,
                   sf.subfields,
                   CASE WHEN sf.top_share > {DOMINANT_SHARE} THEN sf.top END AS dominant_subfield,
                   sf.top_share AS dominant_subfield_share
            FROM read_parquet('{works}/*.parquet') w
            JOIN want USING (work_idx)
            LEFT JOIN fields f USING (work_idx)
            LEFT JOIN subfields sf USING (work_idx)
            ORDER BY w.work_idx
        ) TO '{out_path}' (FORMAT parquet)""")
    return con.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[0]
