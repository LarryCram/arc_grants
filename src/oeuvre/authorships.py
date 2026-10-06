"""
Step 2: every authorship of the ACIFs' author records, from OpenAlex's FULL authorships table
(OPENALEX_COMPACT_DIR/authorships; user, 2026-10-06: once an author is linked, its whole output
counts, not only works with an Australian co-author). One row per (ACIF, author record, work,
institution on that authorship); an authorship with no institution keeps one row with nulls.
OpenAlex's table holds some exact duplicate rows; they are dropped.
"""

from __future__ import annotations

import duckdb
import pandas as pd

from config.settings import DUCKDB_TMP_DIR, OPENALEX_COMPACT_DIR

AUTHORSHIPS = OPENALEX_COMPACT_DIR / "authorships"


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{DUCKDB_TMP_DIR}'")
    return con


def pull_authorships(records: pd.DataFrame, out_path, con=None, source=AUTHORSHIPS) -> int:
    """Write the authorships of `records`' author records to `out_path` (parquet); returns rows."""
    con = con or connect()
    con.register("rec", records[["cluster_id", "author_idx"]])
    con.execute(f"""
        COPY (
            SELECT DISTINCT r.cluster_id, a.author_idx, a.work_idx, a.institution_idx,
                   a.institution_name, a.country_code
            FROM read_parquet('{source}/*.parquet') a
            JOIN rec r ON r.author_idx = a.author_idx
            ORDER BY r.cluster_id, a.author_idx, a.work_idx
        ) TO '{out_path}' (FORMAT parquet)""")
    return con.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[0]
