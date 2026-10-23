"""
OpenAlex citation baselines for the author performance features (Sinatra et al. 2016; docs/pipeline_todo.md
2026-10-23 item 9), built over all of OpenAlex (user, 2026-10-23: "normalise over all openalex").

A work's field is the OpenAlex field of its highest-scoring topic. A work counts in a cohort (field x
publication year) when its type is one the oeuvre extractor keeps (src/oeuvre/work_filter.KEEP_TYPES) and it
is neither paratext nor retracted. Citations come from compact/references (one row per citation) and are
placed at the paper's age = citing work's publication year - cited work's publication year (negative ages,
a citer dated before the cited work, count at age 0); citers of any type count, citers with no year don't.
Both full and fractional counts are kept (user, 2026-10-23: "fractionate all and also keep full count"): a
citation counts 1 in `citations` and 1 / authors_count of the cited work in `citations_frac` (a work with no
author count counts as one author), so a person's fractional c is normalised by a fractional field average.

Outputs in processed/citation_baselines/:
  work_year_field.parquet  work_idx, publication_year, field_idx, authors_count, counted -- every OpenAlex
                           work with a year
  cohort_citations.parquet field_idx, field_name, pub_year, age, n_works, citations, citations_frac --
                           citations received at
                           each age by the cohort's counted works; the cohort's mean citations k years after
                           publication is sum(citations, age <= k) / n_works
Run: .venv/bin/python src/00h_extract_citation_baselines.py [--parts N]   (N reference files only, for a trial)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb

from config.settings import DUCKDB_TMP_DIR, OPENALEX_COMPACT_DIR, PROCESSED_DATA
from src.oeuvre.work_filter import KEEP_TYPES

OUT_DIR = PROCESSED_DATA / "citation_baselines"
WORK_YEAR_FIELD = OUT_DIR / "work_year_field.parquet"
COHORT_CITATIONS = OUT_DIR / "cohort_citations.parquet"
MIN_YEAR, MAX_YEAR = 1900, 2026


def build(parts: int | None = None, threads: int = 12, out=COHORT_CITATIONS, memory: str = "16GB") -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # on-disk working database and a memory cap: the 327M-row lookups spill instead of exhausting memory
    # (an in-memory run beside the pipeline was killed by the OOM killer, 2026-10-23)
    db = DUCKDB_TMP_DIR / "citation_baselines.duckdb"
    db.unlink(missing_ok=True)
    con = duckdb.connect(str(db))
    con.execute(f"SET temp_directory='{DUCKDB_TMP_DIR}'; SET threads={threads}; SET memory_limit='{memory}'; "
                "SET preserve_insertion_order=false")
    t = time.time()
    if not WORK_YEAR_FIELD.exists():
        keep = ", ".join(f"'{k}'" for k in KEEP_TYPES)
        con.execute(f"""COPY (
            SELECT w.work_idx, w.publication_year::SMALLINT AS publication_year, f.field_idx::SMALLINT AS field_idx,
                   greatest(coalesce(w.authors_count, 1), 1)::INTEGER AS authors_count,
                   (w.type IN ({keep}) AND NOT coalesce(w.is_paratext, FALSE) AND NOT coalesce(w.is_retracted, FALSE)) AS counted
            FROM read_parquet('{OPENALEX_COMPACT_DIR}/works/*.parquet') w
            LEFT JOIN (SELECT work_idx, arg_max(field_idx, score) AS field_idx
                       FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_topics/*.parquet') GROUP BY work_idx) f USING (work_idx)
            WHERE w.publication_year BETWEEN {MIN_YEAR} AND {MAX_YEAR}
        ) TO '{WORK_YEAR_FIELD}' (FORMAT parquet)""")
        print(f"work_year_field written ({time.time() - t:.0f} s)", flush=True)
    con.execute(f"CREATE TABLE wyf AS SELECT * FROM read_parquet('{WORK_YEAR_FIELD}')")
    con.execute("CREATE TABLE cited AS SELECT work_idx, publication_year, field_idx, authors_count FROM wyf WHERE counted AND field_idx IS NOT NULL")
    con.execute("CREATE TABLE agg (field_idx SMALLINT, pub_year SMALLINT, age SMALLINT, citations BIGINT, citations_frac DOUBLE)")
    files = sorted((OPENALEX_COMPACT_DIR / "references").glob("*.parquet"))[:parts]
    for f in files:
        t1 = time.time()
        con.execute(f"""INSERT INTO agg
            SELECT c.field_idx, c.publication_year, greatest(g.publication_year - c.publication_year, 0), count(*), sum(1.0 / c.authors_count)
            FROM read_parquet('{f}') r
            JOIN cited c ON c.work_idx = r.cited_idx
            JOIN wyf g ON g.work_idx = r.citer_idx
            GROUP BY 1, 2, 3""")
        print(f"{f.name} ({time.time() - t1:.0f} s)", flush=True)
    fields = f"(SELECT DISTINCT field_idx, field_name FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_topics/*.parquet'))"
    con.execute(f"""COPY (
        SELECT n.field_idx, fn.field_name, n.pub_year, a.age, n.n_works, coalesce(a.citations, 0) AS citations,
               coalesce(a.citations_frac, 0) AS citations_frac
        FROM (SELECT field_idx, publication_year AS pub_year, count(*) AS n_works FROM cited GROUP BY 1, 2) n
        JOIN (SELECT field_idx, pub_year, age, sum(citations) AS citations, sum(citations_frac) AS citations_frac FROM agg GROUP BY 1, 2, 3) a USING (field_idx, pub_year)
        LEFT JOIN {fields} fn USING (field_idx)
        ORDER BY 1, 3, 4
    ) TO '{out}' (FORMAT parquet)""")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    print(f"{out.name}: {n:,} rows ({time.time() - t:.0f} s)")
    con.close()
    db.unlink(missing_ok=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--parts", type=int, default=None)
    ap.add_argument("--threads", type=int, default=12)
    ap.add_argument("--memory", default="16GB")
    a = ap.parse_args()
    build(a.parts, a.threads, COHORT_CITATIONS if a.parts is None else OUT_DIR / f"cohort_citations_trial{a.parts}.parquet",
          a.memory)
