"""
analysis/37_citation_proxy_check.py -- step 1 of the author performance features (docs/pipeline_todo.md,
2026-10-23 item 9): does the short-window proxy c_k / (k+1), normalised by the same-field, same-year OpenAlex
mean at the same age, rank papers and people as c10 does?

Works: every work accepted for a kept ACIF (acif_works_classified decision 'accept'), counted once per work
for the paper-level check. Field = OpenAlex field of the work's highest-scoring topic and age = citing year -
the version of record's publication year, as in src/00h_extract_citation_baselines.py; c_k = citations up to
age k. Papers with a c10 (version of record published 2015 or earlier; 2025 is the last complete year).
Normalised: x / mean x of the work's (field, year) cohort over all OpenAlex at the same age; the +1 of
c_k/(k+1) cancels there. Works with c = 0 are left out of the log-based (person) comparison (user decision).

Paper level: Spearman of normalised c_k with normalised c10, k = 1, 2, 3, 5, by OpenAlex domain.
Person level: mean log of normalised c (the paper's <log c10>, the core of Q) from c10 and from c_k, ACIFs with
10+ such works, by the domain of most of their works: correlation and mean difference.
Writes processed/citation_proxy_check.md.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import numpy as np
import pandas as pd

from config.settings import ACIFS_ARC, OEUVRE_DIR, OPENALEX_COMPACT_DIR, PROCESSED_DATA

CB = PROCESSED_DATA / "citation_baselines"
OUT = PROCESSED_DATA / "citation_proxy_check.md"
LAST_YEAR = 2025
KS = (1, 2, 3, 5)


def main():
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    con.execute(f"""CREATE TABLE acc AS
        SELECT DISTINCT k.cluster_id, k.work_idx, s.vor_publication_year AS y
        FROM read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') k
        JOIN read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s USING (cluster_id, work_idx)
        JOIN read_parquet('{ACIFS_ARC}') a USING (cluster_id)
        WHERE k.decision = 'accept' AND NOT a.excluded AND s.vor_publication_year BETWEEN 1950 AND {LAST_YEAR - 10}""")
    con.execute(f"""CREATE TABLE wf AS SELECT w.work_idx, w.field_idx FROM read_parquet('{CB}/work_year_field.parquet') w
                    WHERE w.work_idx IN (SELECT work_idx FROM acc)""")
    dom = con.execute(f"""SELECT DISTINCT field_idx, field_name, domain_name
                          FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_topics/*.parquet')""").df()
    con.register("dom", dom)
    # cumulative citations per work at ages 1, 2, 3, 5, 10 (negative ages count at 0)
    sel = ", ".join(f"sum(c.citations) FILTER (WHERE greatest(c.year - a.y, 0) <= {k}) AS c{k}" for k in (*KS, 10))
    con.execute(f"""CREATE TABLE pc AS
        SELECT a.work_idx, any_value(a.y) AS y, {sel}
        FROM (SELECT DISTINCT work_idx, y FROM acc) a
        LEFT JOIN read_parquet('{OEUVRE_DIR}/acif_work_citations.parquet') c USING (work_idx)
        GROUP BY a.work_idx""")
    # cohort means at the same ages
    means = ", ".join(f"sum(citations) FILTER (WHERE age <= {k}) / any_value(n_works) AS m{k}" for k in (*KS, 10))
    con.execute(f"""CREATE TABLE cm AS SELECT field_idx, pub_year, {means}
                    FROM read_parquet('{CB}/cohort_citations.parquet') GROUP BY 1, 2""")
    norm = ", ".join(f"coalesce(p.c{k}, 0) / m.m{k} AS n{k}" for k in (*KS, 10))
    w = con.execute(f"""SELECT p.work_idx, p.y, d.domain_name, {norm}
        FROM pc p JOIN wf USING (work_idx) JOIN cm m ON m.field_idx = wf.field_idx AND m.pub_year = p.y
        JOIN dom d ON d.field_idx = wf.field_idx""").df()

    L = ["# Short-window citation proxy vs c10 (step 1 check, 2026-10-23)", "",
         f"- accepted works of kept ACIFs with a c10 (version of record {1950}-{LAST_YEAR - 10}): {len(w):,}; "
         f"with c10 = 0: {(w.n10 == 0).mean():.1%}", "",
         "## Papers: Spearman of normalised c_k with normalised c10", "",
         "| domain | works | " + " | ".join(f"k={k}" for k in KS) + " |", "|---|---|" + "---|" * len(KS)]
    for d, g in [("all", w), *sorted(w.groupby("domain_name"))]:
        r = [g[f"n{k}"].rank().corr(g.n10.rank()) for k in KS]
        L.append(f"| {d} | {len(g):,} | " + " | ".join(f"{x:.3f}" for x in r) + " |")

    pw = con.execute("SELECT DISTINCT cluster_id, work_idx FROM acc").df().merge(w, on="work_idx")
    L += ["", "## People: mean log normalised citations (works with c > 0) from c10 vs from c_k", "",
          "ACIFs with 10+ works that have c10 > 0 and c_k > 0; domain = the domain of most of the ACIF's works.", "",
          "| domain | ACIFs | " + " | ".join(f"k={k}: r / mean diff" for k in KS) + " |", "|---|---|" + "---|" * len(KS)]
    main_dom = pw.groupby("cluster_id").domain_name.agg(lambda s: s.mode().iloc[0])
    rows = {}
    for k in KS:
        q = pw[(pw.n10 > 0) & (pw[f"n{k}"] > 0)]
        g = q.groupby("cluster_id").agg(n=("work_idx", "size"), q10=("n10", lambda s: np.log(s).mean()),
                                         qk=(f"n{k}", lambda s: np.log(s).mean()))
        rows[k] = g[g.n >= 10].join(main_dom)
    for d in ["all", *sorted(main_dom.unique())]:
        cells, n = [], 0
        for k in KS:
            g = rows[k] if d == "all" else rows[k][rows[k].domain_name == d]
            n = max(n, len(g))
            cells.append(f"{g.q10.corr(g.qk):.3f} / {(g.qk - g.q10).mean():+.2f}" if len(g) > 2 else "-")
        L.append(f"| {d} | {n:,} | " + " | ".join(cells) + " |")
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
