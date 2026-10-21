"""
analysis/27_keyword_test.py -- do OpenAlex keywords (Oct26 snapshot: compact/work_keywords) tell a
person's own works from other people's better than topics or subfields? (2026-10-22, user: "look at
keywords"). A measurement only; nothing in the pipeline changes.

Per ACIF, the profile is the ACIF's CORE works (step 4, acif_work_graph.in_core): for each vocabulary
(keyword, topic, subfield) the sum over core works of score x idf (idf = log(N / works with the term)
over the whole snapshot). A work's similarity to the profile is the cosine between its own term vector
and the profile -- leaving the work itself out of the profile when it is a core work.

Label (the reference used for the Scopus rule): ACIFs with a trusted Scopus profile (one carrying the
ACIF's ORCID); works that are articles or reviews with a DOI, published 2000+; positive = on the
person's Scopus profile, negative = absent. Absent is noisy (17% of core articles are absent), so the
AUCs are lower bounds; they are compared between vocabularies on the same works. Reported for all such
works and for the works outside the core, where the decision is made.

Writes processed/keyword_test.md.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import numpy as np
import pandas as pd

from config.settings import DUCKDB_TMP_DIR, OEUVRE_DIR, OPENALEX_COMPACT_DIR, OPENALEX_DIR, PROCESSED_DATA

OUT = PROCESSED_DATA / "keyword_test.md"


def auc(y, s) -> float:
    """Mann-Whitney AUC with ties counted half."""
    y = np.asarray(y, bool)
    r = pd.Series(s).rank().to_numpy()
    n1, n0 = y.sum(), (~y).sum()
    return float((r[y].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)) if n1 and n0 else float("nan")


def main():
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{DUCKDB_TMP_DIR}'")
    con.execute(f"""CREATE TABLE w AS
        SELECT s.cluster_id, s.work_idx, s.type, s.doi, s.publication_year, g.in_core, c.decision, c.rule, c.on_scopus_profile
        FROM read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s
        JOIN read_parquet('{OEUVRE_DIR}/acif_work_graph.parquet') g USING (cluster_id, work_idx)
        JOIN read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') c USING (cluster_id, work_idx)""")
    con.execute("CREATE TABLE ids AS SELECT DISTINCT work_idx FROM w")
    n_works = con.execute(f"SELECT count(*) FROM read_parquet('{OPENALEX_COMPACT_DIR}/works/*.parquet')").fetchone()[0]
    # term tables: (work_idx, term, weight = score x idf)
    con.execute(f"""CREATE TABLE t_kw AS
        SELECT k.work_idx, k.keyword AS term, k.score * ln({n_works} / greatest(d.works_count, 1)) AS wt
        FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_keywords/*.parquet') k JOIN ids USING (work_idx)
        JOIN read_parquet('{OPENALEX_DIR}/keywords.parquet') d ON d.keyword = k.keyword""")
    con.execute(f"""CREATE TABLE wt_all AS SELECT work_idx, topic_idx, subfield_idx, score
        FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_topics/*.parquet') JOIN ids USING (work_idx)""")
    for name, col in (("topic", "topic_idx"), ("subfield", "subfield_idx")):
        con.execute(f"""CREATE TABLE df_{name} AS SELECT {col} AS term, count(DISTINCT work_idx) AS df
            FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_topics/*.parquet') GROUP BY 1""")
        con.execute(f"""CREATE TABLE t_{name} AS
            SELECT a.work_idx, a.{col}::VARCHAR AS term, sum(a.score) * ln({n_works} / greatest(any_value(d.df), 1)) AS wt
            FROM wt_all a JOIN df_{name} d ON d.term = a.{col} GROUP BY 1, 2""")
    res = {}
    for name in ("kw", "topic", "subfield"):
        con.execute(f"""CREATE OR REPLACE TABLE prof AS
            SELECT w.cluster_id, t.term, sum(t.wt) AS p FROM w JOIN t_{name} t USING (work_idx)
            WHERE w.in_core GROUP BY 1, 2""")
        con.execute("CREATE OR REPLACE TABLE pnorm AS SELECT cluster_id, sum(p * p) AS pp FROM prof GROUP BY 1")
        res[name] = con.execute(f"""
            WITH x AS (SELECT w.cluster_id, w.work_idx, w.in_core, t.wt, coalesce(p.p, 0) AS p
                       FROM w JOIN t_{name} t USING (work_idx) LEFT JOIN prof p ON p.cluster_id = w.cluster_id AND p.term = t.term),
                 agg AS (SELECT cluster_id, work_idx, any_value(in_core) AS in_core, sum(wt * p) AS wp, sum(wt * wt) AS ww
                         FROM x GROUP BY 1, 2)
            SELECT a.cluster_id, a.work_idx,
                   CASE WHEN a.in_core THEN (a.wp - a.ww) / nullif(sqrt(a.ww) * sqrt(greatest(n.pp - 2 * a.wp + a.ww, 0)), 0)
                        ELSE a.wp / nullif(sqrt(a.ww) * sqrt(n.pp), 0) END AS sim
            FROM agg a JOIN pnorm n USING (cluster_id)""").df().set_index(["cluster_id", "work_idx"]).sim
    w = con.execute("SELECT * FROM w").df().set_index(["cluster_id", "work_idx"])
    for name, s in res.items():
        w[f"sim_{name}"] = s
    cover = {n: float(w[f"sim_{n}"].notna().mean()) for n in res}
    lab = w[w.on_scopus_profile.notna() & w.type.isin(["article", "review"]) & w.doi.notna() & (w.publication_year >= 2000)]
    L = ["# Keywords vs topics vs subfields: telling a person's works from others' (Oct26)", "",
         f"- works scored: {len(w):,} (ACIF x work); with any keyword {cover['kw']:.1%}, any topic {cover['topic']:.1%}",
         f"- labelled works (trusted Scopus profile; article/review with DOI, 2000+): {len(lab):,}, "
         f"on the profile {lab.on_scopus_profile.mean():.1%}", "",
         "AUC (on-profile vs absent; works with no score count as the lowest), higher is better:", "",
         "| works | n | on profile | keywords | topics | subfields |", "|---|---|---|---|---|---|"]
    for title, sub in (("all labelled", lab), ("core", lab[lab.in_core]), ("outside the core", lab[~lab.in_core]),
                       ("outside the core, rule 'fits core'", lab[(~lab.in_core) & (lab.rule == "fits core")]),
                       ("outside the core, other rules", lab[(~lab.in_core) & (lab.rule != "fits core")])):
        y = sub.on_scopus_profile.astype(bool)
        a = {n: auc(y, sub[f"sim_{n}"].fillna(-1)) for n in res}
        L.append(f"| {title} | {len(sub):,} | {y.mean():.1%} | {a['kw']:.3f} | {a['topic']:.3f} | {a['subfield']:.3f} |")
    out = lab[~lab.in_core]
    L += ["", "Outside the core, share on the profile by similarity decile (keywords | topics):", "",
          "| decile | keywords: on profile | topics: on profile |", "|---|---|---|"]
    qk = pd.qcut(out.sim_kw.fillna(-1).rank(method="first"), 10, labels=False)
    qt = pd.qcut(out.sim_topic.fillna(-1).rank(method="first"), 10, labels=False)
    for d in range(10):
        L.append(f"| {d + 1} | {out.on_scopus_profile[qk == d].mean():.1%} | {out.on_scopus_profile[qt == d].mean():.1%} |")
    both = out[["sim_kw", "sim_topic"]].fillna(-1)
    L += ["", f"- correlation of keyword and topic similarity outside the core: {both.sim_kw.corr(both.sim_topic):.2f}",
          f"- AUC of the average of the two (ranks): {auc(out.on_scopus_profile.astype(bool), both.sim_kw.rank() + both.sim_topic.rank()):.3f}"]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    w.reset_index()[["cluster_id", "work_idx", "sim_kw", "sim_topic", "sim_subfield"]].to_parquet(
        PROCESSED_DATA / "keyword_test_scores.parquet", index=False)
    print("\n".join(L))


if __name__ == "__main__":
    main()
