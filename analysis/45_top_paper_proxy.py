"""
analysis/45_top_paper_proxy.py -- does a person's top paper by the short-window proxy match their top paper by c10?
(2026-10-28, user: "sinatra was looking at the highest, and I thought that max(c/t) might be similar -- I think that
c10 points to the year the c10 work is published, and the same could happen for max(c/t)").

Sinatra et al. 2016 study each scientist's highest-impact paper c10* and when in the career it appears. At an
evaluation year T (an "award year") a paper of age a = T - y has only c_a known; analysis/37's proxy is its
citations per year since publication, normalised by the same-field, same-year OpenAlex mean at the same age
(c_a / (a+1) over m_a / (a+1); the +1 cancels). Papers already 10+ years old at T use c10 itself.

Works: accepted works of kept ACIFs, version of record 1950-2015 (so every paper has a c10 to compare with); field
and cohort means as in analysis/37 (src/00h baselines). For T in EVAL_YEARS, people with MIN_PAPERS+ papers published
by T. Per person:
  top_true   max normalised c10 over the papers published by T (what we would know with hindsight)
  top_proxy  max of the proxy at T over the same papers (what was visible at T)
  same paper, publication years of the two top papers, and how old the proxy's top paper was at T;
  and the mean-log measure of analysis/37 (Q's core; papers with c > 0) for comparison.
Writes processed/top_paper_proxy.md.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import numpy as np
import pandas as pd

from config.settings import ACIFS_ARC, OEUVRE_DIR, OPENALEX_COMPACT_DIR, PROCESSED_DATA

CB = PROCESSED_DATA / "citation_baselines"
OUT = PROCESSED_DATA / "top_paper_proxy.md"
LAST_YEAR = 2025
EVAL_YEARS = (2005, 2010, 2015)
MIN_PAPERS = 10


def normalised_by_age() -> pd.DataFrame:
    """work_idx, y, domain_name, n0..n10: citations up to age a over the cohort mean up to age a."""
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    con.execute(f"""CREATE TABLE acc AS
        SELECT DISTINCT k.cluster_id, k.work_idx, s.vor_publication_year AS y
        FROM read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') k
        JOIN read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s USING (cluster_id, work_idx)
        JOIN read_parquet('{ACIFS_ARC}') a USING (cluster_id)
        WHERE k.decision = 'accept' AND NOT a.excluded AND s.vor_publication_year BETWEEN 1950 AND {LAST_YEAR - 10}""")
    con.execute(f"""CREATE TABLE wf AS SELECT work_idx, field_idx FROM read_parquet('{CB}/work_year_field.parquet')
                    WHERE work_idx IN (SELECT work_idx FROM acc)""")
    dom = con.execute(f"""SELECT DISTINCT field_idx, domain_name
                          FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_topics/*.parquet')""").df()
    con.register("dom", dom)
    ages = range(11)
    sel = ", ".join(f"sum(c.citations) FILTER (WHERE greatest(c.year - a.y, 0) <= {k}) AS c{k}" for k in ages)
    con.execute(f"""CREATE TABLE pc AS SELECT a.work_idx, any_value(a.y) AS y, {sel}
        FROM (SELECT DISTINCT work_idx, y FROM acc) a
        LEFT JOIN read_parquet('{OEUVRE_DIR}/acif_work_citations.parquet') c USING (work_idx) GROUP BY a.work_idx""")
    means = ", ".join(f"sum(citations) FILTER (WHERE age <= {k}) / any_value(n_works) AS m{k}" for k in ages)
    con.execute(f"""CREATE TABLE cm AS SELECT field_idx, pub_year, {means}
                    FROM read_parquet('{CB}/cohort_citations.parquet') GROUP BY 1, 2""")
    norm = ", ".join(f"coalesce(p.c{k}, 0) / m.m{k} AS n{k}" for k in ages)
    w = con.execute(f"""SELECT p.work_idx, p.y, d.domain_name, {norm}
        FROM pc p JOIN wf USING (work_idx) JOIN cm m ON m.field_idx = wf.field_idx AND m.pub_year = p.y
        JOIN dom d ON d.field_idx = wf.field_idx""").df()
    pw = con.execute("SELECT DISTINCT cluster_id, work_idx FROM acc").df()
    return pw.merge(w, on="work_idx")


def at_year(pw: pd.DataFrame, T: int) -> pd.DataFrame:
    """One row per person with MIN_PAPERS+ papers by T: the top papers by c10 and by the proxy at T."""
    d = pw[pw.y <= T].copy()
    age = np.minimum(T - d.y.to_numpy(), 10)
    d["age"] = age
    d["proxy"] = d[[f"n{k}" for k in range(11)]].to_numpy()[np.arange(len(d)), age]
    d = d.replace([np.inf, -np.inf], np.nan).dropna(subset=["proxy", "n10"])
    d = d[d.groupby("cluster_id").work_idx.transform("size") >= MIN_PAPERS]
    it = d.loc[d.groupby("cluster_id").n10.idxmax()].set_index("cluster_id")
    ip = d.loc[d.groupby("cluster_id").proxy.idxmax()].set_index("cluster_id")
    pos = d[(d.n10 > 0) & (d.proxy > 0)]
    ml = pos.groupby("cluster_id").agg(q10=("n10", lambda s: np.log(s).mean()), qp=("proxy", lambda s: np.log(s).mean()))
    out = pd.DataFrame({"top_true": it.n10, "top_proxy": ip.proxy, "same": it.work_idx == ip.work_idx,
                        "y_true": it.y, "y_proxy": ip.y, "age_proxy": ip.age, "age_true": it.age,
                        "domain": d.groupby("cluster_id").domain_name.agg(lambda s: s.mode().iloc[0])}).join(ml)
    return out


def summary(r: pd.DataFrame) -> str:
    lt, lp = np.log(r.top_true.clip(lower=1e-3)), np.log(r.top_proxy.clip(lower=1e-3))
    dy = (r.y_proxy - r.y_true).abs()
    ok = r.q10.notna() & r.qp.notna()
    return (f"| {len(r):,} | {r.top_true.rank().corr(r.top_proxy.rank()):.2f} | {lt.corr(lp):.2f} | {(lp - lt).mean():+.2f} | "
            f"{r.same.mean():.0%} | {(dy <= 1).mean():.0%} | {dy.median():.0f} | {(r.age_proxy < 3).mean():.0%} / "
            f"{(r.age_true < 3).mean():.0%} | {r.loc[ok, 'q10'].corr(r.loc[ok, 'qp']):.2f} |")


def main():
    pw = normalised_by_age()
    head = ("| set | people | Spearman top | r log top | proxy - true (log) | same paper | top year within 1 | "
            "median year gap | top paper < 3 yrs old (proxy / true) | r mean log (Q core) |")
    L = ["# Top paper by the short-window proxy vs by c10 (2026-10-28)", "",
         f"Accepted works with a c10 (version of record 1950-{LAST_YEAR - 10}): {pw.work_idx.nunique():,}. At an "
         f"evaluation year T each paper of age a < 10 is scored by c_a / (a+1) over the same-field, same-year OpenAlex "
         f"mean (older papers by c10); people with {MIN_PAPERS}+ papers published by T. 'True' = normalised c10 of the "
         "same papers (hindsight).", ""]
    for T in EVAL_YEARS:
        r = at_year(pw, T)
        L += [f"## T = {T}", "", head, "|---|" + "---|" * 9]
        L.append("| all " + summary(r))
        for dname, g in sorted(r.groupby("domain")):
            L.append(f"| {dname} " + summary(g))
        L.append("")
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))


if __name__ == "__main__":
    main()
