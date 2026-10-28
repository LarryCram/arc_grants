"""
analysis/46_sinatra_measures.py -- step 1 of the Sinatra et al. 2016 author measures (docs/pipeline_todo.md 2026-10-23
item 9), redesigned 2026-10-28 with the user: every kept, linked ACIF, every career year, no fits.

Per (ACIF, year T) from the first accepted work to LAST_YEAR, using only what was known at T (columns year = T,
career_year = t):
  t              career year, T - first_year + 1. first_year = start of the main run of accepted works: leading works
                 ISOLATED (3) or more years before the next are dropped from every measure (user, 2026-10-28, after
                 a test against known PhD years: with the first two works 3+ years apart the PhD is nearer the
                 second for 71-74%; the rule changes 20% of people, cuts starts more than 6 years before the PhD
                 from 14% to 9%; second / third / mean-of-first-5 rules moved 70-98% of people and were rejected)
  N              accepted works published by T
  gamma          ln N / ln t (t >= 2): Sinatra's N(t) ~ t^gamma read off the cumulative count, no fit (user: N is
                 already a cumulant; the prefactor's effect, ln A / ln t, fades with t)
  c10 measures   papers 10+ years old at T, each by its normalised c10 (user: c10 where possible, never mixed with
                 the proxy): n_c10, qcore_c10 = mean ln c10 over papers with c10 > 0 (Sinatra's <log c10>, Q's
                 core: ln Q = <log c10> - mu_p, mu_p a population constant from the later fit), its standard error,
                 top_c10 and its publication year
  proxy measures every paper at T by its citations per year since publication, c_a / (a+1) at age a = T - y,
                 normalised by the same-field, same-year OpenAlex mean at the same age (analysis/37, 45): proxy_max
                 (max(c/t)) and its year, qcore_proxy = mean ln proxy over papers with proxy > 0, its standard error
Each impact measure also as fractional counts (_frac: c / authors, normalised by the OpenAlex fractional mean).
The LAST_YEAR row is the person's asymptotic value; Q is a career constant in Sinatra's model, so qcore(T) is its
estimate from the papers known at T, not a changing quantity.
Per paper: the person's own institution class on it (user, 2026-10-28, institutional support as a driver of Q):
go8 (an own institution is a Group of Eight university -- "for now"), other_hep (another ARC university), other
(an institution, none an ARC university), none.
Group and area: the ACIF's earliest fellowship (tier) and its grant's AREA5, else non-fellow CI with the first
grant's area (analysis/42's cohort()).
Writes processed/sinatra/{paper_measures,person_year,person}.parquet and sinatra_measures.md.
"""

import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import numpy as np
import pandas as pd

from config.settings import ACIFS_ARC, OAX_LINK_DIR, OEUVRE_DIR, OPENALEX_DIR, PROCESSED_DATA

CB = PROCESSED_DATA / "citation_baselines"
OUT_DIR = PROCESSED_DATA / "sinatra"
LAST_YEAR = 2025
ISOLATED = 3
GO8 = {"ANU", "UM", "USY", "UNSW", "UQ", "MON", "UWA", "UA"}
m42 = importlib.import_module("analysis.42_ecr_boxplots")
m43 = importlib.import_module("analysis.43_trove_phd_match")


def groups() -> pd.DataFrame:
    """cluster_id, group (Early-career / Mid-career / Senior / Non-fellow CI), area, award_year."""
    rows = []
    for t, (tier, _, _, name) in m42.TIERS.items():
        c = m42.cohort(False, tier).assign(group=name)
        rows.append(c[["cluster_id", "group", "area", "award_year"]])
    g = pd.concat(rows)
    fel = g[g.group != "Non-fellow CI"].sort_values(["cluster_id", "award_year"]).drop_duplicates("cluster_id")
    return pd.concat([fel, g[g.group == "Non-fellow CI"]]).reset_index(drop=True)


def main_run_start(years: list) -> int:
    """First year of the main run: leading works ISOLATED or more years before the next are skipped."""
    i = 0
    while i + 1 < len(years) and years[i + 1] - years[i] >= ISOLATED:
        i += 1
    return years[i]


def build(con) -> None:
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    con.execute(f"""CREATE TABLE pp AS
        SELECT DISTINCT k.cluster_id, k.work_idx, s.vor_publication_year AS y
        FROM read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') k
        JOIN read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s USING (cluster_id, work_idx)
        JOIN read_parquet('{ACIFS_ARC}') a USING (cluster_id)
        WHERE k.decision = 'accept' AND NOT a.excluded AND s.vor_publication_year BETWEEN 1950 AND {LAST_YEAR}""")
    # drop isolated leading works (ISOLATED+ years before the next accepted work), repeatedly
    ys = con.execute("SELECT cluster_id, list(y ORDER BY y) AS ys FROM pp GROUP BY 1").df()
    con.register("starts", pd.DataFrame({"cluster_id": ys.cluster_id, "start": [main_run_start(list(v)) for v in ys.ys]}))
    con.execute("DELETE FROM pp WHERE y < (SELECT start FROM starts WHERE starts.cluster_id = pp.cluster_id)")
    con.execute(f"""CREATE TABLE w AS
        SELECT p.work_idx, any_value(p.y) AS y, any_value(f.field_idx) AS field_idx,
               greatest(any_value(f.authors_count), 1) AS authors
        FROM (SELECT DISTINCT work_idx, y FROM pp) p
        JOIN read_parquet('{CB}/work_year_field.parquet') f USING (work_idx) GROUP BY p.work_idx""")
    # cumulative citations by age 0..(LAST_YEAR - y), negative ages counted at 0
    con.execute(f"""CREATE TABLE wa AS
        WITH ages AS (SELECT work_idx, y, field_idx, authors, unnest(range(0, {LAST_YEAR} - y + 1)) AS a FROM w),
             ca AS (SELECT c.work_idx, greatest(c.year - w.y, 0) AS a, sum(c.citations) AS c
                    FROM read_parquet('{OEUVRE_DIR}/acif_work_citations.parquet') c JOIN w USING (work_idx)
                    WHERE c.year <= {LAST_YEAR} GROUP BY 1, 2)
        SELECT g.work_idx, g.y, g.field_idx, g.authors, g.a,
               sum(coalesce(ca.c, 0)) OVER (PARTITION BY g.work_idx ORDER BY g.a ROWS UNBOUNDED PRECEDING) AS cum
        FROM ages g LEFT JOIN ca USING (work_idx, a)""")
    con.execute(f"""CREATE TABLE cm AS
        SELECT field_idx, pub_year, age,
               sum(citations) OVER (PARTITION BY field_idx, pub_year ORDER BY age ROWS UNBOUNDED PRECEDING) / n_works AS m,
               sum(citations_frac) OVER (PARTITION BY field_idx, pub_year ORDER BY age ROWS UNBOUNDED PRECEDING) / n_works AS mf
        FROM read_parquet('{CB}/cohort_citations.parquet')""")
    con.execute("""CREATE TABLE wn AS
        SELECT wa.work_idx, wa.y, wa.a,
               CASE WHEN cm.m > 0 THEN wa.cum / cm.m END AS n,
               CASE WHEN cm.mf > 0 THEN wa.cum / wa.authors / cm.mf END AS nf
        FROM wa JOIN cm ON cm.field_idx = wa.field_idx AND cm.pub_year = wa.y AND cm.age = wa.a""")
    con.execute("CREATE TABLE w10 AS SELECT work_idx, n AS n10, nf AS nf10 FROM wn WHERE a = 10")


def paper_institutions(con) -> pd.DataFrame:
    """cluster_id, work_idx, inst_class from the person's own authorship institutions."""
    names = m43.hep_names()
    ror = importlib.import_module("src.00e_extract_orcid_bulk")._ror
    ror_hep = dict(zip(names[names.kind == "ror"].key, names[names.kind == "ror"].hep_code))
    inst = con.execute(f"SELECT institution_idx, ror FROM read_parquet('{OPENALEX_DIR}/institutions.parquet') "
                       "WHERE ror IS NOT NULL").df()
    inst["hep"] = inst.ror.map(lambda r: ror_hep.get(ror(r)))
    con.register("ih", inst.dropna(subset=["hep"])[["institution_idx", "hep"]])
    con.register("go8", pd.DataFrame({"hep": sorted(GO8)}))
    return con.execute(f"""
        WITH own AS (SELECT s.cluster_id, s.work_idx,
                            unnest(list_transform(flatten(list_transform(list_filter(s.authorships,
                                   a -> a.author_idx IS NOT NULL OR a.link_status IS NOT NULL), a -> a.institutions)),
                                   i -> i.institution_idx)) AS institution_idx
                     FROM read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s
                     SEMI JOIN pp USING (cluster_id, work_idx))
        SELECT p.cluster_id, p.work_idx,
               CASE WHEN bool_or(ih.hep IN (SELECT hep FROM go8)) THEN 'go8'
                    WHEN bool_or(ih.hep IS NOT NULL) THEN 'other_hep'
                    WHEN count(o.institution_idx) > 0 THEN 'other' ELSE 'none' END AS inst_class
        FROM pp p LEFT JOIN own o USING (cluster_id, work_idx) LEFT JOIN ih USING (institution_idx)
        GROUP BY 1, 2""").df()


def person_year(con) -> pd.DataFrame:
    return con.execute(f"""
        WITH x AS (SELECT p.cluster_id, p.work_idx, p.y, wn.a, p.y + wn.a AS yr, wn.n, wn.nf, w10.n10, w10.nf10
                   FROM pp p JOIN wn USING (work_idx) LEFT JOIN w10 USING (work_idx)),
             f AS (SELECT cluster_id, min(y) AS first_year FROM pp GROUP BY 1)
        SELECT x.cluster_id, x.yr AS year, x.yr - f.first_year + 1 AS career_year, count(*) AS N,
               ln(count(*)) / ln(CASE WHEN x.yr - f.first_year + 1 >= 2 THEN x.yr - f.first_year + 1 END) AS gamma,
               count(*) FILTER (WHERE x.a >= 10 AND x.n10 IS NOT NULL) AS n_c10,
               avg(ln(CASE WHEN x.n10 > 0 THEN x.n10 END)) FILTER (WHERE x.a >= 10 AND x.n10 > 0) AS qcore_c10,
               stddev_samp(ln(CASE WHEN x.n10 > 0 THEN x.n10 END)) FILTER (WHERE x.a >= 10 AND x.n10 > 0)
                   / sqrt(count(*) FILTER (WHERE x.a >= 10 AND x.n10 > 0)) AS qcore_c10_se,
               max(x.n10) FILTER (WHERE x.a >= 10) AS top_c10,
               arg_max(x.y, x.n10) FILTER (WHERE x.a >= 10) AS top_c10_year,
               avg(ln(CASE WHEN x.nf10 > 0 THEN x.nf10 END)) FILTER (WHERE x.a >= 10 AND x.nf10 > 0) AS qcore_c10_frac,
               max(x.nf10) FILTER (WHERE x.a >= 10) AS top_c10_frac,
               max(x.n) AS proxy_max, arg_max(x.y, x.n) AS proxy_max_year,
               avg(ln(CASE WHEN x.n > 0 THEN x.n END)) FILTER (WHERE x.n > 0) AS qcore_proxy,
               stddev_samp(ln(CASE WHEN x.n > 0 THEN x.n END)) FILTER (WHERE x.n > 0) / sqrt(count(*) FILTER (WHERE x.n > 0)) AS qcore_proxy_se,
               max(x.nf) AS proxy_max_frac, avg(ln(CASE WHEN x.nf > 0 THEN x.nf END)) FILTER (WHERE x.nf > 0) AS qcore_proxy_frac
        FROM x JOIN f USING (cluster_id) GROUP BY x.cluster_id, x.yr, f.first_year ORDER BY 1, 2""").df()


def q(s: pd.Series) -> str:
    s = s.dropna()
    return f"{s.quantile(.1):.2f} / {s.median():.2f} / {s.quantile(.9):.2f}" if len(s) else "-"


def report(py: pd.DataFrame, person: pd.DataFrame, inst: pd.DataFrame) -> str:
    L = ["# Sinatra measures per ACIF and career year (step 1, 2026-10-28)", "",
         f"ACIFs {person.cluster_id.nunique():,}; person-years {len(py):,}. Values: 10th / median / 90th percentile.", "",
         "## Asymptotic values (2025) by group", "",
         "| group | ACIFs | gamma | qcore c10 | qcore proxy | ln proxy_max | with c10 papers |", "|---|---|---|---|---|---|---|"]
    for g, x in person.groupby("group"):
        L.append(f"| {g} | {len(x):,} | {q(x.gamma)} | {q(x.qcore_c10)} | {q(x.qcore_proxy)} | {q(np.log(x.proxy_max[x.proxy_max > 0]))} "
                 f"| {x.n_c10.gt(0).mean():.0%} |")
    L += ["", "## Path to the asymptote: |value(t) - value(2025)| by career year t (ACIFs with 20+ career years)", "",
          "| t | people | gamma | qcore proxy | qcore c10 |", "|---|---|---|---|---|"]
    last = py[py.year == LAST_YEAR].set_index("cluster_id")
    long_ = last.index[last.career_year >= 20]
    d = py[py.cluster_id.isin(long_)].join(last[["gamma", "qcore_proxy", "qcore_c10"]], on="cluster_id", rsuffix="_end")
    for t in (2, 3, 5, 8, 10, 15, 20):
        x = d[d.career_year == t]
        L.append(f"| {t} | {len(x):,} | {q((x.gamma - x.gamma_end).abs())} | {q((x.qcore_proxy - x.qcore_proxy_end).abs())} "
                 f"| {q((x.qcore_c10 - x.qcore_c10_end).abs())} |")
    vc = inst.inst_class.value_counts(normalize=True)
    L += ["", "## Own institution on each paper", "", ", ".join(f"{k} {v:.0%}" for k, v in vc.items())]
    return "\n".join(L) + "\n"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    build(con)
    linked = set(pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet").cluster_id)
    inst = paper_institutions(con)
    pm = con.execute("SELECT p.cluster_id, p.work_idx, p.y, w.field_idx, w.authors, w10.n10, w10.nf10 "
                     "FROM pp p JOIN w USING (work_idx) LEFT JOIN w10 USING (work_idx)").df()
    pm = pm.merge(inst, on=["cluster_id", "work_idx"], how="left")
    pm = pm[pm.cluster_id.isin(linked)]
    pm.to_parquet(OUT_DIR / "paper_measures.parquet", index=False)
    py = person_year(con)
    py = py[py.cluster_id.isin(linked)]
    py.to_parquet(OUT_DIR / "person_year.parquet", index=False)
    g = groups()
    person = py[py.year == LAST_YEAR].merge(g, on="cluster_id", how="left")
    person.to_parquet(OUT_DIR / "person.parquet", index=False)
    txt = report(py, person, pm)
    (OUT_DIR / "sinatra_measures.md").write_text(txt, encoding="utf-8")
    print(txt)


if __name__ == "__main__":
    main()
