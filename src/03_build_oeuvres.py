"""
src/03_build_oeuvres.py -- the oeuvre extractor (2026-10-06/07): works of each ACIF from the OpenAlex
authors (author_idx) accepted for it by linker stage 1. Built one step at a time; each step writes
its table to OEUVRE_DIR and a section of report.md. Plan: /home/lc/.claude/plans/sunny-sauteeing-peach.md.

Steps so far:
  1. acif_works.parquet   one row per (ACIF, work): the authorships through the ACIF's linked
                          authors (printed name, institutions), work metadata, field weights
                          (src/oeuvre/acif_works.py)
Next: evidence per (ACIF, work), decision (the person's / not), combine versions, person report.

Usage: .venv/bin/python src/03_build_oeuvres.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import OEUVRE_DIR
from src.oeuvre.acif_works import DOMINANT_SHARE, LINKS, accepted_links, build_acif_works, connect


def acif_works_section(con, path, links: pd.DataFrame, seconds: float) -> list[str]:
    acc = accepted_links(links)
    con.execute(f"CREATE OR REPLACE TEMP VIEW aw AS SELECT * FROM read_parquet('{path}')")
    one = lambda q: con.execute(q).fetchone()
    n_rows, n_acif, n_work = one("SELECT count(*), count(DISTINCT cluster_id), count(DISTINCT work_idx) FROM aw")
    per_acif = con.execute("SELECT count(*) n FROM aw GROUP BY cluster_id").fetchdf().n
    multi_author = one("SELECT count(*) FROM aw WHERE len(authorships) > 1")[0]
    shared = one("SELECT count(*) FROM (SELECT work_idx FROM aw GROUP BY 1 HAVING count(*) > 1)")[0]
    au = con.execute("""
        SELECT a.author_idx, count(*) AS n,
               count(*) FILTER (WHERE len(a.institutions) = 0) AS no_inst,
               count(*) FILTER (WHERE list_contains([i.country FOR i IN a.institutions], 'AU')) AS au
        FROM (SELECT unnest(authorships) AS a FROM aw) GROUP BY 1""").fetchdf()
    wc = links.loc[links.status.str.startswith("accept"), ["author_idx", "works_count_global"]].astype({"author_idx": "int64"})
    gap = au.merge(wc, on="author_idx").assign(d=lambda d: d.n - d.works_count_global)
    yrs = con.execute("SELECT quantile_cont(publication_year, [0.01, 0.5]), min(publication_year), "
                      "max(publication_year), count(*) FILTER (WHERE publication_year IS NULL) FROM aw").fetchone()
    flags = one("""SELECT count(*) FILTER (WHERE is_retracted), count(*) FILTER (WHERE is_paratext),
                          count(*) FILTER (WHERE doi IS NULL), count(*) FILTER (WHERE title IS NULL),
                          count(*) FILTER (WHERE fields IS NULL),
                          count(*) FILTER (WHERE dominant_field IS NOT NULL),
                          count(*) FILTER (WHERE dominant_subfield IS NOT NULL),
                          count(*) FILTER (WHERE authors_count >= 100), count(*) FILTER (WHERE authors_count >= 1000),
                          max(authors_count) FROM aw""")
    L = ["## Step 1: ACIF works", "",
         f"- built in {seconds:,.0f} s from {acc.cluster_id.nunique():,} ACIFs' {acc.author_idx.nunique():,} accepted "
         f"OpenAlex authors",
         f"- rows (ACIF, work): {n_rows:,}; ACIFs with works: {n_acif:,}; distinct works: {n_work:,}",
         f"- works reached through 2+ of the same ACIF's authors: {multi_author:,}; "
         f"works held by 2+ ACIFs: {shared:,}",
         "- works per ACIF: " + ", ".join(f"p{q}: {per_acif.quantile(q / 100):,.0f}" for q in (10, 50, 90, 99))
         + f", max {per_acif.max():,}",
         f"- authorships with no institution: {int(au.no_inst.sum()):,}; with an Australian institution: {int(au.au.sum()):,}",
         "- works pulled per author minus OpenAlex's works_count: "
         + ", ".join(f"p{q}: {gap.d.quantile(q / 100):,.0f}" for q in (1, 10, 50, 90, 99)),
         f"- publication year: min {int(yrs[1])}, p1 {int(yrs[0][0])}, median {int(yrs[0][1])}, max {int(yrs[2])}; "
         f"missing {yrs[3]:,}",
         f"- retracted {flags[0]:,}; paratext {flags[1]:,}; no DOI {flags[2]:,}; no title {flags[3]:,}; "
         f"no topics {flags[4]:,}",
         f"- dominant field (> {DOMINANT_SHARE:.0%} of topic weight): {flags[5]:,}; dominant subfield: {flags[6]:,}",
         f"- works with 100+ authors {flags[7]:,}, 1000+ {flags[8]:,}, max {flags[9]:,}", "",
         "| type | (ACIF, work) rows |", "|---|---|"]
    for t_, n in con.execute("SELECT type, count(*) n FROM aw GROUP BY 1 ORDER BY 2 DESC").fetchall():
        L.append(f"| {t_} | {n:,} |")
    return L + [""]


def main():
    OEUVRE_DIR.mkdir(parents=True, exist_ok=True)
    links = pd.read_parquet(LINKS)
    con = connect()
    t = time.time()
    build_acif_works(links, OEUVRE_DIR / "acif_works.parquet", con)
    secs = time.time() - t
    text = "\n".join(["# Oeuvres", ""] + acif_works_section(con, OEUVRE_DIR / "acif_works.parquet", links, secs))
    (OEUVRE_DIR / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
