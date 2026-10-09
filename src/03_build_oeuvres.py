"""
src/03_build_oeuvres.py -- the oeuvre extractor (2026-10-06/07): works of each ACIF from the OpenAlex
authors (author_idx) accepted for it by the linker (processed/oax_link/accepted_links.parquet: every
stage since 2026-10-09, user; ORCID links only before). Built one step at a time; each step writes
its table to OEUVRE_DIR and a section of report.md. Plan: /home/lc/.claude/plans/sunny-sauteeing-peach.md.

Steps so far:
  1. acif_works.parquet   one row per (ACIF, work): the authorships through the ACIF's linked
                          authors (printed name, institutions), work metadata, field weights
                          (src/oeuvre/acif_works.py)
  2. acif_works_kept.parquet / work_drops.parquet   rows dropped as paratext, retracted, a type not
                          kept, or no institution on the ACIF's own authorship and no DOI (src/oeuvre/work_filter.py)
  3. acif_works_single.parquet   one row per (ACIF, work): versions (same DOI or usable normalised
                          title) reduced to the version of record, earliest year as publication_year
                          (src/oeuvre/versions.py)
  4. acif_work_graph.parquet     each ACIF's works joined by shared co-author / own institution /
                          venue; connected components; anchors (ARC co-investigator co-author,
                          grant university in grant years); the anchored core (src/oeuvre/work_graph.py)
  5. acif_works_classified.parquet   accept / reject / unsure per (ACIF, work): rules, then the
                          Gemini verdicts already saved by src/03a_gemini_judge.py, then the person's trusted
                          Scopus profile for 'fits core' accepts and works still unjudged (src/oeuvre/classify.py,
                          src/oeuvre/scopus_check.py; documents fetched by 00f)
Next: person report.

Usage: .venv/bin/python src/03_build_oeuvres.py [--from-step 5]   (5: reuse steps 1-4's files)
"""

import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import ACIFS_ARC, OEUVRE_DIR
from src.oeuvre import classify as cls
from src.oeuvre.acif_works import DOMINANT_SHARE, LINKS, accepted_links, build_acif_works, connect
from src.oeuvre.gemini import load_verdicts
from src.oeuvre.orcid_works import claimed_dois
from src.oeuvre.scopus_check import trusted_profile_dois
from src.oeuvre.versions import MAX_TITLE_WORKS, reduce_versions
from src.oeuvre.work_filter import KEEP_TYPES, filter_works
from src.oeuvre.work_graph import HYPER_AUTHORS, VENUE_MAX_WORKS, acif_inputs, build_work_graph

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
    by_stage = links.groupby("stage").cluster_id.nunique() if "stage" in links else pd.Series(dtype=int)
    L = ["## Step 1: ACIF works", "",
         "- linked ACIFs by linker stage: " + ", ".join(f"{k} {v:,}" for k, v in by_stage.items()),
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


def filter_section(con, n_in: int, kept: int, dropped: int) -> list[str]:
    kp, dp = OEUVRE_DIR / "acif_works_kept.parquet", OEUVRE_DIR / "work_drops.parquet"
    one = lambda q: con.execute(q).fetchone()
    L = ["## Step 2: drop non-research and corrupt records", "",
         f"- kept types: {', '.join(KEEP_TYPES)}",
         f"- rows in {n_in:,}; kept {kept:,}; dropped {dropped:,}", "",
         "| drop reason | rows |", "|---|---|"]
    for r, n in con.execute(f"SELECT drop_reason, count(*) FROM read_parquet('{dp}') GROUP BY 1").fetchall():
        L.append(f"| {r} | {n:,} |")
    L += ["", "Dropped types (reason 'type'):", "", "| type | rows |", "|---|---|"]
    for t_, n in con.execute(f"SELECT coalesce(type, '(none)'), count(*) n FROM read_parquet('{dp}') "
                             "WHERE drop_reason = 'type' GROUP BY 1 ORDER BY 2 DESC").fetchall():
        L.append(f"| {t_} | {n:,} |")
    L += ["", "Dropped as no_inst_no_doi, by decade:", "", "| decade | rows |", "|---|---|"]
    for d, n in con.execute(f"SELECT (publication_year // 10) * 10 AS d, count(*) FROM read_parquet('{dp}') "
                            "WHERE drop_reason = 'no_inst_no_doi' GROUP BY 1 ORDER BY 1 NULLS LAST").fetchall():
        L.append(f"| {'missing' if d is None else int(d)} | {n:,} |")
    n_acif, n_work = one(f"SELECT count(DISTINCT cluster_id), count(DISTINCT work_idx) FROM read_parquet('{kp}')")
    L += ["", f"- kept: {n_acif:,} ACIFs, {n_work:,} distinct works", "", "| type | kept rows |", "|---|---|"]
    for t_, n in con.execute(f"SELECT type, count(*) n FROM read_parquet('{kp}') GROUP BY 1 ORDER BY 2 DESC").fetchall():
        L.append(f"| {t_} | {n:,} |")
    return L + [""]


def versions_section(con, counts: dict) -> list[str]:
    path = OEUVRE_DIR / "acif_works_single.parquet"
    con.execute(f"CREATE OR REPLACE TEMP VIEW sv AS SELECT * FROM read_parquet('{path}')")
    q = lambda s_: con.execute(s_).fetchall()
    one = lambda s_: con.execute(s_).fetchone()
    multi = "n_versions > 1"
    L = ["## Step 3: one version per work", "",
         f"- rows in {counts['rows_in']:,}; works out {counts['rows_out']:,}; works with 2+ versions "
         f"{counts['groups']:,} (rows merged away: {counts['rows_in'] - counts['rows_out']:,})",
         f"- titles refused as generic (held by more than {MAX_TITLE_WORKS} distinct works): {counts['titles_refused']:,}",
         f"- version groups split into editions (same source, or book chapters, in different years): "
         f"{counts['split_groups']:,}; editions out: " + f"{one('SELECT count(*) FROM sv WHERE edition_year IS NOT NULL')[0]:,}",
         f"- duplicate records (same source, volume, issue, first page) resolved to the most cited: "
         f"{counts['duplicate_places']:,}",
         "- ACIFs / distinct work_idx out: " + " / ".join(f"{x:,}" for x in one(
             "SELECT count(DISTINCT cluster_id), count(DISTINCT work_idx) FROM sv")), "",
         "| versions | works |", "|---|---|"]
    L += [f"| {n} | {c:,} |" for n, c in q("SELECT least(n_versions, 6), count(*) FROM sv GROUP BY 1 ORDER BY 1")]
    L += ["", "| linked by | works |", "|---|---|"]
    L += [f"| {k} | {c:,} |" for k, c in q(f"SELECT linked_by, count(*) FROM sv WHERE {multi} GROUP BY 1 ORDER BY 2 DESC")]
    L += ["", "Years between the earliest version and the version of record:", "", "| years | works |", "|---|---|"]
    L += [f"| {'missing' if d is None else d} | {c:,} |" for d, c in q(
        f"SELECT least(vor_publication_year - publication_year, 11), count(*) FROM sv WHERE {multi} GROUP BY 1 ORDER BY 1 NULLS LAST")]
    L += ["", "Version of record, works with 2+ versions:", "", "| type | source type | works |", "|---|---|---|"]
    L += [f"| {t_} | {st} | {c:,} |" for t_, st, c in q(
        f"SELECT type, coalesce(source_type, '(none)'), count(*) FROM sv WHERE {multi} GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15")]
    later = one(f"""SELECT count(*) FROM sv WHERE {multi} AND
                    list_max([x.publication_year FOR x IN versions]) > vor_publication_year""")[0]
    share = one(f"""SELECT quantile_cont(cited_by_count / cited_by_count_versions, [0.1, 0.25, 0.5]) FROM sv
                    WHERE {multi} AND cited_by_count_versions > 0""")[0]
    L += ["", f"- version of record older than another version: {later:,}",
          "- version of record's share of all versions' citations: "
          + ", ".join(f"p{p}: {x:.2f}" for p, x in zip((10, 25, 50), share)), "", "Examples (random works with 2+ versions):", ""]
    def show(where, n):
        out = []
        for r in con.execute(f"SELECT cluster_id, title, versions FROM sv WHERE {where} "
                             f"ORDER BY hash(cluster_id || work_idx) LIMIT {n}").fetchall():
            out.append(f"- {r[0]}: {r[1][:90]} -- " + "; ".join(
                f"{x['type']} {x['publication_year']} {x['source_type'] or '-'} {x['doi'] or 'no DOI'} ({x['cited_by_count']} cites)"
                for x in r[2]))
        return out
    L += show(multi, 10)
    L += ["", "Examples, versions 10+ years apart:", ""] + show(f"{multi} AND vor_publication_year - publication_year >= 10", 6)
    L += ["", "Examples, 5+ versions:", ""] + show("n_versions >= 5", 4)
    L += ["", "| editions in the group | type of record | works |", "|---|---|---|"]
    L += [f"| {n} | {t_} | {c:,} |" for n, t_, c in q(
        "SELECT least(n_editions, 5), type, count(*) FROM sv WHERE n_editions > 1 GROUP BY 1, 2 ORDER BY 1, 3 DESC")]
    L += ["", "Examples, editions (one line per edition):", ""]
    for g in con.execute("SELECT DISTINCT cluster_id, version_group FROM sv WHERE n_editions > 1 "
                         "ORDER BY hash(cluster_id || version_group) LIMIT 6").fetchall():
        L += show(f"cluster_id = '{g[0]}' AND version_group = {g[1]}", 10)
    return L + [""]


def graph_section(con, counts: dict) -> list[str]:
    path = OEUVRE_DIR / "acif_work_graph.parquet"
    con.execute(f"CREATE OR REPLACE TEMP VIEW gv AS SELECT * FROM read_parquet('{path}')")
    q = lambda s_: con.execute(s_).fetchall()
    one = lambda s_: con.execute(s_).fetchone()
    n = one("SELECT count(*), count(DISTINCT cluster_id) FROM gv")
    L = ["## Step 4: work graph and core", "",
         f"- links (work, feature) kept: " + ", ".join(f"{k[6:]} {v:,}" for k, v in counts.items() if k.startswith("links_"))
         + f"; co-author links only on works with < {HYPER_AUTHORS} authors; venues: no repositories or ebook "
           f"platforms, none with > {VENUE_MAX_WORKS:,} works",
         f"- label propagation converged in {counts['iterations']} iterations",
         f"- (ACIF, work) rows {n[0]:,}; ACIFs {n[1]:,}", "",
         "| work is | rows | share |", "|---|---|---|"]
    for k, c in q("""SELECT CASE WHEN in_core THEN 'in the core' WHEN component_size = 1 THEN 'isolated (no link)'
                     ELSE 'in another component' END, count(*) FROM gv GROUP BY 1 ORDER BY 2 DESC"""):
        L.append(f"| {k} | {c:,} | {c / n[0]:.1%} |")
    L += ["", "Links of works in the core (a work can have several kinds):", ""]
    L += [f"- {k}: {c:,}" for k, c in q("""SELECT unnest(['coauthor', 'institution', 'venue']),
          unnest([count(*) FILTER (WHERE coauthor_links > 0), count(*) FILTER (WHERE institution_links > 0),
                  count(*) FILTER (WHERE venue_links > 0)]) FROM gv WHERE in_core""")]
    L += ["", "Anchored works: " + ", ".join(f"{k} {c:,}" for k, c in q(
        """SELECT unnest(['co-investigator co-author', 'grant university', 'either', 'either, outside the core']),
                  unnest([count(*) FILTER (WHERE anchor_coinvestigator), count(*) FILTER (WHERE anchor_grant_university),
                          count(*) FILTER (WHERE anchor_coinvestigator OR anchor_grant_university),
                          count(*) FILTER (WHERE (anchor_coinvestigator OR anchor_grant_university) AND NOT in_core)])
           FROM gv"""))]
    con.execute("""CREATE OR REPLACE TEMP TABLE acs AS
        SELECT cluster_id, count(*) n, count(*) FILTER (WHERE in_core) n_core, any_value(core_by) core_by,
               count(DISTINCT component) n_comp, count(*) FILTER (WHERE component_size = 1) n_iso,
               max(component_size) FILTER (WHERE NOT in_core) AS n_second, max(component_size) AS biggest
        FROM gv GROUP BY 1""")
    L += ["", "Per ACIF, share of works in the core:", "", "| core share | ACIFs |", "|---|---|"]
    L += [f"| {k} | {c:,} |" for k, c in q("""SELECT CASE WHEN n_core = n THEN '100%' WHEN n_core >= 0.95 * n THEN '95-99%'
        WHEN n_core >= 0.8 * n THEN '80-94%' WHEN n_core >= 0.5 * n THEN '50-79%' ELSE '< 50%' END k, count(*)
        FROM acs GROUP BY 1 ORDER BY min(n_core / n) DESC""")]
    L += ["", "- core chosen by anchors / by size: " + " / ".join(f"{c:,}" for c in one(
        "SELECT count(*) FILTER (WHERE core_by = 'anchors'), count(*) FILTER (WHERE core_by = 'size') FROM acs")),
          f"- core is not the largest component: {one('SELECT count(*) FROM acs WHERE n_core < biggest')[0]:,}",
          f"- ACIFs with a second component of 5+ works and 10%+ of works (possible mixed record): "
          f"{one('SELECT count(*) FROM acs WHERE n_second >= 5 AND n_second >= 0.1 * n')[0]:,}", "",
          "Largest second components:", ""]
    for r in q("""SELECT cluster_id, n, n_core, n_second, n_iso, core_by FROM acs WHERE n_second >= 5
                  ORDER BY n_second DESC LIMIT 15"""):
        L.append(f"- {r[0]}: {r[1]} works, core {r[2]}, second component {r[3]}, isolated {r[4]} (core by {r[5]})")
    L += ["", "Test cases:", ""]
    for pat in ("%_ian_white", "%_linda_graham", "%_peter_hoffmann", "%_kaile_su", "%_yasir_ali", "%_willy_susilo"):
        for r in q(f"""SELECT cluster_id, n, n_core, n_comp, n_iso, n_second, core_by FROM acs WHERE cluster_id LIKE '{pat}'"""):
            L.append(f"- {r[0]}: {r[1]} works, core {r[2]}, components {r[3]}, isolated {r[4]}, "
                     f"largest other component {r[5]}, core by {r[6]}")
    return L + [""]


def graph_section(con, counts: dict) -> list[str]:
    path = OEUVRE_DIR / "acif_work_graph.parquet"
    con.execute(f"CREATE OR REPLACE TEMP VIEW gv AS SELECT * FROM read_parquet('{path}')")
    q = lambda s_: con.execute(s_).fetchall()
    one = lambda s_: con.execute(s_).fetchone()
    n = one("SELECT count(*), count(DISTINCT cluster_id) FROM gv")
    L = ["## Step 4: work graph and core", "",
         f"- links (work, feature) kept: " + ", ".join(f"{k[6:]} {v:,}" for k, v in counts.items() if k.startswith("links_"))
         + f"; co-author links only on works with < {HYPER_AUTHORS} authors; venues: no repositories or ebook "
           f"platforms, none with > {VENUE_MAX_WORKS:,} works",
         f"- label propagation converged in {counts['iterations']} iterations",
         f"- (ACIF, work) rows {n[0]:,}; ACIFs {n[1]:,}", "",
         "| work is | rows | share |", "|---|---|---|"]
    for k, c in q("""SELECT CASE WHEN in_core THEN 'in the core' WHEN component_size = 1 THEN 'isolated (no link)'
                     ELSE 'in another component' END, count(*) FROM gv GROUP BY 1 ORDER BY 2 DESC"""):
        L.append(f"| {k} | {c:,} | {c / n[0]:.1%} |")
    L += ["", "Links of works in the core (a work can have several kinds):", ""]
    L += [f"- {k}: {c:,}" for k, c in q("""SELECT unnest(['coauthor', 'institution', 'venue']),
          unnest([count(*) FILTER (WHERE coauthor_links > 0), count(*) FILTER (WHERE institution_links > 0),
                  count(*) FILTER (WHERE venue_links > 0)]) FROM gv WHERE in_core""")]
    L += ["", "Anchored works: " + ", ".join(f"{k} {c:,}" for k, c in q(
        """SELECT unnest(['co-investigator co-author', 'grant university', 'either', 'either, outside the core']),
                  unnest([count(*) FILTER (WHERE anchor_coinvestigator), count(*) FILTER (WHERE anchor_grant_university),
                          count(*) FILTER (WHERE anchor_coinvestigator OR anchor_grant_university),
                          count(*) FILTER (WHERE (anchor_coinvestigator OR anchor_grant_university) AND NOT in_core)])
           FROM gv"""))]
    con.execute("""CREATE OR REPLACE TEMP TABLE acs AS
        SELECT cluster_id, count(*) n, count(*) FILTER (WHERE in_core) n_core, any_value(core_by) core_by,
               count(DISTINCT component) n_comp, count(*) FILTER (WHERE component_size = 1) n_iso,
               max(component_size) FILTER (WHERE NOT in_core) AS n_second, max(component_size) AS biggest
        FROM gv GROUP BY 1""")
    L += ["", "Per ACIF, share of works in the core:", "", "| core share | ACIFs |", "|---|---|"]
    L += [f"| {k} | {c:,} |" for k, c in q("""SELECT CASE WHEN n_core = n THEN '100%' WHEN n_core >= 0.95 * n THEN '95-99%'
        WHEN n_core >= 0.8 * n THEN '80-94%' WHEN n_core >= 0.5 * n THEN '50-79%' ELSE '< 50%' END k, count(*)
        FROM acs GROUP BY 1 ORDER BY min(n_core / n) DESC""")]
    L += ["", "- core chosen by anchors / by size: " + " / ".join(f"{c:,}" for c in one(
        "SELECT count(*) FILTER (WHERE core_by = 'anchors'), count(*) FILTER (WHERE core_by = 'size') FROM acs")),
          f"- core is not the largest component: {one('SELECT count(*) FROM acs WHERE n_core < biggest')[0]:,}",
          f"- ACIFs with a second component of 5+ works and 10%+ of works (possible mixed record): "
          f"{one('SELECT count(*) FROM acs WHERE n_second >= 5 AND n_second >= 0.1 * n')[0]:,}", "",
          "Largest second components:", ""]
    for r in q("""SELECT cluster_id, n, n_core, n_second, n_iso, core_by FROM acs WHERE n_second >= 5
                  ORDER BY n_second DESC LIMIT 15"""):
        L.append(f"- {r[0]}: {r[1]} works, core {r[2]}, second component {r[3]}, isolated {r[4]} (core by {r[5]})")
    L += ["", "Test cases:", ""]
    for pat in ("%_ian_white", "%_linda_graham", "%_peter_hoffmann", "%_kaile_su", "%_yasir_ali", "%_willy_susilo"):
        for r in q(f"""SELECT cluster_id, n, n_core, n_comp, n_iso, n_second, core_by FROM acs WHERE cluster_id LIKE '{pat}'"""):
            L.append(f"- {r[0]}: {r[1]} works, core {r[2]}, components {r[3]}, isolated {r[4]}, "
                     f"largest other component {r[5]}, core by {r[6]}")
    return L + [""]


def classify_section(con, req: pd.DataFrame, verdicts: dict) -> list[str]:
    path = OEUVRE_DIR / "acif_works_classified.parquet"
    con.execute(f"CREATE OR REPLACE TEMP VIEW kv AS SELECT * FROM read_parquet('{path}')")
    q = lambda s_: con.execute(s_).fetchall()
    n = con.execute("SELECT count(*) FROM kv").fetchone()[0]
    L = ["## Step 5: accept / reject / unsure", "",
         f"Rules: pre-career {cls.PRE_CAREER} years; namesake component = {cls.BIG_COMPONENT}+ works, "
         f">= {cls.SAME_YEARS:.0%} in the core's years, field cosine < {cls.SIMILAR_FIELD}; fits core = core field "
         f"(>= {cls.FIELD_MIN_SHARE:.0%} of core works) or unknown, within {cls.FIT_PAD} years of the core.", "",
         "| decision | rule | decided by | rows | share |", "|---|---|---|---|---|"]
    for d, r, b, c in q("SELECT decision, rule, coalesce(decided_by, '-'), count(*) FROM kv GROUP BY ALL ORDER BY 1, 4 DESC"):
        L.append(f"| {d} | {r} | {b} | {c:,} | {c / n:.2%} |")
    L += ["", "| decision | rows |", "|---|---|"]
    L += [f"| {d} | {c:,} |" for d, c in q("SELECT decision, count(*) FROM kv GROUP BY 1 ORDER BY 2 DESC")]
    L += ["", "Scopus: 'fits core' accepts and unjudged works decided from the person's trusted Scopus profile "
              "(carrying the ACIF's ORCID): on it -> accept; an article or review with a DOI absent from it -> reject; "
              "otherwise unsure.", "",
          "- ACIFs with a fetched trusted profile: " + f"{q('SELECT count(DISTINCT cluster_id) FROM kv WHERE on_scopus_profile IS NOT NULL')[0][0]:,}",
          "", "Share of works on the trusted profile, by rule (ACIFs with a trusted profile):", "",
          "| rule | works | on profile |", "|---|---|---|"]
    L += [f"| {r} | {n:,} | {s_:.1%} |" for r, n, s_ in q(
        "SELECT rule, count(*), avg(on_scopus_profile::int) FROM kv WHERE on_scopus_profile IS NOT NULL GROUP BY 1 ORDER BY 2 DESC")]
    L += ["", f"Reference-work entries: {cls.REF_AUTHORS}+ authors, or {cls.REF_AUTHORS_MULTI}+ with "
              f"{cls.REF_SAME_BOOK}+ chapters of one book held by the ACIF (book = ISBN in the DOI).", "",
          "Gemini requests (works already judged are not re-sent; their verdicts are applied per work):", "",
          "| kind | requests | answered | works in them | est. input tokens of unanswered |", "|---|---|---|---|---|"]
    for k, sub in req.groupby("kind"):
        a = sub.request_key.isin(set(verdicts))
        L.append(f"| {k} | {len(sub):,} | {int(a.sum()):,} | {int(sub.work_idxs.map(len).sum()):,} | "
                 f"{int(sub.loc[~a, 'prompt'].str.len().sum() / 4):,} |")
    used = [v for v in verdicts.values()]
    if used:
        L += ["", f"Gemini calls saved: {len(used):,}; input tokens {sum(v['input_tokens'] or 0 for v in used):,}; "
              f"output tokens {sum(v['output_tokens'] or 0 for v in used):,}; unparsed answers "
              f"{sum(v['answer'] is None for v in used):,}", "", "Component verdicts:", ""]
        comp = [v for v in used if v["kind"] == "component" and isinstance(v["answer"], dict)]
        L += [f"- {k}: {c}" for k, c in Counter((v["answer"].get("verdict"), v["answer"].get("confidence")) for v in comp).most_common()]
        L += ["", "Examples:", ""]
        rk = req.set_index("request_key")
        for v in comp[:40]:
            r = rk.loc[v["request_key"]] if v["request_key"] in rk.index else None
            n_w = len(r.work_idxs) if r is not None else "?"
            L.append(f"- {v['cluster_id']} ({n_w} works): {v['answer'].get('verdict')} / {v['answer'].get('confidence')} -- "
                     f"{v['answer'].get('reason')}")
    return L + [""]


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-step", type=int, default=1, choices=[1, 5])
    args = ap.parse_args()
    OEUVRE_DIR.mkdir(parents=True, exist_ok=True)
    con = connect()
    if args.from_step == 5:
        old = (OEUVRE_DIR / "report.md").read_text(encoding="utf-8").split("## Step 5")[0].rstrip("\n").split("\n")
        text = "\n".join(old + [""] + step5(con))
        (OEUVRE_DIR / "report.md").write_text(text, encoding="utf-8")
        print("\n".join(step5_lines_cache))
        return
    links = pd.read_parquet(LINKS)
    t = time.time()
    n_in = build_acif_works(links, OEUVRE_DIR / "acif_works.parquet", con)
    secs = time.time() - t
    kept, dropped = filter_works(con, OEUVRE_DIR / "acif_works.parquet", OEUVRE_DIR / "acif_works_kept.parquet",
                                 OEUVRE_DIR / "work_drops.parquet")
    counts = reduce_versions(con, OEUVRE_DIR / "acif_works_kept.parquet", OEUVRE_DIR / "acif_works_single.parquet")
    acifs, coinv = acif_inputs()
    gcounts = build_work_graph(con, OEUVRE_DIR / "acif_works_single.parquet", OEUVRE_DIR / "acif_work_graph.parquet",
                               acifs, coinv)
    text = "\n".join(["# Oeuvres", ""] + acif_works_section(con, OEUVRE_DIR / "acif_works.parquet", links, secs)
                     + filter_section(con, n_in, kept, dropped) + versions_section(con, counts)
                     + graph_section(con, gcounts) + step5(con))
    (OEUVRE_DIR / "report.md").write_text(text, encoding="utf-8")
    print(text)


step5_lines_cache: list[str] = []


def step5(con) -> list[str]:
    acifs_full = pd.read_parquet(ACIFS_ARC)
    verdicts = load_verdicts(OEUVRE_DIR / "gemini_verdicts.jsonl")
    sdois, sacifs = trusted_profile_dois(acifs_full)
    req = cls.classify(con, OEUVRE_DIR / "acif_works_single.parquet", OEUVRE_DIR / "acif_work_graph.parquet",
                       acifs_full, verdicts, OEUVRE_DIR / "acif_works_classified.parquet",
                       OEUVRE_DIR / "gemini_requests.parquet", orcid_dois=claimed_dois(acifs_full),
                       scopus_dois=sdois, scopus_acifs=sacifs)
    lines = classify_section(con, req, verdicts)
    step5_lines_cache[:] = lines
    return lines


if __name__ == "__main__":
    main()
