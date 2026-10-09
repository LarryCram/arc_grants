"""
Linker stage 2, first kind (2026-10-08, user): name plus institution-in-time, for kept ACIFs that
stage 1 (ORCID links) did not link. Nothing else is used -- no field, co-author or works evidence.

Candidates: OpenAlex pool authors (openalex_authors_prep) that share a first initial and family name
with the ACIF on any parser key (blocking only), then judged on MAIN names -- first given name +
family: the ACIF's records' 00a full_name_key, the record's display-name full_name_key -- in two tiers:
  full   a main name with a full given name is shared: an ACIF main key is among the record's keys,
         or the record's main key is among the ACIF's keys (as stage 1's 'main' test)
  loose  the main names are not incompatible: same family, and the first given names are equal or
         one is a single initial of the other ("J Bloggs" ~ "Jim Bloggs"; "Jim" !~ "Joan"). Middle
         names never make names compatible (a first run let "Jillian R. Sewell" pass for Robert)
The loose tier is used only when no full-tier candidate passes the test (calibration 2026-10-08:
the loose tier alone lets initial-only OpenAlex records compete and decides fewer ACIFs).
Records already accepted by stage 1 for any ACIF are not candidates; a record carrying an ORCID
other than the ACIF's own (when the ACIF has one) is not a candidate.

Test: the record's own OpenAlex affiliations (authors.affiliations: institution + years) show at
least MIN_YEARS distinct years at a grant university of one of the ACIF's single-institution grants
(n_eligible_orgs = 1; the institution or any institution whose lineage includes it), within
WINDOW_BEFORE years before to WINDOW_AFTER years after the grant's commencement year, closed
one year after the grant's anticipated end when that is earlier (a grant ended early).

Decision per ACIF: accept when exactly one candidate passes in the tier used; otherwise several_pass,
none_pass, no_candidate or no_single_institution_grant (left for the works-first route).
Calibration on ORCID-linked ACIFs (2026-10-08): when exactly one record passes, it is the
ORCID-linked one in ~99% of cases.
"""

from __future__ import annotations

import pandas as pd

from config.settings import ACIF_ARC_RECORDS, OAX_AUTHORS, PROCESSED_DATA

AUTHORS_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
GRANTS_FLAT = PROCESSED_DATA / "grants_flat.parquet"
MIN_YEARS = 2
WINDOW_BEFORE = 1
WINDOW_AFTER = 3


def single_institution_windows(acif_ids, records=ACIF_ARC_RECORDS, grants=GRANTS_FLAT) -> pd.DataFrame:
    """(cluster_id, grant_code, inst, y0, y1) for the ACIFs' single-institution grants: inst is the
    grant's administering organisation(s) as an OpenAlex institution id ('I...')."""
    from src.acif.build import load_grant_org_facts
    rec = pd.read_parquet(records, columns=["cluster_id", "grant_code"]).drop_duplicates()
    rec = rec[rec.cluster_id.isin(set(acif_ids))]
    g = pd.read_parquet(grants, columns=["grant_code", "funding_commence_year", "n_eligible_orgs", "end_year"])
    g = g[g.n_eligible_orgs == 1]
    facts = load_grant_org_facts()
    gi = pd.DataFrame([(gc, i.rsplit("/", 1)[1]) for gc, f in facts.items() for i in f["inst_ids"]],
                      columns=["grant_code", "inst"])
    w = rec.merge(g, on="grant_code").merge(gi, on="grant_code")
    w["y0"] = (w.funding_commence_year - WINDOW_BEFORE).astype("int64")
    w["y1"] = (w.funding_commence_year + WINDOW_AFTER).astype("int64")
    # a grant that ended early (relinquished, e.g. Pryce's 2013 DECRA, ended 2014) closes the window
    cap = w.end_year + 1
    w["y1"] = w.y1.where(cap.isna() | (cap >= w.y1), cap).astype("int64")
    return w[["cluster_id", "grant_code", "inst", "y0", "y1"]].reset_index(drop=True)


def acif_main_keys(acif_ids, records=ACIF_ARC_RECORDS, names=PROCESSED_DATA / "arc_names.parquet") -> pd.DataFrame:
    """(cluster_id, k): each ACIF's records' main name keys from 00a (full_name_key, else the raw
    key), initial-only ones included."""
    rec = pd.read_parquet(records, columns=["unique_id", "cluster_id"])
    rec = rec[rec.cluster_id.isin(set(acif_ids))]
    a = pd.read_parquet(names, columns=["unique_id", "full_name_key", "full_name_key_raw"])
    a["k"] = a.full_name_key.where(a.full_name_key.fillna("") != "", a.full_name_key_raw)
    m = rec.merge(a[["unique_id", "k"]], on="unique_id")
    m = m[m.k.fillna("").str.contains("_")]
    return m[["cluster_id", "k"]].drop_duplicates().reset_index(drop=True)


def _key_parts(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    k = df[[id_col, "full_name_keys"]].explode("full_name_keys").dropna().rename(columns={"full_name_keys": "k"})
    k["giv"] = k.k.str.split("_", n=1).str[0]
    k["fam"] = k.k.str.split("_", n=1).str[1]
    k["ini"] = k.giv.str[0]
    return k.dropna(subset=["fam"])


def name_links(con, acifs: pd.DataFrame, taken, windows: pd.DataFrame, arc_main: pd.DataFrame,
               prep=AUTHORS_PREP, authors=OAX_AUTHORS) -> tuple[pd.DataFrame, pd.DataFrame]:
    """acifs: the ACIFs to link (cluster_id, orcids, full_name_keys); taken: author_idx already linked
    by stage 1; windows: single_institution_windows(); arc_main: acif_main_keys(). Returns (pairs, decisions): every candidate
    with its tier, years at a grant university and whether it passes; one decision row per ACIF."""
    con.register("nl_ak", _key_parts(acifs, "cluster_id"))
    con.register("nl_acif", acifs[["cluster_id"]].assign(
        orcid=acifs.orcids.map(lambda o: o[0] if o is not None and len(o) else None)))
    con.register("nl_taken", pd.DataFrame({"author_idx": pd.Series(sorted(set(taken)), dtype="int64")}))
    con.register("nl_win", windows)
    am = arc_main.copy()
    am["giv"] = am.k.str.split("_", n=1).str[0]
    am["fam"] = am.k.str.split("_", n=1).str[1]
    con.register("nl_am", am)
    con.execute("""CREATE OR REPLACE TEMP TABLE nl_af AS
        SELECT cluster_id, ini, fam, coalesce(list(DISTINCT giv) FILTER (WHERE len(giv) > 1), []) AS fulls
        FROM nl_ak GROUP BY ALL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nl_ok AS
        SELECT author_idx, orcid, full_name, main_key, k, split_part(k, '_', 1) AS giv,
               substr(k, strpos(k, '_') + 1) AS fam, left(k, 1) AS ini
        FROM (SELECT author_idx, orcid, full_name, full_name_key AS main_key, unnest(full_name_keys) AS k
              FROM read_parquet('{prep}'))
        WHERE strpos(k, '_') > 0""")
    con.execute("""CREATE OR REPLACE TEMP TABLE nl_of AS
        SELECT o.author_idx, any_value(o.orcid) AS orcid, any_value(o.full_name) AS full_name, o.ini, o.fam,
               coalesce(list(DISTINCT o.giv) FILTER (WHERE len(o.giv) > 1), []) AS fulls
        FROM nl_ok o JOIN (SELECT DISTINCT ini, fam FROM nl_af) USING (ini, fam)
        WHERE o.author_idx NOT IN (SELECT author_idx FROM nl_taken) GROUP BY o.author_idx, o.ini, o.fam""")
    con.execute("""CREATE OR REPLACE TEMP TABLE nl_block AS
        SELECT DISTINCT a.cluster_id, o.author_idx
        FROM nl_af a JOIN nl_of o USING (ini, fam) JOIN nl_acif c USING (cluster_id)
        WHERE c.orcid IS NULL OR o.orcid IS NULL OR o.orcid = c.orcid""")
    con.execute("""CREATE OR REPLACE TEMP TABLE nl_om AS
        SELECT DISTINCT author_idx, main_key AS k, split_part(main_key, '_', 1) AS giv,
               substr(main_key, strpos(main_key, '_') + 1) AS fam
        FROM nl_ok WHERE strpos(main_key, '_') > 0 AND author_idx IN (SELECT author_idx FROM nl_block)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE nl_full AS
        SELECT DISTINCT b.cluster_id, b.author_idx FROM nl_block b
        JOIN nl_am a ON a.cluster_id = b.cluster_id AND len(a.giv) > 1
        JOIN nl_ok o ON o.author_idx = b.author_idx AND o.k = a.k
        UNION
        SELECT DISTINCT b.cluster_id, b.author_idx FROM nl_block b
        JOIN nl_om m ON m.author_idx = b.author_idx AND len(m.giv) > 1
        JOIN nl_ak k ON k.cluster_id = b.cluster_id AND k.k = m.k""")
    con.execute("""CREATE OR REPLACE TEMP TABLE nl_loose AS
        SELECT DISTINCT b.cluster_id, b.author_idx FROM nl_block b
        JOIN nl_am a ON a.cluster_id = b.cluster_id
        JOIN nl_om m ON m.author_idx = b.author_idx AND m.fam = a.fam
        WHERE a.giv = m.giv OR (len(a.giv) = 1 AND left(m.giv, 1) = a.giv) OR (len(m.giv) = 1 AND left(a.giv, 1) = m.giv)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE nl_cand AS
        SELECT b.cluster_id, b.author_idx, any_value(o.full_name) AS author_name, any_value(o.orcid) AS author_orcid,
               (f.author_idx IS NOT NULL) AS full_tier, (f.author_idx IS NOT NULL OR l.author_idx IS NOT NULL) AS compatible
        FROM nl_block b JOIN nl_ok o ON o.author_idx = b.author_idx
        LEFT JOIN nl_full f ON f.cluster_id = b.cluster_id AND f.author_idx = b.author_idx
        LEFT JOIN nl_loose l ON l.cluster_id = b.cluster_id AND l.author_idx = b.author_idx
        GROUP BY b.cluster_id, b.author_idx, f.author_idx, l.author_idx""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nl_aff AS
        WITH a AS (SELECT au.author_idx, split_part(af.institution.id, '/', -1) AS inst,
                          [split_part(x, '/', -1) FOR x IN coalesce(af.institution.lineage, [])] AS lin, af.years AS ys
                   FROM read_parquet('{authors}/*.parquet') au, unnest(au.affiliations) t(af)
                   WHERE au.author_idx IN (SELECT DISTINCT author_idx FROM nl_cand WHERE compatible))
        SELECT a.author_idx, g.inst, unnest(a.ys) AS y
        FROM a JOIN (SELECT DISTINCT inst FROM nl_win) g ON a.inst = g.inst OR list_contains(a.lin, g.inst)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE nl_years AS
        SELECT w.cluster_id, f.author_idx, count(DISTINCT f.y) AS years_at_grant_university
        FROM nl_win w JOIN nl_aff f ON f.inst = w.inst AND f.y BETWEEN w.y0 AND w.y1 GROUP BY 1, 2""")
    pairs = con.execute(f"""
        SELECT c.cluster_id, c.author_idx, c.author_name, c.author_orcid,
               CASE WHEN c.full_tier THEN 'full' ELSE 'loose' END AS tier,
               coalesce(y.years_at_grant_university, 0) AS years_at_grant_university,
               coalesce(y.years_at_grant_university, 0) >= {MIN_YEARS} AS passes
        FROM nl_cand c LEFT JOIN nl_years y USING (cluster_id, author_idx)
        WHERE c.compatible ORDER BY 1, 2""").fetchdf()
    return pairs, decide(acifs.cluster_id, pairs, set(windows.cluster_id))


def name_tier(con, pairs: pd.DataFrame, acifs: pd.DataFrame, arc_main: pd.DataFrame, prep=AUTHORS_PREP) -> pd.DataFrame:
    """The name tier of given (cluster_id, author_idx) pairs by the same tests as name_links():
    'full' (a main name with a full given name is shared), 'loose' (main names not incompatible) or
    None. acifs: cluster_id, full_name_keys; arc_main: acif_main_keys()."""
    p = pairs[["cluster_id", "author_idx"]].astype({"author_idx": "int64"}).drop_duplicates()
    con.register("nt_p", p)
    con.register("nt_ak", _key_parts(acifs[acifs.cluster_id.isin(set(p.cluster_id))], "cluster_id")[["cluster_id", "k"]])
    am = arc_main[arc_main.cluster_id.isin(set(p.cluster_id))].copy()
    am["giv"] = am.k.str.split("_", n=1).str[0]
    am["fam"] = am.k.str.split("_", n=1).str[1]
    con.register("nt_am", am)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nt_ok AS
        SELECT author_idx, main_key, k FROM (SELECT author_idx, full_name_key AS main_key, unnest(full_name_keys) AS k
            FROM read_parquet('{prep}') WHERE author_idx IN (SELECT author_idx FROM nt_p))""")
    con.execute("""CREATE OR REPLACE TEMP TABLE nt_om AS
        SELECT DISTINCT author_idx, main_key AS k, split_part(main_key, '_', 1) AS giv,
               substr(main_key, strpos(main_key, '_') + 1) AS fam FROM nt_ok WHERE strpos(main_key, '_') > 0""")
    return con.execute("""
        WITH f AS (SELECT DISTINCT p.cluster_id, p.author_idx FROM nt_p p
                   JOIN nt_am a ON a.cluster_id = p.cluster_id AND len(a.giv) > 1
                   JOIN nt_ok o ON o.author_idx = p.author_idx AND o.k = a.k
                   UNION
                   SELECT DISTINCT p.cluster_id, p.author_idx FROM nt_p p
                   JOIN nt_om m ON m.author_idx = p.author_idx AND len(m.giv) > 1
                   JOIN nt_ak k ON k.cluster_id = p.cluster_id AND k.k = m.k),
             l AS (SELECT DISTINCT p.cluster_id, p.author_idx FROM nt_p p
                   JOIN nt_am a ON a.cluster_id = p.cluster_id
                   JOIN nt_om m ON m.author_idx = p.author_idx AND m.fam = a.fam
                   WHERE a.giv = m.giv OR (len(a.giv) = 1 AND left(m.giv, 1) = a.giv)
                         OR (len(m.giv) = 1 AND left(a.giv, 1) = m.giv))
        SELECT p.cluster_id, p.author_idx,
               CASE WHEN f.author_idx IS NOT NULL THEN 'full' WHEN l.author_idx IS NOT NULL THEN 'loose' END AS tier
        FROM nt_p p LEFT JOIN f USING (cluster_id, author_idx) LEFT JOIN l USING (cluster_id, author_idx)""").fetchdf()


def decide(acif_ids, pairs: pd.DataFrame, testable: set) -> pd.DataFrame:
    """One row per ACIF: status, tier used, the accepted author_idx (if any), passing count."""
    out = []
    by = {c: d for c, d in pairs.groupby("cluster_id")}
    for cid in acif_ids:
        d = by.get(cid)
        if cid not in testable:
            out.append((cid, "no_single_institution_grant", None, None, 0, 0 if d is None else len(d)))
            continue
        if d is None or not len(d):
            out.append((cid, "no_candidate", None, None, 0, 0))
            continue
        full = d[(d.tier == "full") & d.passes]
        used, p = ("full", full) if len(full) else ("loose", d[d.passes])
        status = "accept" if len(p) == 1 else ("several_pass" if len(p) > 1 else "none_pass")
        out.append((cid, status, used if len(p) else None, int(p.author_idx.iloc[0]) if len(p) == 1 else None,
                    len(p), len(d)))
    return pd.DataFrame(out, columns=["cluster_id", "status", "tier", "author_idx", "n_passing", "n_candidates"])


def calibrate(pairs: pd.DataFrame, dec: pd.DataFrame, correct: pd.DataFrame) -> dict:
    """Score stage 2 on ACIFs whose true record is known (stage-1 ORCID links): `pairs`/`dec` from
    name_links() run on those ACIFs with nothing taken; `correct`: (cluster_id, author_idx) of their
    accepted ORCID links. Returns shares of ACIFs where only the correct record passes, the correct
    and another, only others, or nothing; and how often a single passing record is a correct one."""
    ok = set(zip(correct.cluster_id, correct.author_idx.astype("int64")))
    p = pairs[pairs.passes].copy()
    p["correct"] = [(c, int(a)) in ok for c, a in zip(p.cluster_id, p.author_idx)]
    g = p.groupby("cluster_id").agg(cp=("correct", "any"), op=("correct", lambda s: int((~s).sum())))
    n = dec[dec.status != "no_single_institution_grant"].cluster_id.nunique()
    acc = dec[dec.status == "accept"]
    acc_ok = sum((c, int(a)) in ok for c, a in zip(acc.cluster_id, acc.author_idx))
    return {"acifs": n,
            "only_correct": int((g.cp & (g.op == 0)).sum()), "correct_and_other": int((g.cp & (g.op > 0)).sum()),
            "only_others": int((~g.cp & (g.op > 0)).sum()), "accepted": len(acc), "accepted_correct": acc_ok}
