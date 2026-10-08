"""
Linker stage 2, second kind (2026-10-08, user): works-first, for ACIFs the first kind left open
(several candidates passed, none passed, or no single-institution grant). The candidates are the
same name-compatible pool records as the first kind (src/oax/name_link.py); here their WORKS are
looked at, and a record is linked on work-level anchors:

  co-investigator  a work of the record is co-authored by an OpenAlex author already linked (stage 1
                   or stage 2) to one of the ACIF's ARC co-investigators
  university       the record's own affiliation on a work is an administering university of one of
                   the ACIF's grants (any grant, multi-institution ones included), published from
                   WINDOW_BEFORE years before the grant's commencement to WINDOW_AFTER years after
                   its funded years end

Decision per ACIF:
  accept_coinvestigator  the one record with the most co-investigator works, when it has >=
                         MIN_COINV_WORKS and no other record ties it (calibration 2026-10-08 on
                         ORCID-linked ACIFs: that record is the ORCID-linked one in ~99%; linking
                         every record with a co-investigator work was right for only 77% of records
                         -- namesake records reach a co-investigator through mixed records)
  several_coinvestigator 2+ records tie for the most co-investigator works
  accept_university      otherwise, exactly one record has university works in >= MIN_UNI_YEARS
                         distinct years
  several_university     2+ records have such university works
  no_anchor / no_candidate
Records already linked by stage 1 to any ACIF are not candidates (as in the first kind).
"""

from __future__ import annotations

import pandas as pd

from config.settings import ACIF_ARC_RECORDS, OPENALEX_COMPACT_DIR, OPENALEX_DIR, PROCESSED_DATA

AUTHORSHIPS = OPENALEX_COMPACT_DIR / "authorships"
WORKS = OPENALEX_COMPACT_DIR / "works"
GRANTS_FLAT = PROCESSED_DATA / "grants_flat.parquet"
WINDOW_BEFORE = 1
WINDOW_AFTER = 2
MIN_COINV_WORKS = 2
MIN_UNI_YEARS = 2


def grant_windows(acif_ids, records=ACIF_ARC_RECORDS, grants=GRANTS_FLAT) -> pd.DataFrame:
    """(cluster_id, grant_code, inst, y0, y1) for every grant of the ACIFs: inst = an administering
    organisation (current or at announcement) as an OpenAlex institution id."""
    from src.acif.build import load_grant_org_facts
    rec = pd.read_parquet(records, columns=["cluster_id", "grant_code"]).drop_duplicates()
    rec = rec[rec.cluster_id.isin(set(acif_ids))]
    g = pd.read_parquet(grants, columns=["grant_code", "funding_commence_year", "years_funded"])
    facts = load_grant_org_facts()
    gi = pd.DataFrame([(gc, i.rsplit("/", 1)[1]) for gc, f in facts.items() for i in f["inst_ids"]],
                      columns=["grant_code", "inst"])
    w = rec.merge(g, on="grant_code").merge(gi, on="grant_code")
    w["y0"] = (w.funding_commence_year - WINDOW_BEFORE).astype("int64")
    w["y1"] = (w.funding_commence_year + w.years_funded.fillna(3) + WINDOW_AFTER).astype("int64")
    # also every OpenAlex institution whose lineage includes the university (its faculties, institutes)
    import duckdb
    con = duckdb.connect()
    con.register("u", pd.DataFrame({"inst": sorted(set(w.inst))}))
    child = con.execute(f"""SELECT DISTINCT u.inst, 'I' || CAST(i.institution_idx AS VARCHAR) AS child
        FROM read_parquet('{OPENALEX_DIR}/institutions.parquet') i, unnest(i.lineage) t(l)
        JOIN u ON split_part(l, '/', -1) = u.inst""").fetchdf()
    w = w.merge(child, on="inst", how="left")
    w["inst"] = w.child.fillna(w.inst)
    return w[["cluster_id", "grant_code", "inst", "y0", "y1"]].drop_duplicates().reset_index(drop=True)


def coinvestigator_authors(acifs: pd.DataFrame, linked: pd.DataFrame) -> pd.DataFrame:
    """(cluster_id, author_idx): the linked OpenAlex authors of each ACIF's ARC co-investigators.
    acifs: cluster_id, coawardee_acif_ids; linked: (cluster_id, author_idx) accepted links."""
    co = acifs[["cluster_id", "coawardee_acif_ids"]].explode("coawardee_acif_ids").dropna()
    co = co.merge(linked.rename(columns={"cluster_id": "coawardee_acif_ids"}), on="coawardee_acif_ids")
    return co[["cluster_id", "author_idx"]].astype({"author_idx": "int64"}).drop_duplicates()


def works_evidence(con, candidates: pd.DataFrame, coinv: pd.DataFrame, windows: pd.DataFrame,
                   authorships=AUTHORSHIPS, works=WORKS) -> pd.DataFrame:
    """One row per (cluster_id, author_idx) candidate: its works, co-investigator works, university
    works and the distinct years of those."""
    cand = candidates[["cluster_id", "author_idx"]].astype({"author_idx": "int64"}).drop_duplicates()
    # a candidate that is itself a co-investigator's linked record would count every work it has
    cand = cand.merge(coinv.assign(_c=1), on=["cluster_id", "author_idx"], how="left")
    con.register("wl_cand", cand[cand._c.isna()].drop(columns="_c"))
    con.register("wl_coinv", coinv)
    win = windows.assign(inst_idx=windows.inst.str.lstrip("I").astype("int64"))
    con.register("wl_win", win)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE wl_au AS
        SELECT DISTINCT a.author_idx, a.work_idx, a.institution_idx
        FROM read_parquet('{authorships}/*.parquet') a
        WHERE a.author_idx IN (SELECT author_idx FROM wl_cand UNION SELECT author_idx FROM wl_coinv)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE wl_y AS
        SELECT w.work_idx, w.publication_year AS y FROM read_parquet('{works}/*.parquet') w
        WHERE w.work_idx IN (SELECT work_idx FROM wl_au)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE wl_cw AS
        SELECT DISTINCT c.cluster_id, c.author_idx, a.work_idx FROM wl_cand c JOIN wl_au a USING (author_idx)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE wl_coinv_works AS
        SELECT DISTINCT ci.cluster_id, a.work_idx FROM wl_coinv ci JOIN wl_au a USING (author_idx)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE wl_uni AS
        SELECT DISTINCT c.cluster_id, c.author_idx, a.work_idx, y.y
        FROM wl_cand c JOIN wl_au a USING (author_idx) JOIN wl_y y USING (work_idx)
        JOIN wl_win w ON w.cluster_id = c.cluster_id AND w.inst_idx = a.institution_idx
                     AND y.y BETWEEN w.y0 AND w.y1""")
    return con.execute("""
        WITH n AS (SELECT cluster_id, author_idx, count(*) AS n_works FROM wl_cw GROUP BY 1, 2),
             cv AS (SELECT cw.cluster_id, cw.author_idx, count(*) AS coinv_works
                    FROM wl_cw cw JOIN wl_coinv_works v USING (cluster_id, work_idx) GROUP BY 1, 2),
             u AS (SELECT cluster_id, author_idx, count(DISTINCT work_idx) AS uni_works, count(DISTINCT y) AS uni_years
                   FROM wl_uni GROUP BY 1, 2)
        SELECT c.cluster_id, c.author_idx, coalesce(n.n_works, 0) AS n_works, coalesce(cv.coinv_works, 0) AS coinv_works,
               coalesce(u.uni_works, 0) AS uni_works, coalesce(u.uni_years, 0) AS uni_years
        FROM wl_cand c LEFT JOIN n USING (cluster_id, author_idx) LEFT JOIN cv USING (cluster_id, author_idx)
        LEFT JOIN u USING (cluster_id, author_idx) ORDER BY 1, 2""").fetchdf()


def decide(acif_ids, ev: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(decisions, links): one decision row per ACIF; one link row per accepted (ACIF, record)."""
    by = {c: d for c, d in ev.groupby("cluster_id")}
    dec, links = [], []
    for cid in acif_ids:
        d = by.get(cid)
        if d is None or not len(d):
            dec.append((cid, "no_candidate", 0, 0))
            continue
        top = d[d.coinv_works == d.coinv_works.max()] if d.coinv_works.max() >= MIN_COINV_WORKS else d.iloc[0:0]
        uni = d[d.uni_years >= MIN_UNI_YEARS]
        if len(top) == 1:
            status, acc = "accept_coinvestigator", top
        elif len(top) > 1:
            status, acc = "several_coinvestigator", d.iloc[0:0]
        elif len(uni) == 1:
            status, acc = "accept_university", uni
        elif len(uni) > 1:
            status, acc = "several_university", d.iloc[0:0]
        else:
            status, acc = "no_anchor", d.iloc[0:0]
        dec.append((cid, status, len(acc), len(d)))
        links += [(cid, int(r.author_idx), status) for r in acc.itertuples()]
    return (pd.DataFrame(dec, columns=["cluster_id", "status", "n_linked", "n_candidates"]),
            pd.DataFrame(links, columns=["cluster_id", "author_idx", "status"]))


def calibrate(dec: pd.DataFrame, links: pd.DataFrame, correct: pd.DataFrame) -> dict:
    """On ACIFs whose true records are known (stage-1 ORCID links): of the records linked, how many
    are the ORCID-linked ones, per accepting status."""
    ok = set(zip(correct.cluster_id, correct.author_idx.astype("int64")))
    out = {}
    for st, sub in links.groupby("status"):
        n_ok = sum((c, a) in ok for c, a in zip(sub.cluster_id, sub.author_idx))
        out[st] = {"acifs": sub.cluster_id.nunique(), "records": len(sub), "correct_records": n_ok,
                   "acifs_with_correct": sub[[(c, a) in ok for c, a in zip(sub.cluster_id, sub.author_idx)]].cluster_id.nunique()}
    out["decisions"] = dec.status.value_counts().to_dict()
    return out
