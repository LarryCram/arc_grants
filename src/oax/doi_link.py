"""
Linker stages 7 and 8 (2026-10-25, user: "do it"), for ACIFs the earlier stages left unlinked: the OpenAlex
record holding most of the works that independent sources tie to the person.

  stage 7, ORCID works   works whose DOI is on the ACIF's own ORCID record (the ORCID record cache;
                         src/oeuvre/orcid_works.claimed_dois). Reaches ACIFs whose ORCID OpenAlex put on no
                         record, or on another person's (rejected) record: Abdullah Al Mamun (310 of his 404
                         ORCID DOIs on one record carrying his second ORCID), Ewald Weber.
  stage 8, grant acks    works acknowledging one of the ACIF's ARC grants (OpenAlex awards, ARC funder
                         F4320334704, funder_award_id = grant code; compact/work_awards), as in
                         analysis/36_grant_ack_test.py.
Evidence: per (ACIF, OpenAlex author) the number of those works carrying an authorship of that author whose
printed name fits the ACIF (src/oeuvre/orcid_works.printed_name_fits: compatible parser keys, or a first
initial matching a full given name under the same family name).
Rule (both stages): the record with the most such works is accepted when it holds MIN_WORKS+ and strictly more
than the next record -- for stage 7 also MIN_SHARE+ of the ACIF's ORCID DOIs found in OpenAlex -- unless it is
linked to another ACIF (record_linked_elsewhere), carries an ORCID another kept ACIF holds or one refused by
hand for the ACIF's records (record_other_orcid). A record carrying an ORCID no ACIF holds is accepted (often
the person's second ORCID account) and listed in the report.
Decisions: accept_orcid_doi / accept_grant_ack, no_evidence, tied, below_threshold, record_linked_elsewhere,
record_other_orcid. calibrate(): the same rule on ORCID-linked ACIFs (taken records ignored); the picked record
should be one of the ACIF's ORCID-linked records.
"""

from __future__ import annotations

import re

import pandas as pd

from config.settings import OAX_AUTHORS, OPENALEX_COMPACT_DIR, OPENALEX_DIR
from src.oeuvre.orcid_works import claimed_dois, printed_name_fits

ARC_FUNDER = "F4320334704"
MIN_WORKS = 2
MIN_SHARE = 0.2


def _fitting(con, cw: pd.DataFrame, keys: dict, authorships=OPENALEX_COMPACT_DIR / "authorships") -> pd.DataFrame:
    """(cluster_id, work_idx, author_idx) for authorships with an author record on the works in cw
    (cluster_id, work_idx) whose printed name fits the ACIF."""
    if cw.empty:
        return pd.DataFrame(columns=["cluster_id", "work_idx", "author_idx"])
    con.register("dl_cw", cw[["work_idx"]].drop_duplicates())
    au = con.execute(f"""SELECT DISTINCT work_idx, author_idx, author_name FROM read_parquet('{authorships}/*.parquet')
                         WHERE work_idx IN (SELECT work_idx FROM dl_cw) AND author_idx IS NOT NULL""").df()
    x = cw.merge(au, on="work_idx")
    names: dict = {}
    p = x[["cluster_id", "author_name"]].drop_duplicates()
    p = p[[printed_name_fits(keys.get(c, []), n, names) for c, n in zip(p.cluster_id, p.author_name)]]
    return x.merge(p, on=["cluster_id", "author_name"])[["cluster_id", "work_idx", "author_idx"]].drop_duplicates()


def orcid_doi_evidence(con, acifs: pd.DataFrame, cache=None, works=OPENALEX_COMPACT_DIR / "works",
                       authorships=OPENALEX_COMPACT_DIR / "authorships") -> tuple[pd.DataFrame, pd.Series]:
    """(evidence: cluster_id, author_idx, n; works found in OpenAlex per ACIF) for `acifs` (cluster_id, orcids,
    full_name_keys)."""
    cd = claimed_dois(acifs, cache)
    if cd.empty:
        return pd.DataFrame(columns=["cluster_id", "author_idx", "n"]), pd.Series(dtype=int)
    con.register("dl_dois", cd)
    cw = con.execute(f"""SELECT d.cluster_id, w.work_idx FROM read_parquet('{works}/*.parquet') w
                         JOIN dl_dois d ON lower(w.doi) = d.doi""").df().drop_duplicates()
    found = cw.groupby("cluster_id").work_idx.nunique()
    x = _fitting(con, cw, dict(zip(acifs.cluster_id, acifs.full_name_keys)), authorships)
    return x.groupby(["cluster_id", "author_idx"]).work_idx.nunique().rename("n").reset_index(), found


def grant_ack_evidence(con, acifs: pd.DataFrame, awards=OPENALEX_DIR / "awards.parquet",
                       work_awards=OPENALEX_COMPACT_DIR / "work_awards",
                       authorships=OPENALEX_COMPACT_DIR / "authorships") -> tuple[pd.DataFrame, pd.Series]:
    """(evidence: cluster_id, author_idx, n; acknowledging works per ACIF) for `acifs` (cluster_id, grant_codes,
    full_name_keys)."""
    aw = con.execute(f"SELECT id, funder_award_id FROM read_parquet('{awards}') WHERE funder.id LIKE '%{ARC_FUNDER}'").df()
    aw["award_idx"] = aw.id.str.extract(r"G(\d+)")[0].astype("int64")
    aw["grant_code"] = aw.funder_award_id.fillna("").str.upper().map(lambda s: re.findall(r"[A-Z]{2}\d{6,9}", s))
    ag = aw.explode("grant_code").dropna(subset=["grant_code"])[["award_idx", "grant_code"]]
    g = acifs[["cluster_id", "grant_codes"]].explode("grant_codes").rename(columns={"grant_codes": "grant_code"})
    ga = g.merge(ag, on="grant_code")
    if ga.empty:
        return pd.DataFrame(columns=["cluster_id", "author_idx", "n"]), pd.Series(dtype=int)
    con.register("dl_aw", ga[["award_idx"]].drop_duplicates())
    wa = con.execute(f"SELECT DISTINCT work_idx, award_idx FROM read_parquet('{work_awards}/*.parquet') "
                     "WHERE award_idx IN (SELECT award_idx FROM dl_aw)").df()
    cw = ga.merge(wa, on="award_idx")[["cluster_id", "work_idx"]].drop_duplicates()
    found = cw.groupby("cluster_id").work_idx.nunique()
    x = _fitting(con, cw, dict(zip(acifs.cluster_id, acifs.full_name_keys)), authorships)
    return x.groupby(["cluster_id", "author_idx"]).work_idx.nunique().rename("n").reset_index(), found


def record_orcids(con, ids, authors=OAX_AUTHORS) -> dict:
    """author_idx -> ORCID (bare) for the OpenAlex authors in `ids` that carry one."""
    con.register("dl_ids", pd.DataFrame({"author_idx": sorted({int(i) for i in ids})}, dtype="int64"))
    r = con.execute(f"""SELECT author_idx, regexp_replace(orcid, '^https?://orcid.org/', '') AS orcid
                        FROM read_parquet('{authors}/*.parquet')
                        WHERE author_idx IN (SELECT author_idx FROM dl_ids) AND orcid IS NOT NULL""").fetchall()
    return {int(a): o for a, o in r}


def decide(acif_ids, ev: pd.DataFrame, found: pd.Series, accept_status: str, taken: dict, rec_orcid: dict,
           held: dict, refused: dict, min_share: float = 0.0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(decisions, links) by the module rule. taken: author_idx -> ACIF linked by an earlier stage; rec_orcid:
    author_idx -> ORCID; held: ORCID -> kept ACIF holding it; refused: cluster_id -> ORCIDs refused by hand."""
    by = {c: d.sort_values(["n", "author_idx"], ascending=[False, True]) for c, d in ev.groupby("cluster_id")}
    dec, links = [], []
    for cid in acif_ids:
        d = by.get(cid)
        nf = int(found.get(cid, 0))
        if d is None or d.empty:
            dec.append((cid, "no_evidence", None, 0, 0, nf))
            continue
        top, n1 = int(d.author_idx.iloc[0]), int(d.n.iloc[0])
        n2 = int(d.n.iloc[1]) if len(d) > 1 else 0
        orc = rec_orcid.get(top)
        if n1 < MIN_WORKS or (min_share and n1 < min_share * nf):
            status = "below_threshold"
        elif n1 == n2:
            status = "tied"
        elif top in taken and taken[top] != cid:
            status = "record_linked_elsewhere"
        elif orc and ((orc in held and held[orc] != cid) or orc in refused.get(cid, set())):
            status = "record_other_orcid"
        else:
            status = accept_status
            links.append((cid, top, n1, n2, nf, orc, status))
        dec.append((cid, status, top, n1, n2, nf))
    return (pd.DataFrame(dec, columns=["cluster_id", "status", "author_idx", "n", "n_second", "works_found"]),
            pd.DataFrame(links, columns=["cluster_id", "author_idx", "n", "n_second", "works_found", "record_orcid", "status"]))


def calibrate(kdec: pd.DataFrame, correct: pd.DataFrame, accept_status: str) -> dict:
    """On ORCID-linked ACIFs: accepted, and how many picked one of the ACIF's ORCID-linked records."""
    ok = set(zip(correct.cluster_id, correct.author_idx.astype("int64")))
    a = kdec[kdec.status == accept_status]
    right = sum((c, int(x)) in ok for c, x in zip(a.cluster_id, a.author_idx))
    return {"acifs": len(kdec), "accepted": len(a), "right": right}
