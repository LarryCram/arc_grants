"""
Linker stage 2, third kind (2026-10-08, user): a Scopus-to-OpenAlex DOI bridge, for ACIFs the name
and works-first routes left unlinked. 00d's Author Search (name + any grant university anywhere in
the profile's affiliation history) found Scopus author profiles for many of them; 00f
(--targets unlinked) fetched every document of those profiles. Scopus has already decided which
papers belong to that profile, so its DOIs point at the OpenAlex record(s) holding the same papers.

Profiles used: the profiles 00d's searches found for the ACIF's records, less any carrying an ORCID
other than the ACIF's own and any whose ORCID was refused by hand for one of the ACIF's records
(reject_scopus rows of arc_name_overrides.csv). The ACIF is used only when exactly ONE profile is
left: the same single-profile rule as Scopus pass one in the ACIF build (a namesake's profile is
possible when the true person has none at that university -- see the calibration line in the report).

Candidates: every OpenAlex author on the profile's works who shares a first initial and family
name with the ACIF on any parser key (the blocking level of stage 2; Scopus has already put the
papers under one person, so "Lyn D. Beazley" may stand for ARC's "Lynda Beazley"). The record holding
the most of the profile's DOIs is taken when it holds MIN_SHARED+ of them and MIN_SHARE+ of the
profile's DOIs found in OpenAlex, and no other record ties it; it is linked unless an earlier stage
linked it to another ACIF (record_linked_elsewhere: possibly the same person, reported only) or it
carries an ORCID other than the ACIF's (record_other_orcid), or its main name is incompatible with
the ACIF's by stage 2's test (record_name_differs: e.g. "Senyuan Zhang" for "Shao-wu Zhang", but also
nicknames such as "Lyn D. Beazley" for "Lynda Beazley" -- reported only, unless (2026-10-09, user:
yes to nicknames) a full given name of each is nickname-related (src/utils/names.py::
given_names_related: the `nicknames` package's English list, or a clipped form of 3+ letters):
accept_scopus_nickname). The share floor stops a minor record
being linked when the person's main record is out of reach (e.g. "Maria A. Fiatarone Singh", whose
OpenAlex name parses to family "singh": 2 of 280 DOIs fell on a minor record).

Calibration (2026-10-09) on 10,418 ORCID-linked ACIFs whose trusted profile (carrying the ACIF's ORCID)
was fetched, nothing taken: a record is taken for 10,393, and it is the ORCID-linked one for 10,365
(99.7%). (A first version limited to stage 2's name-compatible candidates linked minor records when
the main one was not a candidate -- 55 of 436 links held < 30% of the profile's DOIs.)

Decision per ACIF: accept_scopus, accept_scopus_nickname, record_linked_elsewhere, record_other_orcid, record_name_differs,
several_records (a tie),
weak_shared_record, no_candidate (no same-name author on the profile's works), several_profiles,
no_profile (no usable profile, or its documents not fetched).
"""

from __future__ import annotations

import pandas as pd

from config.settings import ACIF_ARC_RECORDS, OPENALEX_COMPACT_DIR, PROCESSED_DATA, SCOPUS_EXTRACT_DIR

AUTHORSHIPS = OPENALEX_COMPACT_DIR / "authorships"
WORKS = OPENALEX_COMPACT_DIR / "works"
AUTHORS_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
MIN_SHARED = 2
MIN_SHARE = 0.2


def searched_profiles(acif_ids, records=ACIF_ARC_RECORDS, extract=SCOPUS_EXTRACT_DIR) -> pd.DataFrame:
    """(cluster_id, scopus_id, profile_orcid): every profile 00d's searches found for the ACIFs'
    records (00d searched per ACIF of an earlier build stage; its records map to the current ACIFs)."""
    rec = pd.read_parquet(records, columns=["unique_id", "cluster_id"])
    rec = rec[rec.cluster_id.isin(set(acif_ids))]
    summ = pd.read_parquet(extract / "scopus_acif_summary.parquet", columns=["cluster_id", "unique_ids"])
    m = (summ.explode("unique_ids").rename(columns={"cluster_id": "search_id", "unique_ids": "unique_id"})
         .merge(rec, on="unique_id")[["search_id", "cluster_id"]].drop_duplicates())
    prof = pd.read_parquet(extract / "scopus_acif_profiles.parquet", columns=["cluster_id", "scopus_id", "orcid"])
    p = prof.rename(columns={"cluster_id": "search_id", "orcid": "profile_orcid"}).merge(m, on="search_id")
    p = p.astype({"scopus_id": str})[["cluster_id", "scopus_id", "profile_orcid"]]
    p["profile_orcid"] = p.profile_orcid.where(p.profile_orcid.map(lambda x: isinstance(x, str)), None)
    return p.drop_duplicates(["cluster_id", "scopus_id"]).sort_values(["cluster_id", "scopus_id"]).reset_index(drop=True)


def usable_profiles(profiles: pd.DataFrame, acif_orcid: dict, refused: pd.DataFrame) -> pd.DataFrame:
    """profiles less those carrying another ORCID than the ACIF's, or an ORCID refused by hand for the
    ACIF (refused: cluster_id, orcid)."""
    bad = set(zip(refused.cluster_id, refused.orcid))
    keep = [not (isinstance(o, str) and ((acif_orcid.get(c) and o != acif_orcid[c]) or (c, o) in bad))
            for c, o in zip(profiles.cluster_id, profiles.profile_orcid)]
    return profiles[keep].reset_index(drop=True)


def refused_orcids(acif_ids, records=ACIF_ARC_RECORDS, extract=SCOPUS_EXTRACT_DIR) -> pd.DataFrame:
    """(cluster_id, orcid) refused by hand (reject_scopus) for one of the ACIF's records."""
    rec = pd.read_parquet(records, columns=["unique_id", "cluster_id"])
    rej = pd.read_parquet(extract / "scopus_rejections.parquet")
    r = rej.merge(rec, on="unique_id")
    return r[r.cluster_id.isin(set(acif_ids))][["cluster_id", "orcid"]].drop_duplicates()


def bridge_evidence(con, acifs: pd.DataFrame, profiles: pd.DataFrame, docs: pd.DataFrame,
                    works=WORKS, authorships=AUTHORSHIPS, prep=AUTHORS_PREP) -> pd.DataFrame:
    """One row per (cluster_id, scopus_id, author_idx): every OpenAlex author on the profile's works
    who shares a first initial and family name with the ACIF on any parser key (blocking level only:
    Scopus has already put these papers under one person), with the profile's DOIs, how many are
    OpenAlex works, and how many of those are the author's. acifs: cluster_id, full_name_keys;
    profiles: (cluster_id, scopus_id); docs: (scopus_id, doi)."""
    from src.oax.name_link import _key_parts
    d = docs[docs.doi.notna()][["scopus_id", "doi"]].astype({"scopus_id": str}).drop_duplicates()
    d = d.assign(doi=d.doi.str.lower())
    con.register("sb_pd", profiles[["cluster_id", "scopus_id"]].merge(d, on="scopus_id"))
    con.register("sb_ak", _key_parts(acifs[acifs.cluster_id.isin(set(profiles.cluster_id))], "cluster_id")[["cluster_id", "ini", "fam"]])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sb_w AS
        SELECT DISTINCT lower(w.doi) AS doi, w.work_idx FROM read_parquet('{works}/*.parquet') w
        WHERE lower(w.doi) IN (SELECT DISTINCT doi FROM sb_pd)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sb_aw AS
        SELECT DISTINCT a.author_idx, a.work_idx FROM read_parquet('{authorships}/*.parquet') a
        WHERE a.work_idx IN (SELECT work_idx FROM sb_w) AND a.author_idx IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sb_ok AS
        SELECT author_idx, orcid AS author_orcid, full_name AS author_name, left(k, 1) AS ini,
               substr(k, strpos(k, '_') + 1) AS fam
        FROM (SELECT author_idx, orcid, full_name, unnest(full_name_keys) AS k FROM read_parquet('{prep}')
              WHERE author_idx IN (SELECT author_idx FROM sb_aw))
        WHERE strpos(k, '_') > 0""")
    return con.execute("""
        WITH pw AS (SELECT DISTINCT p.cluster_id, p.scopus_id, p.doi, w.work_idx FROM sb_pd p JOIN sb_w w USING (doi)),
             pn AS (SELECT p.cluster_id, p.scopus_id, count(DISTINCT p.doi) AS profile_dois,
                           count(DISTINCT w.doi) AS profile_dois_in_openalex
                    FROM sb_pd p LEFT JOIN sb_w w USING (doi) GROUP BY 1, 2),
             cand AS (SELECT DISTINCT pw.cluster_id, pw.scopus_id, pw.doi, a.author_idx
                      FROM pw JOIN sb_aw a USING (work_idx)
                      WHERE EXISTS (SELECT 1 FROM sb_ok o JOIN sb_ak k ON k.ini = o.ini AND k.fam = o.fam
                                    WHERE o.author_idx = a.author_idx AND k.cluster_id = pw.cluster_id)),
             nm AS (SELECT author_idx, any_value(author_orcid) AS author_orcid, any_value(author_name) AS author_name
                    FROM sb_ok GROUP BY 1)
        SELECT c.cluster_id, c.scopus_id, c.author_idx, nm.author_name, nm.author_orcid, pn.profile_dois,
               pn.profile_dois_in_openalex, count(DISTINCT c.doi) AS shared_dois
        FROM cand c JOIN pn USING (cluster_id, scopus_id) JOIN nm USING (author_idx)
        GROUP BY ALL ORDER BY 1, 2, 3""").fetchdf()


def top_records(d: pd.DataFrame | None) -> pd.DataFrame:
    """The record(s) holding the most of the profile's DOIs, if that is MIN_SHARED+ and MIN_SHARE+ of
    the profile's OpenAlex DOIs (2+ rows = a tie)."""
    if d is None or not len(d):
        return d.iloc[0:0] if d is not None else pd.DataFrame(columns=["author_idx", "shared_dois"])
    top = d[d.shared_dois == d.shared_dois.max()]
    r = top.iloc[0]
    if r.shared_dois < MIN_SHARED or r.shared_dois < MIN_SHARE * r.profile_dois_in_openalex:
        return d.iloc[0:0]
    return top


def full_givens(keys) -> set:
    """The full given names (2+ letters) in a list of parser keys."""
    return {k.split("_", 1)[0] for k in (keys if keys is not None else []) if "_" in k and len(k.split("_", 1)[0]) > 1}


def nickname_related(acif_keys, record_keys) -> bool:
    """A full given name of the ACIF and one of the record are nickname-related (names.given_names_related)."""
    from src.utils.names import given_names_related
    return any(given_names_related(a, b) for a in full_givens(acif_keys) for b in full_givens(record_keys))


def decide(acif_ids, profiles: pd.DataFrame, fetched: set, ev: pd.DataFrame, acif_orcid: dict,
           taken: dict, tiers: dict | None = None, nicknames: set | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(decisions, links). profiles: usable profiles; fetched: scopus_ids whose documents were fetched;
    acif_orcid: cluster_id -> the ACIF's ORCID; taken: author_idx -> the ACIF an earlier stage linked it to;
    tiers: (cluster_id, author_idx) -> name tier of the top records (src/oax/name_link.py::name_tier);
    nicknames: (cluster_id, author_idx) top records whose given name is a nickname of the ACIF's."""
    tiers, nicknames = tiers or {}, nicknames or set()
    np_ = profiles.groupby("cluster_id").scopus_id.apply(list).to_dict()
    by = {c: d for c, d in ev.groupby("cluster_id")}
    dec, links = [], []
    for cid in acif_ids:
        ps = np_.get(cid, [])
        sid = ps[0] if len(ps) == 1 else None
        if len(ps) > 1:
            dec.append((cid, "several_profiles", None, None, None, len(ps)))
            continue
        if sid is None or sid not in fetched:
            dec.append((cid, "no_profile", sid, None, None, len(ps)))
            continue
        d = by.get(cid)
        if d is None or not len(d):
            dec.append((cid, "no_candidate", sid, None, None, 1))
            continue
        top = top_records(d)
        if not len(top):
            dec.append((cid, "weak_shared_record", sid, None, None, 1))
            continue
        if len(top) > 1:
            dec.append((cid, "several_records", sid, None, None, 1))
            continue
        r = top.iloc[0]
        aid, other = int(r.author_idx), taken.get(int(r.author_idx))
        own = acif_orcid.get(cid)
        if other is not None:
            status = "record_linked_elsewhere"
        elif isinstance(r.author_orcid, str) and own and r.author_orcid != own:
            status = "record_other_orcid"
        elif pd.isna(tiers.get((cid, aid))) and (cid, aid) not in nicknames:
            status = "record_name_differs"
        else:
            status = "accept_scopus" if not pd.isna(tiers.get((cid, aid))) else "accept_scopus_nickname"
            links.append((cid, aid, r.author_name, sid, int(r.shared_dois), int(r.profile_dois_in_openalex), status))
        dec.append((cid, status, sid, aid, other, 1))
    return (pd.DataFrame(dec, columns=["cluster_id", "status", "scopus_id", "author_idx", "linked_to_acif", "n_profiles"]),
            pd.DataFrame(links, columns=["cluster_id", "author_idx", "author_name", "scopus_id", "shared_dois",
                                         "profile_dois_in_openalex", "status"]))


def calibrate(ev: pd.DataFrame, correct: pd.DataFrame) -> dict:
    """On ACIFs whose true records are known (stage-1 ORCID links), with evidence from their trusted
    profile (carrying the ACIF's ORCID): how often the rule picks a record, and how often that record
    is the ORCID-linked one."""
    ok = set(zip(correct.cluster_id, correct.author_idx.astype("int64")))
    linked = right = ties = 0
    for cid, d in ev.groupby("cluster_id"):
        t = top_records(d)
        ties += len(t) > 1
        if len(t) == 1:
            linked += 1
            right += (cid, int(t.author_idx.iloc[0])) in ok
    return {"acifs": ev.cluster_id.nunique(), "linked": linked, "correct": right, "ties": ties}
