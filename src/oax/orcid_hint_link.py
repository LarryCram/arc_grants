"""
Linker stage 2, fourth kind (2026-10-22, user): ORCID hints, for ACIFs every earlier route left unlinked.

An OpenAlex record that carries an ORCID is a person who curates their own identity -- most records carry
none -- so a same-name record with an ORCID is worth checking through that ORCID's own record (fetched
live from the ORCID API by src/00g_extract_orcid_hints.py into the ORCID record cache). The person's ORCID
names confirm the chain ARC name -> OpenAlex record -> ORCID; the record's works, compared with the ACIF's
grant fields, check that the record is not a namesake in another field (user: "works topics / ARC fields
check whether the author_idx's whole works are incompatible"). No institution test: ORCID job lists are
often current-only (James Dalziel) and early grants name only the administering university (Lineweaver).

Candidates (candidates()), for each ACIF, not linked by an earlier stage to another ACIF:
  name      OpenAlex pool records (openalex_authors_prep) whose parser keys are compatible with the ACIF's
            (names_compatible(): a shared full-given-name key, or a related family name -- equal, equal
            without separators, or one a part of the other's compound name -- with a related full given
            name -- equal without separators, or related in the `nicknames` list: Sue/Susan, Brad/Bradley,
            Killugudi Swaminatha Iyer. No clipped forms: on the calibration they let Hong Ki An stand for
            Hongjie An and Zheng Yang for Zhe Yang)
  orcid_name records carrying an ORCID whose ORCID-bulk-file names share a full-given-name key with the
            ACIF (Valeri Ougrinovski's ORCID is on OpenAlex's "Ugrinovskii, Valery A.")
Only records carrying an ORCID can pass; n_candidates counts every compatible record.

A candidate passes when (evidence(), decide()):
  - its ORCID's own names (live record, else the cached record, else the bulk file) are compatible with
    the ACIF's, and with the record's (OpenAlex put the ORCID on that record);
  - FIT_MIN+ of the record's own works (all its authorships) have their top topic's field among the
    OpenAlex fields of the ACIF's FOR2020 codes (src/utils/for_resolve.oax_field_name);
  - when the ACIF has more than COMMON compatible records, the ORCID record itself lists an Australian
    employment or education (a country, not a university: ORCID job lists are often current-only);
  - the ORCID is not held by another ACIF nor refused by hand for one of the ACIF's records.
Rule choice (2026-10-22), on 1,500 ORCID-linked ACIFs run as if they had no ORCID (records linked to other
ACIFs left out): clipped given names allowed and Australia (ORCID or own authorships) required above 10 records:
97.1% of 940 links right; no clipped names: 97.5%; and the ORCID's own Australian affiliation required above 3
records: 99.0% of 973 (the errors were same-field namesakes with common names: Nick Gill, Ian McLean, Helen
Young, Michael Adams).

Decision per ACIF: accept_orcid_hint (the passing records carry exactly one ORCID; every passing record
carrying it is linked -- OpenAlex fragments of one person, as in stage 1); pass_other_orcid (a candidate passes but the
ACIF holds a different ORCID -- for hand review: one person with two ORCID accounts, or not); several_pass;
none_pass; no_orcid_candidate; no_candidate.
"""

from __future__ import annotations

import re

import pandas as pd

from config.settings import OPENALEX_COMPACT_DIR, ORCID_BULK_PARQUET, PROCESSED_DATA
from src.utils.names import HumanNameParser, given_names_related

AUTHORS_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
AUTHORSHIPS = OPENALEX_COMPACT_DIR / "authorships"
WORK_TOPICS = OPENALEX_COMPACT_DIR / "work_topics"
FIT_MIN = 0.5
COMMON = 3
MIN_CLIPPED = 99        # no clipped given names (module docstring)
CALIBRATION_SAMPLE = 1500
_P = HumanNameParser()


def _compact(s: str) -> str:
    return re.sub(r"[\s\-'’]", "", s)


def _parts(f: str) -> set[str]:
    return {t for t in re.split(r"[\s\-]", f) if len(t) >= 3}


def full_keys(keys) -> set[str]:
    return {k for k in (keys if keys is not None else []) if "_" in k and len(k.split("_", 1)[0]) > 1}


def family_related(a: str, b: str) -> bool:
    return a == b or _compact(a) == _compact(b) or b in _parts(a) or a in _parts(b)


def given_related(a: str, b: str) -> bool:
    return _compact(a) == _compact(b) or given_names_related(a, b, MIN_CLIPPED)


def names_compatible(a_keys, b_keys) -> bool:
    """Two sets of parser keys can be one person's (module docstring)."""
    fa, fb = full_keys(a_keys), full_keys(b_keys)
    if fa & fb:
        return True
    pa = [k.split("_", 1) for k in fa]
    pb = [k.split("_", 1) for k in fb]
    return any(family_related(f1, f2) and given_related(g1, g2) for g1, f1 in pa for g2, f2 in pb)


def candidates(con, acifs: pd.DataFrame, taken: set, prep=AUTHORS_PREP, bulk=ORCID_BULK_PARQUET) -> pd.DataFrame:
    """(cluster_id, author_idx, oax_name, oax_orcid, author_keys, route, n_candidates) for the ACIFs in
    `acifs` (cluster_id, full_name_keys); records in `taken` are left out."""
    keys = {c: full_keys(k) for c, k in zip(acifs.cluster_id, acifs.full_name_keys)}
    tok = pd.DataFrame([(c, t) for c, ks in keys.items() for t in {p for k in ks for f in [k.split("_", 1)[1]]
                                                                 for p in _parts(f) | {f}}],
                       columns=["cluster_id", "tok"]).drop_duplicates()
    con.register("ht_tok", tok)
    rows = con.execute(f"""
        WITH fam AS (SELECT author_idx, unnest(family_names) AS f FROM read_parquet('{prep}')),
             t AS (SELECT DISTINCT author_idx, f AS tok FROM fam
                   UNION SELECT DISTINCT author_idx, unnest(string_split_regex(f, '[ -]')) FROM fam)
        SELECT DISTINCT h.cluster_id, t.author_idx FROM t JOIN ht_tok h USING (tok)""").df()
    con.register("ht_ids", pd.DataFrame({"author_idx": sorted(set(rows.author_idx))}))
    info = con.execute(f"""SELECT author_idx, full_name AS oax_name, orcid AS oax_orcid, full_name_keys AS author_keys
                           FROM read_parquet('{prep}') WHERE author_idx IN (SELECT author_idx FROM ht_ids)""").df()
    rows = rows.merge(info, on="author_idx")
    rows = rows[~rows.author_idx.isin(taken)]
    rows = rows[[names_compatible(keys[c], k) for c, k in zip(rows.cluster_id, rows.author_keys)]].assign(route="name")

    kk = pd.DataFrame([(c, k) for c, ks in keys.items() for k in ks], columns=["cluster_id", "k"])
    con.register("ht_k", kk)
    via = con.execute(f"""
        WITH b AS (SELECT orcid, unnest(all_full_name_keys) AS k FROM read_parquet('{bulk}')),
             o AS (SELECT DISTINCT h.cluster_id, b.orcid FROM b JOIN ht_k h USING (k))
        SELECT o.cluster_id, p.author_idx, p.full_name AS oax_name, p.orcid AS oax_orcid, p.full_name_keys AS author_keys
        FROM o JOIN read_parquet('{prep}') p ON p.orcid = o.orcid""").df()
    via = via[~via.author_idx.isin(taken)].assign(route="orcid_name")
    out = pd.concat([rows, via], ignore_index=True)
    out["route"] = out.groupby(["cluster_id", "author_idx"]).route.transform(lambda s: "+".join(sorted(set(s))))
    out = out.drop_duplicates(["cluster_id", "author_idx"]).reset_index(drop=True)
    out["n_candidates"] = out.groupby("cluster_id").author_idx.transform("size")
    return out


def affiliations(rec: dict) -> list[tuple]:
    """(section, org name, country, start year, end year) of every employment and education entry."""
    def year(d):
        try:
            return int(((d or {}).get("year") or {}).get("value"))
        except (TypeError, ValueError):
            return None
    out = []
    for sec in ("employments", "educations"):
        for g in (((rec.get("activities-summary") or {}).get(sec) or {}).get("affiliation-group") or []):
            for s in g.get("summaries") or []:
                e = next(iter(s.values()), {}) or {}
                org = e.get("organization") or {}
                out.append((sec[:-1], org.get("name"), (org.get("address") or {}).get("country"),
                            year(e.get("start-date")), year(e.get("end-date"))))
    return sorted(out, key=lambda x: (x[3] or 0))


def orcid_facts(con, orcids: set[str], cache, bulk=ORCID_BULK_PARQUET) -> pd.DataFrame:
    """Per ORCID: source (live = fetched by 00g / cache / bulk / none), the record's own name keys,
    name (given + family), au (an Australian employment or education), affiliations (text)."""
    import importlib
    d00 = importlib.import_module("src.00d_extract_scopus")
    con.register("hf_o", pd.DataFrame({"orcid": sorted(orcids)}))
    b = con.execute(f"""SELECT orcid, name, aliases, countries FROM read_parquet('{bulk}')
                        WHERE orcid IN (SELECT orcid FROM hf_o)""").df().set_index("orcid")
    rows = []
    for o in sorted(orcids):
        rec = cache.get(o)
        if d00._usable(rec, o):
            nm = (rec.get("person") or {}).get("name") or {}
            aff = affiliations(rec)
            rows.append((o, "live" if rec.get("_fetched_live") else "cache", sorted(d00.record_name_keys(rec)),
                         " ".join(filter(None, [(nm.get("given-names") or {}).get("value"),
                                                (nm.get("family-name") or {}).get("value")])),
                         any(z[2] == "AU" for z in aff),
                         "; ".join(f"{s[:3]} {n} ({c}) {y0 or ''}-{y1 or ''}" for s, n, c, y0, y1 in aff)))
        elif o in b.index:
            r = b.loc[o]
            names = [n for n in [r["name"], *(list(r.aliases) if r.aliases is not None else [])] if n]
            rows.append((o, "bulk", sorted({k for n in names for k in _P.parse(n).full_name_keys}), r["name"],
                         r.countries is not None and "AU" in list(r.countries), ""))
        else:
            rows.append((o, "none", [], None, False, ""))
    return pd.DataFrame(rows, columns=["orcid", "orcid_source", "orcid_keys", "orcid_name", "orcid_au", "orcid_affiliations"])


def record_evidence(con, author_ids: set[int], authorships=AUTHORSHIPS, topics=WORK_TOPICS) -> tuple[dict, dict, dict]:
    """(author_idx -> {field: works}, author_idx -> works, author_idx -> any Australian own authorship),
    over each record's own authorships (one work counted once; field = its top topic's field)."""
    con.register("hr_ids", pd.DataFrame({"author_idx": sorted(author_ids)}))
    au = con.execute(f"""
        SELECT author_idx, work_idx, bool_or(country_code = 'AU') AS au
        FROM read_parquet('{authorships}/*.parquet') WHERE author_idx IN (SELECT author_idx FROM hr_ids)
        GROUP BY author_idx, work_idx""").df()
    con.register("hr_w", au[["work_idx"]].drop_duplicates())
    f = con.execute(f"""
        SELECT work_idx, field_name FROM read_parquet('{topics}/*.parquet') WHERE work_idx IN (SELECT work_idx FROM hr_w)
        QUALIFY row_number() OVER (PARTITION BY work_idx ORDER BY score DESC) = 1""").df()
    au = au.merge(f, on="work_idx", how="left")
    fields = {a: d.field_name.value_counts().to_dict() for a, d in au.groupby("author_idx")}
    return fields, au.groupby("author_idx").size().to_dict(), au.groupby("author_idx").au.any().to_dict()


def context(acifs: pd.DataFrame, refused_by_record: dict) -> tuple[dict, dict, dict, dict, dict]:
    """(acif_keys, acif_fields, acif_orcid, held, refused) for evidence(), from acifs (cluster_id,
    full_name_keys, orcids, unique_ids, for2020_codes) and build.load_refused_orcids() (unique_id -> ORCIDs)."""
    from src.utils.for_resolve import oax_field_name
    keys = dict(zip(acifs.cluster_id, acifs.full_name_keys))
    fields = {c: {oax_field_name(f["code"]) for f in codes} - {None} for c, codes in zip(acifs.cluster_id, acifs.for2020_codes)}
    own = {c: (o[0] if len(o) else None) for c, o in zip(acifs.cluster_id, acifs.orcids)}
    held = {o[0]: c for c, o in zip(acifs.cluster_id, acifs.orcids) if len(o)}
    refused = {c: set().union(*[refused_by_record.get(u, set()) for u in us]) for c, us in zip(acifs.cluster_id, acifs.unique_ids)}
    return keys, fields, own, held, refused


def evidence(cands: pd.DataFrame, facts: pd.DataFrame, rec: tuple[dict, dict, dict], acif_keys: dict,
             acif_fields: dict, acif_orcid: dict, held: dict, refused: dict) -> pd.DataFrame:
    """One row per candidate carrying an ORCID, with its checks and `passes`. acif_keys / acif_fields /
    acif_orcid: cluster_id -> the ACIF's parser keys / OpenAlex fields / own ORCID (or None); held: ORCID ->
    the ACIF holding it; refused: cluster_id -> ORCIDs refused by hand for its records."""
    fields, works, au = rec
    w = cands[cands.oax_orcid.notna()].merge(facts, left_on="oax_orcid", right_on="orcid", how="left")
    out = []
    for r in w.itertuples():
        aid = int(r.author_idx)
        fd = fields.get(aid, {})
        n = works.get(aid, 0)
        fit = sum(v for k, v in fd.items() if k in acif_fields.get(r.cluster_id, set())) / n if n else 0.0
        ok_arc = names_compatible(acif_keys[r.cluster_id], r.orcid_keys)
        ok_rec = names_compatible(r.orcid_keys, r.author_keys)
        other = held.get(r.oax_orcid)
        if r.orcid_source == "none":
            check = "orcid_record_missing"
        elif other and other != r.cluster_id:
            check = "orcid_held_by_another_acif"
        elif r.oax_orcid in refused.get(r.cluster_id, set()):
            check = "orcid_refused_by_hand"
        elif not ok_arc:
            check = "orcid_names_differ_from_acif"
        elif not ok_rec:
            check = "orcid_names_differ_from_record"
        elif fit < FIT_MIN:
            check = "works_outside_acif_fields"
        elif r.n_candidates > COMMON and not r.orcid_au:
            check = "common_name_no_australian_evidence"
        else:
            check = "pass"
        top = sorted(fd.items(), key=lambda x: -x[1])[:3]
        out.append({"cluster_id": r.cluster_id, "author_idx": aid, "oax_name": r.oax_name, "oax_orcid": r.oax_orcid,
                    "route": r.route, "n_candidates": r.n_candidates, "orcid_source": r.orcid_source,
                    "orcid_name": r.orcid_name, "orcid_au": bool(r.orcid_au), "record_au": bool(au.get(aid, False)),
                    "works": n, "fit": round(fit, 3), "top_fields": "; ".join(f"{k} {v}" for k, v in top),
                    "orcid_affiliations": r.orcid_affiliations, "acif_orcid": acif_orcid.get(r.cluster_id),
                    "check": check, "passes": check == "pass"})
    return pd.DataFrame(out, columns=["cluster_id", "author_idx", "oax_name", "oax_orcid", "route", "n_candidates",
                                      "orcid_source", "orcid_name", "orcid_au", "record_au", "works", "fit",
                                      "top_fields", "orcid_affiliations", "acif_orcid", "check", "passes"])


def decide(acif_ids, cands: pd.DataFrame, ev: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(decisions, links): one decision per ACIF (module docstring); links = accept_orcid_hint rows."""
    nc = cands.groupby("cluster_id").size().to_dict()
    no = cands[cands.oax_orcid.notna()].groupby("cluster_id").size().to_dict()
    by = {c: d for c, d in ev.groupby("cluster_id")}
    dec, links = [], []
    for cid in acif_ids:
        d = by.get(cid)
        p = d[d.passes] if d is not None else ev.iloc[0:0]
        orcid = None
        if not nc.get(cid):
            st = "no_candidate"
        elif not no.get(cid):
            st = "no_orcid_candidate"
        elif not len(p):
            st = "none_pass"
        elif p.oax_orcid.nunique() > 1:
            st = "several_pass"
        else:       # one ORCID passes; every passing record carrying it is linked (OpenAlex fragments of one person)
            orcid = p.oax_orcid.iloc[0]
            own = p.acif_orcid.iloc[0]
            if isinstance(own, str) and own != orcid:
                st = "pass_other_orcid"
            else:
                st = "accept_orcid_hint"
                links += [(cid, int(r.author_idx), r.oax_name, orcid, r.fit, st) for r in p.itertuples()]
        dec.append((cid, st, orcid, nc.get(cid, 0), no.get(cid, 0)))
    return (pd.DataFrame(dec, columns=["cluster_id", "status", "orcid", "n_candidates", "n_with_orcid"]),
            pd.DataFrame(links, columns=["cluster_id", "author_idx", "author_name", "orcid", "fit", "status"]))


def calibrate(dec: pd.DataFrame, links: pd.DataFrame, correct: pd.DataFrame) -> dict:
    """On ACIFs whose records are known (stage-1 ORCID links), run as if they had no ORCID."""
    ok = set(zip(correct.cluster_id, correct.author_idx.astype("int64")))
    right = sum((c, int(a)) in ok for c, a in zip(links.cluster_id, links.author_idx))
    return {"acifs": len(dec), "linked": len(links), "correct": right,
            "several": int((dec.status == "several_pass").sum()), "none": int((dec.status == "none_pass").sum())}
