"""
analysis/31_single_candidate_orcid.py -- unlinked ACIFs with exactly one same-name OpenAlex record, checked
through that record's ORCID (2026-10-22, user: "many of these might provide an OAX ORCID; you can then check
the current API (we only have a few 100). Lineweaver is easy").

Candidates: kept ACIFs the linker left unlinked (unlinked_composition.parquet) whose records' main name keys
(src/acif/name_merge.main_keys) reach exactly one author in openalex_authors_prep (any of its full_name_keys),
that author not accepted for another ACIF. For each candidate record carrying an ORCID, the ORCID's live
/record is fetched from the ORCID Public API (registered client, ORCID_CLIENT_ID/SECRET in .env) and stored in
the project's ORCID record cache (DISKCACHE_DIR/orcid_records_authenticated), replacing an older copy.

Per case:
  names   -- the ORCID record's own names (00d record_name_keys: given+family, credit name, other names) share
             a full-given-name key with the ACIF
  employer -- the record's employments resolved to ARC HEP codes (00e hep_crosswalk / resolve_employers):
             at a HEP on one of the ACIF's grants / at another ARC HEP / none listed at an ARC HEP
  field   -- one of the OpenAlex record's top-3 fields is a field of the ACIF's FOR2020 codes
  held    -- the ORCID is already held by another ACIF, or refused for one of this ACIF's records
Writes processed/single_candidate_orcid.md and .parquet. A report only: nothing is linked.
"""

import importlib
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import diskcache
import duckdb
import pandas as pd
import requests

from config.settings import (ACIF_ARC_RECORDS, ACIFS_ARC, DISKCACHE_DIR, OAX_LINK_DIR, ORCID_CLIENT_ID,
                             ORCID_CLIENT_SECRET, PROCESSED_DATA)
from src.acif.build import load_refused_orcids
from src.acif.name_merge import main_keys
from src.utils.for_resolve import oax_field_name

OUT = PROCESSED_DATA / "single_candidate_orcid"
ORCID_API = "https://pub.orcid.org/v3.0"
d00 = importlib.import_module("src.00d_extract_scopus")
e00 = importlib.import_module("src.00e_extract_orcid_bulk")


def token() -> str:
    r = requests.post("https://orcid.org/oauth/token", headers={"Accept": "application/json"}, timeout=20,
                      data={"client_id": ORCID_CLIENT_ID, "client_secret": ORCID_CLIENT_SECRET,
                            "grant_type": "client_credentials", "scope": "/read-public"})
    r.raise_for_status()
    return r.json()["access_token"]


def fetch(orcids: list[str], cache) -> None:
    tok = token()
    for k, o in enumerate(orcids, 1):
        r = requests.get(f"{ORCID_API}/{o}/record", timeout=30,
                         headers={"Accept": "application/json", "Authorization": f"Bearer {tok}"})
        cache.set(o, r.json() if r.status_code == 200 else {"_error": r.status_code})
        if k % 50 == 0:
            print(f"  fetched {k}/{len(orcids)}")
        time.sleep(0.1)


def affiliations(rec: dict) -> list[tuple]:
    """(section, org name, country, start year, end year) of every employment and education entry."""
    out = []
    for sec in ("employments", "educations"):
        for g in (((rec.get("activities-summary") or {}).get(sec) or {}).get("affiliation-group") or []):
            for s in g.get("summaries") or []:
                e = next(iter(s.values()), {}) or {}
                org = e.get("organization") or {}
                out.append((sec[:-1], org.get("name"), (org.get("address") or {}).get("country"),
                            e00._year(e.get("start-date")), e00._year(e.get("end-date"))))
    return sorted(out, key=lambda x: (x[3] or 0))


def candidates() -> pd.DataFrame:
    mk = main_keys()
    rec = pd.read_parquet(ACIF_ARC_RECORDS, columns=["unique_id", "cluster_id"])
    am = pd.DataFrame([(c, mk[u]) for u, c in zip(rec.unique_id, rec.cluster_id) if u in mk],
                      columns=["cluster_id", "key"]).drop_duplicates()
    con = duckdb.connect()
    con.register("am", am)
    cand = con.execute(f"""
        SELECT am.cluster_id, p.author_idx, p.full_name, p.orcid AS oax_orcid, p.works_count_global, p.sorted_fields
        FROM am JOIN (SELECT author_idx, full_name, orcid, unnest(full_name_keys) AS key, works_count_global,
                             sorted_fields FROM read_parquet('{PROCESSED_DATA}/openalex_authors_prep.parquet')) p
        USING (key)""").df().drop_duplicates(["cluster_id", "author_idx"])
    n = cand.groupby("cluster_id").author_idx.nunique()
    one = cand[cand.cluster_id.isin(n[n == 1].index)]
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    u = pd.read_parquet(PROCESSED_DATA / "unlinked_composition.parquet")
    return one[one.cluster_id.isin(set(u.cluster_id)) & ~one.author_idx.isin(set(acc.author_idx))].merge(
        u[["cluster_id", "div", "first_year", "last_year", "n_grants"]], on="cluster_id")


def main():
    force = "--refetch" in sys.argv
    a = pd.read_parquet(ACIFS_ARC).set_index("cluster_id")
    c = candidates()
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    orcids = sorted(set(c.oax_orcid.dropna()))
    todo = orcids if force else [o for o in orcids if not d00._usable(cache.get(o), o)
                                 or not (cache.get(o) or {}).get("_fetched_live")]
    print(f"{len(c)} candidates, {len(orcids)} with an OpenAlex ORCID; fetching {len(todo)} live records")
    if todo:
        fetch(todo, cache)
        for o in todo:          # mark the copy as fetched by this check, so a rerun reads it from the cache
            rec = cache.get(o)
            if isinstance(rec, dict) and "_error" not in rec:
                rec["_fetched_live"] = time.strftime("%Y-%m-%d")
                cache.set(o, rec)

    con = duckdb.connect()
    emp = e00.resolve_employers(e00.cache_employments(cache, set(orcids), d00._usable), e00.hep_crosswalk(con))
    emp_hep = emp.groupby("orcid").hep_code.apply(set).to_dict()
    held = {o: cid for cid, os_ in zip(a.index, a.orcids) for o in os_}
    refused = load_refused_orcids()
    uids = a.unique_ids.to_dict()

    def acif_fields(cid):
        return {oax_field_name(f["code"]) for f in a.loc[cid, "for2020_codes"]} - {None}

    rows = []
    for t in c.itertuples():
        x = a.loc[t.cluster_id]
        rf = [f["field"] for f in (list(t.sorted_fields) if t.sorted_fields is not None else [])][:3]
        row = {"cluster_id": t.cluster_id, "arc_name": list(x.full_names)[0], "years": f"{t.first_year:.0f}-{t.last_year:.0f}",
               "n_grants": t.n_grants, "div": t.div, "author_idx": t.author_idx, "oax_name": t.full_name,
               "works": t.works_count_global, "field_ok": bool(acif_fields(t.cluster_id) & set(rf)),
               "oax_orcid": t.oax_orcid if isinstance(t.oax_orcid, str) else None,
               "acif_orcid": ", ".join(x.orcids)}
        o = row["oax_orcid"]
        if o:
            rec = cache.get(o)
            if not d00._usable(rec, o):
                row["status"] = "orcid_record_unavailable"
            else:
                person = (rec.get("person") or {}).get("name") or {}
                row["orcid_name"] = " ".join(filter(None, [(person.get("given-names") or {}).get("value"),
                                                           (person.get("family-name") or {}).get("value")]))
                keys = {k for k in d00.record_name_keys(rec) if len(k.split("_", 1)[0]) > 1}
                acif_keys = {k for k in x.full_name_keys if len(k.split("_", 1)[0]) > 1}
                names_ok = bool(keys & acif_keys)
                h = emp_hep.get(o, set())
                er = emp[emp.orcid == o]
                row["employers"] = "; ".join(f"{r.hep_code} {r.start_year or ''}-{r.end_year or ''}"
                                             for r in er.sort_values("start_year").itertuples())
                aff = affiliations(rec)
                row["affiliations"] = "; ".join(f"{k[0][:3]} {n} ({c}) {y0 or ''}-{y1 or ''}" for k, n, c, y0, y1 in
                                                [(z[0], z[1], z[2], z[3], z[4]) for z in aff])
                row["employer"] = ("grant_hep" if h & set(x.hep_codes) else "other_hep" if h
                                   else "other_au" if any(z[2] == "AU" for z in aff)
                                   else "foreign_only" if aff else "none_listed")
                other = held.get(o)
                if len(x.orcids) and o not in x.orcids:
                    row["status"] = "acif_holds_another_orcid"
                elif other and other != t.cluster_id:
                    row["status"] = f"orcid_held_by {other}"
                elif any(o in refused.get(uid, set()) for uid in uids[t.cluster_id]):
                    row["status"] = "orcid_refused_by_hand"
                elif not names_ok:
                    row["status"] = "names_disagree"
                else:
                    row["status"] = f"names_agree_{row['employer']}"
        else:
            row["status"] = "no_oax_orcid"
        rows.append(row)
    r = pd.DataFrame(rows)
    r.to_parquet(OUT.with_suffix(".parquet"), index=False)

    L = [f"# Unlinked ACIFs with one same-name OpenAlex record, checked through its ORCID ({len(r)} ACIFs)", "",
         "Live ORCID records fetched from the ORCID Public API; employer = the ORCID record's employments resolved to ARC HEP codes.", "",
         "| status | ACIFs | field agrees |", "|---|---|---|"]
    st = r.status.str.replace(r"orcid_held_by .*", "orcid_held_by_another_acif", regex=True)
    for s, g in r.groupby(st):
        L.append(f"| {s} | {len(g)} | {int(g.field_ok.sum())} |")
    for s, g in r.groupby(st):
        if s == "no_oax_orcid":
            continue
        L += ["", f"## {s} ({len(g)})", "", "| ACIF | ARC name | grants | field | OpenAlex record | ORCID | ORCID name | ORCID employments and education | field ok |",
              "|---|---|---|---|---|---|---|---|---|"]
        for t in g.sort_values("cluster_id").itertuples():
            extra = f" (ACIF holds {t.acif_orcid})" if t.acif_orcid and t.status == "acif_holds_another_orcid" else ""
            L.append(f"| {t.cluster_id} | {t.arc_name} | {t.n_grants} ({t.years}) | {t.div} | A{t.author_idx} {t.oax_name} ({t.works}) | "
                     f"{t.oax_orcid}{extra} | {getattr(t, 'orcid_name', '') or ''} | {(getattr(t, 'affiliations', '') or '')[:300]} | {'yes' if t.field_ok else 'no'} |")
    g = r[r.status == "no_oax_orcid"]
    L += ["", f"## no_oax_orcid ({len(g)}): the record carries no ORCID", "",
          f"field agrees: {int(g.field_ok.sum())}; by division: " + ", ".join(f"{k} {v}" for k, v in g['div'].value_counts().head(8).items())]
    OUT.with_suffix(".md").write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT.with_suffix(".md"))
    print(st.value_counts().to_string())


if __name__ == "__main__":
    main()
