"""
analysis/32_oax_orcid_hints.py -- unlinked ACIFs checked through the ORCIDs their same-name OpenAlex records
carry (2026-10-22, user: "the cases where there are two OAX -- or even if there are more, ones with ORCID. If
we get the hint of an OAX ORCID that potentially links to an ACIF, the ORCID API is probably worth checking").
Widens analysis/31 (single-candidate ACIFs only) to every candidate record.

Candidates: for each kept ACIF the linker left unlinked (unlinked_composition.parquet), every author in
openalex_authors_prep sharing one of the ACIF's full-given-name keys (acifs_arc.full_name_keys, initial-only keys
left out) in any of the author's full_name_keys, not accepted for another ACIF, and carrying an ORCID. Each
ORCID's live /record is fetched from the ORCID Public API (registered client, ORCID_CLIENT_ID/SECRET in .env;
4 parallel requests) into the project's ORCID record cache (DISKCACHE_DIR/orcid_records_authenticated),
replacing an older copy and marked `_fetched_live`; a rerun reads marked copies from the cache.

Per candidate:
  names    -- the ORCID record's own names (00d record_name_keys) share a full-given-name key with the ACIF
  employer -- the ORCID record's employments resolved to ARC HEP codes (00e hep_crosswalk / resolve_employers):
              grant_hep_in_years (a HEP of the ACIF's grants, employment overlapping first grant - 3 .. last
              grant + 3), grant_hep (years unknown or outside), other_hep, other_au (an Australian employer or
              education that is not an ARC HEP), foreign_only, none_listed
  field    -- one of the OpenAlex record's top-3 fields is a field of the ACIF's FOR2020 codes
  blocked  -- the ORCID is held by another ACIF, refused for one of this ACIF's records, or the ACIF holds
              a different ORCID (reported, not counted as a pass)
Per ACIF, among candidates whose names agree and are not blocked: the strongest employer class present, and
whether exactly one candidate holds it.
Writes processed/oax_orcid_hints.md and .parquet (one row per candidate). A report only: nothing is linked.
--sample N: only N random ACIFs (seed 42) that have a candidate carrying an ORCID (the full set is ~9,700 ORCIDs,
about 16 minutes at 4 parallel requests).
"""

import importlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import diskcache
import duckdb
import pandas as pd
import requests

from config.settings import ACIFS_ARC, DISKCACHE_DIR, OAX_LINK_DIR, ORCID_CLIENT_ID, ORCID_CLIENT_SECRET, PROCESSED_DATA
from src.acif.build import load_refused_orcids
from src.utils.for_resolve import oax_field_name

OUT = PROCESSED_DATA / "oax_orcid_hints"
ORCID_API = "https://pub.orcid.org/v3.0"
WINDOW = 3
CLASSES = ["grant_hep_in_years", "grant_hep", "other_hep", "other_au", "foreign_only", "none_listed"]
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
    session = requests.Session()
    session.headers.update({"Accept": "application/json", "Authorization": f"Bearer {tok}"})

    def get(o):
        for attempt in range(4):
            try:
                r = session.get(f"{ORCID_API}/{o}/record", timeout=30)
            except requests.RequestException:
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            return o, (r.json() if r.status_code == 200 else {"_error": r.status_code})
        return o, None

    with ThreadPoolExecutor(4) as ex:
        for k, (o, rec) in enumerate(ex.map(get, orcids), 1):
            if rec is not None:
                if "_error" not in rec:
                    rec["_fetched_live"] = time.strftime("%Y-%m-%d")
                cache.set(o, rec)
            if k % 500 == 0:
                print(f"  fetched {k}/{len(orcids)}", flush=True)


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


def candidates(a: pd.DataFrame) -> pd.DataFrame:
    u = pd.read_parquet(PROCESSED_DATA / "unlinked_composition.parquet")
    keep = set(u.cluster_id)
    ak = pd.DataFrame([(c, k) for c, ks in zip(a.index, a.full_name_keys) if c in keep for k in ks
                       if len(k.split("_", 1)[0]) > 1], columns=["cluster_id", "key"])
    con = duckdb.connect()
    con.register("ak", ak)
    c = con.execute(f"""
        SELECT ak.cluster_id, p.author_idx, any_value(p.full_name) AS oax_name, any_value(p.orcid) AS oax_orcid,
               any_value(p.works_count_global) AS works, any_value(p.sorted_fields) AS sorted_fields
        FROM ak JOIN (SELECT author_idx, full_name, orcid, unnest(full_name_keys) AS key, works_count_global,
                             sorted_fields FROM read_parquet('{PROCESSED_DATA}/openalex_authors_prep.parquet')) p
        USING (key) GROUP BY ak.cluster_id, p.author_idx""").df()
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    c = c[~c.author_idx.isin(set(acc.author_idx))]
    c["n_candidates"] = c.groupby("cluster_id").author_idx.transform("size")
    return c.merge(u[["cluster_id", "div", "first_year", "last_year", "n_grants"]], on="cluster_id")


def main():
    a = pd.read_parquet(ACIFS_ARC).set_index("cluster_id")
    c = candidates(a)
    w = c[c.oax_orcid.notna()].copy()
    if "--sample" in sys.argv:      # N random ACIFs with an ORCID-carrying candidate (user: a sample if > 10 minutes)
        n = int(sys.argv[sys.argv.index("--sample") + 1])
        ids = set(pd.Series(sorted(set(w.cluster_id))).sample(n, random_state=42))
        w, c = w[w.cluster_id.isin(ids)], c[c.cluster_id.isin(ids)]
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    orcids = sorted(set(w.oax_orcid))
    todo = [o for o in orcids if not (isinstance(cache.get(o), dict) and cache.get(o).get("_fetched_live"))]
    print(f"{c.cluster_id.nunique()} unlinked ACIFs with candidates; {len(c)} candidates, {len(w)} with an ORCID "
          f"({len(orcids)} ORCIDs); fetching {len(todo)} live records", flush=True)
    if todo:
        fetch(todo, cache)

    con = duckdb.connect()
    emp = e00.resolve_employers(e00.cache_employments(cache, set(orcids), d00._usable), e00.hep_crosswalk(con))
    emp_by = {o: g for o, g in emp.groupby("orcid")}
    held = {o: cid for cid, os_ in zip(a.index, a.orcids) for o in os_}
    refused = load_refused_orcids()
    fields = {cid: {oax_field_name(f["code"]) for f in codes} - {None} for cid, codes in a.for2020_codes.items()}

    rows = []
    for t in w.itertuples():
        x = a.loc[t.cluster_id]
        o = t.oax_orcid
        rf = [f["field"] for f in (list(t.sorted_fields) if t.sorted_fields is not None else [])][:3]
        row = {"cluster_id": t.cluster_id, "arc_name": list(x.full_names)[0],
               "years": f"{t.first_year:.0f}-{t.last_year:.0f}", "n_grants": t.n_grants, "div": t.div,
               "n_candidates": t.n_candidates, "author_idx": t.author_idx, "oax_name": t.oax_name, "works": t.works,
               "oax_orcid": o, "field_ok": bool(fields[t.cluster_id] & set(rf)), "acif_orcid": ", ".join(x.orcids)}
        rec = cache.get(o)
        if not d00._usable(rec, o):
            row.update(status="orcid_record_unavailable", names_ok=False, employer=None)
            rows.append(row)
            continue
        nm = (rec.get("person") or {}).get("name") or {}
        row["orcid_name"] = " ".join(filter(None, [(nm.get("given-names") or {}).get("value"),
                                                   (nm.get("family-name") or {}).get("value")]))
        keys = {k for k in d00.record_name_keys(rec) if len(k.split("_", 1)[0]) > 1}
        row["names_ok"] = bool(keys & {k for k in x.full_name_keys if len(k.split("_", 1)[0]) > 1})
        er = emp_by.get(o, pd.DataFrame(columns=emp.columns))
        gh = er[er.hep_code.isin(set(x.hep_codes))]
        lo, hi = t.first_year - WINDOW, t.last_year + WINDOW
        in_years = any((s is not None and not pd.isna(s) and s <= hi) and (pd.isna(e) or e is None or e >= lo)
                       for s, e in zip(gh.start_year, gh.end_year))
        aff = affiliations(rec)
        row["employer"] = ("grant_hep_in_years" if in_years else "grant_hep" if len(gh) else "other_hep" if len(er)
                           else "other_au" if any(z[2] == "AU" for z in aff) else "foreign_only" if aff
                           else "none_listed")
        row["arc_heps"] = "; ".join(f"{r.hep_code} {'' if pd.isna(r.start_year) else int(r.start_year)}-"
                                    f"{'' if pd.isna(r.end_year) else int(r.end_year)}"
                                    for r in er.sort_values("start_year").itertuples())
        row["affiliations"] = "; ".join(f"{s[:3]} {n} ({cc}) {y0 or ''}-{y1 or ''}" for s, n, cc, y0, y1 in aff)
        other = held.get(o)
        row["status"] = ("acif_holds_another_orcid" if len(x.orcids) and o not in x.orcids
                         else "orcid_held_by_another_acif" if other and other != t.cluster_id
                         else "orcid_refused_by_hand" if any(o in refused.get(uid, set()) for uid in x.unique_ids)
                         else "names_agree" if row["names_ok"] else "names_disagree")
        rows.append(row)
    r = pd.DataFrame(rows)

    # per ACIF: strongest employer class among passing candidates, and how many candidates hold it
    p = r[r.status == "names_agree"].copy()
    p["rank"] = p.employer.map({k: i for i, k in enumerate(CLASSES)})
    best = p.groupby("cluster_id")["rank"].min()
    p = p[p["rank"] == p.cluster_id.map(best)]
    nbest = p.groupby("cluster_id").size()
    r["acif_best"] = r.cluster_id.map(best.map(dict(enumerate(CLASSES))))
    r["acif_n_best"] = r.cluster_id.map(nbest)
    r["is_best"] = r.index.isin(p.index)
    r.to_parquet(OUT.with_suffix(".parquet"), index=False)

    acifs = r.drop_duplicates("cluster_id")
    L = [f"# Unlinked ACIFs checked through their same-name OpenAlex records' ORCIDs", "",
         f"{c.cluster_id.nunique():,} unlinked ACIFs have same-name OpenAlex records ({len(c):,} records); "
         f"{w.cluster_id.nunique():,} ACIFs have at least one record carrying an ORCID ({len(w):,} records, "
         f"{len(orcids):,} ORCIDs, fetched live from the ORCID API).", "",
         "## Candidates by status", "", "| status | records | ACIFs |", "|---|---|---|"]
    for s, g in r.groupby("status"):
        L.append(f"| {s} | {len(g):,} | {g.cluster_id.nunique():,} |")
    L += ["", "## Per ACIF: strongest evidence among records whose ORCID names agree", "",
          "| strongest employer class | ACIFs | of which one record holds it | ... and its field agrees |", "|---|---|---|---|"]
    for k in CLASSES:
        g = acifs[acifs.acif_best == k]
        one = r[(r.acif_best == k) & (r.acif_n_best == 1) & r.is_best]
        L.append(f"| {k} | {len(g):,} | {len(one):,} | {int(one.field_ok.sum()):,} |")
    L.append(f"| (no record passes the name test) | {acifs.acif_best.isna().sum():,} | | |")
    cols = "| ACIF | ARC name | grants | field | records | OpenAlex record | ORCID | ORCID name | ARC HEPs in ORCID | other affiliations | field ok |"
    for k in CLASSES[:4]:
        one = r[(r.acif_best == k) & (r.acif_n_best == 1) & r.is_best].sort_values("cluster_id")
        L += ["", f"## {k}, one record ({len(one)})", "", cols, "|" + "---|" * 11]
        for t in one.itertuples():
            L.append(f"| {t.cluster_id} | {t.arc_name} | {t.n_grants} ({t.years}) | {t.div} | {t.n_candidates} | "
                     f"A{t.author_idx} {t.oax_name} ({t.works}) | {t.oax_orcid} | {t.orcid_name} | {t.arc_heps} | "
                     f"{(t.affiliations or '')[:220]} | {'yes' if t.field_ok else 'no'} |")
    sev = r[(r.acif_n_best > 1) & r.is_best & r.acif_best.isin(CLASSES[:3])].sort_values(["cluster_id", "author_idx"])
    L += ["", f"## Several records at the strongest class (ARC HEP classes; {sev.cluster_id.nunique()} ACIFs)", "", cols, "|" + "---|" * 11]
    for t in sev.itertuples():
        L.append(f"| {t.cluster_id} | {t.arc_name} | {t.n_grants} ({t.years}) | {t.div} | {t.n_candidates} | "
                 f"A{t.author_idx} {t.oax_name} ({t.works}) | {t.oax_orcid} | {t.orcid_name} | {t.arc_heps} | "
                 f"{(t.affiliations or '')[:160]} | {'yes' if t.field_ok else 'no'} |")
    hold = r[(r.status == "acif_holds_another_orcid") & r.names_ok & r.employer.isin(CLASSES[:4])].sort_values("cluster_id")
    L += ["", f"## ACIF holds another ORCID; the record's ORCID names agree and show an Australian employer ({len(hold)})", "",
          "| ACIF | ARC name | grants | ACIF ORCID | OpenAlex record | record ORCID | ORCID name | ARC HEPs in ORCID | field ok |",
          "|---|---|---|---|---|---|---|---|---|"]
    for t in hold.itertuples():
        L.append(f"| {t.cluster_id} | {t.arc_name} | {t.n_grants} ({t.years}) | {t.acif_orcid} | A{t.author_idx} {t.oax_name} "
                 f"({t.works}) | {t.oax_orcid} | {t.orcid_name} | {t.arc_heps} | {'yes' if t.field_ok else 'no'} |")
    OUT.with_suffix(".md").write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT.with_suffix(".md"))


if __name__ == "__main__":
    main()
