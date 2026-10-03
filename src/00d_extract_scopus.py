"""
00d_extract_scopus.py

PURPOSE:
    The Scopus facts the ACIF build uses (src/acif/scopus.py, passes one and two), fetched and
    persisted once so the build itself never calls Scopus or reads the ORCID sources.

    One Scopus Author Search per ACIF after the ARC ORCID merge (build_stage_zero() ->
    merge_by_orcid(); 2026-10-01 decision: search per ACIF, not per record): OR over the ACIF's
    full-given-name keys AND OR over AFFIL(name) for every ARC university on any of its grants
    (admin, announcement admin, eligible and collaborating organisations -- the person-org link is
    unknown on multi-org grants). Query parts are sorted so the query string, pybliometrics' cache
    key, is reproducible; a rerun with an unchanged build spends no quota.

    Then, for every ORCID the build may need to judge -- ARC's own, those on the profiles found,
    and those whose ORCID record names one of those profiles -- the ORCID's names (as
    full_name_keys, through NameParser()) and the Scopus author ids its own record lists.
    Sources: the ORCID record cache (DISKCACHE_DIR/orcid_records_authenticated, full /record
    responses) first, then the ORCID bulk file (ORCID_BULK_PARQUET, Oct-2023 dump: name, aliases,
    external ids). The cache is not a register of researchers -- it holds ARC ORCIDs and the old
    name-search hits only.

INPUT:
    the ACIF build's first stage (src/acif/build.py), grants_flat.parquet, admin_orgs.csv,
    data_persisted/HEP_concordances.xlsx, data_persisted/arc_name_overrides.csv (reject_scopus rows,
    via 00a_extract_arc.load_name_overrides), investigators_raw.parquet, the ORCID cache and bulk
    file, Scopus (pybliometrics; credentials and cache from .env -- see src/utils/scopus.py).

OUTPUT (SCOPUS_EXTRACT_DIR = PROCESSED_DATA/scopus_extract/):
    scopus_acif_summary.parquet    one row per ACIF searched: cluster_id, unique_ids (its records),
                                   universities, query, n_profiles, arc_orcids, scopus_orcids, status
    scopus_acif_profiles.parquet   one row per (cluster_id, scopus_id): the profile's ORCID, name,
                                   current affiliation, documents, subject areas
    scopus_orcid_facts.parquet     one row per ORCID: name_source (cache / bulk / None),
                                   name_keys, listed_scopus_ids (from the cache and the bulk file),
                                   cache_redirected_to (a cached record ORCID redirected; not used)
    scopus_profile_claims.parquet  (scopus_id, orcid): an ORCID record that lists a profile found
    scopus_rejections.parquet      (unique_id, orcid): reject_scopus rows resolved to records
    scopus_extract.md              counts

Usage: .venv/bin/python src/00d_extract_scopus.py
"""

import importlib
import sys
from pathlib import Path

import diskcache
import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import DISKCACHE_DIR, ORCID_BULK_PARQUET, PROCESSED_DATA, SCOPUS_EXTRACT_DIR
from src.acif.build import UnionFind, build_stage_zero, compute_orcids, merge_by_orcid
from src.utils.names import HumanNameParser
from src.utils.scopus import (acif_query, affiliation_names, grant_universities, init_scopus,
                              load_university_map, search_keys)

_P = HumanNameParser()


def status(arc_orcids: set[str], scopus_orcids: set[str], n_profiles: int) -> str:
    """ARC vs Scopus: confirmed / other_orcid / no_scopus_orcid when ARC has an ORCID;
    one_orcid / several_orcids / no_scopus_orcid when it hasn't; no_profile when nothing found."""
    if n_profiles == 0:
        return "no_profile"
    if arc_orcids:
        if arc_orcids & scopus_orcids:
            return "confirmed"
        return "other_orcid" if scopus_orcids else "no_scopus_orcid"
    if len(scopus_orcids) == 1:
        return "one_orcid"
    return "several_orcids" if scopus_orcids else "no_scopus_orcid"


# ── Scopus search ────────────────────────────────────────────────────────────

def search_acifs(acifs) -> tuple[pd.DataFrame, pd.DataFrame]:
    from pybliometrics.scopus import AuthorSearch
    init_scopus()
    affil = affiliation_names(load_university_map())
    gunis = grant_universities()
    profiles, summary = [], []
    for i, a in enumerate(acifs, 1):
        keys = sorted({k for it in a.items for k in it.full_name_keys})
        grants = sorted({it.grant_code for it in a.items})
        heps = sorted(set().union(*[gunis.get(g, set()) for g in grants]))
        q = acif_query(keys, heps, affil)
        try:
            found = AuthorSearch(q).authors or []
        except Exception as e:
            raise SystemExit(f"Scopus search failed for {a.cluster_id}: {type(e).__name__}: {e}")
        arc_orcids = set(a.orcids)
        scopus_orcids = set()
        for p in found:
            orcid = (p.orcid or "").strip("[]") or None
            if orcid:
                scopus_orcids.add(orcid)
            profiles.append({
                "cluster_id": a.cluster_id, "scopus_id": p.eid.split("-")[-1], "orcid": orcid,
                "given_name": p.givenname, "surname": p.surname, "documents": p.documents,
                "current_affiliation": p.affiliation, "current_affiliation_id": p.affiliation_id,
                "country": p.country, "areas": p.areas,
            })
        summary.append({
            "cluster_id": a.cluster_id, "unique_ids": sorted(it.unique_id for it in a.items),
            "universities": heps, "search_keys": search_keys(keys), "query": q,
            "n_profiles": len(found), "arc_orcids": sorted(arc_orcids),
            "scopus_orcids": sorted(scopus_orcids),
            "status": status(arc_orcids, scopus_orcids, len(found)),
        })
        if i % 5000 == 0:
            print(f"  searched {i:,}/{len(acifs):,}")
    return pd.DataFrame(summary), pd.DataFrame(profiles)


# ── ORCID names and listed Scopus ids ────────────────────────────────────────

def record_name_keys(record: dict) -> set[str]:
    """full_name_keys of an ORCID record's own names: given+family as a pair, plus the credit
    name and every other-name as single strings -- all through NameParser()."""
    person = (record or {}).get("person") or {}
    name = person.get("name") or {}
    given = (name.get("given-names") or {}).get("value") or ""
    family = (name.get("family-name") or {}).get("value") or ""
    keys = set(_P.parse((given, family)).full_name_keys) if (given or family) else set()
    others = [(name.get("credit-name") or {}).get("value")]
    others += [o.get("content") for o in (person.get("other-names") or {}).get("other-name") or []]
    for o in others:
        if o:
            keys |= set(_P.parse(o).full_name_keys)
    return keys


def record_scopus_ids(record: dict) -> set[str]:
    """The Scopus author ids an ORCID record lists among its external identifiers."""
    ids = ((record or {}).get("person") or {}).get("external-identifiers") or {}
    return {str(e.get("external-id-value")).strip() for e in ids.get("external-identifier") or []
            if e.get("external-id-type") == "Scopus Author ID" and e.get("external-id-value")}


def _usable(record, orcid: str) -> bool:
    """A cached /record that is this ORCID's own. ORCID answers a deprecated ORCID with the record
    it now redirects to (e.g. 0000-0001-5019-5182, Shaomin Liu in the Oct-2023 bulk file, comes back
    as 0000-0002-9865-9596, "Yuanyuan Chu"); such a record is another ORCID's and is not used --
    the bulk file is used instead. 12 cached records were redirected (2026-10-03)."""
    if not isinstance(record, dict) or "_error" in record:
        return False
    path = (record.get("orcid-identifier") or {}).get("path")
    return path in (None, orcid)


def bulk_rows(con, orcids: set[str]) -> pd.DataFrame:
    con.register("wanted", pd.DataFrame({"orcid": sorted(orcids)}))
    return con.execute(f"""
        SELECT o.orcid, o.name, o.aliases,
               [o.xref_values[i] FOR i IN range(1, len(o.xref_keys) + 1) IF o.xref_keys[i] = 'scopus'] AS sids
        FROM read_parquet('{ORCID_BULK_PARQUET}') o JOIN wanted USING (orcid)""").fetchdf()


def bulk_claims(con, scopus_ids: set[str]) -> pd.DataFrame:
    """(scopus_id, orcid) for every bulk-file ORCID record that lists one of these Scopus ids."""
    con.register("sids", pd.DataFrame({"scopus_id": sorted(scopus_ids)}))
    return con.execute(f"""
        SELECT DISTINCT v AS scopus_id, orcid FROM (
            SELECT orcid, unnest(xref_keys) AS k, unnest(xref_values) AS v
            FROM read_parquet('{ORCID_BULK_PARQUET}') WHERE list_contains(xref_keys, 'scopus'))
        WHERE k = 'scopus' AND v IN (SELECT scopus_id FROM sids)""").fetchdf()


def orcid_facts(orcids: set[str], cache, bulk: pd.DataFrame) -> pd.DataFrame:
    b = {r.orcid: r for r in bulk.itertuples()}
    rows = []
    for o in sorted(orcids):
        rec = cache.get(o)
        own = _usable(rec, o)
        listed = set(record_scopus_ids(rec)) if own else set()
        br = b.get(o)
        if br is not None and br.sids is not None:
            listed |= {str(s) for s in br.sids}
        if own:
            source, keys = "cache", record_name_keys(rec)
        elif br is not None:
            names = [n for n in [br.name, *(list(br.aliases) if br.aliases is not None else [])] if n]
            source, keys = "bulk", set().union(*[set(_P.parse(n).full_name_keys) for n in names]) if names else set()
        else:
            source, keys = None, set()
        redirect = None
        if isinstance(rec, dict) and "_error" not in rec and not own:
            redirect = (rec.get("orcid-identifier") or {}).get("path")
        rows.append({"orcid": o, "name_source": source, "name_keys": sorted(keys),
                     "listed_scopus_ids": sorted(listed), "cache_redirected_to": redirect})
    return pd.DataFrame(rows)


def cache_claims(cache, scopus_ids: set[str]) -> list[tuple[str, str]]:
    out = []
    for key in list(cache):          # cache.iterkeys() yields almost nothing on this cache
        if not isinstance(key, str):
            continue
        rec = cache.get(key)
        if _usable(rec, key):
            out += [(s, key) for s in record_scopus_ids(rec) & scopus_ids]
    return out


def scopus_rejections() -> pd.DataFrame:
    """reject_scopus rows of arc_name_overrides.csv resolved to unique_ids (rows are keyed on
    grant + ARC's raw names, matched against investigators_raw.parquet). Raises if a row matches
    no record or several."""
    ov = importlib.import_module("src.00a_extract_arc").load_name_overrides()
    inv = pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet",
                          columns=["unique_id", "grant_code", "first_name", "family_name"])
    rows = []
    for (grant, first, family), (orcid, _note) in sorted(ov.reject_scopus.items()):
        hit = inv[(inv.grant_code == grant) & (inv.first_name == first) & (inv.family_name == family)]
        if len(hit) != 1:
            raise SystemExit(f"reject_scopus row {grant} {first!r} {family!r} matches {len(hit)} records")
        rows.append({"unique_id": hit.unique_id.iloc[0], "orcid": orcid})
    return pd.DataFrame(rows, columns=["unique_id", "orcid"])


def main():
    acifs, _ = merge_by_orcid(build_stage_zero(), UnionFind({}))
    for a in acifs:
        compute_orcids(a)
    print(f"ACIFs after the ARC ORCID merge: {len(acifs):,}")
    summary, profiles = search_acifs(acifs)

    profile_ids = set(profiles.scopus_id.astype(str))
    con = duckdb.connect()
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    claims = pd.concat([bulk_claims(con, profile_ids),
                        pd.DataFrame(cache_claims(cache, profile_ids), columns=["scopus_id", "orcid"])])
    claims = claims.drop_duplicates().sort_values(["scopus_id", "orcid"]).reset_index(drop=True)

    wanted = ({o for os in summary.arc_orcids for o in os} | set(profiles.orcid.dropna())
              | set(claims.orcid))
    facts = orcid_facts(wanted, cache, bulk_rows(con, wanted))
    rejected = scopus_rejections()

    SCOPUS_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(SCOPUS_EXTRACT_DIR / "scopus_acif_summary.parquet", index=False)
    profiles.to_parquet(SCOPUS_EXTRACT_DIR / "scopus_acif_profiles.parquet", index=False)
    facts.to_parquet(SCOPUS_EXTRACT_DIR / "scopus_orcid_facts.parquet", index=False)
    claims.to_parquet(SCOPUS_EXTRACT_DIR / "scopus_profile_claims.parquet", index=False)
    rejected.to_parquet(SCOPUS_EXTRACT_DIR / "scopus_rejections.parquet", index=False)

    lines = ["# Scopus extract", "", f"ACIFs searched: {len(summary):,}", "",
             "| status | ACIFs |", "|---|---|"]
    lines += [f"| {k} | {v:,} |" for k, v in summary.status.value_counts().items()]
    lines += ["", f"Profiles found: {len(profiles):,} rows, {len(profile_ids):,} distinct",
              f"ORCIDs described: {len(facts):,} (names from: "
              + ", ".join(f"{k} {v:,}" for k, v in facts.name_source.fillna("none").value_counts().items()) + ")",
              f"ORCIDs listing a Scopus id: {(facts.listed_scopus_ids.map(len) > 0).sum():,}",
              f"Cached records redirected to another ORCID (not used): {facts.cache_redirected_to.notna().sum():,}",
              f"Profiles named by an ORCID record: {claims.scopus_id.nunique():,}",
              f"reject_scopus rows: {len(rejected):,}"]
    text = "\n".join(lines) + "\n"
    (SCOPUS_EXTRACT_DIR / "scopus_extract.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
