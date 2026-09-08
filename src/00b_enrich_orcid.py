"""
src/00b_enrich_orcid.py

Enrich every ARC investigator name with ORCID data.

Two DiskCache stores (DISKCACHE_DIR/):
  record_cache[orcid]          → full ORCID /record JSON
  search_cache[(first,family)] → resolved search result dict

For each distinct (first_name, family_name) pair in scope:
  - Has ARC ORCID  → fetch /record into record_cache (no search)
  - No ORCID       → local bulk ORCID snapshot first, live API only as fallback →
                     resolve AU candidates → store in search_cache
                     (also fetches /record for each candidate → record_cache)

2026-08-31: the local ORCID bulk snapshot is now tried FIRST for every no-ORCID search
(_search_bulk_db(), called from _search_orcid()) -- it supersedes the live ORCID Public API as
the primary discovery mechanism, not just a source for widening name forms of already-resolved
ORCIDs (that's a separate, older use -- awards_cif.py::widen_names_with_orcid_bulk_db()). The
live API is now the fallback for whatever the bulk snapshot's own real, known coverage gaps miss
(a frozen crawl -- e.g. a real case this project found where a person's genuine ORCID simply
wasn't in the snapshot at all), not the first thing tried.

2026-09-02: rewired onto src/utils/orcid_processor.py's OrcidProcessor.discover() (backed by
orcid_bulk.parquet, the FULL 17.15M-person Zenodo population, matching keys computed with this
project's own HumanNameParser) -- supersedes the older orcid_bulk_lookup.py/orcid_persons.parquet
path, which only covered the narrower ~4.8M "HQ" subset with a bare, project-agnostic name parse.
See docs/pipeline_todo.md #19 for the full before/after account; orcid_bulk_lookup.py itself is
retired, not just superseded in this one call site.

2026-09-08: orcid_processor.py now uses HumanNameParser directly as its own default normalizer
(item #26) -- OrcidProcessor() below no longer needs an explicit name_normalizer= override at
all, since the default already is the hardened parse this project always wanted here.

Both caches are checked before any API call; re-runs make zero network calls
unless forced.

Output:
    processed/orcid_enrichment.parquet  — written from search_cache
    columns: first_name, family_name, name_key, orcid, confidence, num_found,
             works_count, external_ids, au_candidates, source
    confidence: 'high' | 'au_match' | 'low' | 'not_found' | 'too_common'
                'wildcard_high' | 'wildcard_au_match'
                ('high'/'au_match' can now come from either the bulk snapshot or the live API --
                see `source` to tell which; the semantics of each confidence level are identical
                either way, see _search_bulk_db()'s own docstring for why.)
    works_count:   int — work groups for chosen orcid (0 if none chosen)
    external_ids:  JSON string dict — Scopus/ResearcherID etc. for chosen orcid
    au_candidates: JSON string list of {orcid, works_count, external_ids} for all
                   AU-qualifying candidates
    source:        'bulk_db_institution' | 'bulk_db_name_unique' | 'live_api' — which mechanism
                   produced this row (rows written before 2026-08-31 are all 'live_api')

Flags:
    --dry-run              Print counts, make no API calls
    --update-orcid ORCID   Re-fetch one /record (ignores record_cache)
    --update-name  FIRST FAMILY   Re-run search for one person (ignores search_cache)
"""

import json
import sys
import time
from pathlib import Path

import diskcache
import requests
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, DISKCACHE_DIR, ADMIN_ORGS_CSV
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import name_part_tokens
from src.utils.name_diacritic_variants import strip_diacriticals
from src.utils.io import setup_stdout_utf8
from src.utils.orcid_cache import orcid_addresses, orcid_external_ids, orcid_works_count
from src.utils.era_journals import load_era_lookup, orcid_for_codes
from src.utils.orcid_processor import OrcidProcessor
from src.utils.orcid_processor_arc_adapter import institution_matched_candidates
from src.utils.orcid_client import get_access_token, ORCID_CLIENT_ID, default_cache

PROJECT_DATA = Path(__file__).resolve().parents[1] / "data_persisted"

ORCID_API          = "https://pub.orcid.org/v3.0"
ORCID_SEARCH       = f"{ORCID_API}/search/"
ORCID_EXPANDED     = f"{ORCID_API}/expanded-search/"
# Authenticated (registered client, 2026-08-18) when credentials are configured -- clears the
# anonymous daily quota that stalled this script's original run (CLAUDE.md, "2026-08-08
# follow-up"). Falls back to anonymous, unauthenticated headers otherwise so this script still
# runs (at the old, quota-limited rate) in an environment without ORCID_CLIENT_ID/SECRET set.
RATE_RECORD_SEC    = 0.05 if ORCID_CLIENT_ID else 0.1   # /record fetches — low fanout, safe to be fast
RATE_SEARCH_SEC    = 0.2  if ORCID_CLIENT_ID else 0.5   # search queries — each may trigger multiple /record fetches
TOO_COMMON         = 10


def _headers() -> dict:
    if ORCID_CLIENT_ID:
        return {"Accept": "application/json", "Authorization": f"Bearer {get_access_token()}"}
    return {"Accept": "application/json"}


# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------

def _norm_family(s: str) -> str:
    return strip_diacriticals(s or "").lower().strip()

def _first_initial(s: str) -> str | None:
    toks = name_part_tokens(s or "")
    return toks[0][0] if toks else None

def _name_key(family: str, first: str) -> str | None:
    f = _norm_family(family)
    i = _first_initial(first)
    return f"{f}_{i}" if (f and i) else None


# ---------------------------------------------------------------------------
# Institution-name resolution (ARC admin_org -> an ORCID-searchable phrase)
# ---------------------------------------------------------------------------

def load_admin_org_to_institution_name() -> dict[str, str]:
    """ARC's own admin_org string (grants_flat.admin_org / admin_orgs.csv's
    organisationName_alias) -> admin_orgs.csv's institution_name column.

    2026-08-21: institution_name is OpenAlex's own naming convention for the institution (e.g.
    "UNSW Sydney"), not ARC's administrative legal name ("The University of New South Wales") --
    confirmed empirically these are NOT interchangeable for ORCID's expanded-search
    affiliation-org-name field: it does exact (case/punctuation-insensitive but word-order-
    sensitive) phrase matching against whatever string a person's own employment entry actually
    contains, with no synonym expansion -- "The University of New South Wales, Sydney" returned
    zero hits for a person whose employer field literally reads "UNSW Sydney". institution_name
    happened to match real ORCID employment strings in every case tested this session, so it's
    the right phrase to search with, not admin_org itself.
    """
    import csv as _csv
    out: dict[str, str] = {}
    with open(ADMIN_ORGS_CSV, newline="", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            alias = (row.get("organisationName_alias") or "").strip()
            iname = (row.get("institution_name") or "").strip()
            if alias and iname:
                out[alias] = iname
    return out


def _query_expanded(q: str) -> dict | None:
    try:
        r = requests.get(ORCID_EXPANDED, params={"q": q, "rows": TOO_COMMON + 1},
                         headers=_headers(), timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


_ORCID_PROC: OrcidProcessor | None = None


def _get_orcid_proc() -> OrcidProcessor:
    """Lazy module-level singleton -- one OrcidProcessor (one duckdb connection) reused across
    every name pair a run processes, rather than opening a fresh connection per call. Uses
    OrcidProcessor's own default normalizer (names.py's HumanNameParser directly, since
    2026-09-08's item #26 refactor) -- no explicit override needed, that default already carries
    the exact same NFC/NFKC/zero-width/postnominal-strip/diacritic-widening/nickname hardening
    as every other ARC-side name comparison."""
    global _ORCID_PROC
    if _ORCID_PROC is None:
        _ORCID_PROC = OrcidProcessor()
    return _ORCID_PROC


def _search_bulk_db(first: str, family: str, institution_names: list[str],
                    record_cache: diskcache.Cache, for_cache: diskcache.Cache,
                    era_lookup: dict) -> dict | None:
    """Try the local ORCID bulk snapshot (OrcidProcessor.discover(), backed by
    orcid_bulk.parquet -- the full 17.15M-person Zenodo population, no rate limit, no daily
    quota) before ever reaching for the live ORCID Public API search. Supersedes the live API as
    the PRIMARY discovery mechanism -- confirmed this session that the bulk snapshot is already
    capable of exactly this name+institution matching (it was used successfully, ad hoc, for
    individual cases during the 4u under-merge review, e.g. DP160100119_JianZhao,
    LP160100828_RobertEvans) but had never been wired into the bulk, population-scale NO_ORCID
    search this function performs -- only into widen_names_with_orcid_bulk_db() (awards_cif.py),
    which only widens name forms for clusters that ALREADY have a resolved ORCID, never
    discovers a new one.

    Returns a result dict in the same shape _resolve_results()/_search_by_institution() produce,
    or None if the bulk snapshot doesn't yield a confident answer -- callers should then fall
    through to the existing live-API search, not treat None as a final answer (the snapshot is a
    frozen crawl with real, known coverage gaps -- e.g. Yang Song's real ORCID wasn't in it
    at all in an earlier session).

    Confidence levels are the EXISTING, already-trusted vocabulary
    (_apply_enriched_orcids()/apply_enriched_orcids() already promote 'high'/'au_match'
    unchanged) -- deliberately not inventing a new label, since the semantics line up exactly:
      - exactly one bulk-snapshot candidate has an institution_matched_candidates() hit ->
        'au_match' (same meaning as the live path's "single AU-country-address candidate", just
        institution-corroborated rather than country-corroborated -- at least as strong).
      - no institution corroboration, but the name is globally unique in the bulk snapshot
        (exactly one candidate at all) -> 'high' (same meaning as the live path's "num_found==1
        globally", which is *also* not AU-filtered -- see _resolve_results()).
    Multiple institution-matched candidates, or multiple candidates with no institution
    corroboration, return None -- same conservatism as _search_by_institution(): a genuine
    ambiguity here should defer to the existing broader mechanism, not guess.
    """
    candidates = _get_orcid_proc().discover(first, family)
    if not candidates:
        return None

    inst_matched = institution_matched_candidates(candidates, institution_names) if institution_names else []
    if len(inst_matched) == 1:
        orcid = inst_matched[0]["orcid"]
        rec = fetch_record(orcid, record_cache, for_cache, era_lookup)
        meta = _candidate_meta(orcid, rec)
        return {
            "orcid": orcid, "confidence": "au_match", "num_found": len(candidates),
            "works_count":   meta["works_count"],
            "external_ids":  json.dumps(meta["external_ids"]),
            "au_candidates": json.dumps([meta]),
            "source":        "bulk_db_institution",
        }
    if inst_matched:
        return None  # 2+ institution-matched candidates -- genuine ambiguity, defer

    if len(candidates) == 1:
        orcid = candidates[0]["orcid"]
        rec = fetch_record(orcid, record_cache, for_cache, era_lookup)
        meta = _candidate_meta(orcid, rec)
        return {
            "orcid": orcid, "confidence": "high", "num_found": 1,
            "works_count":   meta["works_count"],
            "external_ids":  json.dumps(meta["external_ids"]),
            "au_candidates": json.dumps([meta]),
            "source":        "bulk_db_name_unique",
        }
    return None  # 2+ candidates, no institution corroboration -- defer to the live API's own
                 # country-filter logic rather than guess among them


def _search_by_institution(first: str, family: str, institution_names: list[str],
                           record_cache: diskcache.Cache, for_cache: diskcache.Cache,
                           era_lookup: dict) -> dict | None:
    """Try expanded-search with an affiliation-org-name filter for each candidate institution
    phrase in turn (a name pair can span 2+ grants at different institutions). Returns a result
    dict in the same shape _resolve_results() produces (confidence='institution_match') on the
    first institution that yields exactly one hit, or None if every institution draws a blank --
    callers should then fall back to the existing name-only search, not treat None as a final
    answer. Deliberately requires an EXACT single hit, not "fewest candidates" -- 2+ hits at the
    same institution is exactly the kind of common-name-within-one-institution collision this
    project has already found real cases of (e.g. two different "Yang Liu"s), and picking one
    arbitrarily would be worse than deferring to the existing broader mechanism.
    """
    for iname in institution_names:
        q = f'given-names:{first} AND family-name:{family} AND affiliation-org-name:"{iname}"'
        data = _query_expanded(q)
        time.sleep(RATE_SEARCH_SEC)
        if data is None:
            continue
        n = data.get("num-found", 0)
        if n != 1:
            continue
        result = (data.get("expanded-result") or [None])[0]
        if not result:
            continue
        orcid = result.get("orcid-id")
        rec = fetch_record(orcid, record_cache, for_cache, era_lookup)
        meta = _candidate_meta(orcid, rec)
        return {
            "orcid": orcid, "confidence": "institution_match", "num_found": 1,
            "works_count":   meta["works_count"],
            "external_ids":  json.dumps(meta["external_ids"]),
            "au_candidates": json.dumps([meta]),
        }
    return None


# ---------------------------------------------------------------------------
# Record cache (orcid → /record JSON)
# ---------------------------------------------------------------------------

def fetch_record(orcid: str, record_cache: diskcache.Cache,
                 for_cache: diskcache.Cache | None = None,
                 era_lookup: dict | None = None,
                 force: bool = False) -> dict:
    """Return full ORCID /record, using record_cache unless force=True.

    If for_cache and era_lookup are supplied, also derives and caches FOR codes
    from the record's works — no extra API call needed.
    """
    if not orcid:
        # Guard against a caller passing a missing/blank orcid (e.g. a malformed API result
        # dict with no "orcid-id" key) -- writing record_cache[None] here would silently poison
        # the cache for every future run's "derive FOR codes for cached records" pre-pass, which
        # iterates every record_cache key expecting it to be a real, re-readable ORCID.
        return {"_error": "no_orcid"}
    if not force and orcid in record_cache:
        data = record_cache[orcid]
    else:
        try:
            r = requests.get(f"{ORCID_API}/{orcid}/record", headers=_headers(), timeout=10)
            data = r.json() if r.status_code == 200 else {"_error": r.status_code}
        except Exception as e:
            data = {"_error": str(e)}
        record_cache[orcid] = data
        time.sleep(RATE_RECORD_SEC)

    if for_cache is not None and era_lookup is not None and orcid not in for_cache:
        for_cache[orcid] = orcid_for_codes(data, era_lookup)

    return data


# ---------------------------------------------------------------------------
# Search + resolve
# ---------------------------------------------------------------------------

_EMPTY = {"works_count": 0, "external_ids": "{}", "au_candidates": "[]"}


def _candidate_meta(orcid: str, rec: dict) -> dict:
    return {
        "orcid":        orcid,
        "works_count":  orcid_works_count(rec),
        "external_ids": orcid_external_ids(rec),
    }


def _query_orcid(q: str) -> dict | None:
    try:
        r = requests.get(ORCID_SEARCH, params={"q": q, "rows": TOO_COMMON + 1},
                         headers=_headers(), timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def _resolve_results(data: dict, record_cache: diskcache.Cache,
                     for_cache: diskcache.Cache, era_lookup: dict) -> dict:
    """Resolve a non-empty search response to a result dict with all metadata."""
    num_found = data.get("num-found", 0)
    if num_found > TOO_COMMON:
        return {"orcid": None, "confidence": "too_common", "num_found": num_found, **_EMPTY}

    results = data.get("result", [])
    orcids  = [r["orcid-identifier"]["path"] for r in results]

    if num_found == 1:
        rec  = fetch_record(orcids[0], record_cache, for_cache, era_lookup)
        meta = _candidate_meta(orcids[0], rec)
        countries = {a.get("country", {}).get("value") for a in orcid_addresses(rec)}
        au_cands  = [meta] if "AU" in countries else []
        return {
            "orcid": orcids[0], "confidence": "high", "num_found": 1,
            "works_count":   meta["works_count"],
            "external_ids":  json.dumps(meta["external_ids"]),
            "au_candidates": json.dumps(au_cands),
        }

    au_candidates = []
    for oid in orcids:
        try:
            rec       = fetch_record(oid, record_cache, for_cache, era_lookup)
            countries = {a.get("country", {}).get("value") for a in orcid_addresses(rec)}
            if "AU" in countries:
                au_candidates.append(_candidate_meta(oid, rec))
        except Exception:
            continue

    if len(au_candidates) == 1:
        chosen = au_candidates[0]
        return {
            "orcid": chosen["orcid"], "confidence": "au_match", "num_found": num_found,
            "works_count":   chosen["works_count"],
            "external_ids":  json.dumps(chosen["external_ids"]),
            "au_candidates": json.dumps(au_candidates),
        }
    return {
        "orcid": None, "confidence": "low", "num_found": num_found,
        "works_count": 0, "external_ids": "{}",
        "au_candidates": json.dumps(au_candidates),
    }


def _search_orcid(first: str, family: str,
                  record_cache: diskcache.Cache,
                  search_cache: diskcache.Cache,
                  for_cache: diskcache.Cache,
                  era_lookup: dict,
                  institution_names: list[str] | None = None,
                  force: bool = False) -> dict:
    """Search for a name; return resolved dict. Uses search_cache unless force=True.

    2026-08-31: tries the local ORCID bulk snapshot first (_search_bulk_db()) -- no rate limit,
    no daily quota, already proven capable of this exact search during manual review, but never
    before wired in as the primary bulk-population mechanism. Only when the bulk snapshot has no
    confident answer does this fall through to the live ORCID Public API (institution-targeted,
    then plain, then wildcard) exactly as before -- the live API is now the fallback for the
    bulk snapshot's own known coverage gaps, not the first thing tried.

    2026-08-21: tries expanded-search with an affiliation-org-name filter first (one attempt per
    distinct institution this name pair's own ARC grants were administered at -- a person can
    hold grants at 2+ institutions). This is what actually rescues a common name from the
    too_common short-circuit below: institution-targeted search never needs to fetch/inspect
    every candidate the way the plain search + country-filter path does, so it isn't gated by
    TOO_COMMON at all. Falls through to the existing name-only search unchanged when no
    institution is supplied, or every institution attempt draws a blank (num-found != 1)."""
    key = (first, family)
    if not force and key in search_cache:
        return search_cache[key]

    bulk_result = _search_bulk_db(first, family, institution_names or [],
                                  record_cache, for_cache, era_lookup)
    if bulk_result is not None:
        search_cache[key] = bulk_result
        return bulk_result

    if institution_names:
        inst_result = _search_by_institution(first, family, institution_names,
                                             record_cache, for_cache, era_lookup)
        if inst_result is not None:
            search_cache[key] = inst_result
            return inst_result

    data = _query_orcid(f'given-names:{first} AND family-name:{family}')
    if data is None:
        result = {"orcid": None, "confidence": "error", "num_found": -1, **_EMPTY}
    elif data.get("num-found", 0) > 0:
        result = _resolve_results(data, record_cache, for_cache, era_lookup)
    else:
        # Wildcard prefix fallback
        time.sleep(RATE_SEARCH_SEC)
        data_wild = _query_orcid(f'given-names:{first}* AND family-name:{family}')
        if data_wild is None or data_wild.get("num-found", 0) == 0:
            result = {"orcid": None, "confidence": "not_found", "num_found": 0, **_EMPTY}
        else:
            result = _resolve_results(data_wild, record_cache, for_cache, era_lookup)
            if result["confidence"] in ("high", "au_match"):
                result["confidence"] = f"wildcard_{result['confidence']}"

    time.sleep(RATE_SEARCH_SEC)
    search_cache[key] = result
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False,
         update_orcid: str | None = None,
         update_name: tuple[str, str] | None = None):
    setup_stdout_utf8()

    # 2026-08-21: was its own separate diskcache.Cache(DISKCACHE_DIR/"orcid_records") --
    # consolidated onto orcid_client.py's single canonical record cache (previously only used
    # by ad hoc session lookups) after the project accumulated three separate ORCID record
    # stores (this one, orcid_client.py's, and orcid_cache.py's per-file JSON used by
    # 04a_orcid_assist.py) that had silently diverged. See the one-off migration that copied
    # every entry from all three into this cache before this change landed.
    record_cache = default_cache()
    search_cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_searches"))
    for_cache    = diskcache.Cache(str(DISKCACHE_DIR / "orcid_for"))

    print("Loading ERA journal lookup...", end=" ", flush=True)
    era_lookup = load_era_lookup(PROJECT_DATA)
    print(f"{len(era_lookup)} titles")

    # Targeted update modes
    if update_orcid:
        print(f"Re-fetching /record for {update_orcid}")
        fetch_record(update_orcid, record_cache, for_cache, era_lookup, force=True)
        print("Done.")
        return

    if update_name:
        first, family = update_name
        print(f"Re-running search for {first!r} {family!r}")
        _search_orcid(first, family, record_cache, search_cache, for_cache, era_lookup, force=True)
        key = (first, family)
        print(f"Result: {search_cache[key]}")
        return

    # Load investigators
    inv = pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet")
    inv = inv[inv["role_code"].isin(KEEP_ROLES)]
    inv = inv[inv["grant_code"].str[:2].isin(KEEP_SCHEMES)]
    inv["name_key"] = inv.apply(lambda r: _name_key(r.family_name, r.first_name), axis=1)
    inv = inv[inv["name_key"].notna()]

    # 2026-08-21: institution-targeted search -- map each name pair to the distinct ORCID-
    # searchable institution phrase(s) its own ARC grants were administered at (a person can
    # hold grants at 2+ institutions, so this is a list, tried in order by _search_orcid()).
    grants = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")[["grant_code", "admin_org"]]
    admin_org_to_iname = load_admin_org_to_institution_name()
    inv_inst = inv.merge(grants, on="grant_code", how="left")
    inv_inst["institution_name"] = inv_inst["admin_org"].map(admin_org_to_iname)
    pair_institutions: dict[tuple[str, str], list[str]] = (
        inv_inst.dropna(subset=["institution_name"])
        .groupby(["first_name", "family_name"])["institution_name"]
        .apply(lambda s: sorted(set(s)))
        .to_dict()
    )

    # All distinct name pairs
    pairs = (
        inv[["name_key", "first_name", "family_name", "orcid"]]
        .drop_duplicates(subset=["first_name", "family_name"])
        .reset_index(drop=True)
    )
    # Representative ARC ORCID per name pair (first non-null, if any)
    arc_orcid = (
        inv[inv["orcid"].notna()]
        .drop_duplicates(subset=["first_name", "family_name"])
        [["first_name", "family_name", "orcid"]]
        .rename(columns={"orcid": "arc_orcid"})
    )
    pairs = pairs.drop(columns=["orcid"]).merge(arc_orcid, on=["first_name", "family_name"], how="left")

    has_arc_orcid   = pairs["arc_orcid"].notna()
    need_record     = pairs[has_arc_orcid & ~pairs["arc_orcid"].apply(lambda o: o in record_cache)]
    need_search     = pairs[~has_arc_orcid & ~pairs.apply(
                          lambda r: (r.first_name, r.family_name) in search_cache, axis=1)]

    for_with_codes = sum(1 for oid in for_cache.iterkeys() if for_cache[oid])
    print(f"Distinct name pairs:          {len(pairs)}")
    print(f"  Has ARC ORCID:              {has_arc_orcid.sum()}")
    print(f"    /record not yet cached:   {len(need_record)}")
    print(f"  No ARC ORCID:               {(~has_arc_orcid).sum()}")
    print(f"    search not yet cached:    {len(need_search)}")
    print(f"record_cache size:            {len(record_cache)}")
    print(f"search_cache size:            {len(search_cache)}")
    print(f"for_cache size:               {len(for_cache)}  ({for_with_codes} with ≥1 FOR code)")

    if dry_run:
        print("Dry run — no API calls made.")
        return

    # Pre-pass: derive FOR codes for all already-cached records (no API calls)
    need_for = [oid for oid in record_cache.iterkeys() if oid not in for_cache]
    if need_for:
        print(f"\nDeriving FOR codes for {len(need_for)} cached records (no API)...")
        for i, oid in enumerate(need_for):
            rec = record_cache[oid]
            for_cache[oid] = orcid_for_codes(rec, era_lookup)
            if (i + 1) % 1000 == 0 or (i + 1) == len(need_for):
                print(f"  [{i+1}/{len(need_for)}]", flush=True)
        print(f"for_cache size: {len(for_cache)}")

    # Fetch /record for ARC-ORCID names not yet cached
    n = len(need_record)
    if n:
        print(f"\nFetching {n} ORCID records...")
        for i, (_, row) in enumerate(need_record.iterrows()):
            fetch_record(row.arc_orcid, record_cache, for_cache, era_lookup)
            if (i + 1) % 200 == 0 or (i + 1) == n:
                print(f"  [{i+1}/{n}]", flush=True)

    # Search for no-ORCID names not yet cached
    n = len(need_search)
    if n:
        print(f"\nSearching {n} names...")
        for i, (_, row) in enumerate(need_search.iterrows()):
            insts = pair_institutions.get((row.first_name, row.family_name), [])
            _search_orcid(row.first_name, row.family_name,
                          record_cache, search_cache, for_cache, era_lookup,
                          institution_names=insts)
            if (i + 1) % 50 == 0 or (i + 1) == n:
                counts = {}
                for key in search_cache.iterkeys():
                    v = search_cache[key]
                    c = v.get("confidence", "?")
                    counts[c] = counts.get(c, 0) + 1
                print(f"  [{i+1}/{n}] {counts}", flush=True)

    # Write orcid_enrichment.parquet from search_cache
    _write_enrichment(search_cache, pairs[~has_arc_orcid])

    print(f"\nrecord_cache: {len(record_cache)} entries")
    print(f"search_cache: {len(search_cache)} entries")
    print(f"for_cache:    {len(for_cache)} entries")


def _write_enrichment(search_cache: diskcache.Cache, no_orcid_pairs: pd.DataFrame):
    """Materialise search_cache → orcid_enrichment.parquet."""
    rows = []
    for _, row in no_orcid_pairs.iterrows():
        key = (row.first_name, row.family_name)
        if key not in search_cache:
            continue
        result = search_cache[key]
        rows.append({
            "first_name":    row.first_name,
            "family_name":   row.family_name,
            "name_key":      row.name_key,
            "orcid":         result.get("orcid"),
            "confidence":    result.get("confidence"),
            "num_found":     result.get("num_found"),
            "works_count":   result.get("works_count", 0),
            "external_ids":  result.get("external_ids", "{}"),
            "au_candidates": result.get("au_candidates", "[]"),
            # provenance -- which mechanism produced this result. Existing (pre-2026-08-31)
            # result dicts never set this key, so default to "live_api" rather than leaving it
            # null for every row written before the bulk-DB search existed.
            "source":        result.get("source", "live_api"),
        })
    out = PROCESSED_DATA / "orcid_enrichment.parquet"
    pd.DataFrame(rows).to_parquet(out, index=False)
    print(f"Wrote {len(rows)} rows → {out}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run",       action="store_true")
    ap.add_argument("--update-orcid",  metavar="ORCID",
                    help="Re-fetch /record for one ORCID")
    ap.add_argument("--update-name",   nargs=2, metavar=("FIRST", "FAMILY"),
                    help="Re-run search for one person")
    args = ap.parse_args()
    main(
        dry_run=args.dry_run,
        update_orcid=args.update_orcid,
        update_name=tuple(args.update_name) if args.update_name else None,
    )
