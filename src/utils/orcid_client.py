"""
src/utils/orcid_client.py

Authenticated ORCID Public API client with disk caching.

Uses the registered ORCID Public API client (ORCID_CLIENT_ID/ORCID_CLIENT_SECRET in .env,
obtained 2026-08-18) via OAuth client-credentials, not anonymous access -- anonymous access hit
ORCID's daily quota during 00b_enrich_orcid.py's original run (CLAUDE.md, "2026-08-08
follow-up"), a registered client should clear that wall. Kept as a separate module from
src/utils/orcid_cache.py (the existing anonymous, per-file-JSON cache used by
00b_enrich_orcid.py/05_orcid_assist.py) rather than merged into it -- this one authenticates
and that one doesn't; not changing 00b's already-working behavior as a side effect of this.

Cache: diskcache.Cache (user's stated preference) at
DISKCACHE_DIR/orcid_records_authenticated -- one entry per ORCID, the full /record response
(person + activities-summary: works, employments, educations, fundings, ...). A single
/record call already carries everything this module's accessors need; no separate per-section
fetch required.

Motivation: OpenAlex has no reliable employment-history or education-date concept (only
per-work institutional affiliation, and even that can be wrong -- see CLAUDE.md's Adam Hulme
and Frank Gruetzner findings, 2026-08-18). A person's own ORCID record has both, plus an
independent, OpenAlex-blind publication-year timeline -- useful for validating whichever
OpenAlex candidate eventually gets linked, or for a future person-relative implausible-year
check, neither built yet. This module is the fetch/cache foundation only; no pipeline stage
consumes it yet.
"""

import time

import diskcache
import requests

from config.settings import DISKCACHE_DIR, ORCID_CLIENT_ID, ORCID_CLIENT_SECRET

ORCID_API  = "https://pub.orcid.org/v3.0"
TOKEN_URL  = "https://orcid.org/oauth/token"
RATE_SLEEP = 0.05  # authenticated -- conservative default, not yet empirically tuned against ORCID's real authenticated-client rate limit

_token: str | None = None


def get_access_token(force: bool = False) -> str:
    """OAuth client-credentials token for the registered Public API client. Cached in-process
    for this Python process's lifetime -- the token itself is long-lived (ORCID returns
    expires_in ~= 20 years), so one fetch per run is enough; force=True re-fetches (e.g. on a
    401 from a stale/revoked token)."""
    global _token
    if _token is not None and not force:
        return _token
    r = requests.post(
        TOKEN_URL,
        headers={"Accept": "application/json"},
        data={
            "client_id": ORCID_CLIENT_ID,
            "client_secret": ORCID_CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": "/read-public",
        },
        timeout=10,
    )
    r.raise_for_status()
    _token = r.json()["access_token"]
    return _token


CACHE_SIZE_LIMIT = 20 * 1024**3  # 20 GiB -- default diskcache limit (1 GiB) was already hit at
                                  # only 10,403 records during the 2026-08-18 seed-copy, silently
                                  # evicting ~10 entries with no log of which ones. 20GiB is far
                                  # beyond any realistic size even fetching every ARC person's ORCID.


def default_cache() -> diskcache.Cache:
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    if cache.size_limit < CACHE_SIZE_LIMIT:
        cache.reset("size_limit", CACHE_SIZE_LIMIT)
    return cache


def seed_from_anonymous_cache(cache: diskcache.Cache | None = None) -> int:
    """One-time (re-runnable) backfill from the older, pre-existing anonymous-access cache
    (src/utils/orcid_cache.py / 00b_enrich_orcid.py's own diskcache store,
    DISKCACHE_DIR/orcid_records -- 10,413 entries as of 2026-08-18, 3 of them error responses).
    Same /record endpoint, same response shape, auth only affects the request not the data --
    confirmed directly (2026-08-18) by inspecting a real cached entry -- so there is no reason
    to re-fetch anyone already fetched there. Skips error entries (a fresh authenticated fetch
    is cheap and might succeed where the old anonymous one didn't) and anyone already present in
    the target cache. Returns the number of records actually copied."""
    cache = cache or default_cache()
    old = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records"))
    n_copied = 0
    for orcid in old.iterkeys():
        if orcid in cache:
            continue
        rec = old[orcid]
        if isinstance(rec, dict) and "_error" in rec:
            continue
        cache[orcid] = rec
        n_copied += 1
    return n_copied


def fetch_orcid_record(orcid: str, cache: diskcache.Cache, force: bool = False) -> dict:
    """Full ORCID /record response for one ORCID, authenticated, cached. Returns
    {'_error': ...} on failure rather than raising -- same convention as the existing
    orcid_cache.py/00b_enrich_orcid.py fetchers, so callers already know to check for it."""
    if not force and orcid in cache:
        return cache[orcid]

    token = get_access_token()
    try:
        r = requests.get(
            f"{ORCID_API}/{orcid}/record",
            headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if r.status_code == 401:  # stale/invalid token -- refresh once and retry
            token = get_access_token(force=True)
            r = requests.get(
                f"{ORCID_API}/{orcid}/record",
                headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
                timeout=10,
            )
        data = r.json() if r.status_code == 200 else {"_error": r.status_code}
    except Exception as e:
        data = {"_error": str(e)}

    cache[orcid] = data
    time.sleep(RATE_SLEEP)
    return data


# ---------------------------------------------------------------------------
# Accessors -- pull structured facts out of a /record response.
# ---------------------------------------------------------------------------

def _year(date_block: dict | None) -> int | None:
    if not date_block:
        return None
    y = date_block.get("year") or {}
    val = y.get("value") if isinstance(y, dict) else None
    return int(val) if val else None


def _affiliation_entries(rec: dict, section: str, summary_key: str) -> list[dict]:
    """Shared shape for employments/educations: [{organization, country, role_title,
    start_year, end_year}], end_year=None meaning current/ongoing."""
    out = []
    try:
        groups = rec["activities-summary"][section]["affiliation-group"]
    except (KeyError, TypeError):
        return out
    for group in groups:
        for summary in group.get("summaries", []):
            s = summary.get(summary_key, {})
            org = s.get("organization", {}) or {}
            out.append({
                "organization": org.get("name"),
                "country": (org.get("address") or {}).get("country"),
                "role_title": s.get("role-title"),
                "start_year": _year(s.get("start-date")),
                "end_year": _year(s.get("end-date")),
            })
    return out


def orcid_employments(rec: dict) -> list[dict]:
    """This person's own recorded employment history -- OpenAlex has no equivalent concept."""
    return _affiliation_entries(rec, "employments", "employment-summary")


def orcid_educations(rec: dict) -> list[dict]:
    """This person's own recorded education history, same shape as orcid_employments()."""
    return _affiliation_entries(rec, "educations", "education-summary")


def orcid_qualifications(rec: dict) -> list[dict]:
    """Non-degree professional qualifications/certifications, same shape as orcid_employments()."""
    return _affiliation_entries(rec, "qualifications", "qualification-summary")


def orcid_distinctions(rec: dict) -> list[dict]:
    """Named honours/distinguished appointments (e.g. an honorary professorship), same shape."""
    return _affiliation_entries(rec, "distinctions", "distinction-summary")


def orcid_invited_positions(rec: dict) -> list[dict]:
    """Visiting/invited appointments, same shape as orcid_employments()."""
    return _affiliation_entries(rec, "invited-positions", "invited-position-summary")


def orcid_memberships(rec: dict) -> list[dict]:
    """Professional society/association membership, same shape as orcid_employments()."""
    return _affiliation_entries(rec, "memberships", "membership-summary")


def orcid_services(rec: dict) -> list[dict]:
    """Committee/editorial/reviewing service roles, same shape as orcid_employments()."""
    return _affiliation_entries(rec, "services", "service-summary")


def orcid_all_affiliations(rec: dict) -> dict[str, list[dict]]:
    """Every affiliation-shaped section in one call -- {"employments": [...], "educations":
    [...], ...}, all seven types, same per-entry shape throughout. Convenience wrapper; each
    section is still available individually above for a caller that only wants one."""
    return {
        "employments": orcid_employments(rec),
        "educations": orcid_educations(rec),
        "qualifications": orcid_qualifications(rec),
        "distinctions": orcid_distinctions(rec),
        "invited_positions": orcid_invited_positions(rec),
        "memberships": orcid_memberships(rec),
        "services": orcid_services(rec),
    }


def orcid_work_years(rec: dict) -> list[int]:
    """Every distinct publication year across this ORCID's own work groups -- an independent,
    OpenAlex-blind timeline, useful for sanity-checking whatever OpenAlex candidate is linked."""
    years = set()
    try:
        groups = rec["activities-summary"]["works"]["group"]
    except (KeyError, TypeError):
        return []
    for group in groups:
        for summary in group.get("work-summary", []):
            y = _year(summary.get("publication-date"))
            if y:
                years.add(y)
    return sorted(years)


def orcid_names(rec: dict) -> dict:
    """This ORCID's own recorded name(s) -- given_names, family_name, credit_name (the
    person's chosen display form, if set), other_names (every alternate spelling the person has
    themselves added to their record). 2026-08-19: built for orcid_name_table() -- comparing an
    ORCID's own name(s) against whatever name OpenAlex's own author record carries for the same
    ORCID is exactly the mechanism that surfaced the Grutzner/Gruetzner spelling split."""
    try:
        name = rec["person"]["name"] or {}
    except (KeyError, TypeError):
        name = {}
    given = (name.get("given-names") or {}).get("value")
    family = (name.get("family-name") or {}).get("value")
    credit = (name.get("credit-name") or {}).get("value")
    other = []
    try:
        for o in rec["person"]["other-names"]["other-name"]:
            v = (o or {}).get("content")
            if v:
                other.append(v)
    except (KeyError, TypeError):
        pass
    return {"given_names": given, "family_name": family, "credit_name": credit, "other_names": other}
