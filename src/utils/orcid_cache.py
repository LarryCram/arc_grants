"""
Shared ORCID /record accessor functions.

2026-09-01: the per-file fetch/cache mechanism this module used to own
(fetch_orcid() -> PROCESSED_DATA/orcid_cache/{orcid}.json) was retired 2026-08-21 in favour of
orcid_client.py's consolidated diskcache and confirmed here to have zero remaining callers --
removed outright, along with the now-empty PROCESSED_DATA/orcid_cache/ directory (part of the
OrcidProcessor legacy cleanup, docs/pipeline_todo.md #19). The accessor functions below are
still load-bearing (00b_enrich_orcid.py, 04a_orcid_assist.py) and are kept as-is.
"""


def orcid_addresses(rec: dict) -> list[dict]:
    """Extract address entries from a /record response.

    Checks both personal addresses (person.addresses) and employment
    organisation addresses (activities-summary.employments). Many researchers
    have AU country only via employment, not as a personal address.
    """
    addresses = []
    try:
        addresses += rec["person"]["addresses"]["address"]
    except (KeyError, TypeError):
        pass
    try:
        for group in rec["activities-summary"]["employments"]["affiliation-group"]:
            for summary in group["summaries"]:
                country = (
                    summary["employment-summary"]["organization"]
                    .get("address", {})
                    .get("country")
                )
                if country:
                    addresses.append({"country": {"value": country}})
    except (KeyError, TypeError):
        pass
    return addresses


def orcid_keywords(rec: dict) -> list[str]:
    """Extract keyword strings from a /record response."""
    try:
        return [k["content"].lower() for k in rec["person"]["keywords"]["keyword"]]
    except (KeyError, TypeError):
        return []


def orcid_external_ids(rec: dict) -> dict[str, str]:
    """Extract external identifier map from a /record response.
    Returns {type: value}, e.g. {"Scopus Author ID": "7006560946"}.
    """
    try:
        return {
            e["external-id-type"]: e["external-id-value"]
            for e in rec["person"]["external-identifiers"]["external-identifier"]
        }
    except (KeyError, TypeError):
        return {}


def orcid_works_count(rec: dict) -> int:
    """Count work groups in a /record response (proxy for publication count)."""
    try:
        return len(rec["activities-summary"]["works"]["group"])
    except (KeyError, TypeError):
        return 0
