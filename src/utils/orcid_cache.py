"""
Shared ORCID record fetcher with disk cache.

Cache location: PROCESSED_DATA/orcid_cache/{orcid}.json
One file per ORCID, containing the full /record endpoint response.

Both 00b_enrich_orcid.py and 05_orcid_assist.py use this so they share
the same cache and format.
"""

import json
import time
from pathlib import Path

import requests

ORCID_API  = "https://pub.orcid.org/v3.0"
HEADERS    = {"Accept": "application/json"}
RATE_SLEEP = 0.05


def fetch_orcid(orcid: str, cache_dir: Path) -> dict:
    """Return the full ORCID /record response, reading from disk cache if available."""
    cache_path = cache_dir / f"{orcid}.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text())
    try:
        r = requests.get(f"{ORCID_API}/{orcid}/record", headers=HEADERS, timeout=10)
        data = r.json() if r.status_code == 200 else {"_error": r.status_code}
    except Exception as e:
        data = {"_error": str(e)}
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(data))
    time.sleep(RATE_SLEEP)
    return data


def orcid_addresses(rec: dict) -> list[dict]:
    """Extract address list from a /record response."""
    try:
        return rec["person"]["addresses"]["address"]
    except (KeyError, TypeError):
        return []


def orcid_keywords(rec: dict) -> list[str]:
    """Extract keyword strings from a /record response."""
    try:
        return [k["content"].lower() for k in rec["person"]["keywords"]["keyword"]]
    except (KeyError, TypeError):
        return []
