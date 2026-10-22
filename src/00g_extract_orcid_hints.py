"""
src/00g_extract_orcid_hints.py -- live ORCID records for the ORCID-hint stage of the linker
(src/oax/orcid_hint_link.py), 2026-10-22.

For the kept ACIFs the last linker run left unlinked (acifs_arc.parquet less accepted_links.parquet), the
candidate OpenAlex records of orcid_hint_link.candidates() that carry an ORCID; each such ORCID's /record is
fetched from the ORCID Public API (registered client: ORCID_CLIENT_ID / ORCID_CLIENT_SECRET in .env; 4
parallel requests, back-off on 429 / 5xx) into the project's ORCID record cache
(DISKCACHE_DIR/orcid_records_authenticated), replacing an older copy, and marked `_fetched_live` (date). A
marked record is not fetched again (--refetch fetches all). Same pattern as 00f --targets unlinked: run after
02, then rerun 02.

Usage: .venv/bin/python src/00g_extract_orcid_hints.py [--refetch]
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import diskcache
import duckdb
import pandas as pd
import requests

from config.settings import (ACIFS_ARC, DISKCACHE_DIR, DUCKDB_TMP_DIR, OAX_LINK_DIR, ORCID_CLIENT_ID,
                             ORCID_CLIENT_SECRET)
from src.oax.orcid_hint_link import candidates

ORCID_API = "https://pub.orcid.org/v3.0"


def token() -> str:
    r = requests.post("https://orcid.org/oauth/token", headers={"Accept": "application/json"}, timeout=20,
                      data={"client_id": ORCID_CLIENT_ID, "client_secret": ORCID_CLIENT_SECRET,
                            "grant_type": "client_credentials", "scope": "/read-public"})
    r.raise_for_status()
    return r.json()["access_token"]


def fetch(orcids: list[str], cache) -> int:
    session = requests.Session()
    session.headers.update({"Accept": "application/json", "Authorization": f"Bearer {token()}"})

    def get(o):
        for attempt in range(5):
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

    done = 0
    with ThreadPoolExecutor(4) as ex:
        for k, (o, rec) in enumerate(ex.map(get, orcids), 1):
            if rec is not None:
                if "_error" not in rec:
                    rec["_fetched_live"] = time.strftime("%Y-%m-%d")
                cache.set(o, rec)
                done += 1
            if k % 500 == 0:
                print(f"  fetched {k:,}/{len(orcids):,}", flush=True)
    return done


def main():
    acifs = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "full_name_keys", "excluded"])
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    todo = acifs[~acifs.excluded & ~acifs.cluster_id.isin(set(acc.cluster_id))]
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{DUCKDB_TMP_DIR}'")
    c = candidates(con, todo, set(acc.author_idx.astype("int64")))
    orcids = sorted(set(c.oax_orcid.dropna()))
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    want = orcids if "--refetch" in sys.argv else [
        o for o in orcids if not (isinstance(cache.get(o), dict) and cache.get(o).get("_fetched_live"))]
    print(f"unlinked ACIFs {len(todo):,}; with candidates {c.cluster_id.nunique():,}; candidates {len(c):,}; "
          f"carrying an ORCID {int(c.oax_orcid.notna().sum()):,} ({len(orcids):,} ORCIDs); fetching {len(want):,}", flush=True)
    t0 = time.time()
    n = fetch(want, cache) if want else 0
    print(f"fetched {n:,} live ORCID records in {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
