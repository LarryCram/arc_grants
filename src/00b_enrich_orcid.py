"""
src/00b_enrich_orcid.py

Enrich ARC investigator records with ORCIDs from the ORCID public API.

Targets name groups that are:
  - multi-row (n >= 2 distinct grant records)
  - no ORCID at all in ARC data

For each group, looks up every distinct (first_name, family_name) pair
separately. Two different ORCIDs returned for the same name group = confirmed
split, learned before Splink runs.

Results are cached to avoid re-hitting the API. Re-run is safe: only looks
up names not already in the cache.

Input:
    processed/investigators_raw.parquet

Output:
    processed/orcid_enrichment.parquet
    columns: first_name, family_name, name_key, orcid, confidence, num_found
    confidence: 'high' (1 result), 'au_match' (AU employer among N results),
                'low' (multiple, no AU filter), 'not_found', 'too_common'
"""

import sys
import time
from pathlib import Path

import requests
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import strip_diacriticals, name_part_tokens
from src.utils.io import setup_stdout_utf8
from src.utils.orcid_cache import fetch_orcid, orcid_addresses

ORCID_SEARCH   = "https://pub.orcid.org/v3.0/search/"
HEADERS        = {"Accept": "application/json"}
CACHE_DIR      = PROCESSED_DATA / "orcid_cache"
RATE_LIMIT_SEC = 1.0
TOO_COMMON     = 10


def _norm_family(s: str) -> str:
    return strip_diacriticals(s or "").lower().strip()

def _first_initial(s: str) -> str | None:
    toks = name_part_tokens(s or "")
    return toks[0][0] if toks else None

def _name_key(family: str, first: str) -> str | None:
    f = _norm_family(family)
    i = _first_initial(first)
    return f"{f}_{i}" if (f and i) else None


def _search_orcid(first: str, family: str) -> dict:
    """Search ORCID for a name. Returns dict with orcid, confidence, num_found."""
    q = f'given-names:{first} AND family-name:{family}'
    try:
        r = requests.get(ORCID_SEARCH, params={"q": q, "rows": TOO_COMMON + 1},
                         headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"orcid": None, "confidence": "error", "num_found": -1, "error": str(e)}

    num_found = data.get("num-found", 0)

    if num_found == 0:
        return {"orcid": None, "confidence": "not_found", "num_found": 0}

    if num_found > TOO_COMMON:
        return {"orcid": None, "confidence": "too_common", "num_found": num_found}

    results = data.get("result", [])
    orcids = [r["orcid-identifier"]["path"] for r in results]

    if num_found == 1:
        return {"orcid": orcids[0], "confidence": "high", "num_found": 1}

    # Multiple results: check each for Australian address via shared cache
    au_orcids = []
    for oid in orcids:
        try:
            rec = fetch_orcid(oid, CACHE_DIR)
            countries = {a.get("country", {}).get("value") for a in orcid_addresses(rec)}
            if "AU" in countries:
                au_orcids.append(oid)
        except Exception:
            continue

    if len(au_orcids) == 1:
        return {"orcid": au_orcids[0], "confidence": "au_match", "num_found": num_found}

    return {"orcid": None, "confidence": "low", "num_found": num_found}


def main(dry_run: bool = False):
    setup_stdout_utf8()

    cache_path = PROCESSED_DATA / "orcid_enrichment.parquet"

    # Load cache
    if cache_path.exists():
        cache = pd.read_parquet(cache_path)
        cached_keys = set(zip(cache.first_name, cache.family_name))
        print(f"Cache: {len(cache)} entries")
    else:
        cache = pd.DataFrame()
        cached_keys = set()

    # Load and filter investigators
    inv = pd.read_parquet(PROCESSED_DATA / "investigators_raw.parquet")
    inv = inv[inv["role_code"].isin(KEEP_ROLES)]
    inv = inv[inv["grant_code"].str[:2].isin(KEEP_SCHEMES)]
    inv["name_key"] = inv.apply(lambda r: _name_key(r.family_name, r.first_name), axis=1)
    inv = inv[inv["name_key"].notna()]

    # Identify target groups: multi-row, no ORCID
    has_orcid = inv.groupby("name_key")["orcid"].apply(lambda s: s.notna().any())
    multi     = inv.groupby("name_key").size() >= 2
    targets   = inv[inv["name_key"].isin(has_orcid[~has_orcid & multi].index)]

    # Distinct name pairs in target groups
    pairs = (
        targets[["name_key", "first_name", "family_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    pairs = pairs[~pairs.apply(lambda r: (r.first_name, r.family_name) in cached_keys, axis=1)]

    print(f"Target name groups:    {targets['name_key'].nunique()}")
    print(f"Distinct name pairs:   {len(pairs) + len(cached_keys)} total, {len(pairs)} not yet cached")

    if dry_run or len(pairs) == 0:
        print("Dry run — no API calls made.")
        return

    rows_so_far = list(cache.to_dict("records")) if len(cache) else []
    for i, row in pairs.iterrows():
        print(f"  [{i+1}/{len(pairs)}] {row.first_name} {row.family_name} ...", end=" ", flush=True)
        result = _search_orcid(row.first_name, row.family_name)
        rec = {
            "first_name":  row.first_name,
            "family_name": row.family_name,
            "name_key":    row.name_key,
            "orcid":       result["orcid"],
            "confidence":  result["confidence"],
            "num_found":   result["num_found"],
        }
        rows_so_far.append(rec)
        print(f"{result['confidence']}  {result['orcid'] or ''}")
        pd.DataFrame(rows_so_far).to_parquet(cache_path, index=False)
        time.sleep(RATE_LIMIT_SEC)

    print(f"\nSaved {len(rows_so_far)} entries → {cache_path}")

    # Summary
    if rows_so_far:
        print("\nResults:")
        print(pd.DataFrame(rows_so_far)["confidence"].value_counts().to_string())


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    main(dry_run=args.dry_run)
