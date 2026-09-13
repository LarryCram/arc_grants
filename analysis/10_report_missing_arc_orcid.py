"""
Find ARC-sourced orcids that don't exist in the local ORCID bulk snapshot at all, and check
each one directly against the live ORCID API.

"ARC-sourced" means the orcid came from ARC's own raw investigator data -- no `enriched_orcid`
or `manual_orcid` event in the ACIF's own provenance -- as opposed to an orcid `00b_enrich_orcid.py`
or a manual override added later. Of that population, most orcids are present in
`orcid_bulk.parquet` (the local Zenodo "Easy ORCID" snapshot) and can be checked entirely
offline; a small residual isn't in the snapshot at all (2026-09-11 investigation: ~59/11,365
population-wide) for a mix of reasons found that session -- genuinely real accounts the snapshot
just doesn't have, ORCID account merges/redirects, or (rarely) a fabricated/invalid value. This
script finds exactly that residual for the current ARC-sourced-orcid ACIF population and checks
each one live, since a local-only check structurally cannot say anything about them.

Usage:
  .venv/bin/python analysis/10_report_missing_arc_orcid.py

Output:
  $OUTPUT_ROOT/analysis/missing_arc_orcid_report.parquet
  Also printed to stdout as a table.
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd

from config.settings import PROCESSED_DATA, ORCID_BULK_PARQUET, OUTPUT_ROOT
from src.utils.names import HumanNameParser, name_part_tokens
from src.utils.orcid_client import default_cache, fetch_orcid_record

ANALYSIS_OUT = OUTPUT_ROOT / "analysis"
ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)
ARC_ONLY_PARQUET = PROCESSED_DATA / "awards_cif_arc_only.parquet"
OUT = ANALYSIS_OUT / "missing_arc_orcid_report.parquet"


def find_arc_sourced_orcid_not_in_bulk() -> pd.DataFrame:
    """One row per ACIF whose orcid (a) came from ARC's own raw data (no enriched_orcid/
    manual_orcid provenance event) and (b) is not present anywhere in orcid_bulk.parquet.
    Columns: cluster_id, grant_ids, full_names, orcid."""
    df = duckdb.sql(f"""
        SELECT cluster_id, grant_ids, full_names, provenance, orcids
        FROM read_parquet('{ARC_ONLY_PARQUET}')
        WHERE excluded = FALSE AND orcid_status = 'HAS_ORCID'
    """).df()

    def is_arc_sourced(prov_json: str) -> bool:
        events = {e.get("event") for e in json.loads(prov_json)}
        return not ({"enriched_orcid", "manual_orcid"} & events)

    df = df[df["provenance"].apply(is_arc_sourced)].copy()
    df["orcid"] = df["orcids"].apply(lambda l: l[0])

    con = duckdb.connect()
    con.register("arc_df", df[["cluster_id", "grant_ids", "full_names", "orcid"]])
    missing = con.execute(f"""
        SELECT a.cluster_id, a.grant_ids, a.full_names, a.orcid
        FROM arc_df a
        LEFT JOIN read_parquet('{ORCID_BULK_PARQUET}') b ON b.orcid = a.orcid
        WHERE b.orcid IS NULL
        ORDER BY a.cluster_id
    """).fetchdf()
    return missing


def check_live(orcid: str, cache) -> dict:
    """Fetch this orcid's own /record live (cache-first). Reports whether it exists, whether
    it redirected to a different canonical orcid (an ORCID account merge/deactivation --
    2026-09-11's Samuel Baron finding: this is silently masked by a naive 'got a 200' check),
    and its registered given/family name when available."""
    rec = fetch_orcid_record(orcid, cache)
    if "_error" in rec:
        return {"exists_live": False, "error": rec["_error"], "redirected_to": None,
                "given_name": None, "family_name": None}

    actual = rec.get("orcid-identifier", {}).get("path")
    name = rec.get("person", {}).get("name") or {}
    given = (name.get("given-names") or {}).get("value")
    family = (name.get("family-name") or {}).get("value")
    return {
        "exists_live": True,
        "error": None,
        "redirected_to": actual if actual and actual != orcid else None,
        "given_name": given,
        "family_name": family,
    }


_PARSER = HumanNameParser()


def _acif_keys(names: list[str]) -> set[str]:
    keys: set[str] = set()
    for n in names:
        keys |= set(_PARSER.parse(n).full_name_keys)
    return keys


def _acif_given_tokens(names: list[str]) -> set[str]:
    toks: set[str] = set()
    for n in names:
        p = _PARSER.parse(n)
        toks |= set(p.given_tokens) | set(p.nickname_tokens)
    return toks


def categorize(row: pd.Series) -> str:
    """Live-name compatibility category for one row (2026-09-12). Not a single given+family
    key check -- an incomplete live ORCID profile (blank family name, or both fields blank/
    private) is common and must not be conflated with a genuine mismatch; each shape gets its
    own honest bucket rather than a lumped 'incompatible' verdict driven by missing data."""
    if pd.notna(row["redirected_to"]):
        return "redirected (account merge)"

    given = row["given_name"] if pd.notna(row["given_name"]) else ""
    family = row["family_name"] if pd.notna(row["family_name"]) else ""

    if not given and not family:
        return "private name (no name data live)"

    if given and not family:
        given_toks = set(name_part_tokens(given))
        if given_toks & _acif_given_tokens(row["full_names"]):
            return "given-name-only match (family name blank live)"
        return "INCOMPATIBLE -- needs review"

    live_keys = set(_PARSER.parse(f"{given} {family}".strip()).full_name_keys)
    if live_keys & _acif_keys(row["full_names"]):
        return "compatible (ordinary coverage gap)"
    return "INCOMPATIBLE -- needs review"


def grant_year(cluster_id: str) -> str:
    """2-digit funding-round year embedded in the grant code prefix of a cluster_id
    (e.g. 'DP0209958_...' -> '02', 'DE220101577_...' -> '22') -- grant_code[2:4]."""
    grant_code = cluster_id.split("_", 1)[0]
    return grant_code[2:4]


def main():
    t0 = time.time()
    missing = find_arc_sourced_orcid_not_in_bulk()
    print(f"ARC-sourced orcids not in local bulk snapshot: {len(missing)}")
    print()

    cache = default_cache()
    rows = []
    for i, row in missing.iterrows():
        live = check_live(row["orcid"], cache)
        rows.append({
            "cluster_id":     row["cluster_id"],
            "grant_ids":      list(row["grant_ids"]),
            "full_names":     list(row["full_names"]),
            "orcid":          row["orcid"],
            **live,
        })
        time.sleep(0.05)
        if (i + 1) % 20 == 0:
            print(f"  [{i + 1}/{len(missing)}]", flush=True)

    out = pd.DataFrame(rows)
    out["category"] = out.apply(categorize, axis=1)
    out["year"] = out["cluster_id"].apply(grant_year)
    out.to_parquet(OUT, index=False)
    print(f"\nWrote {len(out)} rows -> {OUT}  ({time.time() - t0:.0f}s)")

    print()
    print("=== category counts ===")
    print(out["category"].value_counts().to_string())

    print()
    print("=== detail, by category then year ===")
    pd.set_option("display.max_colwidth", 40)
    pd.set_option("display.width", 200)
    out_sorted = out.sort_values(["category", "year", "cluster_id"])
    print(out_sorted[["category", "year", "cluster_id", "orcid", "given_name", "family_name",
                       "redirected_to"]].to_string(index=False))


if __name__ == "__main__":
    main()
