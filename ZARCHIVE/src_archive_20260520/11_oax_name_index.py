"""
11_oax_name_index.py  —  Pre-processing (run once before Layer 2)

Build the reverse OAX name index used by Layer 2 (script 14).

For every ever-AU OAX author (any affiliation with country_code='AU'):
  - Apply norm_np normalisation to display_name and every display_name_alternative.
  - For each result, emit a full-name row and (if first is multi-char) an initial row.

Schema written to oax_name_index.parquet:
  norm_family_token  str   normalised last-name token (nameparser .last after norm_np)
  oax_id             str   OpenAlex author ID (e.g. "A12345678")
  name_form          str   normalised "first last" string
  is_initial_form    bool  True when first name is replaced with its initial

Layer 2 retrieves all rows matching norm_family_token, then applies
first_names_compatible() to filter to plausible candidates.

INPUT:  OAX_AUTHORS/*.parquet  (full Feb 2026 partition files, ~1.1M AU authors)
OUTPUT: PROCESSED_DATA/oax_name_index.parquet
"""

import re
import sys
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROCESSED_DATA
from src.utils.io import setup_stdout_utf8
from src.utils.names import strip_diacriticals, strip_parens


# ── Name helpers ──────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    return re.sub(r"[^a-z ]", "", strip_diacriticals(s).lower()).strip()


def _index_rows(oax_id: str, display_name: str) -> list[tuple]:
    """Emit index rows for one OAX display name (full form + optional initial form)."""
    clean = strip_diacriticals(strip_parens(display_name))
    n = HumanName(clean)
    family = _norm(n.last)
    first  = _norm(n.first)
    if not family:
        return []
    name_full = f"{first} {family}".strip() if first else family
    rows = [(family, oax_id, name_full, False)]
    if first and len(first) > 1:
        rows.append((family, oax_id, f"{first[0]} {family}", True))
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()

    glob = str(OAX_AUTHORS / "*.parquet")
    print(f"Source: {glob}")

    con = duckdb.connect()
    print("Filtering to ever-AU authors...")
    df = con.execute(f"""
        SELECT
            ids.openalex              AS oax_id,
            display_name,
            display_name_alternatives
        FROM read_parquet('{glob}')
        WHERE len(list_filter(affiliations, x -> x.institution.country_code = 'AU')) > 0
          AND ids.openalex IS NOT NULL
    """).df()
    print(f"  Ever-AU authors: {len(df):,}")

    print("Building name index rows...")
    rows = []
    n_authors = len(df)
    for i, row in enumerate(df.itertuples(index=False), 1):
        if i % 100_000 == 0:
            print(f"  {i:,} / {n_authors:,}")
        raw  = row.display_name_alternatives
        alts = list(raw) if raw is not None else []
        for name in [row.display_name] + list(alts):
            if name:
                rows.extend(_index_rows(row.oax_id, name))

    print(f"  Raw rows: {len(rows):,}")

    df_out = (
        pd.DataFrame(rows, columns=["norm_family_token", "oax_id", "name_form", "is_initial_form"])
        .drop_duplicates()
        .sort_values("norm_family_token")
        .reset_index(drop=True)
    )
    print(f"  Deduplicated rows: {len(df_out):,}")

    out = PROCESSED_DATA / "oax_name_index.parquet"
    df_out.to_parquet(out, index=False)
    print(f"\nSaved: {out}")
    print(f"  Size: {out.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
