"""
08_match_layer3_oax_api.py

Layer 3 API pass: use the OpenAlex authors API to find the no-match residual.

These are persons for whom the local family-name index found zero OAX candidates —
either the name is stored differently in OAX (compound surname, diacritical,
transliteration variant) or they are genuinely absent.

Strategy per person:
  1. Build a search query: strip parentheticals, join first + family name.
  2. Call Authors().search_filter(display_name=...) → top MAX_RESULTS candidates.
  3. Retain only candidates with at least one AU affiliation.
  4. Score each AU candidate with first_score (given name) + family token check.
  5. Classify:
       confident  — exactly 1 AU candidate passes both name checks
       review     — multiple AU candidates pass, or score is borderline
       not_found  — no AU candidate passes

OUTPUT:
    PROCESSED_DATA/layer3_api_matches.parquet
        Same schema as layer2_matches.parquet; tier = 7.
        Only 'confident' matches written here.

    PROCESSED_DATA/layer3_api_review.csv
        All AU candidates for 'review' cases — for manual inspection.

    PROCESSED_DATA/layer3_residual.parquet   (overwritten)
"""

import re
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd
import pyalex

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OPENALEX_API_KEY, OPENALEX_EMAIL, PROCESSED_DATA
from src.utils.io import setup_stdout_utf8

MAX_RESULTS   = 10   # API results to fetch per query
SLEEP_BETWEEN = 0.15 # seconds between requests (polite)

# ── Name helpers (mirrors 05_match_layer2_names.py) ──────────────────────────

_EXOTIC_HYPHENS = re.compile(r"[­‐‑‒–—―−－]")


def _strip_diacriticals(s: str) -> str:
    s = _EXOTIC_HYPHENS.sub("-", s)
    s = s.replace("ı", "i").replace("İ", "I")
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")

def _tokens(name: str) -> list[str]:
    return re.findall(r"[a-z]+", _strip_diacriticals(name).lower())

def _is_initial(first_name: str) -> bool:
    return bool(re.match(r"^[A-Za-z]\.?$", first_name.strip()))

def _clean_parens(s: str) -> str:
    return re.sub(r"\s*\(.*?\)", "", s).strip()

def first_score(arc_first: str, oax_names: list[str]) -> float:
    arc_initial = _is_initial(arc_first)
    arc_given   = _tokens(arc_first)
    if not arc_given:
        return 0.0
    oax_all_toks = set(t for n in oax_names for t in _tokens(n))
    best = 0.0
    for t in arc_given:
        if t in oax_all_toks:
            return 1.0
        if arc_initial and len(t) == 1:
            if any(s.startswith(t) for s in oax_all_toks if len(s) > 1):
                best = max(best, 0.8)
        elif len(t) > 1:
            if t[0] in oax_all_toks and t not in oax_all_toks:
                best = max(best, 0.8)
    return best

def family_match(arc_family: str, oax_display: str) -> bool:
    """True if any token of arc_family (stripped) appears in oax_display tokens."""
    arc_toks = set(_tokens(_clean_parens(arc_family)))
    oax_toks = set(_tokens(oax_display))
    return bool(arc_toks & oax_toks)


def main():
    setup_stdout_utf8()

    pyalex.config.email   = OPENALEX_EMAIL
    pyalex.config.api_key = OPENALEX_API_KEY
    pyalex.config.max_retries = 3
    pyalex.config.retry_backoff_factor = 0.5
    pyalex.config.retry_http_codes = [429, 500, 503]

    # ── Load no-match residual ────────────────────────────────────────────────
    df_res  = pd.read_parquet(PROCESSED_DATA / "layer3_residual.parquet")
    no_match = (
        df_res[df_res["sub_case"] == "no_match"]
        [["first_name", "family_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    print(f"No-match persons to search: {len(no_match)}")

    confident_rows = []
    review_rows    = []
    not_found      = []

    for _, person in no_match.iterrows():
        first  = _clean_parens(str(person["first_name"]))
        family = _clean_parens(str(person["family_name"]))
        query  = f"{first} {family}"

        try:
            results = pyalex.Authors().search_filter(display_name=query).get(
                per_page=MAX_RESULTS
            )
        except Exception as e:
            print(f"  API error for {query!r}: {e}")
            not_found.append((person["first_name"], person["family_name"], "api_error"))
            time.sleep(SLEEP_BETWEEN)
            continue

        # Keep only AU-affiliated candidates
        au_candidates = []
        for r in results:
            affs = r.get("affiliations") or []
            countries = {a.get("institution", {}).get("country_code") for a in affs}
            if "AU" in countries:
                au_candidates.append(r)

        # Score each AU candidate
        passing = []
        for r in au_candidates:
            alts      = r.get("display_name_alternatives") or []
            all_names = [r["display_name"]] + list(alts)
            fs = first_score(person["first_name"], all_names)
            fm = family_match(person["family_name"], r["display_name"])
            if fs > 0 and fm:
                passing.append((r, fs))

        arc_fn = person["first_name"]
        arc_ln = person["family_name"]

        if len(passing) == 1:
            r, fs = passing[0]
            oax_id = r["id"].replace("https://openalex.org/", "")
            confident_rows.append({
                "arc_first_name":   arc_fn,
                "arc_family_name":  arc_ln,
                "arc_orcid":        None,
                "arc_sub_case":     "no_match",
                "arc_admin_orgs":   None,
                "arc_oax_inst_ids": None,
                "n_arc_grants":     None,
                "oax_id":           oax_id,
                "oax_display_name": r["display_name"],
                "oax_orcid":        r.get("orcid"),
                "first_score":      fs,
                "institution_match": False,
                "tier":             7,
            })
            print(f"  CONFIDENT  {arc_fn} {arc_ln}  →  {r['display_name']}  (fs={fs})")

        elif len(passing) > 1:
            for r, fs in passing:
                review_rows.append({
                    "arc_first_name":   arc_fn,
                    "arc_family_name":  arc_ln,
                    "oax_id":           r["id"].replace("https://openalex.org/", ""),
                    "oax_display_name": r["display_name"],
                    "oax_orcid":        r.get("orcid"),
                    "first_score":      fs,
                    "n_au_passing":     len(passing),
                })
            print(f"  REVIEW     {arc_fn} {arc_ln}  →  {len(passing)} AU candidates")

        else:
            not_found.append((arc_fn, arc_ln, "no_au_pass"))
            print(f"  NOT FOUND  {arc_fn} {arc_ln}  (AU hits={len(au_candidates)})")

        time.sleep(SLEEP_BETWEEN)

    # ── Build outputs ─────────────────────────────────────────────────────────
    keep_cols = [
        "arc_first_name", "arc_family_name", "arc_orcid", "arc_sub_case",
        "arc_admin_orgs", "arc_oax_inst_ids", "n_arc_grants",
        "oax_id", "oax_display_name", "oax_orcid",
        "first_score", "institution_match", "tier",
    ]
    df_matches = pd.DataFrame(confident_rows, columns=keep_cols) if confident_rows \
                 else pd.DataFrame(columns=keep_cols)

    df_review = pd.DataFrame(review_rows) if review_rows \
                else pd.DataFrame(columns=["arc_first_name","arc_family_name",
                                            "oax_id","oax_display_name","oax_orcid",
                                            "first_score","n_au_passing"])

    # ── Update layer3_residual ────────────────────────────────────────────────
    resolved = set(
        zip(df_matches["arc_first_name"], df_matches["arc_family_name"])
    )
    df_res["_key"] = list(zip(df_res["first_name"], df_res["family_name"]))
    df_res["sub_case"] = df_res.apply(
        lambda r: None if r["_key"] in resolved else r["sub_case"], axis=1
    )
    df_layer3 = (
        df_res[df_res["sub_case"].notna()]
        .drop(columns=["_key"])
        .reset_index(drop=True)
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*54}")
    print(f"LAYER 3 API PASS SUMMARY")
    print(f"{'─'*54}")
    print(f"  Searched:                          {len(no_match):>6,}")
    print(f"  Tier 7 — confident (1 AU match):   {len(df_matches):>6,}")
    print(f"  Review  — multiple AU candidates:  {len(df_review.groupby(['arc_first_name','arc_family_name']) if len(df_review) else []):>6,}")
    print(f"  Not found:                         {len(not_found):>6,}")

    # ── Save ─────────────────────────────────────────────────────────────────
    matches_path = PROCESSED_DATA / "layer3_api_matches.parquet"
    review_path  = PROCESSED_DATA / "layer3_api_review.csv"
    layer3_path  = PROCESSED_DATA / "layer3_residual.parquet"

    df_matches.to_parquet(matches_path, index=False)
    df_review.to_csv(review_path, index=False)
    df_layer3.to_parquet(layer3_path, index=False)

    print(f"\nSaved:")
    print(f"  {matches_path}  ({len(df_matches):,} rows)")
    print(f"  {review_path}  ({len(df_review):,} rows)")
    print(f"  {layer3_path}  ({len(df_layer3):,} rows)")


if __name__ == "__main__":
    main()
