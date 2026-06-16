"""
src/05_orcid_assist.py

Batch-fetches ORCID public API records for OAX candidates in the deferred set.
Scores each candidate against the ARC person's FOR field tokens via ORCID keywords.
Outputs high-confidence suggestions to a staging CSV for human review before
promoting to config/manual_resolutions.csv.

Caches all HTTP responses under PROCESSED_DATA/orcid_cache/ so re-runs are cheap.

Output: PROCESSED_DATA/orcid_suggestions.csv
    arc_id, arc_name, oax_id, action, score, gap, n_unscored,
    orcid_keywords, arc_for, note
"""

import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.utils.names import for_name_tokens
from src.utils.orcid_cache import fetch_orcid, orcid_keywords

CACHE_DIR        = PROCESSED_DATA / "orcid_cache"
MIN_WINNER_SCORE = 0.10
MIN_GAP          = 0.08


def _lst(val) -> list:
    if val is None:
        return []
    try:
        return list(val)
    except TypeError:
        return []


def field_score(kw_list: list[str], arc_for_toks: set[str]) -> float:
    """Jaccard overlap between ORCID keyword tokens and ARC FOR field tokens."""
    if not kw_list or not arc_for_toks:
        return 0.0
    kw_toks = {tok for kw in kw_list for tok in kw.split()}
    inter = kw_toks & arc_for_toks
    union = kw_toks | arc_for_toks
    return len(inter) / len(union) if union else 0.0


def main(test: bool = False):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    deferred = pd.read_parquet(PROCESSED_DATA / "arc_ambiguous_deferred.parquet")
    arc      = pd.read_parquet(PROCESSED_DATA / "arc_persons.parquet")
    oax_prep = pd.read_parquet(PROCESSED_DATA / "openalex_authors_prep.parquet")

    if test:
        sample_ids = deferred["arc_id"].unique()[:16]
        deferred = deferred[deferred["arc_id"].isin(sample_ids)]
        print(f"[TEST] Limited to {deferred['arc_id'].nunique()} arc_ids")

    arc_by_id = {r["cluster_id"]: r for _, r in arc.iterrows()}
    oax_by_id = {r["unique_id"]:  r for _, r in oax_prep.iterrows()}

    # -- 1. Collect ORCIDs to fetch --
    orcids_needed = set()
    for arc_id, grp in deferred.groupby("arc_id"):
        for oax_id in grp["oax_id"]:
            orcid = oax_by_id[oax_id]["orcid"]
            if pd.notna(orcid) and orcid:
                orcids_needed.add(orcid)

    print(f"[1/3] Fetching {len(orcids_needed)} ORCID records...")
    orcid_data = {}
    for i, orcid in enumerate(sorted(orcids_needed)):
        orcid_data[orcid] = fetch_orcid(orcid, CACHE_DIR)
        if (i + 1) % 50 == 0:
            print(f"  {i+1} / {len(orcids_needed)}")
    print(f"  Done.")

    # -- 2. Score candidates --
    print("[2/3] Scoring...")
    suggestions = []
    verbose = test

    for arc_id, grp in deferred.groupby("arc_id"):
        arc_row = arc_by_id[arc_id]

        arc_for_toks = set()
        for fn in _lst(arc_row["for_names"]):
            arc_for_toks.update(for_name_tokens(fn))

        if verbose:
            arc_name = (_lst(arc_row["full_names"]) or ["?"])[0]
            print(f"\n{arc_name}  FOR={_lst(arc_row['for_names'])}")

        scores = []
        for _, row in grp.iterrows():
            oax_id    = row["oax_id"]
            oax_orcid = oax_by_id[oax_id]["orcid"]
            if pd.isna(oax_orcid) or not oax_orcid:
                if verbose:
                    print(f"  {oax_id}  orcid=none  score=-")
                scores.append((oax_id, None, None))
                continue
            rec  = orcid_data.get(oax_orcid, {})
            kws  = orcid_keywords(rec)
            fs   = field_score(kws, arc_for_toks)
            if verbose:
                print(f"  {oax_id}  orcid={oax_orcid}  score={fs:.3f}  kw={kws[:4]}")
            scores.append((oax_id, fs, kws))

        scored     = [(oid, s, kws) for oid, s, kws in scores if s is not None]
        n_unscored = sum(1 for _, s, _ in scores if s is None)

        if not scored:
            continue

        scored.sort(key=lambda x: x[1], reverse=True)
        best_id, best_score, best_kws = scored[0]
        second_score = scored[1][1] if len(scored) > 1 else 0.0
        gap = best_score - second_score

        if best_score >= MIN_WINNER_SCORE and gap >= MIN_GAP:
            suggestions.append({
                "arc_id":         arc_id,
                "arc_name":       (_lst(arc_row["full_names"]) or ["?"])[0],
                "oax_id":         best_id,
                "action":         "resolve",
                "score":          round(best_score, 3),
                "gap":            round(gap, 3),
                "n_unscored":     n_unscored,
                "orcid_keywords": "; ".join(best_kws[:6]),
                "arc_for":        "; ".join(_lst(arc_row["for_names"])),
                "note":           (
                    f"ORCID keyword match. score={best_score:.3f} gap={gap:.3f}"
                    + (f" ({n_unscored} unscored)" if n_unscored else "")
                ),
            })

    out_path = PROCESSED_DATA / "orcid_suggestions.csv"
    sug_df = pd.DataFrame(suggestions) if suggestions else pd.DataFrame(
        columns=["arc_id","arc_name","oax_id","action","score","gap",
                 "n_unscored","orcid_keywords","arc_for","note"])
    sug_df.to_csv(out_path, index=False)

    print(f"[3/3] {len(sug_df)} suggestions → {out_path}")
    if len(sug_df):
        pd.set_option("display.max_colwidth", 50)
        pd.set_option("display.width", 160)
        print(sug_df[["arc_name","score","gap","n_unscored","orcid_keywords","arc_for"]].to_string(index=False))


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--test", action="store_true", help="Run on first 16 arc_ids only")
    main(test=p.parse_args().test)
