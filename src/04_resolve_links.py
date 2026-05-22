"""
src/04_resolve_links.py

Disambiguate ARC persons with multiple high-confidence OAX matches.

Input:  arc_oax_links.parquet  (all HC + sub-HC candidate pairs)
        arc_persons.parquet    (orcids, inst_arr per cluster)
        openalex_authors_prep.parquet (orcid, inst_ids per OAX author)
        OAX raw authors parquet (works_count)

Output: arc_oax_resolved.parquet
            arc_id, oax_id, match_probability, resolved_by, secondary_oax_ids
            One row per ARC person (only those with a resolved HC match).
            secondary_oax_ids: other HC candidates not chosen (e.g. split OAX records).

        arc_ambiguous_deferred.parquet
            arc_id, oax_id, match_probability, inst_overlap
            All HC candidate rows for ARC persons that remain ambiguous after all steps.

Resolution strategy (applied to HC matches only):
  1. ORCID exact match: if exactly 1 HC candidate shares the ARC person's ORCID → resolve.
  2. Institution overlap: restrict to candidates with maximum overlap (if any > 0).
  3. Unique highest match_probability among remaining candidates → resolve.
  4. Highest works_count: among remaining ties, pick the OAX record with most indexed works.
  5. Still tied → defer (genuine common-name collisions).

ARC persons with 0 HC matches are in arc_unlinked_deferred.parquet (step 03).
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, OAX_AUTHORS

LINK_THRESHOLD = 0.9


def main():
    arc_path  = PROCESSED_DATA / "arc_persons.parquet"
    oax_path  = PROCESSED_DATA / "openalex_authors_prep.parquet"
    link_path = PROCESSED_DATA / "arc_oax_links.parquet"
    out_resolved  = PROCESSED_DATA / "arc_oax_resolved.parquet"
    out_ambiguous = PROCESSED_DATA / "arc_ambiguous_deferred.parquet"

    con = duckdb.connect()

    print("[1/4] Loading data...")
    links = con.execute(f"SELECT * FROM read_parquet('{link_path}')").fetchdf()
    arc   = con.execute(f"SELECT cluster_id, orcids, inst_arr FROM read_parquet('{arc_path}')").fetchdf()
    oax   = con.execute(f"SELECT unique_id, orcid, inst_ids FROM read_parquet('{oax_path}')").fetchdf()

    arc["orcid"] = arc["orcids"].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    arc_orcid = dict(zip(arc["cluster_id"], arc["orcid"]))
    arc_inst  = dict(zip(arc["cluster_id"], arc["inst_arr"]))
    oax_orcid = dict(zip(oax["unique_id"],  oax["orcid"]))
    oax_inst  = dict(zip(oax["unique_id"],  oax["inst_ids"]))

    hc = links[links["high_confidence"]].copy()
    per_arc = hc.groupby("arc_id").size()

    # Fetch works_count for all HC candidate OAX IDs
    print("[2/4] Fetching OAX works_count...")
    hc_oax_ids = hc["oax_id"].unique().tolist()
    ids_sql = ", ".join(f"'{i}'" for i in hc_oax_ids)
    wc_df = con.execute(f"""
        SELECT id, works_count
        FROM read_parquet('{OAX_AUTHORS}/*.parquet')
        WHERE id IN ({ids_sql})
    """).fetchdf()
    oax_works = dict(zip(wc_df["id"], wc_df["works_count"]))
    print(f"  Retrieved works_count for {len(oax_works):,} / {len(hc_oax_ids):,} OAX IDs")

    # Persons with exactly 1 HC match — already resolved
    single_ids = per_arc[per_arc == 1].index
    resolved_single = hc[hc["arc_id"].isin(single_ids)][
        ["arc_id", "oax_id", "match_probability"]
    ].copy()
    resolved_single["resolved_by"] = "unique_hc"
    resolved_single["secondary_oax_ids"] = [[] for _ in range(len(resolved_single))]

    # Persons with 2+ HC matches — need disambiguation
    ambig_ids = per_arc[per_arc >= 2].index
    ambig = hc[hc["arc_id"].isin(ambig_ids)].copy()

    print(f"  ARC persons with 1 HC match:    {len(single_ids):,}")
    print(f"  ARC persons with 2+ HC matches: {len(ambig_ids):,}")

    print("[3/4] Disambiguating...")

    def _inst_overlap(arc_id, oax_id):
        a = arc_inst.get(arc_id)
        o = oax_inst.get(oax_id)
        if a is None or o is None or len(a) == 0 or len(o) == 0:
            return 0
        return len(set(a) & set(o))

    ambig["inst_overlap"] = ambig.apply(
        lambda r: _inst_overlap(r["arc_id"], r["oax_id"]), axis=1
    )
    ambig["orcid_match"] = ambig.apply(
        lambda r: (
            arc_orcid.get(r["arc_id"]) is not None
            and arc_orcid.get(r["arc_id"]) == oax_orcid.get(r["oax_id"])
        ),
        axis=1,
    )
    ambig["works_count"] = ambig["oax_id"].map(oax_works).fillna(0).astype(int)

    resolved_rows = []
    deferred_rows = []

    for arc_id, group in ambig.groupby("arc_id"):
        all_oax = set(group["oax_id"])

        # Step 1: unique ORCID match
        orcid_matches = group[group["orcid_match"]]
        if len(orcid_matches) == 1:
            r = orcid_matches.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"], "resolved_by": "orcid",
                "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
            })
            continue

        # Step 2: restrict to max institution overlap
        max_ov = group["inst_overlap"].max()
        if max_ov > 0:
            candidates = group[group["inst_overlap"] == max_ov]
            by = "inst_overlap"
        else:
            candidates = group
            by = "probability"

        # Step 3: unique highest probability
        max_prob = candidates["match_probability"].max()
        best = candidates[candidates["match_probability"] == max_prob]
        if len(best) == 1:
            r = best.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"], "resolved_by": by,
                "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
            })
            continue

        # Step 4: one OAX record holds >90% of combined works → split record, take dominant
        sum_wc = best["works_count"].sum()
        max_wc = best["works_count"].max()
        if sum_wc > 0 and max_wc / sum_wc > 0.9:
            top = best[best["works_count"] == max_wc]
            if len(top) == 1:
                r = top.iloc[0]
                resolved_rows.append({
                    "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                    "match_probability": r["match_probability"], "resolved_by": "works_count",
                    "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
                })
                continue

        # Step 5: defer
        for _, r in group.iterrows():
            deferred_rows.append({
                "arc_id":            r["arc_id"],
                "oax_id":            r["oax_id"],
                "match_probability": r["match_probability"],
                "inst_overlap":      r["inst_overlap"],
            })

    resolved_ambig = pd.DataFrame(resolved_rows)
    deferred       = pd.DataFrame(deferred_rows) if deferred_rows else pd.DataFrame(
        columns=["arc_id", "oax_id", "match_probability", "inst_overlap"]
    )

    print("[4/4] Saving outputs...")
    resolved = pd.concat([resolved_single, resolved_ambig], ignore_index=True)
    resolved.to_parquet(out_resolved, index=False)
    deferred.to_parquet(out_ambiguous, index=False)

    all_arc = con.execute(f"SELECT count(*) FROM read_parquet('{arc_path}')").fetchone()[0]

    by_counts = resolved_ambig["resolved_by"].value_counts() if len(resolved_ambig) else pd.Series(dtype=int)
    print(f"\n  Total ARC persons:              {all_arc:,}")
    print(f"  Resolved (1 HC match):          {len(resolved_single):,}")
    print(f"  Resolved (disambiguated):        {len(resolved_ambig):,}")
    for label in ["orcid", "inst_overlap", "probability", "works_count"]:
        n = by_counts.get(label, 0)
        if n:
            print(f"    of which by {label+':':16s} {n:,}")
    print(f"  Resolved total:                  {len(resolved):,}  ({100*len(resolved)/all_arc:.1f}%)")
    print(f"  Ambiguous deferred:              {deferred['arc_id'].nunique():,}")
    print(f"  Unlinked (no HC match):          {all_arc - len(resolved) - deferred['arc_id'].nunique():,}")
    print(f"\n  → {out_resolved}")
    print(f"  → {out_ambiguous}")


if __name__ == "__main__":
    main()
