"""
07_filter_layer3_for.py

Layer 3 pre-pass: use ARC FoR codes to narrow OAX candidates.

For each ambiguous Layer 3 person:
  1. Collect all FoR codes across their grants → map to OAX topic fields
  2. Among their first_score > 0 candidates, eliminate any whose OAX topic
     fields have zero overlap with the person's allowed fields.
     Candidates with no OAX topics are retained (can't disprove).
  3. Apply the same assignment tiers as Layer 2 to the filtered shortlist:
       Tier 4  unique full-name match  (first_score == 1.0, n == 1)
       Tier 5  unique initial match    (first_score == 0.8, n_full == 0, n == 1)
       Tier 6  inst-confirmed          (exactly 1 inst match at best score level)
  4. Unresolved persons remain in an updated layer3_residual.parquet.

INPUT:
    PROCESSED_DATA/layer3_residual.parquet
    PROCESSED_DATA/layer2_name_candidates.parquet
    PROCESSED_DATA/for_codes_wrangled.parquet
    FOR_OAX_CSV
    OAX_AUTHORS/*.parquet   (for topic fields of candidates)

OUTPUT:
    PROCESSED_DATA/layer3_for_matches.parquet
        Same schema as layer2_matches.parquet plus tier (4/5/6).

    PROCESSED_DATA/layer3_residual.parquet   (overwritten — ambiguous count reduced)
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import FOR_OAX_CSV, OAX_AUTHORS, PROCESSED_DATA
from src.utils.io import setup_stdout_utf8

OAX_GLOB = str(OAX_AUTHORS / "*.parquet")


def main():
    setup_stdout_utf8()
    con = duckdb.connect()

    # ── Load inputs ───────────────────────────────────────────────────────────
    df_res   = pd.read_parquet(PROCESSED_DATA / "layer3_residual.parquet")
    df_cands = pd.read_parquet(PROCESSED_DATA / "layer2_name_candidates.parquet")
    df_for   = pd.read_parquet(PROCESSED_DATA / "for_codes_wrangled.parquet",
                               columns=["grant_code", "for_code"])
    df_conc  = pd.read_csv(FOR_OAX_CSV, sep=";", dtype=str)

    df_amb = df_res[df_res["sub_case"] == "ambiguous"].copy()
    print(f"Ambiguous Layer 3 persons:  {df_amb.groupby(['first_name','family_name'], dropna=False).ngroups:>7,}")

    # ── Build person → allowed OAX fields ────────────────────────────────────
    # For each ARC person, union of OAX fields across all their FoR codes.
    for_to_oax = (
        df_conc.groupby("for_2digit")["oax_field"]
        .apply(set)
        .to_dict()
    )

    df_for["for_2digit"] = df_for["for_code"].str[:2]
    grant_for = df_for.groupby("grant_code")["for_2digit"].apply(set).to_dict()

    # layer3_residual has one row per (person × grant); build grant sets per person
    person_grants = (
        df_amb.groupby(["first_name", "family_name"], dropna=False)["grant_code"]
        .apply(set)
        .reset_index()
    )

    def allowed_fields(grant_set):
        fields = set()
        for gc in grant_set:
            for fd in grant_for.get(gc, set()):
                fields |= for_to_oax.get(fd, set())
        return fields

    person_grants["allowed_oax_fields"] = person_grants["grant_code"].apply(allowed_fields)
    allowed_map = dict(
        zip(
            zip(person_grants["first_name"], person_grants["family_name"]),
            person_grants["allowed_oax_fields"],
        )
    )

    print(f"  Persons with ≥1 allowed OAX field: "
          f"{sum(1 for v in allowed_map.values() if v):>5,}")

    # ── Filter candidates to ambiguous persons, first_score > 0 ──────────────
    amb_names = person_grants[["first_name", "family_name"]].rename(
        columns={"first_name": "arc_first_name", "family_name": "arc_family_name"}
    )
    df_sc = (
        df_cands
        .merge(amb_names, on=["arc_first_name", "arc_family_name"], how="inner")
        .query("first_score > 0")
        .copy()
    )
    print(f"  Scored candidate pairs:            {len(df_sc):>7,}")
    unique_oax_ids = df_sc["oax_id"].unique()
    print(f"  Unique OAX candidate ids:          {len(unique_oax_ids):>7,}")

    # ── Load OAX topic fields for candidate ids ───────────────────────────────
    print("Loading OAX topic fields for candidates...")
    con.register("cand_ids", pd.DataFrame({"id": unique_oax_ids}))
    df_topics = con.execute(f"""
        SELECT
            a.id,
            list_distinct(list_transform(
                list_filter(a.topics, t -> t.field.display_name IS NOT NULL),
                t -> t.field.display_name
            )) AS oax_fields
        FROM read_parquet('{OAX_GLOB}') a
        SEMI JOIN cand_ids c ON a.id = c.id
    """).df()
    print(f"  Topic records loaded:              {len(df_topics):>7,}")

    # Convert to dict: oax_id → set of field names (empty set if no topics)
    topic_map = {
        row.id: set(row.oax_fields) if row.oax_fields is not None and len(row.oax_fields) > 0 else set()
        for row in df_topics.itertuples(index=False)
    }

    # ── Apply FoR filter: drop candidates with zero field overlap ─────────────
    # Retain candidates with no topic data (can't disprove).
    def keep_candidate(row) -> bool:
        cand_fields = topic_map.get(row["oax_id"], set())
        if not cand_fields:
            return True    # no topic data — retain
        allowed = allowed_map.get((row["arc_first_name"], row["arc_family_name"]), set())
        if not allowed:
            return True    # no FoR data — retain
        return bool(cand_fields & allowed)

    print("Applying FoR field filter...")
    df_sc["_keep"] = df_sc.apply(keep_candidate, axis=1)
    df_kept = df_sc[df_sc["_keep"]].drop(columns=["_keep"]).copy()

    n_eliminated = len(df_sc) - len(df_kept)
    print(f"  Pairs eliminated by FoR filter:    {n_eliminated:>7,}  "
          f"({100*n_eliminated/len(df_sc):.1f}%)")
    print(f"  Pairs retained:                    {len(df_kept):>7,}")

    # ── Recount per-person signal counts on filtered set ─────────────────────
    per_person = (
        df_kept.groupby(["arc_first_name", "arc_family_name"], dropna=False)
        .apply(lambda g: pd.Series({
            "n_full":         (g["first_score"] == 1.0).sum(),
            "n_initial":      (g["first_score"] == 0.8).sum(),
            "n_inst_full":    ((g["first_score"] == 1.0) & g["institution_match"]).sum(),
            "n_inst_initial": ((g["first_score"] == 0.8) & g["institution_match"]).sum(),
        }), include_groups=False)
        .reset_index()
    )
    df_kept = df_kept.merge(per_person,
                            on=["arc_first_name", "arc_family_name"], how="left")

    resolved_names: set = set()

    # ── Tier 4: unique full-name match ────────────────────────────────────────
    tier4 = df_kept[
        (df_kept["first_score"] == 1.0) & (df_kept["n_full"] == 1)
    ].copy()
    tier4["tier"] = 4
    resolved_names.update(zip(tier4["arc_first_name"], tier4["arc_family_name"]))

    # ── Tier 5: unique initial match ──────────────────────────────────────────
    tier5 = df_kept[
        ~df_kept.apply(lambda r: (r["arc_first_name"], r["arc_family_name"]) in resolved_names, axis=1) &
        (df_kept["first_score"] == 0.8) &
        (df_kept["n_full"] == 0) &
        (df_kept["n_initial"] == 1)
    ].copy()
    tier5["tier"] = 5
    resolved_names.update(zip(tier5["arc_first_name"], tier5["arc_family_name"]))

    # ── Tier 6: institution resolves filtered shortlist ───────────────────────
    t6_pool = df_kept[
        ~df_kept.apply(lambda r: (r["arc_first_name"], r["arc_family_name"]) in resolved_names, axis=1) &
        (df_kept["first_score"] > 0) &
        (df_kept["institution_match"] == True)
    ].copy()

    tier6_parts = []
    for _, g in t6_pool.groupby(["arc_first_name", "arc_family_name"], dropna=False):
        best_fs = g["first_score"].max()
        best_inst = g[(g["first_score"] == best_fs) & g["institution_match"]]
        if len(best_inst) == 1:
            tier6_parts.append(best_inst.iloc[[0]])

    if tier6_parts:
        tier6 = pd.concat(tier6_parts, ignore_index=True)
        tier6["tier"] = 6
        resolved_names.update(zip(tier6["arc_first_name"], tier6["arc_family_name"]))
    else:
        tier6 = pd.DataFrame(columns=df_kept.columns.tolist() + ["tier"])

    # ── Combine resolved ──────────────────────────────────────────────────────
    keep_cols = [
        "arc_first_name", "arc_family_name", "arc_orcid", "arc_sub_case",
        "arc_admin_orgs", "arc_oax_inst_ids", "n_arc_grants",
        "oax_id", "oax_display_name", "oax_orcid",
        "first_score", "institution_match", "tier",
    ]
    df_matches = pd.concat(
        [tier4[keep_cols], tier5[keep_cols], tier6[keep_cols]],
        ignore_index=True,
    )

    # ── Update layer3_residual ────────────────────────────────────────────────
    df_res["_key"] = list(zip(df_res["first_name"], df_res["family_name"]))
    df_res["sub_case"] = df_res.apply(
        lambda r: None if r["_key"] in resolved_names else r["sub_case"], axis=1
    )
    df_layer3 = (
        df_res[df_res["sub_case"].notna()]
        .drop(columns=["_key"])
        .reset_index(drop=True)
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    l3_persons = df_layer3.groupby(["first_name","family_name"], dropna=False).ngroups
    l3_amb = df_layer3[df_layer3["sub_case"]=="ambiguous"].groupby(["first_name","family_name"], dropna=False).ngroups
    l3_none = df_layer3[df_layer3["sub_case"]=="no_match"].groupby(["first_name","family_name"], dropna=False).ngroups

    print(f"\n{'─'*54}")
    print(f"LAYER 3 FoR PRE-PASS SUMMARY")
    print(f"{'─'*54}")
    print(f"  Tier 4 — unique full after FoR:    {len(tier4):>6,}")
    print(f"  Tier 5 — unique initial after FoR: {len(tier5):>6,}")
    print(f"  Tier 6 — inst-confirmed after FoR: {len(tier6):>6,}")
    print(f"  Total resolved this pass:          {len(df_matches):>6,}")
    print(f"\n  Remaining Layer 3 persons:         {l3_persons:>6,}")
    print(f"    ambiguous:                       {l3_amb:>6,}")
    print(f"    no_match:                        {l3_none:>6,}")

    # ── Save ──────────────────────────────────────────────────────────────────
    matches_path = PROCESSED_DATA / "layer3_for_matches.parquet"
    layer3_path  = PROCESSED_DATA / "layer3_residual.parquet"

    df_matches.to_parquet(matches_path, index=False)
    df_layer3.to_parquet(layer3_path,   index=False)

    print(f"\nSaved:")
    print(f"  {matches_path}  ({len(df_matches):,} rows)")
    print(f"  {layer3_path}  ({len(df_layer3):,} rows)")


if __name__ == "__main__":
    main()
