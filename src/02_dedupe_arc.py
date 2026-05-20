"""
src/02_dedupe_arc.py

Deduplicate ARC grant-level rows into person-level clusters.
Uses Splink dedupe_only on arc_investigators_prep.parquet.

Clustering rules:
  1. Block on first_initial + family_name_main (longest surname token).
  2. Post-process: any cluster with 2+ distinct non-null ORCIDs is split
     into separate ORCID-keyed sub-clusters (hard deterministic rule).
  3. Diagnostic: report institution composition of multi-row clusters.

Output: arc_persons.parquet
    One row per person cluster, with aggregated ORCIDs, institutions and FoR
    codes, ready for ARC→OAX linkage in 03_link_arc_oax.py.
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA

CLUSTER_THRESHOLD = 0.9


def _load_all(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    print(f"  {len(df)} grant rows, {df['unique_id'].nunique()} unique IDs")
    return df


def _first_initial(toks) -> str | None:
    """First character of the longest non-initial token in first_names."""
    if toks is None or len(toks) == 0:
        return None
    full = [t for t in toks if len(t) > 1]
    return full[0][0] if full else toks[0][0]


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["inst_arr"] = df["institution_oax_id"].apply(
        lambda x: [x] if (pd.notna(x) and x) else []
    )
    df["family_name_main"] = df["family_names"].apply(
        lambda x: max(x, key=len) if (x is not None and len(x) > 0) else None
    )
    # Rule 1: first initial as scalar blocking key
    df["first_initial"] = df["first_names"].apply(_first_initial)
    df["for_name_tokens"] = df["for_name_tokens"].apply(
        lambda x: list(x) if x is not None else []
    )
    return df[[
        "unique_id", "full_name",
        "first_names", "family_names", "family_name_main", "first_initial",
        "orcid", "inst_arr", "for_name_tokens",
    ]]


def _split_orcid_conflicts(
    df_cluster_ids: pd.DataFrame, df_original: pd.DataFrame
) -> pd.DataFrame:
    """
    Rule 2: any cluster containing 2+ distinct non-null ORCIDs is split.
    - Rows with an ORCID → sub-cluster keyed on that ORCID.
    - Rows without an ORCID in a conflicted cluster → singleton.
    """
    merged = df_original[["unique_id", "orcid"]].merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])

    out = []
    for cid, grp in merged.groupby("cluster_id"):
        distinct_orcids = grp["orcid"].dropna().unique()
        if len(distinct_orcids) <= 1:
            out.append(grp[["unique_id", "cluster_id"]])
        else:
            sub = grp.copy()
            sub["cluster_id"] = sub.apply(
                lambda r: f"orcid_{r['orcid']}" if pd.notna(r["orcid"]) else r["unique_id"],
                axis=1,
            )
            out.append(sub[["unique_id", "cluster_id"]])

    return pd.concat(out, ignore_index=True)


def _aggregate_clusters(df_original: pd.DataFrame, df_cluster_ids: pd.DataFrame) -> pd.DataFrame:
    merged = df_original.merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])

    def union_lists(series):
        out = set()
        for lst in series:
            if lst is not None:
                out.update(lst)
        return sorted(out)

    def distinct_nonempty(series):
        return sorted({v for v in series if pd.notna(v) and v})

    return (
        merged.groupby("cluster_id")
        .agg(
            full_names=("full_name", lambda s: sorted(set(s))),
            first_names=("first_names", union_lists),
            family_names=("family_names", union_lists),
            orcids=("orcid", distinct_nonempty),
            inst_arr=("inst_arr", union_lists),
            for_names=("for_names", union_lists),
            grant_ids=("unique_id", list),
            n_grants=("unique_id", "count"),
        )
        .reset_index()
    )


def _institution_diagnostic(persons: pd.DataFrame) -> None:
    """
    Rule 3: for every multi-row cluster, report institution composition.
    Single institution → same person on multiple grants (expected).
    Multiple institutions → person moved, or possible mis-merge.
    """
    multi = persons[persons["n_grants"] > 1].sort_values("n_grants", ascending=False)
    print(f"\n  Multi-row clusters: {len(multi)}  "
          f"(singleton clusters: {(persons['n_grants'] == 1).sum()})")

    one_inst  = multi[multi["inst_arr"].apply(len) == 1]
    multi_inst = multi[multi["inst_arr"].apply(len) > 1]
    no_inst   = multi[multi["inst_arr"].apply(len) == 0]

    print(f"    1 institution:  {len(one_inst):>4}  (same person, multiple grants — expected)")
    print(f"    2+ institutions:{len(multi_inst):>4}  (moved institution, or check for mis-merge)")
    print(f"    0 institutions: {len(no_inst):>4}")

    if len(multi_inst) > 0:
        print("\n  Multi-institution clusters (top 20 by grant count):")
        for _, row in multi_inst.head(20).iterrows():
            names  = ", ".join(row["full_names"][:3])
            orcids = ", ".join(row["orcids"]) if row["orcids"] else "—"
            insts  = ", ".join(str(i).split("/")[-1] for i in row["inst_arr"][:4])
            for_n  = ", ".join(row["for_names"][:3]) if row["for_names"] else "—"
            for_consistent = len(set(row["for_names"])) == 1
            flag = "" if for_consistent else "  *** FOR mismatch"
            print(f"    n={row['n_grants']:>3}  orcids=[{orcids}]  names=[{names}]{flag}")
            print(f"           insts=[{insts}]  for=[{for_n}]")


def main():
    arc_path = PROCESSED_DATA / "arc_investigators_prep.parquet"
    out_path  = PROCESSED_DATA / "arc_persons.parquet"

    con = duckdb.connect()

    print("[1/5] Loading full ARC grant rows...")
    df_raw = _load_all(con, arc_path)

    print("[2/5] Preparing Splink input columns...")
    df = _prep(df_raw)
    print(f"  first_initial coverage: "
          f"{df['first_initial'].notna().sum()} / {len(df)} rows")

    settings = SettingsCreator(
        unique_id_column_name="unique_id",
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("family_name_main", "first_initial"),   # Rule 1
            "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        ],
        comparisons=[
            cl.ArrayIntersectAtSizes("first_names", [2, 1]).configure(
                m_probabilities=[0.80, 0.18, 0.02]
            ),
            cl.ArrayIntersectAtSizes("family_names", [2, 1]).configure(
                m_probabilities=[0.75, 0.24, 0.01]
            ),
            cl.ExactMatch("orcid").configure(
                m_probabilities=[0.85, 0.15]
            ),
            cl.ArrayIntersectAtSizes("inst_arr", [1]),
            # 3-level FOR comparison: >=2 tokens (strong), >=1 token (weak/concordance
            # bridge), else (different discipline → anti-match).
            # Low m_prob on else: same person rarely spans completely unrelated fields.
            # u_prob on else is high (random pairs usually differ in field) → negative
            # log-odds → penalty for discipline mismatch.
            cl.ArrayIntersectAtSizes("for_name_tokens", [2, 1]).configure(
                m_probabilities=[0.35, 0.45, 0.20]
            ),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker(df, settings, db_api=db_api)

    print("[3/5] Training EM model...")
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("family_name_main")],
        recall=0.8,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        fix_u_probabilities=True,
    )

    print("[4/5] Predicting matches and clustering...")
    df_pred = linker.inference.predict(threshold_match_probability=0.5)
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_pred, threshold_match_probability=CLUSTER_THRESHOLD
    )
    df_cluster_ids = df_clusters.as_pandas_dataframe()

    print("  Applying ORCID conflict split (Rule 2)...")
    df_cluster_ids = _split_orcid_conflicts(df_cluster_ids, df_raw)

    n_clusters = df_cluster_ids["cluster_id"].nunique()
    n_records  = len(df_cluster_ids)
    print(f"  {n_clusters} clusters from {n_records} records "
          f"({n_records/n_clusters:.2f} grants/person avg)")

    print("[5/5] Aggregating to person-level records...")
    df_raw["inst_arr"] = df_raw["institution_oax_id"].apply(
        lambda x: [x] if (pd.notna(x) and x) else []
    )
    df_raw["for_names"] = df_raw["for_names"].apply(
        lambda x: list(x) if x is not None else []
    )
    persons = _aggregate_clusters(df_raw, df_cluster_ids)

    persons.to_parquet(out_path, index=False)
    print(f"  Saved {len(persons)} person records → {out_path}")

    _institution_diagnostic(persons)


if __name__ == "__main__":
    main()
