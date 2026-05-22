"""
src/03_link_arc_oax.py

Link ARC person clusters to OpenAlex (OAX) Australian authors.
Splink link_only: arc_persons.parquet → openalex_authors_prep.parquet.

Output: arc_oax_links.parquet
    arc_id, oax_id, match_probability, high_confidence
    All candidate pairs ≥ PREDICT_THRESHOLD; high_confidence flags pairs ≥ LINK_THRESHOLD.
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA

PREDICT_THRESHOLD = 0.5
LINK_THRESHOLD    = 0.9


def _prep_arc(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    print(f"  ARC persons: {len(df)}")

    def _max_by_len(lst):
        if lst is None or len(lst) == 0:
            return None
        return max(lst, key=len)

    def _canonical(lst):
        if lst is None or len(lst) == 0:
            return None
        full = [t for t in lst if len(t) > 1]
        return max(full, key=len) if full else lst[0]

    df["family_name_main"]     = df["family_names"].apply(_max_by_len)
    df["first_name_canonical"] = df["first_names"].apply(_canonical)
    df["first_initial"]        = df["first_name_canonical"].apply(
        lambda x: x[0] if isinstance(x, str) else None
    )
    df["full_name_key"] = df.apply(
        lambda r: f"{r['first_name_canonical']}_{r['family_name_main']}"
        if (r["first_name_canonical"] is not None and r["family_name_main"] is not None)
        else None,
        axis=1,
    )
    df["orcid"]    = df["orcids"].apply(lambda lst: lst[0] if lst is not None and len(lst) > 0 else None)
    df["inst_arr"] = df["inst_arr"].apply(lambda x: list(x) if x is not None else [])

    return df[[
        "cluster_id", "family_name_main", "first_initial",
        "first_name_canonical", "full_name_key", "orcid", "inst_arr",
    ]].rename(columns={"cluster_id": "unique_id"})


def _prep_oax(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    df = con.execute(f"""
        SELECT
            unique_id,
            orcid,
            family_names,
            first_initials,
            first_name_full,
            inst_ids
        FROM read_parquet('{path}')
    """).fetchdf()
    print(f"  OAX authors:  {len(df)}")

    def _max_by_len(lst):
        if lst is None or len(lst) == 0:
            return None
        return max(lst, key=len)

    def _canonical(full_toks, initials):
        if full_toks is not None and len(full_toks) > 0:
            return max(full_toks, key=len)
        if initials is not None and len(initials) > 0:
            return initials[0]
        return None

    def _initial(initials):
        if initials is None or len(initials) == 0:
            return None
        return initials[0]

    df["family_name_main"]    = df["family_names"].apply(_max_by_len)
    df["first_name_canonical"] = df.apply(
        lambda r: _canonical(r["first_name_full"], r["first_initials"]), axis=1
    )
    # Derive first_initial from first_name_canonical, not first_initials[0].
    # first_initials ordering is arbitrary (set-derived); canonical is always
    # the longest full given name, matching how ARC computes the same field.
    df["first_initial"] = df["first_name_canonical"].apply(
        lambda x: x[0] if isinstance(x, str) else None
    )
    df["full_name_key"] = df.apply(
        lambda r: f"{r['first_name_canonical']}_{r['family_name_main']}"
        if (r["first_name_canonical"] is not None and r["family_name_main"] is not None)
        else None,
        axis=1,
    )
    df["inst_arr"] = df["inst_ids"].apply(lambda x: list(x) if x is not None else [])

    return df[[
        "unique_id", "family_name_main", "first_initial",
        "first_name_canonical", "full_name_key", "orcid", "inst_arr",
    ]]


def main():
    arc_path = PROCESSED_DATA / "arc_persons.parquet"
    oax_path = PROCESSED_DATA / "openalex_authors_prep.parquet"
    out_path = PROCESSED_DATA / "arc_oax_links.parquet"

    con = duckdb.connect()

    print("[1/4] Preparing input tables...")
    df_arc = _prep_arc(con, arc_path)
    df_oax = _prep_oax(con, oax_path)

    settings = SettingsCreator(
        unique_id_column_name="unique_id",
        link_type="link_only",
        blocking_rules_to_generate_predictions=[
            block_on("family_name_main", "first_initial"),
            "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        ],
        comparisons=[
            cl.CustomComparison(
                output_column_name="first_name_canonical",
                comparison_description="First name: exact / initial-match / full-mismatch",
                comparison_levels=[
                    {"sql_condition": "first_name_canonical_l IS NULL OR first_name_canonical_r IS NULL",
                     "label_for_charts": "null", "is_null_level": True},
                    {"sql_condition": "first_name_canonical_l = first_name_canonical_r",
                     "label_for_charts": "Exact match",
                     "tf_adjustment_column": "first_name_canonical",
                     "tf_adjustment_weight": 1.0},
                    {"sql_condition": (
                        "(length(first_name_canonical_l) = 1"
                        " AND length(first_name_canonical_r) > 1"
                        " AND first_name_canonical_l = substr(first_name_canonical_r, 1, 1))"
                        " OR"
                        " (length(first_name_canonical_r) = 1"
                        " AND length(first_name_canonical_l) > 1"
                        " AND first_name_canonical_r = substr(first_name_canonical_l, 1, 1))"),
                     "label_for_charts": "Initial matches full name"},
                    {"sql_condition": (
                        "length(first_name_canonical_l) > 1"
                        " AND length(first_name_canonical_r) > 1"
                        " AND first_name_canonical_l != first_name_canonical_r"),
                     "label_for_charts": "Full name mismatch",
                     "m_probability": 0.02},
                    {"sql_condition": "ELSE", "label_for_charts": "All other"},
                ],
            ),
            cl.CustomComparison(
                output_column_name="family_name_main",
                comparison_levels=[
                    cll.NullLevel("family_name_main"),
                    cll.ExactMatchLevel("family_name_main").configure(
                        tf_adjustment_column="family_name_main",
                        tf_adjustment_weight=1.0,
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.CustomComparison(
                output_column_name="full_name_key",
                comparison_levels=[
                    {"sql_condition": "full_name_key_l IS NULL OR full_name_key_r IS NULL",
                     "label_for_charts": "null", "is_null_level": True},
                    {"sql_condition": "full_name_key_l = full_name_key_r",
                     "label_for_charts": "Exact match",
                     "tf_adjustment_column": "full_name_key",
                     "tf_adjustment_weight": 1.0,
                     "u_probability": 4.25e-06},
                    {"sql_condition": "ELSE", "label_for_charts": "All other"},
                ],
            ),
            cl.ExactMatch("orcid").configure(
                m_probabilities=[0.85, 0.15],
                u_probabilities=[0.0001, 0.9999],
            ),
            cl.ArrayIntersectAtSizes("inst_arr", [1]),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker([df_arc, df_oax], settings, db_api=db_api)

    print("  Registering name frequency tables...")
    for fname, col in [
        ("oax_tf_family_name.parquet", "family_name_main"),
        ("oax_tf_first_name.parquet",  "first_name_canonical"),
        ("oax_tf_full_name.parquet",   "full_name_key"),
    ]:
        tf = pd.read_parquet(PROCESSED_DATA / fname)
        linker.table_management.register_term_frequency_lookup(tf, col)

    print("[2/4] Training...")
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("family_name_main")], recall=0.8,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        fix_u_probabilities=True,
    )

    print("[3/4] Predicting...")
    df_pred = linker.inference.predict(threshold_match_probability=PREDICT_THRESHOLD)
    links = (
        df_pred.as_pandas_dataframe()
        [["unique_id_l", "unique_id_r", "match_probability"]]
        .rename(columns={"unique_id_l": "arc_id", "unique_id_r": "oax_id"})
        .sort_values(["arc_id", "match_probability"], ascending=[True, False])
    )
    links["high_confidence"] = links["match_probability"] >= LINK_THRESHOLD

    # Force-add ORCID-exact-match pairs missed by predict (corrupted OAX name fields
    # can push score below threshold despite identical ORCID).
    orcid_pairs = (
        df_arc[df_arc["orcid"].notna()][["unique_id", "orcid"]]
        .merge(
            df_oax[df_oax["orcid"].notna()][["unique_id", "orcid"]],
            on="orcid",
            suffixes=("_arc", "_oax"),
        )
        .rename(columns={"unique_id_arc": "arc_id", "unique_id_oax": "oax_id"})
        [["arc_id", "oax_id"]]
    )
    existing = set(zip(links["arc_id"], links["oax_id"]))
    forced = orcid_pairs[
        ~orcid_pairs.apply(lambda r: (r["arc_id"], r["oax_id"]) in existing, axis=1)
    ].copy()
    if len(forced):
        forced["match_probability"] = 1.0
        forced["high_confidence"] = True
        print(f"  Forced {len(forced)} ORCID-exact pairs missed by predict.")
        links = pd.concat([links, forced], ignore_index=True)

    links.to_parquet(out_path, index=False)

    print("[4/4] Summary...")
    hc = links[links["high_confidence"]]
    per_arc = hc.groupby("arc_id").size()
    n_arc = len(df_arc)
    print(f"  ARC persons with ≥1 high-confidence OAX link: {hc['arc_id'].nunique()} / {n_arc} "
          f"({100*hc['arc_id'].nunique()/n_arc:.1f}%)")
    print(f"  Total candidate pairs (≥{PREDICT_THRESHOLD}): {len(links)}")
    print(f"  High-confidence pairs (≥{LINK_THRESHOLD}): {len(hc)}")
    print(f"  ARC persons with exactly 1 HC match: {(per_arc == 1).sum()}")
    print(f"  ARC persons with 2+ HC matches:      {(per_arc > 1).sum()}")
    print(f"  Saved → {out_path}")


if __name__ == "__main__":
    main()
