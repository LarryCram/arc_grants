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
from src.utils.names import max_by_len, parse_given

PREDICT_THRESHOLD = 0.5
LINK_THRESHOLD    = 0.9


def _make_full_name_key(df):
    return (
        (df["first_compound"] + "_" + df["family_name_main"])
        .where(df["first_compound"].notna() & df["family_name_main"].notna())
    )


def _prep_arc(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    print(f"  ARC persons: {len(df)}")

    df["family_name_main"] = df["family_names"].apply(max_by_len)

    parsed = df["full_names"].apply(max_by_len).apply(parse_given)
    df[["first_name", "middle_name", "first_compound", "first_initial", "middle_initial"]] = (
        pd.DataFrame(parsed.tolist(), index=df.index)
    )
    df["full_name_key"] = _make_full_name_key(df)
    df["orcid"]    = df["orcids"].apply(lambda lst: lst[0] if lst is not None and len(lst) > 0 else None)
    df["inst_arr"] = df["inst_arr"].apply(lambda x: list(x) if x is not None else [])

    return df[[
        "cluster_id", "family_name_main",
        "first_name", "middle_name", "first_compound", "first_initial", "middle_initial",
        "full_name_key", "orcid", "inst_arr",
    ]].rename(columns={"cluster_id": "unique_id"})


def _prep_oax(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    # HumanName-parsed columns are persisted by 02_prepare_oax.py — no re-parsing needed.
    df = con.execute(f"""
        SELECT unique_id, orcid, family_name_main,
               first_name, middle_name, first_compound, first_initial, middle_initial,
               inst_ids
        FROM read_parquet('{path}')
    """).fetchdf()
    print(f"  OAX authors:  {len(df)}")

    df["full_name_key"] = _make_full_name_key(df)
    df["inst_arr"] = df["inst_ids"].apply(lambda x: list(x) if x is not None else [])

    return df[[
        "unique_id", "family_name_main",
        "first_name", "middle_name", "first_compound", "first_initial", "middle_initial",
        "full_name_key", "orcid", "inst_arr",
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
            # Cross-blocking: one side's middle initial matches the other's first initial,
            # catching cases like ARC "Z Smith" vs OAX "Herb Z Smith".
            "l.family_name_main = r.family_name_main AND l.middle_initial IS NOT NULL AND l.middle_initial = r.first_initial",
            "l.family_name_main = r.family_name_main AND r.middle_initial IS NOT NULL AND r.middle_initial = l.first_initial",
            "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        ],
        comparisons=[
            cl.CustomComparison(
                output_column_name="given_name",
                comparison_description="Given name: compound / first / cross / initial cascade",
                comparison_levels=[
                    # Null level fires first; all subsequent levels are guaranteed non-null on first_name.
                    {"sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
                     "label_for_charts": "null", "is_null_level": True},
                    # "shi xue" = "shi xue"
                    {"sql_condition": "first_compound_l = first_compound_r",
                     "label_for_charts": "Compound exact"},
                    # "shi" = "shi" — plain equality so Splink recognises the exact match level for TF.
                    {"sql_condition": "first_name_l = first_name_r",
                     "label_for_charts": "First exact",
                     "tf_adjustment_column": "first_name",
                     "tf_adjustment_weight": 1.0},
                    # first of one = middle of other (e.g. ARC "z" vs OAX middle "z" in "herb z")
                    {"sql_condition": (
                        "(first_name_l = middle_name_r AND middle_name_r IS NOT NULL)"
                        " OR (middle_name_l = first_name_r AND middle_name_l IS NOT NULL)"),
                     "label_for_charts": "First/middle cross"},
                    # S.X. = S.X.
                    {"sql_condition": (
                        "first_initial_l = first_initial_r"
                        " AND middle_initial_l IS NOT NULL AND middle_initial_r IS NOT NULL"
                        " AND middle_initial_l = middle_initial_r"),
                     "label_for_charts": "Both initials"},
                    # S. = S.
                    {"sql_condition": "first_initial_l = first_initial_r",
                     "label_for_charts": "First initial"},
                    # two full names present but they disagree
                    {"sql_condition": (
                        "length(first_name_l) > 1 AND length(first_name_r) > 1"
                        " AND first_name_l != first_name_r"),
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
            cl.ArrayIntersectAtSizes("inst_arr", [2, 1]),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker([df_arc, df_oax], settings, db_api=db_api)

    print("  Registering name frequency tables...")
    for fname, col, old_col in [
        ("oax_tf_family_name.parquet", "family_name_main", None),
        ("oax_tf_first_name.parquet",  "first_name",       "first_name_canonical"),
        ("oax_tf_full_name.parquet",   "full_name_key",    None),
    ]:
        tf = pd.read_parquet(PROCESSED_DATA / fname)
        if old_col:
            tf = tf.rename(columns={old_col: col, f"tf_{old_col}": f"tf_{col}"})
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
