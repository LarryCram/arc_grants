"""
src/02_run_splink.py

Splink record linkage: ARC CIF sample → OpenAlex authors.
Reads the prepared parquets from 01_prepare_splink.py.

Blocking rules generate candidate pairs; EM training on ORCID-certain pairs
estimates m-probabilities; predictions saved to splink_sample_predictions.parquet.
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA

SAMPLE_N = 500
SEED = 42


def _load_arc_sample(con: duckdb.DuckDBPyConnection, arc_path: Path) -> pd.DataFrame:
    df = con.execute(f"""
        SELECT *
        FROM read_parquet('{arc_path}')
        USING SAMPLE {SAMPLE_N} ROWS (reservoir, {SEED})
    """).fetchdf()
    print(f"  ARC sample: {len(df)} rows, {df['unique_id'].nunique()} unique IDs")
    return df


def _load_oax_candidates(
    con: duckdb.DuckDBPyConnection,
    oax_path: Path,
    family_tokens: set[str],
    orcids: set[str],
) -> pd.DataFrame:
    """
    Pre-filter OAX to rows that share at least one family name token with the
    ARC sample, or share an ORCID. This avoids a 500 × 1.1M cross-join inside
    Splink's blocking step.
    """
    tokens_literal = "[" + ", ".join(f"'{t}'" for t in sorted(family_tokens)) + "]::VARCHAR[]"
    orcids_literal = "(" + ", ".join(f"'{o}'" for o in sorted(orcids)) + ")"

    orcid_clause = (
        f"OR (orcid IS NOT NULL AND orcid IN {orcids_literal})"
        if orcids
        else ""
    )

    df = con.execute(f"""
        SELECT *
        FROM read_parquet('{oax_path}')
        WHERE list_has_any(family_names, {tokens_literal})
        {orcid_clause}
    """).fetchdf()
    print(f"  OAX candidates: {len(df)} rows")
    return df


SPLINK_COLS = [
    "unique_id", "full_name",
    "first_names", "family_names",
    "family_name_0",   # scalar first token — used only for EM training blocking
    "orcid", "inst_arr",
]


def _prep_for_splink(df_arc: pd.DataFrame, df_oax: pd.DataFrame):
    """
    Align schemas for Splink: both tables must have identical columns.
    ARC has institution_oax_id (scalar); OAX has inst_ids (array).
    Wrap the ARC scalar in a list, rename OAX's column, derive a scalar
    family_name_0 (first token) for use in EM training blocking rules
    (arrays_to_explode is not supported there), then keep only the columns
    Splink actually references.
    """
    df_arc = df_arc.copy()
    df_oax = df_oax.copy()

    df_arc["inst_arr"] = df_arc["institution_oax_id"].apply(
        lambda x: [x] if (pd.notna(x) and x) else []
    )
    df_oax.rename(columns={"inst_ids": "inst_arr"}, inplace=True)

    for df in (df_arc, df_oax):
        df["family_name_0"] = df["family_names"].apply(
            lambda x: x[0] if (x is not None and len(x) > 0) else None
        )

    return df_arc[SPLINK_COLS], df_oax[SPLINK_COLS]


def main():
    arc_path = PROCESSED_DATA / "arc_investigators_prep.parquet"
    oax_path = PROCESSED_DATA / "openalex_authors_prep.parquet"
    out_path = PROCESSED_DATA / "splink_sample_predictions.parquet"

    con = duckdb.connect()

    print(f"[1/5] Sampling {SAMPLE_N} ARC investigators (seed={SEED})...")
    df_arc = _load_arc_sample(con, arc_path)

    family_tokens = {
        tok
        for row in df_arc["family_names"]
        if row is not None
        for tok in row
    }
    orcids = set(df_arc["orcid"].dropna().tolist())
    print(f"  {len(family_tokens)} family tokens, {len(orcids)} ORCIDs")

    print("[2/5] Fetching OAX candidates...")
    df_oax = _load_oax_candidates(con, oax_path, family_tokens, orcids)

    print("[3/5] Preparing columns for Splink...")
    df_arc, df_oax = _prep_for_splink(df_arc, df_oax)

    settings = SettingsCreator(
        unique_id_column_name="unique_id",
        link_type="link_only",
        blocking_rules_to_generate_predictions=[
            # Explode arrays so each token becomes a row, then block on equality
            block_on("family_names", arrays_to_explode=["family_names"]),
            # ORCID: scalar column, raw SQL is fine
            "l.orcid = r.orcid AND l.orcid IS NOT NULL AND r.orcid IS NOT NULL",
        ],
        comparisons=[
            cl.ArrayIntersectAtSizes("first_names", [2, 1]),
            cl.ArrayIntersectAtSizes("family_names", [2, 1]),
            cl.ExactMatch("orcid"),
            cl.ArrayIntersectAtSizes("inst_arr", [1]),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker(
        [df_arc, df_oax],
        settings,
        db_api=db_api,
        input_table_aliases=["arc", "oax"],
    )

    print("[4/5] Training EM model...")
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    # Session 1: ORCID-certain pairs → trains m-probabilities for name + institution
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.orcid = r.orcid AND l.orcid IS NOT NULL AND r.orcid IS NOT NULL",
        fix_u_probabilities=True,
    )
    # Session 2: family name token match → trains first_names and orcid m-probabilities.
    # arrays_to_explode is not supported in EM training, so block on the derived scalar.
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("family_name_0"),
        fix_u_probabilities=True,
    )

    print("[5/5] Predicting...")
    df_pred = linker.inference.predict(threshold_match_probability=0.5)
    df_results = df_pred.as_pandas_dataframe()
    print(f"  {len(df_results)} pairs above 0.5 threshold")

    df_results.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")

    # Quick summary
    show_cols = [c for c in [
        "unique_id_l", "unique_id_r",
        "full_name_l", "full_name_r",
        "orcid_l", "orcid_r",
        "match_probability", "match_weight",
    ] if c in df_results.columns]

    top = df_results.sort_values("match_probability", ascending=False).head(20)
    print("\nTop 20 matches:")
    print(top[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()
