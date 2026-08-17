"""
src/utils/work_piling.py

Phase 2 of the candidate-pool prefilter + work-categorical piling design (see
/home/lc/.claude/plans/drop-into-plan-mode-composed-cat.md, "Candidate-pool prefilter +
work-categorical piling"). Sibling to oeuvre_build.py -- reads its persisted Stage 3 survivor
checkpoint as input; never modifies Stage 1/3 themselves.

This module's first piece: precomputed IDF-style term-frequency tables for the five feature
blocks used by Phase 2's clustering -- coauthor, institution, field, subfield, topic. Same
(value, tf=count/n) shape as the existing oax_tf_*.parquet tables in 02_prepare_oax.py.

Source population: Stage 3 survivors (quality- AND field-filtered), not Stage 1 (quality-filtered
only) -- computing "how common is this value" over the same population Phase 2's clustering
actually operates on keeps the two consistent, rather than measuring commonness over a broader,
noisier population than what gets clustered.

Computed over the full, unpruned candidate set -- not gated by the Phase-1 candidate prefilter,
per the 2026-08-17 sequencing decision (Phase 2 is developed and validated independently of
Phase 1 pruning; recombining the two is deferred until after Phase 2 is validated on its own).
"""

from __future__ import annotations

import math
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix

from config.settings import OPENALEX_DIR, PROCESSED_DATA, DUCKDB_TMP_DIR
from src.utils.oeuvre_build import AUTH_GLOB, TOPIC_GLOB, STAGE3_SURVIVORS

WORK_TF_COAUTHOR    = PROCESSED_DATA / "work_tf_coauthor.parquet"
WORK_TF_INSTITUTION = PROCESSED_DATA / "work_tf_institution.parquet"
WORK_TF_FIELD       = PROCESSED_DATA / "work_tf_field.parquet"
WORK_TF_SUBFIELD    = PROCESSED_DATA / "work_tf_subfield.parquet"
WORK_TF_TOPIC       = PROCESSED_DATA / "work_tf_topic.parquet"


def compute_and_persist_idf_tables(
    con: duckdb.DuckDBPyConnection | None = None,
    survivors_path: Path = STAGE3_SURVIVORS,
    auth_glob: str = AUTH_GLOB,
    topic_glob: str = TOPIC_GLOB,
) -> None:
    """Persist five (value, tf) term-frequency tables -- work_tf_coauthor/institution/field/
    subfield/topic.parquet -- over the full Stage-3 survivor population (every candidate in every
    AwardsCIF's oax_candidates, not a Phase-1-pruned subset).

    n is the total number of (cluster_id, work_idx) survivor rows in oeuvre_stage3_survivors.parquet
    -- the natural counting unit, since institution is inherently a per-(cluster, work) fact (the
    same work_idx can have a different "own candidate" institution under two different clusters,
    on a genuinely shared multi-CI-grant paper) -- using the same unit for all five tables keeps
    the computation consistent rather than mixing two different denominators.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        if own_con:
            con.execute("SET enable_progress_bar = false")
            con.execute("SET memory_limit = '24GB'")
            con.execute(f"SET temp_directory = '{DUCKDB_TMP_DIR}'")

        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{survivors_path}')").fetchone()[0]
        print(f"  n (cluster,work) Stage-3 survivor rows: {n:,}")

        # field / subfield -- already persisted on the survivor rows (field_names/subfield_names,
        # every distinct value across a work's topics, not just the best-scoring one).
        for col, out_path in [("field_names", WORK_TF_FIELD), ("subfield_names", WORK_TF_SUBFIELD)]:
            con.execute(f"""
                COPY (
                    SELECT value, COUNT(*)::DOUBLE / {n} AS tf
                    FROM read_parquet('{survivors_path}'), UNNEST({col}) AS t(value)
                    WHERE value IS NOT NULL
                    GROUP BY value
                    ORDER BY tf DESC
                ) TO '{out_path}' (FORMAT PARQUET)
            """)
            n_vals = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
            print(f"  {out_path.name}: {n_vals:,} unique values")

        # topic -- not persisted on the survivor rows at all (only subfield-level and up), so a
        # fresh join against work_topics restricted to the touched-work set is needed.
        con.execute(f"""
            COPY (
                SELECT t.topic_idx AS value, COUNT(*)::DOUBLE / {n} AS tf
                FROM read_parquet('{survivors_path}') s
                JOIN read_parquet('{topic_glob}') t ON t.work_idx = s.work_idx
                GROUP BY t.topic_idx
                ORDER BY tf DESC
            ) TO '{WORK_TF_TOPIC}' (FORMAT PARQUET)
        """)
        n_vals = con.execute(f"SELECT COUNT(*) FROM read_parquet('{WORK_TF_TOPIC}')").fetchone()[0]
        print(f"  {WORK_TF_TOPIC.name}: {n_vals:,} unique values")

        # coauthor -- every author_idx appearing on any touched work (own-candidates included;
        # excluding a pool's own candidates happens at feature-vector construction time, not
        # here -- this table is a general-purpose frequency statistic).
        con.execute(f"""
            COPY (
                SELECT au.author_idx AS value, COUNT(*)::DOUBLE / {n} AS tf
                FROM read_parquet('{survivors_path}') s
                JOIN read_parquet('{auth_glob}') au ON au.work_idx = s.work_idx
                WHERE au.author_idx IS NOT NULL
                GROUP BY au.author_idx
                ORDER BY tf DESC
            ) TO '{WORK_TF_COAUTHOR}' (FORMAT PARQUET)
        """)
        n_vals = con.execute(f"SELECT COUNT(*) FROM read_parquet('{WORK_TF_COAUTHOR}')").fetchone()[0]
        print(f"  {WORK_TF_COAUTHOR.name}: {n_vals:,} unique values")

        # institution -- restricted to each survivor row's OWN candidate(s) (source_author_idxs),
        # not every coauthor's institution -- the feature block this table weights is "does the
        # work's own-candidate institution match," so the frequency statistic needs to be computed
        # over that same value space, not a blanket "any author on any touched work."
        con.execute(f"""
            COPY (
                SELECT au.institution_idx AS value, COUNT(*)::DOUBLE / {n} AS tf
                FROM read_parquet('{survivors_path}') s
                JOIN read_parquet('{auth_glob}') au
                    ON au.work_idx = s.work_idx
                    AND list_contains(s.source_author_idxs, au.author_idx)
                WHERE au.institution_idx IS NOT NULL
                GROUP BY au.institution_idx
                ORDER BY tf DESC
            ) TO '{WORK_TF_INSTITUTION}' (FORMAT PARQUET)
        """)
        n_vals = con.execute(f"SELECT COUNT(*) FROM read_parquet('{WORK_TF_INSTITUTION}')").fetchone()[0]
        print(f"  {WORK_TF_INSTITUTION.name}: {n_vals:,} unique values")
    finally:
        if own_con:
            con.close()


# --- Step 3: IDF-weighted feature matrices, cross-section development only ---
#
# Scoped to a small, named set of cluster_ids at a time (the cross-section in
# piling_cross_section.csv, or any other explicit list) -- not intended for full-population use
# yet, per the plan's TODO ordering (steps 3-7 validate on the cross-section before step 8
# considers a full run).

CROSS_SECTION_CSV = PROCESSED_DATA / "piling_cross_section.csv"

_BLOCKS = ("coauthor", "institution", "field", "subfield", "topic")
_TF_PATHS = {
    "coauthor": WORK_TF_COAUTHOR,
    "institution": WORK_TF_INSTITUTION,
    "field": WORK_TF_FIELD,
    "subfield": WORK_TF_SUBFIELD,
    "topic": WORK_TF_TOPIC,
}


def _safe_list(x) -> list:
    """A DuckDB LIST column read back via .fetchdf() surfaces a null cell as pandas NA (not
    iterable) on some rows and a bare None on others, and a present value as a numpy array (whose
    truthiness is itself ambiguous) -- this normalizes all three to a plain empty-or-populated
    Python list.
    """
    if x is None:
        return []
    try:
        if pd.isna(x):
            return []
    except (TypeError, ValueError):
        pass  # x is array-like -- pd.isna would return an array, not a scalar; fall through
    return list(x)


def load_idf_weights(con: duckdb.DuckDBPyConnection | None = None) -> dict[str, dict]:
    """value -> idf (log(1/tf)) per block, read back from the five persisted work_tf_*.parquet
    tables. A value with no entry (shouldn't happen for anything actually present in Stage 3
    survivors, since the tables were computed over that same population) gets no weight at
    feature-construction time -- callers should treat a missing lookup as "drop this value",
    not silently substitute a default.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        out = {}
        for block, path in _TF_PATHS.items():
            df = con.execute(f"SELECT value, tf FROM read_parquet('{path}')").fetchdf()
            out[block] = {row.value: math.log(1.0 / row.tf) for row in df.itertuples()}
        return out
    finally:
        if own_con:
            con.close()


def fetch_cross_section_raw(
    cluster_ids: list[str],
    con: duckdb.DuckDBPyConnection | None = None,
    survivors_path: Path = STAGE3_SURVIVORS,
    auth_glob: str = AUTH_GLOB,
    topic_glob: str = TOPIC_GLOB,
) -> dict[str, pd.DataFrame]:
    """Fetch every raw ingredient needed to build feature vectors for the given cluster_ids only
    -- survivor rows (field_names/subfield_names already on them), coauthors (excluding each
    row's own candidate author_idx(s)), own-candidate institution, and topic_idx. All four
    queries are scoped to `cluster_id IN (...)` up front, so this stays cheap regardless of full
    population size.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        if own_con:
            con.execute("SET enable_progress_bar = false")
            con.execute(f"SET temp_directory = '{DUCKDB_TMP_DIR}'")
        ids_sql = ",".join(f"'{c}'" for c in cluster_ids)

        survivors = con.execute(f"""
            SELECT cluster_id, work_idx, source_author_idxs, field_names, subfield_names
            FROM read_parquet('{survivors_path}')
            WHERE cluster_id IN ({ids_sql})
        """).fetchdf()

        coauthors = con.execute(f"""
            SELECT s.cluster_id, s.work_idx, au.author_idx AS coauthor_idx
            FROM read_parquet('{survivors_path}') s
            JOIN read_parquet('{auth_glob}') au ON au.work_idx = s.work_idx
            WHERE s.cluster_id IN ({ids_sql})
              AND NOT list_contains(s.source_author_idxs, au.author_idx)
              AND au.author_idx IS NOT NULL
        """).fetchdf()

        institution = con.execute(f"""
            SELECT s.cluster_id, s.work_idx, au.institution_idx
            FROM read_parquet('{survivors_path}') s
            JOIN read_parquet('{auth_glob}') au
                ON au.work_idx = s.work_idx
                AND list_contains(s.source_author_idxs, au.author_idx)
            WHERE s.cluster_id IN ({ids_sql}) AND au.institution_idx IS NOT NULL
        """).fetchdf()

        topics = con.execute(f"""
            SELECT DISTINCT s.cluster_id, t.work_idx, t.topic_idx
            FROM read_parquet('{survivors_path}') s
            JOIN read_parquet('{topic_glob}') t ON t.work_idx = s.work_idx
            WHERE s.cluster_id IN ({ids_sql}) AND t.topic_idx IS NOT NULL
        """).fetchdf()

        return {
            "survivors": survivors, "coauthors": coauthors,
            "institution": institution, "topics": topics,
        }
    finally:
        if own_con:
            con.close()


def build_feature_matrix(
    cluster_id: str, raw: dict[str, pd.DataFrame], idf: dict[str, dict],
) -> tuple[np.ndarray, csr_matrix, list[tuple[str, object]]]:
    """One ACIF's pooled works -> (work_idx array, IDF-weighted sparse feature matrix, column
    labels as (block, value) pairs). Column vocabulary is local to this ACIF's own pool (only
    values actually appearing in it), not global across the cross-section or population --
    matches the plan's "per ACIF, not global" feature-engineering design.
    """
    surv = raw["survivors"][raw["survivors"]["cluster_id"] == cluster_id]
    work_idxs = surv["work_idx"].to_numpy()
    row_of = {w: i for i, w in enumerate(work_idxs)}
    n_rows = len(work_idxs)

    # per-work value lists, one dict per block
    per_work: dict[int, dict[str, set]] = {w: {b: set() for b in _BLOCKS} for w in work_idxs}
    for row in surv.itertuples():
        per_work[row.work_idx]["field"].update(_safe_list(row.field_names))
        per_work[row.work_idx]["subfield"].update(_safe_list(row.subfield_names))

    coa = raw["coauthors"]
    coa = coa[coa["cluster_id"] == cluster_id]
    for row in coa.itertuples():
        per_work[row.work_idx]["coauthor"].add(row.coauthor_idx)

    inst = raw["institution"]
    inst = inst[inst["cluster_id"] == cluster_id]
    for row in inst.itertuples():
        per_work[row.work_idx]["institution"].add(row.institution_idx)

    top = raw["topics"]
    top = top[top["cluster_id"] == cluster_id]
    for row in top.itertuples():
        per_work[row.work_idx]["topic"].add(row.topic_idx)

    # column vocabulary local to this pool
    col_index: dict[tuple[str, object], int] = {}
    for w in work_idxs:
        for block in _BLOCKS:
            for val in per_work[w][block]:
                key = (block, val)
                if key not in col_index:
                    col_index[key] = len(col_index)

    rows, cols, data = [], [], []
    for w in work_idxs:
        r = row_of[w]
        for block in _BLOCKS:
            for val in per_work[w][block]:
                weight = idf[block].get(val)
                if weight is None:
                    continue  # not present in the IDF table -- drop, don't default
                rows.append(r)
                cols.append(col_index[(block, val)])
                data.append(weight)

    matrix = csr_matrix((data, (rows, cols)), shape=(n_rows, len(col_index)))
    columns = [None] * len(col_index)
    for key, idx in col_index.items():
        columns[idx] = key
    return work_idxs, matrix, columns


# --- Step 4/5: pairwise distance + clustering, cross-section development only ---

def compute_distance_matrix(matrix: csr_matrix) -> np.ndarray:
    """Pairwise cosine distance over the IDF-weighted sparse feature matrix. Cosine, not weighted
    Jaccard: cosine is a single vectorized sparse matrix multiply (sklearn.metrics.pairwise.
    cosine_distances), while weighted/Ruzicka Jaccard has no efficient vectorized form and would
    need a per-pair Python callback -- infeasible at the largest cross-section pool's scale
    (WeiWang, 12,800 works, ~82M pairs), confirmed impractical by measurement, not assumption.
    """
    from sklearn.metrics.pairwise import cosine_distances
    return cosine_distances(matrix)


def cluster_piles_dbscan(distance_matrix: np.ndarray, eps: float = 0.5, min_samples: int = 2) -> np.ndarray:
    """DBSCAN over a precomputed distance matrix. Returns one label per work; -1 = noise
    (genuinely ambiguous, not force-assigned to any pile)."""
    from sklearn.cluster import DBSCAN
    return DBSCAN(metric="precomputed", eps=eps, min_samples=min_samples).fit_predict(distance_matrix)


def cluster_piles_agglomerative(distance_matrix: np.ndarray, distance_threshold: float = 0.5) -> np.ndarray:
    """Agglomerative clustering over a precomputed distance matrix, cut at distance_threshold --
    no fixed cluster count, discovered from the data (see plan rationale: the true number of
    distinct real people blended into a pool is exactly what's unknown ahead of time)."""
    from sklearn.cluster import AgglomerativeClustering
    model = AgglomerativeClustering(
        metric="precomputed", linkage="average",
        distance_threshold=distance_threshold, n_clusters=None,
    )
    return model.fit_predict(distance_matrix)


if __name__ == "__main__":
    compute_and_persist_idf_tables()
