"""
src/utils/work_piling.py

Phase 2 of the group-level ACIF-membership gate design (see CLAUDE.md, "Group-level
ACIF-membership gate design" and "`src/utils/work_piling.py` — Phase 2 piling infrastructure
built and tested"). Sibling to oeuvre_build.py -- reads its persisted Stage 3 survivor checkpoint
as input; never modifies Stage 1/3 themselves.

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
from src.utils.cluster_checks import for2020_all_fields, for2020_all_subfields
from src.utils.awards_cif import _load_institution_hep_crosswalk

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
    no fixed cluster count, discovered from the data (the true number of distinct real people
    blended into a pool is exactly what's unknown ahead of time). Confirmed empirically inferior
    to DBSCAN for this data's chain-like similarity structure -- see CLAUDE.md; kept for
    completeness/comparison, not the recommended default (use cluster_piles_dbscan)."""
    from sklearn.cluster import AgglomerativeClustering
    model = AgglomerativeClustering(
        metric="precomputed", linkage="average",
        distance_threshold=distance_threshold, n_clusters=None,
    )
    return model.fit_predict(distance_matrix)


# --- Step 6: pile-to-ACIF channeling, cross-section development only ---

def fetch_candidate_orcids(cluster_ids: list[str], con: duckdb.DuckDBPyConnection | None = None) -> dict[int, str]:
    """author_idx -> own OpenAlex orcid (or None), for every candidate across the given
    cluster_ids. Used by channel_piles()'s ORCID-first check."""
    own_con = con is None
    con = con or duckdb.connect()
    try:
        ids_sql = ",".join(f"'{c}'" for c in cluster_ids)
        df = con.execute(f"""
            SELECT DISTINCT a.author_idx, a.orcid
            FROM read_parquet('{PROCESSED_DATA}/awards_cif.parquet') c,
                 UNNEST(c.oax_candidates) AS t(url)
            JOIN read_parquet('{OPENALEX_DIR}/authors/*.parquet') a ON a.ids.openalex = t.url
            WHERE c.cluster_id IN ({ids_sql})
        """).fetchdf()
        return dict(zip(df["author_idx"], df["orcid"]))
    finally:
        if own_con:
            con.close()


def fetch_acif_meta(cluster_ids: list[str], con: duckdb.DuckDBPyConnection | None = None) -> pd.DataFrame:
    """cluster_id -> orcids, hep_codes, for2020_codes -- the ACIF-side facts channel_piles()
    corroborates each pile against."""
    own_con = con is None
    con = con or duckdb.connect()
    try:
        ids_sql = ",".join(f"'{c}'" for c in cluster_ids)
        return con.execute(f"""
            SELECT cluster_id, orcids, hep_codes, for2020_codes
            FROM read_parquet('{PROCESSED_DATA}/awards_cif.parquet')
            WHERE cluster_id IN ({ids_sql})
        """).fetchdf().set_index("cluster_id")
    finally:
        if own_con:
            con.close()


def channel_piles(
    cluster_id: str,
    work_idxs: np.ndarray,
    labels: np.ndarray,
    raw: dict[str, pd.DataFrame],
    arc_orcids: set[str],
    acif_hep_codes: set[str],
    acif_fields: set[str],
    acif_subfields: set[str],
    candidate_orcid: dict[int, str],
    hep_crosswalk: dict[int, str],
) -> dict[int, dict]:
    """For each pile (a DBSCAN cluster label, excluding noise -1) in one ACIF's clustered
    work-set, decide whether it's corroborated as belonging to this ACIF -- three signals,
    checked in the established ORCID-first order:
      1. Direct ORCID match -- any work in the pile traces to a candidate author_idx whose own
         OpenAlex orcid matches one of the ACIF's ARC-recorded orcids. Settles identity directly,
         not an inference from a pattern.
      2. HEP-institution overlap -- the pile's own (own-candidate) institution history, mapped
         through the HEP crosswalk, overlaps the ACIF's own hep_codes.
      3. FOR-grounded field overlap -- the pile's own OAX field union overlaps
         for2020_all_fields() of the ACIF's own declared FOR2020 codes. Confirmation uses the
         coarser FIELD level, not subfield (changed 2026-08-18) -- subfield-level matching,
         while more discriminating in principle (see CLAUDE.md's Hayward/MohammadIslam
         findings, the reason it was adopted originally), produces real false negatives: e.g.
         DP0346211_KasperKowalski's declared FOR2020 group "Biochemistry and cell biology"
         resolves via for_resolve's crosswalk to OAX subfield "Biochemistry" only, missing the
         adjacent OAX subfield "Molecular Biology" his actual pile's works land in, even though
         both sides share the exact same OAX FIELD ("Biochemistry, Genetics and Molecular
         Biology"). subfield_match is still computed and returned (not dropped) for a possible
         future waterfall (e.g. subfield first for precision, field as a fallback) -- just not
         what confirmed is based on right now.
    A pile confirmed by none of the three is excluded from this ACIF's oeuvre, not silently kept.
    Multiple piles may be independently confirmed for one ACIF -- fragment-splitting (one real
    person's career split by OAX across several author_idx) is expected, not an error; see
    CLAUDE.md for why a single "keep pile" that might blend different real people is the wrong
    design.

    Known, unresolved limitation (documented, not fixed here): this corroborates each pile as a
    WHOLE, so a pile that's mostly-but-not-entirely the right person (e.g. Hayward's 59%-correct
    main pile, confirmed 2026-08-17) can still pass the HEP/field checks even though part of it is
    contamination from a different real person -- catching that needs a finer-than-per-pile check,
    not yet built.
    """
    surv = raw["survivors"]
    surv = surv[surv["cluster_id"] == cluster_id].set_index("work_idx")
    smap = surv["source_author_idxs"].to_dict()
    fmap = surv["field_names"].to_dict()
    sfmap = surv["subfield_names"].to_dict()

    inst = raw["institution"]
    inst = inst[inst["cluster_id"] == cluster_id]
    work_hep: dict[int, set] = {}
    for row in inst.itertuples():
        hep = hep_crosswalk.get(row.institution_idx)
        if hep:
            work_hep.setdefault(row.work_idx, set()).add(hep)

    results = {}
    for pile in sorted(set(labels) - {-1}):
        idxs = [i for i, l in enumerate(labels) if l == pile]
        pile_works = [work_idxs[i] for i in idxs]

        pile_authors: set = set()
        pile_fields: set = set()
        pile_subfields: set = set()
        pile_hep: set = set()
        for w in pile_works:
            pile_authors.update(_safe_list(smap.get(w)))
            pile_fields.update(_safe_list(fmap.get(w)))
            pile_subfields.update(_safe_list(sfmap.get(w)))
            pile_hep.update(work_hep.get(w, set()))

        orcid_match = any(
            isinstance(candidate_orcid.get(a), str) and any(o in candidate_orcid[a] for o in arc_orcids)
            for a in pile_authors
        )
        hep_match = bool(acif_hep_codes & pile_hep)
        field_match = bool(acif_fields & pile_fields)
        subfield_match = bool(acif_subfields & pile_subfields)

        results[pile] = {
            "n_works": len(pile_works),
            "source_author_idxs": pile_authors,
            "orcid_match": orcid_match,
            "hep_match": hep_match,
            "field_match": field_match,
            "subfield_match": subfield_match,
            "confirmed": orcid_match or hep_match or field_match,
        }
    return results


# --- Persisted pipeline stage: piling + channeling is a batch stage, not a report-time lookup ---
# (2026-08-18) -- Dossier() and any other reporting tool read this checkpoint the same way they
# read arc_persons.parquet/oeuvres.parquet, they never trigger piling computation themselves.

PILING_RESULTS = PROCESSED_DATA / "oeuvre_piling_results.parquet"


def persist_piling_results(
    con: duckdb.DuckDBPyConnection | None = None,
    out_path: Path = PILING_RESULTS,
    eps: float = 0.90,
    batch_size: int = 500,
    only_fellowships: bool = False,
) -> None:
    """Run piling (feature vectors -> cosine distance -> DBSCAN) + channeling
    (ORCID-first -> HEP-overlap -> FOR-grounded field) across every non-excluded ACIF
    with 2+ Stage-3 survivor works, and persist one row per (cluster_id, work_idx): pile_id
    (-1 = noise, never clustered), orcid_match, hep_match, field_match, subfield_match,
    confirmed (confirmed is orcid_match OR hep_match OR field_match -- subfield_match is
    persisted alongside for a possible future waterfall but doesn't gate confirmed itself,
    see channel_piles()'s docstring). ACIFs with
    fewer than 2 works get a single row with pile_id=-1 and confirmed=NULL (not evaluated, not
    "evaluated and failed" -- same three-valued discipline as coinvestigator_match elsewhere in
    this project).

    only_fellowships: when True, restricts to ACIFs holding >=1 grant with
    investigators_raw.is_fellowship = TRUE (2026-08-18, user-directed scope -- this is the
    project's own fellowship flag, broader than config/scope.py's ECR_ROLES, which is only the
    DECRA/APD/APDI early-career subset). A prior run against the full population already exists
    at PILING_RESULTS -- this flag is for a faster, fellowship-only re-run, not the only mode.

    Processed in batches (default 500 ACIFs) rather than one single fetch_cross_section_raw()
    call over the whole population -- the largest mega-pools (WeiWang-scale, tens of thousands
    of works) make a single unbounded feature/distance-matrix pass memory-risky at full
    population scale (2.55M total Stage-3 survivor rows); batching bounds peak memory to one
    batch's worth regardless of population size, same rationale as oeuvre_build.py's original
    OOM fix.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        if own_con:
            con.execute("SET enable_progress_bar = false")
            con.execute("SET memory_limit = '24GB'")
            con.execute(f"SET temp_directory = '{DUCKDB_TMP_DIR}'")

        fellowship_join = ""
        if only_fellowships:
            fellowship_join = f"""
                AND cluster_id IN (
                    SELECT DISTINCT m.cluster_id
                    FROM read_parquet('{PROCESSED_DATA}/arc_grant_cluster_map.parquet') m
                    JOIN read_parquet('{PROCESSED_DATA}/investigators_raw.parquet') i
                        ON m.unique_id = i.unique_id
                    WHERE i.is_fellowship = TRUE
                )
            """
        all_ids = con.execute(f"""
            SELECT cluster_id FROM read_parquet('{PROCESSED_DATA}/awards_cif.parquet')
            WHERE excluded = FALSE
            {fellowship_join}
        """).fetchdf()["cluster_id"].tolist()
        scope_label = "fellowship" if only_fellowships else "non-excluded"
        print(f"  persist_piling_results: {len(all_ids)} {scope_label} ACIFs", flush=True)

        idf = load_idf_weights(con)
        hep_crosswalk = _load_institution_hep_crosswalk()

        first_batch = True
        for start in range(0, len(all_ids), batch_size):
            batch_ids = all_ids[start:start + batch_size]
            raw = fetch_cross_section_raw(batch_ids, con)
            candidate_orcid = fetch_candidate_orcids(batch_ids, con)
            acif_meta = fetch_acif_meta(batch_ids, con)
            surv_by_cluster = raw["survivors"].groupby("cluster_id").size()

            rows = []
            for cid in batch_ids:
                n = surv_by_cluster.get(cid, 0)
                if n < 2:
                    surv = raw["survivors"]
                    for w in surv[surv["cluster_id"] == cid]["work_idx"]:
                        rows.append({"cluster_id": cid, "work_idx": int(w), "pile_id": -1,
                                     "orcid_match": None, "hep_match": None, "field_match": None,
                                     "subfield_match": None, "confirmed": None})
                    continue

                work_idxs, matrix, columns = build_feature_matrix(cid, raw, idf)
                dist = compute_distance_matrix(matrix)
                labels = cluster_piles_dbscan(dist, eps=eps, min_samples=2)

                meta = acif_meta.loc[cid]
                arc_orcids = set(_safe_list(meta["orcids"]))
                acif_hep = set(_safe_list(meta["hep_codes"]))
                codes = [dict(c) for c in _safe_list(meta["for2020_codes"])]
                acif_fields = for2020_all_fields(codes)
                acif_subfields = for2020_all_subfields(codes)

                ch = channel_piles(cid, work_idxs, labels, raw, arc_orcids, acif_hep,
                                    acif_fields, acif_subfields, candidate_orcid, hep_crosswalk)
                pile_of_work = dict(zip(work_idxs, labels))
                for w, pile in pile_of_work.items():
                    if pile == -1:
                        rows.append({"cluster_id": cid, "work_idx": int(w), "pile_id": -1,
                                     "orcid_match": False, "hep_match": False, "field_match": False,
                                     "subfield_match": False, "confirmed": False})
                    else:
                        r = ch[pile]
                        rows.append({"cluster_id": cid, "work_idx": int(w), "pile_id": int(pile),
                                     "orcid_match": r["orcid_match"], "hep_match": r["hep_match"],
                                     "field_match": r["field_match"], "subfield_match": r["subfield_match"],
                                     "confirmed": r["confirmed"]})

            batch_df = pd.DataFrame(rows)
            con.register("_batch_df", batch_df)
            if first_batch:
                con.execute(f"COPY _batch_df TO '{out_path}' (FORMAT PARQUET)")
                first_batch = False
            else:
                con.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet('{out_path}')
                        UNION ALL BY NAME
                        SELECT * FROM _batch_df
                    ) TO '{out_path}' (FORMAT PARQUET)
                """)
            con.unregister("_batch_df")
            print(f"    {min(start + batch_size, len(all_ids))}/{len(all_ids)} ACIFs processed", flush=True)

        n_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
        print(f"  persist_piling_results: {n_rows:,} rows -> {out_path}", flush=True)
    finally:
        if own_con:
            con.close()


if __name__ == "__main__":
    compute_and_persist_idf_tables()
