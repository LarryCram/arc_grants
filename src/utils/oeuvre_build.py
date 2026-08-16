"""
src/utils/oeuvre_build.py

Roadmap step 2 (see /home/lc/.claude/plans/review-this-code-base-groovy-key.md): gives each
AwardsCIF its own work-set. Unions the OpenAlex works reached by every candidate author_idx in
AwardsCIF.oax_candidates, then decides per work whether it belongs in this person's oeuvre.

Sibling to src/utils/awards_cif.py, mirroring the analysis/utils/dossier.py / dossier_build.py
split (data model vs. construction logic) -- CandidateWork/AwardsCIF.oeuvre live in awards_cif.py,
the functions that populate and score them live here.

Only over-merge (a candidate's oeuvre containing someone else's work) is addressed -- under-merge
(real output sitting under a still-unlinked author_idx) is out of scope, as throughout this
project. No AwardsCIF is ever compared against another -- every signal here is computed from one
AwardsCIF's own ARC-declared facts (for_codes, inst_arr, grant co-investigators) or from the
OpenAlex data attached to its own candidate works, never against a different person's cluster.

Composition matches refine_clusters(): small single-responsibility functions, each
list[AwardsCIF] -> list[AwardsCIF], each appending one summary record_event(...) per pipeline
stage (not per work -- provenance stays a scannable identity-history log; per-work detail lives
on CandidateWork.signals/exclusion_reason instead).
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd

from config.settings import OPENALEX_COMPACT_DIR, PROCESSED_DATA, DUCKDB_TMP_DIR
from src.utils.awards_cif import (
    AwardsCIF, CandidateWork, _load_coinvestigator_names, _norm_full,
    _load_institution_hep_crosswalk,
)
from src.utils.cluster_checks import for2020_primary_fields, for2020_primary_subfields
from analysis.utils import dedup, exclusions

AUTH_GLOB  = str(OPENALEX_COMPACT_DIR / "authorships" / "*.parquet")
WORK_GLOB  = str(OPENALEX_COMPACT_DIR / "works" / "*.parquet")
TOPIC_GLOB = str(OPENALEX_COMPACT_DIR / "work_topics" / "*.parquet")

# Stage 1/Stage 3 checkpoint locations (2026-08-15 full-scale-OOM fix) -- PROCESSED_DATA, not a
# separate subfolder: AwardsCIF/oeuvre_build.py is the pipeline going forward, a refactor of
# 01/03/04, not a side experiment (see the plan file's "Framing correction").
STAGE1_SURVIVORS  = PROCESSED_DATA / "oeuvre_stage1_survivors.parquet"
STAGE1_EXCLUSIONS = PROCESSED_DATA / "oeuvre_stage1_exclusions.parquet"
STAGE3_SURVIVORS  = PROCESSED_DATA / "oeuvre_stage3_survivors.parquet"
STAGE3_EXCLUSIONS = PROCESSED_DATA / "oeuvre_stage3_exclusions.parquet"
# Per-candidate, per-work subfield/HEP keep/drop signal (2026-08-16 reframing) -- see
# compute_subfield_hep_signals()'s docstring.
STAGE_SUBFIELD_HEP_SIGNALS = PROCESSED_DATA / "oeuvre_subfield_hep_signals.parquet"

# Work-row columns shared by the stage-1/stage-3 checkpoints -- deliberately no coauthor/
# own_institution_idxs fields: those are only fetched by the (not-yet-restructured) stage 4
# coauthor pull, on the much smaller population that survives stages 1+3 first.
#
# field_name (singular) is the single highest-scoring topic's field, kept for display/context
# only. field_names (plural, list) is every distinct field across ALL of a work's topics --
# what apply_field_filter_stage3() actually matches against (2026-08-15, following up on a real
# finding: works have up to 3 topics, and the top-vs-2nd topic score gap is a near-tie in the
# median case (0.0037) -- picking only the single best topic was discarding real, legitimate
# field information in the majority of multi-topic works, not just a rare edge case (64.2% of
# touched works have >1 distinct field across their topics; 14.6% of best-topic-only field
# exclusions had another topic that actually matched the target field).
_WORK_ROW_COLUMNS = [
    "cluster_id", "work_idx", "source_author_idxs", "publication_year", "cited_by_count",
    "type", "title", "doi", "source_id", "subfield_idx", "subfield_name", "subfield_names",
    "field_name", "field_names", "domain_name",
]


def fetch_and_filter_stage1(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
    path: Path = STAGE1_SURVIVORS,
    exclusions_path: Path = STAGE1_EXCLUSIONS,
    auth_glob: str = AUTH_GLOB,
    work_glob: str = WORK_GLOB,
    topic_glob: str = TOPIC_GLOB,
) -> None:
    """Stage 1 of the 2026-08-15 full-scale-OOM fix: extract every work reached by any
    candidate author_idx and apply all corruption checks *in the same SQL pass*, before any
    coauthor detail is ever fetched -- unlike the original fetch_candidate_oeuvre(), which pulled
    full coauthor rows for every touched work (9,512,745 of them, ~100M authorship rows) before
    any filtering happened at all. Only survivors are persisted; no CandidateWork object is
    constructed for a work that fails here (see the plan file's "provenance shape differs by
    stage" section -- a reason -> count summary fully describes a single-rule exclusion, so
    nothing is lost by not retaining the excluded row itself).

    Returns nothing -- writes directly to `path`/`exclusions_path` from DuckDB, never routing the
    multi-million-row result through a pandas DataFrame (a second real crash, 2026-08-15: the
    first fix's `.fetchdf()` on ~7M rows with two list-typed columns -- source_author_idxs and
    the newer field_names -- materialized real Python object overhead pandas doesn't report and
    DuckDB's own `memory_limit` can't see. Callers needing the data read it back from `path`.

    Six checks, same precedence as apply_deterministic_filters() (first match wins), computed
    once per work_idx (not once per cluster-work pair, since corruption is a property of the work
    itself, not of any one AwardsCIF's relationship to it):
      1-2. exclusions.exclude_reason() -- disallowed type, filename-artifact title
      3-4. missing/implausible/future publication_year
      5. missing DOI
      6. corrupt authorship -- any authorship row (any author, not just a candidate's own) has a
         null author_idx or institution_idx
      7. raw_orcid logical impossibility -- any authorship row has raw_orcid populated while
         publication_year < 2012 (ORCID launched Oct 2012). Column-existence-guarded: raw_orcid
         doesn't exist in the authorships table today, so this check is a no-op until a future
         raw-data iteration adds it -- confirmed not to hard-fail on the missing column.
      8. missing source_id on an article

    Returns the survivor DataFrame (work-row shape, no coauthor/institution detail -- see
    _WORK_ROW_COLUMNS) and persists it, plus a (cluster_id, reason, n) exclusion-count summary,
    to `path`/`exclusions_path`.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        if own_con:
            con.execute("SET enable_progress_bar = false")
            con.execute("SET memory_limit = '24GB'")
            con.execute(f"SET temp_directory = '{DUCKDB_TMP_DIR}'")
        exclusions.register(con)

        # Small (candidate-pair count, ~100K rows at full scale, two short strings each) --
        # built via executemany(), not pandas, for consistency (this size was never the actual
        # problem, but no reason to keep a pandas dependency here either).
        cand_rows = [(c.cluster_id, oid) for c in clusters for oid in c.oax_candidates]
        con.execute("CREATE OR REPLACE TEMP TABLE _cand_raw (cluster_id VARCHAR, oax_id VARCHAR)")
        if cand_rows:
            con.executemany("INSERT INTO _cand_raw VALUES (?, ?)", cand_rows)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _cand_map AS
            SELECT cluster_id,
                   TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) AS author_idx
            FROM _cand_raw
            WHERE TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) IS NOT NULL
        """)

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _cluster_works AS
            SELECT m.cluster_id, a.work_idx, ARRAY_AGG(DISTINCT a.author_idx) AS source_author_idxs
            FROM read_parquet('{auth_glob}') a
            JOIN _cand_map m ON m.author_idx = a.author_idx
            GROUP BY m.cluster_id, a.work_idx
        """)

        # raw_orcid: column-existence guard -- must not hard-fail while the column is absent.
        auth_cols = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{auth_glob}') LIMIT 0"
        ).fetchall()}
        has_raw_orcid = "raw_orcid" in auth_cols
        raw_orcid_select = (
            "bool_or(a.raw_orcid IS NOT NULL) AS any_raw_orcid" if has_raw_orcid
            else "FALSE AS any_raw_orcid"
        )

        # per-work corruption aggregate -- computed once per work_idx, not once per
        # (cluster_id, work_idx) pair, since these facts are properties of the work itself.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _work_corruption AS
            SELECT
                a.work_idx,
                bool_or(a.author_idx IS NULL) AS any_null_author,
                bool_or(a.institution_idx IS NULL) AS any_null_inst,
                {raw_orcid_select}
            FROM read_parquet('{auth_glob}') a
            JOIN (SELECT DISTINCT work_idx FROM _cluster_works) w ON w.work_idx = a.work_idx
            GROUP BY a.work_idx
        """)

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _work_details AS
            SELECT w.work_idx, w.publication_year, w.cited_by_count, w.type, w.doi, w.title, w.source_id
            FROM read_parquet('{work_glob}') w
            JOIN (SELECT DISTINCT work_idx FROM _cluster_works) ids ON ids.work_idx = w.work_idx
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _best_topic AS
            SELECT work_idx, subfield_idx, subfield_name, field_name, domain_name
            FROM (
                SELECT t.work_idx, t.subfield_idx, t.subfield_name, t.field_name, t.domain_name,
                       ROW_NUMBER() OVER (PARTITION BY t.work_idx ORDER BY t.score DESC) AS rn
                FROM read_parquet('{topic_glob}') t
                JOIN (SELECT DISTINCT work_idx FROM _cluster_works) ids ON ids.work_idx = t.work_idx
            ) WHERE rn = 1
        """)
        # Every distinct field across ALL of a work's topics (up to 3), not just the single
        # highest-scoring one -- see _WORK_ROW_COLUMNS' docstring for why: the top-vs-2nd topic
        # score gap is a near-tie in the median case, so "best topic only" was arbitrarily
        # discarding real field information for a majority of multi-topic works.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _all_fields AS
            SELECT t.work_idx, ARRAY_AGG(DISTINCT t.field_name) AS field_names
            FROM read_parquet('{topic_glob}') t
            JOIN (SELECT DISTINCT work_idx FROM _cluster_works) ids ON ids.work_idx = t.work_idx
            WHERE t.field_name IS NOT NULL
            GROUP BY t.work_idx
        """)
        # Same idea, one level finer -- every distinct SUBFIELD across all of a work's topics
        # (2026-08-16, for the per-candidate subfield+HEP keep/drop rule). Same near-tie
        # reasoning applies, just more so: subfield has ~252 categories vs field's ~25, so two
        # of a work's topics are even less likely to share a subfield than a field.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _all_subfields AS
            SELECT t.work_idx, ARRAY_AGG(DISTINCT t.subfield_name) AS subfield_names
            FROM read_parquet('{topic_glob}') t
            JOIN (SELECT DISTINCT work_idx FROM _cluster_works) ids ON ids.work_idx = t.work_idx
            WHERE t.subfield_name IS NOT NULL
            GROUP BY t.work_idx
        """)

        # Single reason column, same first-match-wins precedence as apply_deterministic_filters(),
        # plus the new raw_orcid check inserted after corrupt_authorship (both are "any authorship
        # row" checks).
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _work_reason AS
            SELECT
                d.work_idx,
                CASE
                    WHEN exclude_reason(d.title, d.type, d.doi) IS NOT NULL
                        THEN exclude_reason(d.title, d.type, d.doi)
                    WHEN d.publication_year IS NULL THEN 'missing_year'
                    WHEN d.publication_year < {dedup.MIN_PUB_YEAR} THEN 'implausible_year'
                    WHEN d.publication_year > {dedup.MAX_PUB_YEAR} THEN 'future_year'
                    WHEN d.doi IS NULL THEN 'missing_doi'
                    WHEN wc.any_null_author OR wc.any_null_inst THEN 'corrupt_authorship'
                    WHEN wc.any_raw_orcid AND d.publication_year < 2012 THEN 'raw_orcid_impossible'
                    WHEN d.source_id IS NULL AND d.type = 'article' THEN 'missing_source'
                    ELSE NULL
                END AS reason
            FROM _work_details d
            JOIN _work_corruption wc ON wc.work_idx = d.work_idx
        """)

        # Written directly from DuckDB to parquet -- never materialized as a pandas DataFrame.
        # At full scale this is ~7M rows with two list-typed columns (source_author_idxs,
        # field_names); pandas stores list-typed columns as generic Python objects per cell,
        # real memory overhead .fetchdf() would pay for on top of (and invisible to)
        # DuckDB's own memory_limit -- exactly the class of problem this whole fix started from.
        path.parent.mkdir(parents=True, exist_ok=True)
        exclusions_path.parent.mkdir(parents=True, exist_ok=True)

        con.execute(f"""
            COPY (
                SELECT
                    cw.cluster_id, cw.work_idx, cw.source_author_idxs,
                    d.publication_year, d.cited_by_count, d.type, d.title, d.doi, d.source_id,
                    bt.subfield_idx, bt.subfield_name, sf.subfield_names,
                    bt.field_name, af.field_names, bt.domain_name
                FROM _cluster_works cw
                JOIN _work_reason r ON r.work_idx = cw.work_idx
                JOIN _work_details d ON d.work_idx = cw.work_idx
                LEFT JOIN _best_topic bt ON bt.work_idx = cw.work_idx
                LEFT JOIN _all_fields af ON af.work_idx = cw.work_idx
                LEFT JOIN _all_subfields sf ON sf.work_idx = cw.work_idx
                WHERE r.reason IS NULL
            ) TO '{path}' (FORMAT PARQUET)
        """)

        con.execute(f"""
            COPY (
                SELECT cw.cluster_id, r.reason, COUNT(*) AS n
                FROM _cluster_works cw
                JOIN _work_reason r ON r.work_idx = cw.work_idx
                WHERE r.reason IS NOT NULL
                GROUP BY cw.cluster_id, r.reason
            ) TO '{exclusions_path}' (FORMAT PARQUET)
        """)

        n_survivors = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        reason_totals = con.execute(f"""
            SELECT reason, SUM(n) FROM read_parquet('{exclusions_path}') GROUP BY reason
        """).fetchall()
    finally:
        # These temp tables (several at ~9.5M rows each) are only needed within this function --
        # the result is already persisted to path/exclusions_path. A shared connection (the
        # driver script passes one `con` through every stage) does NOT auto-release TEMP TABLE
        # memory when a function returns, so an explicit drop is required or it accumulates
        # across every later stage sharing this connection (confirmed the hard way, 2026-08-16 --
        # a bare OOM kill with no Python traceback on the very next stage).
        for t in ["_cand_raw", "_cand_map", "_cluster_works", "_work_corruption", "_work_details",
                  "_best_topic", "_all_fields", "_all_subfields", "_work_reason"]:
            con.execute(f"DROP TABLE IF EXISTS {t}")
        if own_con:
            con.close()

    print(f"  Stage 1: {n_survivors:,} survivors persisted to {path}")
    print(f"  Stage 1 exclusions by reason: {dict(reason_totals)}")


def fetch_candidate_oeuvre(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[AwardsCIF]:
    """Populate AwardsCIF.oeuvre: the union of OpenAlex works reached by any of its
    oax_candidates. One batched pass over all clusters, not a per-cluster loop -- matches
    populate_oax_candidates()'s own "filter small set first" precedent (CLAUDE.md's documented
    30x-regression lesson).

    Collapses to one row per (cluster_id, work_idx) even when 2+ of a cluster's own candidate
    author_idx both claim the same work -- a different case from dedup_oeuvre()'s title-based
    preprint/published-version merge, handled here at fetch time so dedup.py needs no change.
    Also pulls every coauthor authorship row for the resulting work_idx set in one bulk query
    (excluding each cluster's own candidate author_idx), so later scoring functions never
    re-query the database per cluster.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        con.execute("SET enable_progress_bar = false")

        cand_rows = [(c.cluster_id, oid) for c in clusters for oid in c.oax_candidates]
        cand_df = pd.DataFrame(cand_rows, columns=["cluster_id", "oax_id"])
        con.register("_cand_raw", cand_df)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _cand_map AS
            SELECT cluster_id,
                   TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) AS author_idx
            FROM _cand_raw
            WHERE TRY_CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) IS NOT NULL
        """)

        own_author_idxs: dict[str, set[int]] = defaultdict(set)
        for cluster_id, author_idx in con.execute("SELECT cluster_id, author_idx FROM _cand_map").fetchall():
            own_author_idxs[cluster_id].add(author_idx)

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _cluster_works AS
            SELECT m.cluster_id, a.work_idx, ARRAY_AGG(DISTINCT a.author_idx) AS source_author_idxs
            FROM read_parquet('{AUTH_GLOB}') a
            JOIN _cand_map m ON m.author_idx = a.author_idx
            GROUP BY m.cluster_id, a.work_idx
        """)

        distinct_work_idxs = [r[0] for r in con.execute(
            "SELECT DISTINCT work_idx FROM _cluster_works"
        ).fetchall()]
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _work_ids AS SELECT UNNEST(?) AS work_idx",
            [distinct_work_idxs],
        )

        auth_rows = con.execute(f"""
            SELECT a.work_idx, a.author_idx, a.author_name, a.institution_idx
            FROM read_parquet('{AUTH_GLOB}') a
            JOIN _work_ids w ON w.work_idx = a.work_idx
        """).fetchall()
        authorships_by_work: dict[int, list[tuple]] = defaultdict(list)
        for work_idx, author_idx, author_name, institution_idx in auth_rows:
            authorships_by_work[work_idx].append((author_idx, author_name, institution_idx))

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _work_details AS
            SELECT w.work_idx, w.publication_year, w.cited_by_count, w.type, w.doi, w.title, w.source_id
            FROM read_parquet('{WORK_GLOB}') w
            JOIN _work_ids ids ON ids.work_idx = w.work_idx
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _best_topic AS
            SELECT work_idx, subfield_idx, subfield_name, field_name, domain_name
            FROM (
                SELECT t.work_idx, t.subfield_idx, t.subfield_name, t.field_name, t.domain_name,
                       ROW_NUMBER() OVER (PARTITION BY t.work_idx ORDER BY t.score DESC) AS rn
                FROM read_parquet('{TOPIC_GLOB}') t
                JOIN _work_ids ids ON ids.work_idx = t.work_idx
            ) WHERE rn = 1
        """)

        details_rows = con.execute("""
            SELECT d.work_idx, d.publication_year, d.cited_by_count, d.type, d.doi, d.title, d.source_id,
                   bt.subfield_idx, bt.subfield_name, bt.field_name, bt.domain_name
            FROM _work_details d
            LEFT JOIN _best_topic bt ON bt.work_idx = d.work_idx
        """).fetchall()
        details_by_work = {r[0]: r for r in details_rows}

        cw_rows = con.execute("SELECT cluster_id, work_idx, source_author_idxs FROM _cluster_works").fetchall()
    finally:
        if own_con:
            con.close()

    by_cluster_id = {c.cluster_id: c for c in clusters}
    grouped: dict[str, list] = defaultdict(list)
    for cluster_id, work_idx, source_author_idxs in cw_rows:
        grouped[cluster_id].append((work_idx, source_author_idxs))

    for cluster_id, rows in grouped.items():
        c = by_cluster_id.get(cluster_id)
        if c is None:
            continue
        own = own_author_idxs.get(cluster_id, set())
        for work_idx, source_author_idxs in rows:
            d = details_by_work.get(work_idx)
            if d is None:
                continue
            (_, pub_year, cited, type_, doi, title, source_id,
             sf_idx, sf_name, field_name, domain_name) = d

            # Every authorship row for this work (this cluster's own candidate(s) AND coauthors)
            # -- counted here, before coauthor_* below filters Nones out of its own typed lists,
            # so apply_deterministic_filters()'s "corrupt_authorship" check has the raw fact to
            # act on (a recorded count, not a bare verdict -- see CandidateWork's own docstring).
            all_rows = authorships_by_work.get(work_idx, [])
            n_null_author = sum(1 for a, _, _ in all_rows if a is None)
            n_null_inst = sum(1 for _, _, i in all_rows if i is None)

            own_rows = [(a, n, i) for a, n, i in all_rows if a in own]
            coauthors = [(a, n, i) for a, n, i in all_rows if a not in own]
            c.oeuvre.append(CandidateWork(
                work_idx=work_idx,
                source_author_idxs=sorted(source_author_idxs),
                publication_year=pub_year,
                cited_by_count=cited or 0,
                type=type_,
                title=title,
                doi=doi,
                source_id=source_id,
                subfield_idx=sf_idx,
                subfield_name=sf_name,
                field_name=field_name,
                domain_name=domain_name,
                own_institution_idxs=sorted({i for _, _, i in own_rows if i is not None}),
                coauthor_author_idxs=[a for a, _, _ in coauthors if a is not None],
                coauthor_names=[n for _, n, _ in coauthors if n],
                coauthor_institution_idxs=[i for _, _, i in coauthors if i is not None],
                signals={
                    "null_author_idx_count": n_null_author,
                    "null_institution_idx_count": n_null_inst,
                },
            ))

    for c in clusters:
        c.record_event("oeuvre_fetched", n_works=len(c.oeuvre), n_candidate_authors=len(c.oax_candidates))

    return clusters


def apply_deterministic_filters(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Hard excludes -- never scored, never combined with the softer signals below (principle:
    deterministic filters kept separate from soft coherence signals). Checked in this order,
    first match wins:

      1. exclusions.exclude_reason() -- disallowed OpenAlex `type`, filename-artifact titles.
      2. publication_year missing or outside dedup.MIN_PUB_YEAR..MAX_PUB_YEAR.
      3. doi IS NULL.
      4. any authorship row for this work (this cluster's own candidate(s) or a coauthor) has
         a null author_idx or institution_idx -- recorded by fetch_candidate_oeuvre() as
         signals["null_author_idx_count"]/["null_institution_idx_count"].
      5. source_id IS NULL AND type == 'article'.

    A raw_orcid-based corruption filter (raw_orcid populated AND pub_year < 2013 -- a logical
    impossibility, ORCID launched Oct 2012) is deliberately NOT included here: verified this
    session that no authorships table variant this pipeline reads (compact/xpac/xpac_raw/the
    manually-regenerated authorships_hep.parquet) carries a raw_orcid column at all -- future
    work, not this pass.
    """
    n_by_reason: dict[str, int] = defaultdict(int)

    for c in clusters:
        for w in c.oeuvre:
            if not w.included:
                continue

            reason = exclusions.exclude_reason(w.title, w.type, w.doi)
            if reason is None:
                if w.publication_year is None:
                    reason = "missing_year"
                elif not (dedup.MIN_PUB_YEAR <= w.publication_year <= dedup.MAX_PUB_YEAR):
                    reason = "implausible_year" if w.publication_year < dedup.MIN_PUB_YEAR else "future_year"
                elif w.doi is None:
                    reason = "missing_doi"
                elif w.signals.get("null_author_idx_count", 0) > 0 or w.signals.get("null_institution_idx_count", 0) > 0:
                    reason = "corrupt_authorship"
                elif w.source_id is None and w.type == "article":
                    reason = "missing_source"

            if reason is not None:
                w.included = False
                w.exclusion_reason = reason
                n_by_reason[reason] += 1

    for c in clusters:
        c.record_event(
            "deterministic_filters_applied",
            n_included=len(c.works),
            n_excluded=sum(1 for w in c.oeuvre if not w.included),
        )

    print(f"  Deterministic filters: {dict(n_by_reason)}")
    return clusters


def dedup_oeuvre(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[AwardsCIF]:
    """Title-based version-merge (preprint + published version of the same paper) -- calls
    dedup.create_deduped_works() unmodified over each cluster's still-included works
    (arc_id=cluster_id), then re-joins its output back onto the richer CandidateWork objects to
    recover doi/source_author_idxs/coauthor fields its fixed output schema drops, applying its
    corrected (earliest-year, summed-citations) publication_year/cited_by_count. Rows present
    before but absent after are the merged-away duplicate versions -- marked included=False,
    exclusion_reason="duplicate_version" (their citations are already folded into the surviving
    version -- an accounting bucket, not a contamination exclusion, kept visually distinct from
    apply_deterministic_filters()/apply_subfield_filter()'s exclusions).

    A genuinely different case from fetch_candidate_oeuvre()'s (cluster_id, work_idx) collapse
    (2+ of a cluster's own candidate author_idx claiming the SAME work_idx) -- dedup.py's job is
    different work_idx values that are the same real paper (e.g. preprint + published version).
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        con.execute("SET enable_progress_bar = false")

        rows = []
        by_cluster_work: dict[tuple[str, int], CandidateWork] = {}
        for c in clusters:
            for w in c.works:
                rows.append({
                    "arc_id": c.cluster_id, "work_idx": w.work_idx,
                    "publication_year": w.publication_year, "cited_by_count": w.cited_by_count,
                    "type": w.type, "field_name": w.field_name, "subfield_name": w.subfield_name,
                    "domain_name": w.domain_name, "title": w.title, "doi": w.doi,
                })
                by_cluster_work[(c.cluster_id, w.work_idx)] = w

        if not rows:
            return clusters

        df = pd.DataFrame(rows)
        con.register("_pre_dedup", df)
        dedup.create_deduped_works(con, source_sql="_pre_dedup", out_table="_deduped")

        deduped_rows = con.execute(
            "SELECT arc_id, work_idx, publication_year, cited_by_count FROM _deduped"
        ).fetchall()
    finally:
        if own_con:
            con.close()

    survivors: set[tuple[str, int]] = set()
    for arc_id, work_idx, pub_year, cited in deduped_rows:
        w = by_cluster_work.get((arc_id, work_idx))
        if w is None:
            continue
        w.publication_year = pub_year
        w.cited_by_count = cited
        survivors.add((arc_id, work_idx))

    n_dup = 0
    for key, w in by_cluster_work.items():
        if key not in survivors:
            w.included = False
            w.exclusion_reason = "duplicate_version"
            n_dup += 1

    for c in clusters:
        c.record_event("oeuvre_deduped", n_included=len(c.works))

    print(f"  Title-dedup: {n_dup} duplicate-version works merged away")
    return clusters


def apply_subfield_filter(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Per cluster, does a candidate work's OpenAlex FIELD match the OAX fields implied by this
    cluster's own PRIMARY FOR2020 codes? Reuses cluster_checks.for2020_primary_fields() unchanged
    -- the same shared, tested function is_suspicious_for2020()/division_mismatch_for2020()
    already use for the ARC-internal cross-division check, applied here per-work instead.
    Non-circular: for2020_codes comes from ARC's own grant records (raw_json.csv's full
    field-of-research list, resolved via Resolver()), never from the candidate's own (possibly
    contaminated) OpenAlex-side oeuvre -- unlike scoring a work against the candidate's own
    accumulated topic mix, which was considered and dropped as circular.

    OAX FIELD (~25 categories), not subfield (252) or ANZSRC's own division (~22, administrative
    boundaries that don't line up with OpenAlex's content-derived ones -- see cluster_checks.py's
    module docstring) -- an earlier version of this filter used literal subfield-string matching
    against a single ARC primary code and was far too aggressive (one real example dropped 158 of
    160 included works for a geochronologist whose own OpenAlex output spans several
    OpenAlex-adjacent-but-not-identical subfields ARC never tagged as his single primary code).
    CandidateWork.field_name needs no resolution at all here -- it's already OpenAlex's own
    field-level classification straight off work_topics, fetched by fetch_candidate_oeuvre().

    If a cluster's for2020_codes carry no primary code mapping to any OAX field at all, this
    filter doesn't apply to it -- same "no mapped reference, don't penalize" guard
    division_mismatch_for2020_pairwise() uses. Works with no assigned field are left alone.
    """
    n_mismatch = 0
    n_no_reference = 0

    for c in clusters:
        target_fields = for2020_primary_fields(c.for2020_codes)
        if not target_fields:
            n_no_reference += 1
            continue

        for w in c.works:
            if w.field_name is None:
                continue
            match = w.field_name in target_fields
            w.signals["field_match"] = match
            if not match:
                w.included = False
                w.exclusion_reason = "field_mismatch"
                n_mismatch += 1

        c.record_event(
            "subfield_filter_applied",
            target_fields=sorted(target_fields),
            n_included=len(c.works),
        )

    print(f"  Field filter: {n_mismatch} works excluded, {n_no_reference} clusters had no "
          f"mapped reference field (skipped)")
    return clusters


def apply_field_filter_stage3(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
    path: Path = STAGE3_SURVIVORS,
    exclusions_path: Path = STAGE3_EXCLUSIONS,
    stage1_path: Path = STAGE1_SURVIVORS,
) -> None:
    """Stage 3 of the 2026-08-15 full-scale-OOM fix: reads the Stage 1 survivor set (persisted
    by fetch_and_filter_stage1()) and applies field-match logic -- same
    cluster_checks.for2020_primary_fields() call, same "no mapped reference, don't penalize"
    guard, same "works with no assigned field are left alone" rule as apply_subfield_filter() --
    entirely in DuckDB (read_parquet -> COPY ... TO parquet), never routing the multi-million-row
    Stage 1 data through a pandas DataFrame (see fetch_and_filter_stage1()'s docstring -- a real
    second crash on the first attempt at this, `.fetchdf()` on ~7M rows with two list-typed
    columns).

    Matches against `field_names` (every distinct field across a work's topics), not the single
    `field_name` apply_subfield_filter() uses -- a real finding (2026-08-15): the top-vs-2nd
    topic score gap is a near-tie in the median case, so matching only the single best-scoring
    topic's field was arbitrarily discarding real field information for a majority of
    multi-topic works (64.2% of touched works have >1 distinct field; 14.6% of best-topic-only
    field exclusions had another topic that actually matched the target field).

    Exclusion accounting matches Stage 1's shape: a per-(cluster_id, reason='field_mismatch', n)
    summary, not a retained per-work record -- this also runs on a still-large population, so full
    per-work retention would reintroduce real cost (see the plan file's "provenance shape differs
    by stage" section).

    `clusters`' per-cluster target-field lookup is small (tens of thousands of rows at most,
    ARC-declared FOR2020 codes -> OAX fields) -- built directly via executemany(), not pandas,
    even though its size was never the actual problem.
    """
    allowed_rows = []
    no_reference_rows = []
    for c in clusters:
        target_fields = for2020_primary_fields(c.for2020_codes)
        if not target_fields:
            no_reference_rows.append((c.cluster_id,))
            continue
        for f in target_fields:
            allowed_rows.append((c.cluster_id, f))

    own_con = con is None
    con = con or duckdb.connect()
    try:
        if own_con:
            con.execute("SET enable_progress_bar = false")
            con.execute("SET memory_limit = '24GB'")
            con.execute(f"SET temp_directory = '{DUCKDB_TMP_DIR}'")

        con.execute("CREATE OR REPLACE TEMP TABLE _allowed (cluster_id VARCHAR, field_name VARCHAR)")
        if allowed_rows:
            con.executemany("INSERT INTO _allowed VALUES (?, ?)", allowed_rows)
        con.execute("CREATE OR REPLACE TEMP TABLE _no_ref (cluster_id VARCHAR)")
        if no_reference_rows:
            con.executemany("INSERT INTO _no_ref VALUES (?)", no_reference_rows)

        path.parent.mkdir(parents=True, exist_ok=True)
        exclusions_path.parent.mkdir(parents=True, exist_ok=True)

        match_expr = """
            s.cluster_id IN (SELECT cluster_id FROM _no_ref)
            OR s.field_names IS NULL
            OR EXISTS (
                SELECT 1 FROM _allowed a
                WHERE a.cluster_id = s.cluster_id AND list_contains(s.field_names, a.field_name)
            )
        """
        con.execute(f"""
            COPY (
                SELECT s.* FROM read_parquet('{stage1_path}') s WHERE {match_expr}
            ) TO '{path}' (FORMAT PARQUET)
        """)

        con.execute(f"""
            COPY (
                SELECT s.cluster_id, 'field_mismatch' AS reason, COUNT(*) AS n
                FROM read_parquet('{stage1_path}') s
                WHERE NOT ({match_expr})
                GROUP BY s.cluster_id
            ) TO '{exclusions_path}' (FORMAT PARQUET)
        """)

        n_survivors = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        n_mismatch = con.execute(
            f"SELECT COALESCE(SUM(n), 0) FROM read_parquet('{exclusions_path}')"
        ).fetchone()[0]
    finally:
        # See fetch_and_filter_stage1()'s matching cleanup comment -- same shared-connection
        # accumulation risk applies here.
        for t in ["_allowed", "_no_ref"]:
            con.execute(f"DROP TABLE IF EXISTS {t}")
        if own_con:
            con.close()

    print(f"  Stage 3: {n_survivors:,} survivors persisted to {path}")
    print(f"  Stage 3: {n_mismatch:,} works excluded (field_mismatch), "
          f"{len(no_reference_rows)} clusters had no mapped reference field (skipped)")


def compute_subfield_hep_signals(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
    path: Path = STAGE_SUBFIELD_HEP_SIGNALS,
    stage3_path: Path = STAGE3_SURVIVORS,
    auth_glob: str = AUTH_GLOB,
) -> None:
    """Per-candidate, per-work keep/drop signal (2026-08-16 reframing -- see the plan file's
    "Reframing" section): does a Stage-3-surviving work's OAX subfield match the AwardsCIF's own
    subfields, and does its institution's HEP code match the AwardsCIF's own HEP codes?

    Both are recorded as explicit tri-state signals (True/False/None), not combined into a
    verdict here -- "keep if both match, weaker/ambiguous if only one" is a decision for whoever
    reads this data, not baked in at persist time (same "persist components, don't bake in
    threshold" principle as institution_arc_match/coinvestigator_match). None (not False) when
    there's genuinely nothing to evaluate against -- a work with no subfield-classified topics,
    a work whose own-candidate institution never resolves to a HEP code, or an AwardsCIF with no
    subfield/HEP reference at all -- so "not evaluated" is never silently read as "evaluated, no
    match" (same principle as the deferred coauthor_count<30 cap's None state).

    The own-candidate institution fetch (source_author_idxs' own authorship row on each work,
    not all authors on it) is NOT the same cost problem as the still-deferred coauthor pull: it's
    bounded by (candidate, work) pairs, not by total authors on a work, so no cap is needed here.
    """
    subfield_rows = [
        (c.cluster_id, sf)
        for c in clusters for sf in for2020_primary_subfields(c.for2020_codes)
    ]
    hep_rows = [(c.cluster_id, hep) for c in clusters for hep in c.hep_codes]
    inst_hep_crosswalk = _load_institution_hep_crosswalk()
    inst_hep_rows = list(inst_hep_crosswalk.items())

    own_con = con is None
    con = con or duckdb.connect()
    try:
        if own_con:
            con.execute("SET enable_progress_bar = false")
            con.execute("SET memory_limit = '24GB'")
            con.execute(f"SET temp_directory = '{DUCKDB_TMP_DIR}'")

        con.execute("CREATE OR REPLACE TEMP TABLE _c_subfields (cluster_id VARCHAR, subfield_name VARCHAR)")
        if subfield_rows:
            con.executemany("INSERT INTO _c_subfields VALUES (?, ?)", subfield_rows)
        con.execute("CREATE OR REPLACE TEMP TABLE _c_hep (cluster_id VARCHAR, hep_code VARCHAR)")
        if hep_rows:
            con.executemany("INSERT INTO _c_hep VALUES (?, ?)", hep_rows)
        con.execute("CREATE OR REPLACE TEMP TABLE _inst_hep (institution_idx BIGINT, hep_code VARCHAR)")
        if inst_hep_rows:
            con.executemany("INSERT INTO _inst_hep VALUES (?, ?)", inst_hep_rows)

        # Own-candidate institution per surviving work.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _own_inst AS
            SELECT s.cluster_id, s.work_idx, ARRAY_AGG(DISTINCT a.institution_idx) AS own_inst_idxs
            FROM read_parquet('{stage3_path}') s
            JOIN read_parquet('{auth_glob}') a
              ON a.work_idx = s.work_idx AND list_contains(s.source_author_idxs, a.author_idx)
            WHERE a.institution_idx IS NOT NULL
            GROUP BY s.cluster_id, s.work_idx
        """)
        # Resolve those institutions to HEP codes.
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _own_hep AS
            SELECT oi.cluster_id, oi.work_idx, ARRAY_AGG(DISTINCT ih.hep_code) AS hep_codes
            FROM _own_inst oi, UNNEST(oi.own_inst_idxs) AS u(institution_idx)
            JOIN _inst_hep ih ON ih.institution_idx = u.institution_idx
            GROUP BY oi.cluster_id, oi.work_idx
        """)

        path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
                SELECT
                    s.cluster_id, s.work_idx,
                    CASE
                        WHEN s.subfield_names IS NULL THEN NULL
                        WHEN NOT EXISTS (SELECT 1 FROM _c_subfields cs WHERE cs.cluster_id = s.cluster_id) THEN NULL
                        ELSE EXISTS (
                            SELECT 1 FROM _c_subfields cs
                            WHERE cs.cluster_id = s.cluster_id AND list_contains(s.subfield_names, cs.subfield_name)
                        )
                    END AS subfield_match,
                    CASE
                        WHEN oh.hep_codes IS NULL THEN NULL
                        WHEN NOT EXISTS (SELECT 1 FROM _c_hep ch WHERE ch.cluster_id = s.cluster_id) THEN NULL
                        ELSE EXISTS (
                            SELECT 1 FROM _c_hep ch
                            WHERE ch.cluster_id = s.cluster_id AND list_contains(oh.hep_codes, ch.hep_code)
                        )
                    END AS hep_match
                FROM read_parquet('{stage3_path}') s
                LEFT JOIN _own_hep oh ON oh.cluster_id = s.cluster_id AND oh.work_idx = s.work_idx
            ) TO '{path}' (FORMAT PARQUET)
        """)

        n_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        counts = con.execute(f"""
            SELECT
                SUM(CASE WHEN subfield_match THEN 1 ELSE 0 END) AS sf_true,
                SUM(CASE WHEN subfield_match = FALSE THEN 1 ELSE 0 END) AS sf_false,
                SUM(CASE WHEN subfield_match IS NULL THEN 1 ELSE 0 END) AS sf_null,
                SUM(CASE WHEN hep_match THEN 1 ELSE 0 END) AS hep_true,
                SUM(CASE WHEN hep_match = FALSE THEN 1 ELSE 0 END) AS hep_false,
                SUM(CASE WHEN hep_match IS NULL THEN 1 ELSE 0 END) AS hep_null,
                SUM(CASE WHEN subfield_match AND hep_match THEN 1 ELSE 0 END) AS both_true
            FROM read_parquet('{path}')
        """).fetchone()
    finally:
        # See fetch_and_filter_stage1()'s matching cleanup comment -- same shared-connection
        # accumulation risk applies here. This is the last stage in the driver today, but future
        # stages appended after this one would inherit the same risk if this weren't dropped.
        for t in ["_c_subfields", "_c_hep", "_inst_hep", "_own_inst", "_own_hep"]:
            con.execute(f"DROP TABLE IF EXISTS {t}")
        if own_con:
            con.close()

    print(f"  subfield/HEP signals: {n_rows:,} rows persisted to {path}")
    print(f"  subfield_match: {counts[0]:,} True / {counts[1]:,} False / {counts[2]:,} None")
    print(f"  hep_match:      {counts[3]:,} True / {counts[4]:,} False / {counts[5]:,} None")
    print(f"  both True (strong keep signal): {counts[6]:,}")


def _inst_id_to_idx(inst_id: str) -> int | None:
    """AwardsCIF.inst_arr entries are OpenAlex institution ID strings
    ("https://openalex.org/I<n>"), confirmed against admin_orgs.csv's institution_id column --
    same prefix-strip-and-cast pattern used throughout this codebase for author_idx."""
    try:
        return int(inst_id.rsplit("I", 1)[-1])
    except (ValueError, AttributeError):
        return None


def score_institution_coherence(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Records signals["institution_arc_match"]: does this work's own-candidate institution
    (own_institution_idxs, from the authorship row(s) matching source_author_idxs) appear in
    this cluster's own ARC-declared inst_arr? Non-circular -- inst_arr comes from ARC's own
    grant records (admin_org), never from the candidate's own OpenAlex-side oeuvre.

    Diagnostic only in this pass -- does not gate `included`. Combining this with the other
    soft signals (coauthor-ARC-corroboration, identity-cluster component) into a weighted
    inclusion score needs empirical calibration against known contamination cases, not done
    here (see oeuvre_build.py's module docstring / the plan file)."""
    for c in clusters:
        target = {idx for iid in c.inst_arr for idx in [_inst_id_to_idx(iid)] if idx is not None}
        for w in c.works:
            w.signals["institution_arc_match"] = bool(target and set(w.own_institution_idxs) & target)

    return clusters


def score_coauthor_arc_corroboration(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Records signals["coinvestigator_match"]: does any of a work's coauthor_names match one
    of this cluster's own recorded ARC co-investigators (_load_coinvestigator_names(), already
    used by refine_clusters()'s split_multi_name_clusters() -- reused unchanged, restricted
    here to this cluster's own grant_code(s)) -- the user's own live idea, not previously built
    anywhere: a real work with an ARC-recorded co-investigator as a coauthor is externally
    corroborated as genuinely belonging to this person, independent of anything OpenAlex itself
    says. Diagnostic only in this pass -- does not gate `included` (see
    score_institution_coherence()'s docstring for why)."""
    coinv_by_grant = _load_coinvestigator_names()

    for c in clusters:
        own_names = {_norm_full(it.full_name) for it in c.items}
        grant_codes = {it.grant_code for it in c.items}
        coinv_names = {
            _norm_full(name) for gc in grant_codes for name in coinv_by_grant.get(gc, [])
        } - own_names

        for w in c.works:
            w.signals["coinvestigator_match"] = bool(
                coinv_names and any(_norm_full(n) in coinv_names for n in w.coauthor_names)
            )

    return clusters


def build_oeuvre(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[AwardsCIF]:
    """Composes the full roadmap-step-2 pipeline in order, same plain-pipeline style as
    src/utils/awards_cif.py's refine_clusters(): fetch -> deterministic filters -> title-dedup
    -> field filter -> institution/coauthor scoring.

    This pass's actual inclusion gate is fully auditable without calibration: deterministic
    corruption/type/year/doi filters + the field filter. The two scoring functions record
    real, useful signals for the later definitive-evidence gate (roadmap step 4) but
    deliberately do not gate `included` themselves yet -- combining them into a weighted score
    needs empirical calibration against known contamination cases, not done here (see the plan
    file's "Not built this pass" section).

    A third scoring function, score_identity_clusters() (shared-coauthor/shared-institution
    union-find grouping), was dropped entirely 2026-08-15 -- confirmed unreliable by construction,
    not just weak: it assumes works connected by a shared coauthor/institution are the same
    person, but there's no evidence a candidate author_idx is ever "pure," and OpenAlex's own
    merge errors most often happen between genuinely close real people (colleagues, frequent
    co-authors) -- exactly the case that would show up as strongly *connected* in this graph. The
    technique's blind spot sits precisely where contamination is most dangerous, so it was removed
    rather than kept as a weak diagnostic. analysis/utils/identity_clustering.py (its only
    consumer) and its test file were deleted as part of the same change.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        clusters = fetch_candidate_oeuvre(clusters, con)
        clusters = apply_deterministic_filters(clusters)
        clusters = dedup_oeuvre(clusters, con)
        clusters = apply_subfield_filter(clusters)
        clusters = score_institution_coherence(clusters)
        clusters = score_coauthor_arc_corroboration(clusters)
    finally:
        if own_con:
            con.close()
    return clusters
