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

from config.settings import OPENALEX_COMPACT_DIR
from src.utils.awards_cif import AwardsCIF, CandidateWork, _load_coinvestigator_names, _norm_full
from src.utils.for_resolve import oax_subfield_name
from analysis.utils import dedup, exclusions

AUTH_GLOB  = str(OPENALEX_COMPACT_DIR / "authorships" / "*.parquet")
WORK_GLOB  = str(OPENALEX_COMPACT_DIR / "works" / "*.parquet")
TOPIC_GLOB = str(OPENALEX_COMPACT_DIR / "work_topics" / "*.parquet")


def fetch_candidate_oeuvre(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[AwardsCIF]:
    """Populate AwardsCIF.oeuvre: the union of OpenAlex works reached by any of its
    oax_candidates. One batched pass over all clusters, not a per-cluster loop -- matches
    populate_oax_candidates()'s and identity_clustering.py's own "filter small set first"
    precedent (CLAUDE.md's documented 30x-regression lesson).

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
    """Per cluster, does a candidate work's OpenAlex subfield match the subfields implied by
    this cluster's OWN ARC-declared for_codes? Reuses for_resolve.oax_subfield_name() unchanged
    -- the same, already-production-validated mapping 04_resolve_links.py's _field_score uses
    for candidate-author disambiguation, applied here per-work instead. Non-circular: for_codes
    comes from ARC's own grant records, never from the candidate's own (possibly contaminated)
    OpenAlex-side oeuvre -- unlike scoring a work against the candidate's own accumulated topic
    mix (which is what this replaces; that approach was considered and dropped as circular).

    If a cluster's for_codes don't map to any OAX subfield at all, this filter doesn't apply to
    it -- same "no mapped reference, don't penalize" guard _field_score already uses. Works with
    no assigned subfield are left alone (nothing to compare).
    """
    n_mismatch = 0
    n_no_reference = 0

    for c in clusters:
        target_subfields = {oax_subfield_name(code) for code in c.for_codes} - {None}
        if not target_subfields:
            n_no_reference += 1
            continue

        for w in c.works:
            if w.subfield_name is None:
                continue
            match = w.subfield_name in target_subfields
            w.signals["subfield_match"] = match
            if not match:
                w.included = False
                w.exclusion_reason = "subfield_mismatch"
                n_mismatch += 1

        c.record_event(
            "subfield_filter_applied",
            target_subfields=sorted(target_subfields),
            n_included=len(c.works),
        )

    print(f"  Subfield filter: {n_mismatch} works excluded, {n_no_reference} clusters had no "
          f"mapped reference subfield (skipped)")
    return clusters
