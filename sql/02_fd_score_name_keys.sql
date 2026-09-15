-- Frequency-distribution (institution/subfield) scoring for AcifOaxLinker.block()'s own
-- candidate pairs. This file replaced (2026-09-15) an earlier standalone version of itself --
-- same name, different content -- archived to
-- ZARCHIVE/sql_archive_20260915/02_fd_score_name_keys.sql because that version's own Stage 1-3
-- duplicated work AcifOaxLinker's __init__ now does once: the institution crosswalk and
-- arc_subfield_fd/arc_institution_fd are already built (as data.arc_subfield_fd_v2/
-- arc_institution_fd_v2, sql/04_acif_linker_setup.sql), and OAX-side FD no longer needs
-- computing at all -- 00b_extract_oax.py's own Phase 4 already persists it population-wide
-- (oax_subfield_fd.parquet/oax_institution_fd.parquet), so this file reads those directly
-- instead of recomputing a candidate-scoped version. Stage 1-3 logic below (normalize,
-- histogram-intersection, CASE-WHEN scoring) is otherwise unchanged from the archived version.
--
-- Depends on: data.blk_candidate_pairs (block()), data.arc_subfield_fd_v2/arc_institution_fd_v2
-- (__init__), oax_subfield_fd.parquet/oax_institution_fd.parquet (00b_extract_oax.py Phase 4).

ATTACH IF NOT EXISTS '/home/lc/k/WORKING_ARC_PROJECT/processed/oax_provenance.duckdb' AS data;

-- ── Stage 1: normalize each side's counts to within-entity proportions (a histogram, not raw
--    counts -- a person with 200 works and one with 5 must be compared on SHAPE, not scale).
--    OAX side reads the population-wide parquet directly, no candidate scoping -- normalizing
--    is one pass over the whole table regardless (measured: see class-side diagnostics for the
--    actual cost at this scale, not assumed cheap just because Phase 4 itself was). -------------

CREATE OR REPLACE TEMP TABLE arc_inst_norm AS
SELECT cluster_id, institution_id, n * 1.0 / SUM(n) OVER (PARTITION BY cluster_id) AS prop
FROM data.arc_institution_fd_v2;

CREATE OR REPLACE TEMP TABLE arc_sf_norm AS
SELECT cluster_id, subfield, n * 1.0 / SUM(n) OVER (PARTITION BY cluster_id) AS prop
FROM data.arc_subfield_fd_v2;

CREATE OR REPLACE TEMP TABLE oax_inst_norm AS
SELECT author_idx, institution_id, n * 1.0 / SUM(n) OVER (PARTITION BY author_idx) AS prop
FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/oax_institution_fd.parquet');

CREATE OR REPLACE TEMP TABLE oax_sf_norm AS
SELECT author_idx, subfield_name, n * 1.0 / SUM(n) OVER (PARTITION BY author_idx) AS prop
FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/oax_subfield_fd.parquet');

-- ── Stage 2: histogram-intersection score per pair -- SUM(LEAST(arc_prop, oax_prop)) over
--    every key both sides have any weight on. NULL (not 0) when either side has no data at all
--    for that dimension, so "no overlap" and "nothing to compare" stay distinguishable
--    downstream (same discipline as the old fd_pair_scores' own has_arc_inst/has_oax_inst
--    flags). Joining against data.blk_candidate_pairs here is what actually scopes this down to
--    just the pairs Stage 1 (blocking) produced -- the join predicate, not a separate
--    candidate_author_idx pre-filter table. ------------------------------------------------------

CREATE OR REPLACE TEMP TABLE inst_overlap AS
SELECT p.arc_id, p.author_idx, SUM(LEAST(a.prop, o.prop)) AS overlap
FROM data.blk_candidate_pairs p
JOIN arc_inst_norm a ON a.cluster_id = p.arc_id
JOIN oax_inst_norm o ON o.author_idx = p.author_idx AND o.institution_id = a.institution_id
GROUP BY p.arc_id, p.author_idx;

CREATE OR REPLACE TEMP TABLE sf_overlap AS
SELECT p.arc_id, p.author_idx, SUM(LEAST(a.prop, o.prop)) AS overlap
FROM data.blk_candidate_pairs p
JOIN arc_sf_norm a ON a.cluster_id = p.arc_id
JOIN oax_sf_norm o ON o.author_idx = p.author_idx AND o.subfield_name = a.subfield
GROUP BY p.arc_id, p.author_idx;

CREATE OR REPLACE TEMP TABLE has_data AS
SELECT p.arc_id, p.author_idx,
       EXISTS (SELECT 1 FROM arc_inst_norm a WHERE a.cluster_id = p.arc_id) AS has_arc_inst,
       EXISTS (SELECT 1 FROM oax_inst_norm o WHERE o.author_idx = p.author_idx) AS has_oax_inst,
       EXISTS (SELECT 1 FROM arc_sf_norm a WHERE a.cluster_id = p.arc_id) AS has_arc_sf,
       EXISTS (SELECT 1 FROM oax_sf_norm o WHERE o.author_idx = p.author_idx) AS has_oax_sf
FROM data.blk_candidate_pairs p;

-- ── Stage 3: the actual score-filtering CASE WHEN -- a separate step from block()'s own
--    blocking CASE, per the sequential design. This file scores institution+subfield FD
--    overlap ONLY -- ORCID status is a different kind of signal (a direct identity check, not a
--    frequency-distribution comparison) and already lives on data.blk_candidate_pairs.orcid_check
--    (block()'s own Stage 6). It briefly appeared here too, as a top "orcid confirmed" priority
--    tier folded into this table's own priority_level/reason -- removed 2026-09-15, user-caught:
--    "the orcid score is not an fd_score() so why not leave it out" -- fd_score() re-surfacing a
--    signal that belongs to a different stage is exactly the kind of two-places-computing-the-
--    same-fact drift this project has repeatedly found and fixed elsewhere (it had also, for a
--    few minutes, been re-derived slightly differently here than in block() itself -- see git
--    history). A caller wanting both signals together joins blk_candidate_pairs.orcid_check
--    against this table's own arc_id/author_idx, same as any other two-stage join in this
--    pipeline -- fd_score() itself now only ever answers the FD-overlap question. ---------------

CREATE OR REPLACE TABLE data.blk_fd_scores AS
SELECT
    p.arc_id,
    p.author_idx,
    p.match_reason,
    CASE WHEN h.has_arc_inst AND h.has_oax_inst THEN COALESCE(io.overlap, 0.0) END AS institution_score,
    CASE WHEN h.has_arc_sf AND h.has_oax_sf THEN COALESCE(sfo.overlap, 0.0) END AS subfield_score,
    CASE
        WHEN COALESCE(io.overlap, 0.0) + COALESCE(sfo.overlap, 0.0) >= 0.7 THEN 2
        WHEN h.has_arc_inst AND h.has_oax_inst AND h.has_arc_sf AND h.has_oax_sf THEN 1
        ELSE 0
    END AS priority_level,
    CASE
        WHEN COALESCE(io.overlap, 0.0) + COALESCE(sfo.overlap, 0.0) >= 0.7 THEN 'inst+subfield >= 0.7'
        WHEN h.has_arc_inst AND h.has_oax_inst AND h.has_arc_sf AND h.has_oax_sf THEN 'inst+subfield < 0.7'
        ELSE 'no FD data to compare'
    END AS reason
FROM data.blk_candidate_pairs p
JOIN has_data h ON h.arc_id = p.arc_id AND h.author_idx = p.author_idx
LEFT JOIN inst_overlap io ON io.arc_id = p.arc_id AND io.author_idx = p.author_idx
LEFT JOIN sf_overlap sfo ON sfo.arc_id = p.arc_id AND sfo.author_idx = p.author_idx;

-- Example reads (not run by this file):
--   SELECT priority_level, reason, COUNT(*) FROM data.blk_fd_scores GROUP BY 1, 2 ORDER BY 1;
--   SELECT * FROM data.blk_fd_scores WHERE arc_id = 'DP0989027_AndrewKillcross'
--     ORDER BY priority_level DESC;
