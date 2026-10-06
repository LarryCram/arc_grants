-- Coauthor/coawardee corroboration -- the signal discussed at length this session: NOT a
-- blocking rule (checking coauthor NAME against coawardee NAME would just be the same
-- unresolved fuzzy-name-matching problem pushed sideways), but a corroboration signal that
-- only becomes well-defined AFTER Stage 1 has already run for every ACIF -- every coawardee is
-- itself an ACIF with its own candidate author_idx pool by that point, so the check becomes
-- identity-to-identity (author_idx to author_idx), not name-to-name:
--
--   For ACIF X's own OAX candidate (author_idx A), does A's own real OpenAlex coauthor list
--   (exact author_idx values from actual authorship records -- OpenAlex's own disambiguation,
--   not a name match) intersect any of X's coawardees' OWN candidate author_idx pools (also
--   exact, from Stage 1)?
--
-- Depends on: data.blk_candidate_pairs (Stage 1) -- both as the pool being scored AND as the
-- source of each coawardee's own candidate pool. acifs_arc.parquet coawardee_acif_ids (every
-- OTHER ACIF on any grant this ACIF holds). No FD/subfield/institution data needed here.

ATTACH IF NOT EXISTS '/home/lc/k/WORKING_ARC_PROJECT/processed/oax_provenance.duckdb' AS data;

-- ── Stage 1: each candidate author_idx's own REAL coauthors (exact OpenAlex identity, from
--    actual authorship co-occurrence on a HEP-context work) -- scoped to just the author_idx
--    values Stage 1 (blocking) produced, same scoping discipline as 02_fd_score_name_keys.sql. ─

CREATE OR REPLACE TEMP TABLE candidate_author_idx AS
SELECT DISTINCT author_idx FROM data.blk_candidate_pairs;

CREATE OR REPLACE TEMP TABLE candidate_coauthors AS
SELECT DISTINCT a1.author_idx AS candidate_author_idx, a2.author_idx AS coauthor_author_idx
FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/authorships_hep.parquet') a1
JOIN candidate_author_idx c ON c.author_idx = a1.author_idx
JOIN read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/authorships_hep.parquet') a2
    ON a2.work_idx = a1.work_idx AND a2.author_idx != a1.author_idx;

-- ── Stage 2: each ACIF's own coawardees, resolved to THEIR OWN candidate author_idx pool.
--    2026-10-06: coawardees are read as exact ACIF ids (acifs_arc.parquet coawardee_acif_ids --
--    the other ACIFs on this ACIF's grants, from the build itself), replacing the earlier match
--    of each coawardee's full_name_keys against arc_name_keys (which let a common coawardee name
--    reach every ACIF sharing it). ---------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE coawardee_to_acif AS
SELECT DISTINCT cluster_id, unnest(coawardee_acif_ids) AS coawardee_acif_id
FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/acifs_arc.parquet')
WHERE excluded = FALSE;

CREATE OR REPLACE TEMP TABLE coawardee_candidate_pool AS
SELECT DISTINCT cta.cluster_id, bcp.author_idx AS coawardee_candidate_author_idx
FROM coawardee_to_acif cta
JOIN data.blk_candidate_pairs bcp ON bcp.arc_id = cta.coawardee_acif_id;

-- ── Stage 3: the actual corroboration check -- does candidate A's real coauthor set intersect
--    ACIF X's coawardees' own candidate pool? One row per confirmed intersection, so the count
--    below is a genuine "how many distinct coawardee-candidates corroborate this pick", not
--    just a boolean. ------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE corroborating_matches AS
SELECT DISTINCT p.arc_id, p.author_idx, cc.coauthor_author_idx
FROM data.blk_candidate_pairs p
JOIN candidate_coauthors cc ON cc.candidate_author_idx = p.author_idx
JOIN coawardee_candidate_pool cp
    ON cp.cluster_id = p.arc_id AND cp.coawardee_candidate_author_idx = cc.coauthor_author_idx;

-- ── Stage 4: attach the count back onto every candidate pair (0, not NULL, when nothing
--    corroborates -- absence of corroboration is a real, informative value here, not missing
--    data, unlike the FD scores' NULL-for-no-data-at-all convention in Stage 2). ---------------

CREATE OR REPLACE TABLE data.blk_coauthor_corroboration AS
SELECT
    p.arc_id,
    p.author_idx,
    COALESCE(cm.n_corroborating_coauthors, 0) AS n_corroborating_coauthors
FROM data.blk_candidate_pairs p
LEFT JOIN (
    SELECT arc_id, author_idx, COUNT(DISTINCT coauthor_author_idx) AS n_corroborating_coauthors
    FROM corroborating_matches
    GROUP BY arc_id, author_idx
) cm ON cm.arc_id = p.arc_id AND cm.author_idx = p.author_idx;

-- Example reads (not run by this file):
--   SELECT COUNT(*) FILTER (WHERE n_corroborating_coauthors > 0) AS corroborated_pairs,
--          COUNT(*) AS total_pairs
--   FROM data.blk_coauthor_corroboration;
--   SELECT * FROM data.blk_coauthor_corroboration WHERE n_corroborating_coauthors > 0
--     ORDER BY n_corroborating_coauthors DESC LIMIT 20;
--   -- Combine with Stage 2's FD scores:
--   SELECT f.*, c.n_corroborating_coauthors
--   FROM data.blk_fd_scores f
--   JOIN data.blk_coauthor_corroboration c USING (arc_id, author_idx)
--   WHERE c.n_corroborating_coauthors > 0 AND f.priority_level < 3;
