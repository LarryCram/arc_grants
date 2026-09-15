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
-- source of each coawardee's own candidate pool. AwardsCIF.coawardees (already computed,
-- src/utils/awards_cif.py::compute_coawardees() -- every OTHER investigator on any grant this
-- ACIF holds, keyed by their own full_name_key). No FD/subfield/institution data needed here.

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

-- ── Stage 2: each ACIF's own coawardees, resolved to THEIR OWN candidate author_idx pool --
--    a coawardee's full_name_keys (the whole unioned list, NOT the single full_name_key
--    scalar -- 2026-09-15 fix: compute_coawardees() now unions full_name_keys across every
--    occurrence of a coawardee, so a genuine identity match can live on a non-primary key; the
--    scalar-only version used here originally would silently miss it) is matched against
--    arc_name_keys (the same lookup Stage 1 uses), not against OAX data directly -- this is
--    ARC-person-to-ARC-person identity, exact by construction (a coawardee IS another ACIF in
--    this same population), never fuzzy. A coawardee's own name colliding with 2+ ACIFs (a
--    common name) is handled permissively -- every matched ACIF's own candidate pool
--    contributes, since this is corroborating evidence, not an identity determination. --------

CREATE OR REPLACE TEMP TABLE arc_coawardee_keys AS
SELECT DISTINCT cluster_id, unnest(co.full_name_keys) AS co_full_name_key
FROM (
    SELECT cluster_id, unnest(coawardees) AS co
    FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/awards_cif_arc_only.parquet')
    WHERE excluded = FALSE AND len(coawardees) > 0
)
WHERE co.full_name_keys IS NOT NULL AND len(co.full_name_keys) > 0;

CREATE OR REPLACE TEMP TABLE coawardee_to_acif AS
SELECT DISTINCT ack.cluster_id, a.acif_id AS coawardee_acif_id
FROM arc_coawardee_keys ack
JOIN data.arc_name_keys a ON a.full_name_key = ack.co_full_name_key
WHERE a.acif_id != ack.cluster_id;  -- a coawardee is never the ACIF's own self

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
