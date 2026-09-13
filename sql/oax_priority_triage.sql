-- Priority triage over every Splink-blocked ARC<->OAX candidate pair.
-- Corrected 2026-09-13 from a user draft, after src/04_filter_candidates.py's
-- _ensure_fd_tables() was extended to bake works_count/n_candidates/arc_orcid/oax_orcid
-- directly into fd_pair_scores at build time -- see that function's own comments for why.
--
-- Persists the FULL 4-tier result (not just the accepted "Match likely" tier) as a real
-- table, data.oax_priority_triage, inside oax_provenance.duckdb -- so downstream code/review
-- can read it directly instead of re-running this query every time. 183,187 rows at last
-- build: priority 0 (orcid unequal) 40,813 / 1 (match_probability<0.9) 47,331 /
-- 2 (inst+subfield<0.7) 63,865 / 3 (Match likely) 31,178.
-- To get just the accepted pairs: SELECT * FROM data.oax_priority_triage WHERE priority_level = 3.
--
-- Corrections vs. the original draft:
--   1. Removed the join to data.oax_provenance entirely -- it joined on oax_id alone, which is
--      not unique (the same OAX candidate is a candidate for many different ACIFs), so the old
--      query could attach one ACIF's works_count/status to a completely different arc_id. It was
--      also reading 04_'s own prior output as an input (a staleness/circularity risk). Both
--      works_count and n_candidates now live on fd_pair_scores itself, computed once, correctly
--      scoped per (arc_id, oax_id) / per arc_id.
--   2. n_candidates (candidate-pool size per ACIF) is a genuine one-row-per-arc_id scalar,
--      computed via a real GROUP BY at table-build time -- the original's window-function
--      version, joined back unreduced, produced an N x N blow-up per arc_id before the trailing
--      GROUP BY ALL collapsed it back down (quadratic in candidate-pool size, a real cost on
--      large pools).
--   3. oax_orcid[19:] (a hardcoded 18-character offset into "https://orcid.org/...") replaced
--      with ends_with(oax_orcid, arc_orcid) -- robust to any prefix format, not just this one
--      exact string, and handles NULL on either side the same way orcid_veto() already does
--      (absence is not a mismatch).
--   4. No more SELECT DISTINCT / GROUP BY ALL -- with no joins left, fd_pair_scores already has
--      exactly one row per (arc_id, oax_id); those were only ever compensating for bug #1/#2's
--      row multiplication.

ATTACH IF NOT EXISTS '/home/lc/k/WORKING_ARC_PROJECT/processed/oax_provenance.duckdb' AS data;

CREATE OR REPLACE TABLE data.oax_priority_triage AS
SELECT
    arc_id,
    oax_id,
    oax_author_name,
    n_candidates,
    works_count,
    COALESCE(institution_score, 0) + COALESCE(subfield_score, 0) AS total_score,
    arc_orcid,
    oax_orcid,
    match_probability,
    institution_score,
    subfield_score,
    CASE
        WHEN arc_orcid IS NOT NULL AND oax_orcid IS NOT NULL
             AND NOT ends_with(oax_orcid, arc_orcid) THEN 0
        WHEN arc_orcid IS NOT NULL AND oax_orcid IS NOT NULL
             AND ends_with(oax_orcid, arc_orcid) THEN 3
        WHEN match_probability < 0.9 THEN 1
        WHEN COALESCE(institution_score, 0) + COALESCE(subfield_score, 0) < 0.7 THEN 2
        ELSE 3
    END AS priority_level,
    CASE
        WHEN arc_orcid IS NOT NULL AND oax_orcid IS NOT NULL
             AND NOT ends_with(oax_orcid, arc_orcid) THEN 'orcid unequal'
        WHEN arc_orcid IS NOT NULL AND oax_orcid IS NOT NULL
             AND ends_with(oax_orcid, arc_orcid) THEN 'orcid confirmed match'
        WHEN match_probability < 0.9 THEN 'Match Prob < 0.9'
        WHEN COALESCE(institution_score, 0) + COALESCE(subfield_score, 0) < 0.7 THEN 'Inst + Subfield < 0.7'
        ELSE 'Match likely'
    END AS reason
FROM data.fd_pair_scores;

-- Example reads (not run by this file):
--   SELECT * FROM data.oax_priority_triage WHERE priority_level = 3
--     ORDER BY n_candidates DESC, works_count, arc_id, total_score DESC, match_probability DESC;
--   SELECT priority_level, reason, COUNT(*) FROM data.oax_priority_triage GROUP BY 1, 2 ORDER BY 1;
