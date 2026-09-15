-- Frequency-distribution (institution/subfield) scoring for the candidate pairs Stage 1
-- (01_blocking_name_keys.sql) produced -- a separate, sequential stage on purpose (user
-- direction: blocking and score-filtering are two different CASE-driven steps, not one query).
--
-- Reuses the SAME histogram-intersection methodology already built and tested in
-- src/04_filter_candidates.py::_ensure_fd_tables() (normalize each side's institution/subfield
-- work-counts to proportions, score a pair as SUM(LEAST(arc_prop, oax_prop)) per shared key) --
-- not reinvented here. Writes to NEW tables (blk_ prefix), never touching that class's own
-- arc_subfield_fd / oax_subfield_fd / arc_institution_fd / oax_institution_fd / fd_pair_scores
-- tables, since those are scoped to the OLD Splink-generated candidate pool and still live/used
-- by the production FilterCandidates class.
--
-- Depends on: data.blk_candidate_pairs (Stage 1), data.for_subfield_dict / data.grant_for2020_cache
-- (already-cached, population-wide FOR2020-group -> OAX-subfield lookups, unrelated to
-- candidate scope so safe to read as-is), admin_orgs.csv (institution crosswalk, rebuilt here
-- in SQL rather than importing the Python dict _load_institution_oax_crosswalk() builds).

ATTACH IF NOT EXISTS '/home/lc/k/WORKING_ARC_PROJECT/processed/oax_provenance.duckdb' AS data;

-- ── Stage 1: institution crosswalk, built directly from admin_orgs.csv --------------------
--    Same "resolve via canonical organisationName group, first non-null institution_id in
--    file order wins" rule as src/utils/awards_cif.py::_load_admin_orgs_rows() -- an alias row
--    can lack its own institution_id even when a sibling alias for the same organisation has
--    one (see the project's own "admin_orgs.csv alias gaps" memory). ------------------------

CREATE OR REPLACE TEMP TABLE admin_orgs_raw AS
SELECT *, row_number() OVER () AS rn
FROM read_csv_auto('/home/lc/Projects/arc_grants/data_persisted/admin_orgs.csv');

CREATE OR REPLACE TEMP TABLE canonical_institution_id AS
SELECT organisationName, arg_min(institution_id, rn) AS institution_id
FROM admin_orgs_raw
WHERE institution_id IS NOT NULL AND institution_id != ''
GROUP BY organisationName;

CREATE OR REPLACE TEMP TABLE institution_crosswalk AS
SELECT DISTINCT r.organisationName_alias AS admin_org, c.institution_id
FROM admin_orgs_raw r
JOIN canonical_institution_id c ON c.organisationName = r.organisationName
WHERE r.organisationName_alias IS NOT NULL AND r.organisationName_alias != '';

-- ── Stage 2: ARC-side subfield/institution frequency distributions, one row per
--    (cluster_id, key, n) -- n = number of this ACIF's own grants touching that subfield /
--    administered (single-org only) at that institution. Population-wide, not candidate-scoped
--    (unlike the OAX side below), since the ARC population itself is small. -------------------

CREATE OR REPLACE TEMP TABLE cluster_grants AS
SELECT DISTINCT cluster_id, regexp_replace(gid, '_[^_]*$', '') AS grant_code
FROM (
    SELECT cluster_id, unnest(grant_ids) AS gid
    FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/awards_cif_arc_only.parquet')
    WHERE excluded = FALSE
);

CREATE OR REPLACE TEMP TABLE arc_subfield_fd AS
SELECT cg.cluster_id, f.subfield, COUNT(*) AS n
FROM cluster_grants cg
JOIN data.grant_for2020_cache g ON g.grant_code = cg.grant_code
CROSS JOIN UNNEST(g.codes) AS t(entry)
JOIN data.for_subfield_dict f ON f.code = t.entry.code
GROUP BY cg.cluster_id, f.subfield;

CREATE OR REPLACE TEMP TABLE arc_institution_fd AS
SELECT cg.cluster_id, cw.institution_id, COUNT(*) AS n
FROM cluster_grants cg
JOIN read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/grants_flat.parquet') g
    ON g.grant_code = cg.grant_code AND g.n_eligible_orgs = 1
JOIN institution_crosswalk cw ON cw.admin_org = g.admin_org
GROUP BY cg.cluster_id, cw.institution_id;

-- ── Stage 3: OAX-side frequency distributions -- scoped to just the author_idx values Stage 1
--    actually produced (candidate_author_idx), same "don't compute FD for authors nobody is
--    considering" optimization as _ensure_fd_tables()'s own 2026-09-13 scoping fix. ------------

CREATE OR REPLACE TEMP TABLE candidate_author_idx AS
SELECT DISTINCT author_idx FROM data.blk_candidate_pairs;

CREATE OR REPLACE TEMP TABLE oax_subfield_fd AS
SELECT a.author_idx, w.subfield_name, COUNT(DISTINCT a.work_idx) AS n
FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/authorships_hep.parquet') a
JOIN candidate_author_idx c ON c.author_idx = a.author_idx
JOIN read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/works_hep.parquet') w USING (work_idx)
WHERE w.subfield_name IS NOT NULL
GROUP BY a.author_idx, w.subfield_name;

CREATE OR REPLACE TEMP TABLE oax_institution_fd AS
SELECT a.author_idx,
       'https://openalex.org/I' || a.institution_idx::VARCHAR AS institution_id,
       COUNT(DISTINCT a.work_idx) AS n
FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/authorships_hep.parquet') a
JOIN candidate_author_idx c ON c.author_idx = a.author_idx
WHERE a.institution_idx IS NOT NULL
GROUP BY a.author_idx, a.institution_idx;

-- ── Stage 4: normalize each side's counts to within-entity proportions (a histogram, not raw
--    counts -- a person with 200 works and one with 5 must be compared on SHAPE, not scale). ---

CREATE OR REPLACE TEMP TABLE arc_inst_norm AS
SELECT cluster_id, institution_id, n * 1.0 / SUM(n) OVER (PARTITION BY cluster_id) AS prop
FROM arc_institution_fd;

CREATE OR REPLACE TEMP TABLE arc_sf_norm AS
SELECT cluster_id, subfield, n * 1.0 / SUM(n) OVER (PARTITION BY cluster_id) AS prop
FROM arc_subfield_fd;

CREATE OR REPLACE TEMP TABLE oax_inst_norm AS
SELECT author_idx, institution_id, n * 1.0 / SUM(n) OVER (PARTITION BY author_idx) AS prop
FROM oax_institution_fd;

CREATE OR REPLACE TEMP TABLE oax_sf_norm AS
SELECT author_idx, subfield_name, n * 1.0 / SUM(n) OVER (PARTITION BY author_idx) AS prop
FROM oax_subfield_fd;

-- ── Stage 5: histogram-intersection score per pair -- SUM(LEAST(arc_prop, oax_prop)) over
--    every key both sides have any weight on. NULL (not 0) when either side has no data at all
--    for that dimension, so "no overlap" and "nothing to compare" stay distinguishable
--    downstream (same discipline as fd_pair_scores' own has_arc_inst/has_oax_inst flags). ------

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

-- ── Stage 6: the actual score-filtering CASE WHEN -- a separate step from Stage-1's blocking
--    CASE, per the sequential design. ORCID (from Stage 1's match_reason) is the hard signal;
--    combined institution+subfield overlap is the soft one. Mirrors sql/oax_priority_triage.sql's
--    existing priority-tier convention, adapted to this file's own match_reason column instead
--    of that file's separate arc_orcid/oax_orcid columns. -------------------------------------

CREATE OR REPLACE TABLE data.blk_fd_scores AS
SELECT
    p.arc_id,
    p.author_idx,
    p.match_reason,
    CASE WHEN h.has_arc_inst AND h.has_oax_inst THEN COALESCE(io.overlap, 0.0) END AS institution_score,
    CASE WHEN h.has_arc_sf AND h.has_oax_sf THEN COALESCE(sfo.overlap, 0.0) END AS subfield_score,
    CASE
        WHEN p.match_reason IN ('orcid_only', 'orcid+name_key') THEN 3
        WHEN COALESCE(io.overlap, 0.0) + COALESCE(sfo.overlap, 0.0) >= 0.7 THEN 2
        WHEN h.has_arc_inst AND h.has_oax_inst AND h.has_arc_sf AND h.has_oax_sf THEN 1
        ELSE 0
    END AS priority_level,
    CASE
        WHEN p.match_reason IN ('orcid_only', 'orcid+name_key') THEN 'orcid confirmed'
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
