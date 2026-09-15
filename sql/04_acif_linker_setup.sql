-- One-time population-wide setup for AcifOaxLinker's __init__ -- everything here is a pure
-- function of already-persisted data (awards_cif_arc_only.parquet, admin_orgs.csv,
-- grant_for2020_cache/for_subfield_dict) with no per-ACIF or per-candidate scoping, so it's
-- computed once at construction, not recomputed per call. OAX-side FD is deliberately NOT
-- built here -- 00b_extract_oax.py's own Phase 4 already persists oax_subfield_fd.parquet/
-- oax_institution_fd.parquet population-wide (measured 4.2s there, see that script's own
-- docstring) -- AcifOaxLinker just reads those files directly.

ATTACH IF NOT EXISTS '/home/lc/k/WORKING_ARC_PROJECT/processed/oax_provenance.duckdb' AS data;

-- ── Stage 1: institution crosswalk, built directly from admin_orgs.csv -- same "resolve via
--    canonical organisationName group, first non-null institution_id in file order wins" rule
--    as src/utils/awards_cif.py::_load_admin_orgs_rows() (an alias row can lack its own
--    institution_id even when a sibling alias for the same organisation has one). ---------------

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
--    (cluster_id, key, n) -- population-wide over every non-excluded ACIF's own grants. Cannot
--    move into 00a_extract_arc.py the way OAX FD moved into 00b_ -- these need the ACIF
--    population (grant_ids aggregated per cluster), which doesn't exist until 01_prepare_arc.py
--    has run; genuinely a step 01_ output would need to produce, not something 00a_/00b_ could
--    ever precompute. Recomputed here for now, at class construction time, same population
--    (~22.9K ACIFs) this project has always found cheap. -----------------------------------------

CREATE OR REPLACE TEMP TABLE cluster_grants AS
SELECT DISTINCT cluster_id, regexp_replace(gid, '_[^_]*$', '') AS grant_code
FROM (
    SELECT cluster_id, unnest(grant_ids) AS gid
    FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/awards_cif_arc_only.parquet')
    WHERE excluded = FALSE
);

CREATE OR REPLACE TABLE data.arc_subfield_fd_v2 AS
SELECT cg.cluster_id, f.subfield, COUNT(*) AS n
FROM cluster_grants cg
JOIN data.grant_for2020_cache g ON g.grant_code = cg.grant_code
CROSS JOIN UNNEST(g.codes) AS t(entry)
JOIN data.for_subfield_dict f ON f.code = t.entry.code
GROUP BY cg.cluster_id, f.subfield;

CREATE OR REPLACE TABLE data.arc_institution_fd_v2 AS
SELECT cg.cluster_id, cw.institution_id, COUNT(*) AS n
FROM cluster_grants cg
JOIN read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/grants_flat.parquet') g
    ON g.grant_code = cg.grant_code AND g.n_eligible_orgs = 1
JOIN institution_crosswalk cw ON cw.admin_org = g.admin_org
GROUP BY cg.cluster_id, cw.institution_id;

-- Named _v2 (not arc_subfield_fd/arc_institution_fd) deliberately -- those names are already
-- owned by src/04_filter_candidates.py's own FilterCandidates class (its own, still-live,
-- Splink-candidate-scoped tables), and this setup must not clobber them while both exist side
-- by side during the redesign.
