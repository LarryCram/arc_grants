-- Candidate-pair generation (ARC <-> OAX) via full_name_key overlap + ORCID exact match --
-- a direct replacement for 03_link_arc_oax.py's Splink `block_on(family_name_main,
-- first_initial)` rules. Motivation: a real case (Simon Killcross, DP0989027_AndrewKillcross)
-- showed Splink's own SCALAR first_initial collapse (one canonical pick out of a candidate's
-- own multi-valued first_initials array) structurally misses a correct, ORCID-bearing match --
-- while a plain full_name_key equality join catches it via the shared bare-initial key
-- "a_killcross". See the session's own discussion for the full trace.
--
-- Reads data.arc_name_keys / data.oax_name_keys (already built: one row per (id, full_name_key),
-- full_name_key = "<given/nickname/initial>_<family-spelling-variant>", see
-- src/utils/names.py::HumanNameParser.parse()'s own full_name_keys field).
--
-- Bidirectional match, done here in SQL rather than by touching names.py: full_name_key is
-- always exactly ONE "<given>_<family>" pair (given/nickname tokens are pure alpha, so the
-- first "_" is the only separator that can ever occur), so a genuine given/family ORDER SWAP
-- (ARC "Ramamohanarao Kotagiri" vs OAX "Kotagiri Ramamohanarao") can be caught by comparing
-- against the SWAPPED key too, without any change to how full_name_keys itself is generated.
--
-- Rarity gate on bare-initial-only matches (added after a first, ungated run produced
-- 5,537,716 name_key_only pairs, ~242/ACIF vs Splink's ~8/ACIF -- the expected consequence of
-- matching on keys like "a_smith"/"j_wang": a common surname + a common bare initial matches
-- almost everyone). A full_name_key whose GIVEN half is multi-character (e.g. "andrew_smith")
-- is left alone -- that's already discriminating. A key whose given half is a bare initial
-- (e.g. "a_killcross") is only trusted when the FAMILY half is rare enough on its own,
-- reusing this project's own already-calibrated RARE_NAME_TF threshold
-- (src/utils/cluster_checks.py, 1e-5, ~p99.9 of the OAX family_name_main tf distribution) via
-- oax_tf_family_name.parquet -- not a new, ad hoc number. Killcross survives this gate (a rare
-- surname); "a_smith" does not.
--
-- Sequential, not one big query (user direction): this file only generates candidate PAIRS with
-- a match_reason tag. Frequency-distribution (institution/subfield) scoring is a separate,
-- later stage -- see 02_fd_score_name_keys.sql. Coauthor/coawardee corroboration is later
-- still -- see 03_coawardee_coauthor.sql.

ATTACH IF NOT EXISTS '/home/lc/k/WORKING_ARC_PROJECT/processed/oax_provenance.duckdb' AS data;

-- ── Stage 1: OAX side gains a swapped key, for order-reversal matching ─────────────────────

CREATE OR REPLACE TEMP TABLE oax_keys_swapped AS
SELECT
    author_idx,
    orcid,
    full_name_key,
    split_part(full_name_key, '_', 2) || '_' || split_part(full_name_key, '_', 1)
        AS full_name_key_swapped
FROM data.oax_name_keys;

-- ── Stage 2: one row per (id, orcid) on each side, deduped -- for exact-ORCID candidate
--    generation, independent of any name-key overlap at all (a real married-name/alias case
--    should still surface via ORCID even when no name-key matches whatsoever). ───────────────

CREATE OR REPLACE TEMP TABLE arc_orcid_scalar AS
SELECT DISTINCT acif_id, unnest(orcid) AS orcid
FROM data.arc_name_keys
WHERE orcid IS NOT NULL AND len(orcid) > 0;

CREATE OR REPLACE TEMP TABLE oax_orcid_scalar AS
SELECT DISTINCT author_idx, orcid
FROM data.oax_name_keys
WHERE orcid IS NOT NULL;

CREATE OR REPLACE TEMP TABLE orcid_pairs AS
SELECT DISTINCT a.acif_id AS arc_id, o.author_idx
FROM arc_orcid_scalar a
JOIN oax_orcid_scalar o USING (orcid);

-- ── Stage 3: raw name-key matches, one row per (pair, matched key) -- kept at this grain
--    (not yet collapsed to one row per pair) so Stage 4 can tell whether a pair's ONLY
--    evidence was a bare-initial key or not.
--
--    Two separate equi-joins UNION ALL'd, NOT one join with an OR across both conditions --
--    measured directly (2026-09-14): the OR form took 2m18s real / 34m user time (heavy
--    multi-threaded work for a modest row count) against this same data; this UNION ALL form
--    took 0.37s. Not explained by any claimed DuckDB internal here -- only measured. The two
--    forms can produce a handful of exact-duplicate rows (where full_name_key already equals
--    its own swap, e.g. a genuine doubled name like "wei_wei" -- 4,822 such rows measured on
--    this population) that the OR form counts once and this UNION ALL counts twice; harmless,
--    since every downstream step already GROUP BY/DISTINCTs on (arc_id, author_idx). ──────────

CREATE OR REPLACE TEMP TABLE name_key_matches_raw AS
SELECT a.acif_id AS arc_id, o.author_idx, a.full_name_key AS matched_key,
       length(split_part(a.full_name_key, '_', 1)) > 1 AS is_multichar
FROM data.arc_name_keys a
JOIN oax_keys_swapped o ON a.full_name_key = o.full_name_key
UNION ALL
SELECT a.acif_id AS arc_id, o.author_idx, a.full_name_key AS matched_key,
       length(split_part(a.full_name_key, '_', 1)) > 1 AS is_multichar
FROM data.arc_name_keys a
JOIN oax_keys_swapped o ON a.full_name_key = o.full_name_key_swapped;

-- ── Stage 4: per-pair rollup + rarity gate. multichar_pairs need no further evidence.
--    bare_initial_only_pairs need the matched key's FAMILY half to be rare. ────────────────────

CREATE OR REPLACE TEMP TABLE pair_rollup AS
SELECT arc_id, author_idx, bool_or(is_multichar) AS has_multichar_match
FROM name_key_matches_raw
GROUP BY arc_id, author_idx;

CREATE OR REPLACE TEMP TABLE multichar_pairs AS
SELECT arc_id, author_idx
FROM pair_rollup
WHERE has_multichar_match;

CREATE OR REPLACE TEMP TABLE oax_tf_family_name AS
SELECT * FROM read_parquet('/home/lc/k/WORKING_ARC_PROJECT/processed/oax_tf_family_name.parquet');

CREATE OR REPLACE TEMP TABLE bare_initial_only_pairs AS
SELECT DISTINCT r.arc_id, r.author_idx
FROM name_key_matches_raw r
JOIN pair_rollup g USING (arc_id, author_idx)
JOIN oax_tf_family_name t ON t.family_name_main = split_part(r.matched_key, '_', 2)
WHERE NOT g.has_multichar_match
  AND t.tf_family_name_main < 1e-5;  -- RARE_NAME_TF, src/utils/cluster_checks.py

-- What the gate actually dropped, kept as its own table for inspection --
-- SELECT COUNT(*) FROM (SELECT arc_id, author_idx FROM pair_rollup WHERE NOT has_multichar_match
--   EXCEPT SELECT arc_id, author_idx FROM bare_initial_only_pairs);
CREATE OR REPLACE TABLE data.blk_bare_initial_dropped AS
SELECT arc_id, author_idx FROM pair_rollup WHERE NOT has_multichar_match
EXCEPT
SELECT arc_id, author_idx FROM bare_initial_only_pairs;

CREATE OR REPLACE TEMP TABLE name_key_pairs AS
SELECT arc_id, author_idx FROM multichar_pairs
UNION
SELECT arc_id, author_idx FROM bare_initial_only_pairs;

-- ── Stage 5: union name-key + ORCID pairs, classify. WHEN-ordered so 'orcid_only' (the
--    strongest, name-independent signal) is distinguishable from 'name_key_only' and from
--    pairs found both ways. ─────────────────────────────────────────────────────────────────

CREATE OR REPLACE TEMP TABLE candidate_pairs_raw AS
SELECT
    arc_id,
    author_idx,
    CASE
        WHEN n.arc_id IS NOT NULL AND o.arc_id IS NOT NULL THEN 'orcid+name_key'
        WHEN o.arc_id IS NOT NULL THEN 'orcid_only'
        ELSE 'name_key_only'
    END AS match_reason
FROM name_key_pairs n
FULL OUTER JOIN orcid_pairs o USING (arc_id, author_idx);

-- ── Stage 6: orcid_check -- an ARC-recorded orcid vs. a given OAX candidate's own orcid,
--    each a genuine scalar (an ACIF has NULL or exactly one orcid; an OAX author_idx has NULL
--    or exactly one orcid), not the "any of a list" question orcid_veto()/orcid_any_match()
--    answer elsewhere. Both sides are already bare (no "https://orcid.org/" prefix) -- ARC's
--    own orcid always has been, and oax_name_keys.orcid is built by 00b_extract_oax.py's Phase 1
--    (`replace(au.orcid, 'https://orcid.org/', '') AS orcid`) -- confirmed directly against real
--    data (2026-09-15), not assumed -- so a plain equality is correct here, no ends_with() needed.
--    'unknown' when either side has nothing to compare (absence is not a mismatch); collapsing
--    arc_orcid_scalar to one row per acif_id (arbitrary pick on the rare MULTI_ORCID conflict
--    case) rather than joining its raw multi-row form, which would fan out candidate_pairs_raw. ---

CREATE OR REPLACE TEMP TABLE arc_orcid_check AS
SELECT acif_id AS arc_id, min(orcid) AS orcid
FROM arc_orcid_scalar
GROUP BY acif_id;

-- ── Stage 7: given_name_check -- a soft, order-agnostic given-name signal, computed here as
--    evidence (not a gate -- user direction 2026-09-16: "rather than gate, the test could
--    return not_applicable/match/mismatch and this can feed into scoring"). Motivation: the
--    bare-initial rarity gate (Stage 4) only requires the FAMILY half to be rare -- it says
--    nothing about whether the given names actually agree, so a pair like ARC "John K. Smith"
--    vs OAX "A. James Smith" could both self-add a bare initial and share nothing else, yet
--    still reach blk_candidate_pairs on family-name rarity alone.
--
--    Tokenize into given-name PARTS only (the family name is never part of this comparison),
--    order-agnostic -- "Adam James" does not exclude "Fred Adam" or "James John" (shares "adam"/
--    "james" respectively). Reuses full_name_key's own given half (split on the first "_") rather
--    than a fresh tokenization -- full_name_keys already IS the given/nickname x family
--    cartesian product (src/utils/names.py::HumanNameParser.parse()), so collecting the DISTINCT
--    given half across every one of an id's own full_name_key rows already recovers its whole
--    given/nickname token set. Bare single-letter initials are excluded (length > 1) -- same
--    "self-added blocking initial carries no identifying information on its own" principle as
--    cluster_checks.py::first_names_compatible(), here applied to the ARC<->OAX candidate pool
--    rather than ARC-internal gap_candidates.
--
--    Three-valued, not a gate: 'not_applicable' when either side has no full (multi-character)
--    given token at all to compare (a genuine ARC-source or OAX-source ambiguity, not evidence
--    of anything); 'match' when the two sides' given-token sets intersect at all, regardless of
--    position; 'mismatch' only when both sides have real given-name evidence and none of it
--    overlaps. ---------------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE arc_given_full AS
SELECT acif_id AS arc_id, list(DISTINCT given) AS given_tokens
FROM (SELECT acif_id, split_part(full_name_key, '_', 1) AS given FROM data.arc_name_keys) sub
WHERE length(given) > 1
GROUP BY acif_id;

CREATE OR REPLACE TEMP TABLE oax_given_full AS
SELECT author_idx, list(DISTINCT given) AS given_tokens
FROM (SELECT author_idx, split_part(full_name_key, '_', 1) AS given FROM data.oax_name_keys) sub
WHERE length(given) > 1
GROUP BY author_idx;

CREATE OR REPLACE TABLE data.blk_candidate_pairs AS
SELECT
    p.arc_id,
    p.author_idx,
    p.match_reason,
    CASE
        WHEN a.orcid IS NULL OR o.orcid IS NULL THEN 'unknown'
        WHEN a.orcid = o.orcid THEN 'match'
        ELSE 'mismatch'
    END AS orcid_check,
    CASE
        WHEN ag.given_tokens IS NULL OR len(ag.given_tokens) = 0
          OR og.given_tokens IS NULL OR len(og.given_tokens) = 0 THEN 'not_applicable'
        WHEN len(list_filter(ag.given_tokens, x -> list_contains(og.given_tokens, x))) > 0 THEN 'match'
        ELSE 'mismatch'
    END AS given_name_check
FROM candidate_pairs_raw p
LEFT JOIN arc_orcid_check a ON a.arc_id = p.arc_id
LEFT JOIN oax_orcid_scalar o ON o.author_idx = p.author_idx
LEFT JOIN arc_given_full ag ON ag.arc_id = p.arc_id
LEFT JOIN oax_given_full og ON og.author_idx = p.author_idx;

-- Example reads (not run by this file):
--   SELECT match_reason, COUNT(*) FROM data.blk_candidate_pairs GROUP BY 1 ORDER BY 1;
--   SELECT orcid_check, COUNT(*) FROM data.blk_candidate_pairs GROUP BY 1 ORDER BY 1;
--   SELECT given_name_check, COUNT(*) FROM data.blk_candidate_pairs GROUP BY 1 ORDER BY 1;
--   SELECT COUNT(DISTINCT arc_id) FROM data.blk_candidate_pairs;
--   SELECT * FROM data.blk_candidate_pairs WHERE arc_id = 'DP0989027_AndrewKillcross';
--   SELECT COUNT(*) FROM data.blk_bare_initial_dropped;  -- pairs the rarity gate removed
