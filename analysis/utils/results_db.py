"""
analysis/utils/results_db.py

Build/refresh logic for results.db (PROCESSED_DATA/results.db) -- the project's new
output-facing database, one row per ACIF (AwardsCIF cluster) in each table. Sibling to
dossier.py/dossier_build.py's data-model/construction split, but the model here is just a
DuckDB schema, not a dataclass -- results.db IS the report's data source, not an intermediate
object something else renders.

Three tables so far, per direct project direction (2026-09-17):
  - title: one row per ACIF -- the report header. ARC-recorded orcid(s), the top 3 FOR2020
    fields across all this ACIF's grants (by declared-entry count, with each one's fraction of
    the total -- counted at whatever granularity awards_cif_arc_only.parquet's own for2020_codes
    already dedupes to: one entry per (grant, resolved FOR2020 group)), the OAX-side analog --
    the top 3 OAX subfields (by work count, with fraction) for whichever OAX identity is
    currently "selected" (old pipeline) for this person, sourced from oax_subfield_fd.parquet
    (the same population-wide table fd_score()'s subfield_score reads) -- and when this row was
    last (re)built.
  - arc: one row per (ACIF, grant) -- grant code, role, fellowship flag, funding amount,
    HEP code (not scheme_name or the raw admin_org institution name -- dropped/recoded per
    direct 2026-09-17 instruction; institution is coded as its short hep_code, matching the
    HEP-codes-not-full-names convention already used elsewhere in this project, e.g.
    AwardsCIF.hep_codes). Grain is per-grant, not per-ACIF, because a person can hold several
    grants and each has its own year/amount -- collapsing to one row per ACIF would either
    lose grants or force a lossy aggregate no report should be built on.
  - oax_candidates: one row per (ACIF, OAX candidate author_idx) -- every candidate seen for
    this ACIF (not just the selected one), with its own works_count/cited_by_count/h_index and
    oax_provenance status (keep/drop/unscored). Grain is per-candidate for the same reason arc
    is per-grant: a report showing only the single selected identity can't answer "was this a
    close call" -- that needs the whole pool.

All tables key on cluster_id (the ACIF's own id, see CLAUDE.md on why this stays the
human-readable string rather than a surrogate key). None compute anything new -- all are thin
joins over already-persisted pipeline outputs (awards_cif_arc_only.parquet,
oax_provenance.duckdb, grants_flat.parquet, investigators_raw.parquet,
openalex_authors_prep.parquet, the raw OpenAlex authors dimension table). Rebuilding results.db
never re-runs upstream resolution -- it only reads whatever those files currently say. The
report tool (analysis/12_acif_report.py) in turn only ever reads results.db, never these
upstream files directly -- everything a report needs must be written into these tables first,
not joined live at render time (direct 2026-09-17 instruction).

"Selected" OAX identity (title.oax_id/oax_full_name): oax_provenance.duckdb's 'keep' rows are
the current output of FilterCandidates.resolve() (src/04_filter_candidates.py) -- an ACIF can
have 0 keep rows (nothing survived orcid_veto()/fd_compare(), or never resolved at all), 1
(the ordinary case), or 2+ (fragment-merged identity, e.g. a split OpenAlex author_idx).
Per direct 2026-09-17 decision: select the highest-works_count keep row as the single
displayed identity for now -- a provisional single-name view over what may be a multi-fragment
truth, not a claim that the other keep rows are wrong (they're preserved in n_keep_candidates
for anyone who needs to know there was a choice made here).
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb
import pandas as pd

from config.settings import PROCESSED_DATA
from src.utils.awards_cif import _load_hep_crosswalk

ARC_ONLY_PARQUET = PROCESSED_DATA / "awards_cif_arc_only.parquet"
OAX_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
OAX_SUBFIELD_FD = PROCESSED_DATA / "oax_subfield_fd.parquet"
GRANTS_FLAT = PROCESSED_DATA / "grants_flat.parquet"
INVESTIGATORS_RAW = PROCESSED_DATA / "investigators_raw.parquet"
PROVENANCE_DB = PROCESSED_DATA / "oax_provenance.duckdb"
RESULTS_DB = PROCESSED_DATA / "results.db"


def _attach_provenance(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"ATTACH IF NOT EXISTS '{PROVENANCE_DB}' AS prov (READ_ONLY)")


def build_title_table(con: duckdb.DuckDBPyConnection) -> int:
    """(Re)build the `title` table: one row per non-excluded ACIF -- report header fields
    (selected OAX identity, tier/status for category filtering) plus the timestamp this row
    was generated. Returns the row count."""
    _attach_provenance(con)
    now = datetime.now(timezone.utc).isoformat()

    con.execute(f"""
        CREATE OR REPLACE TABLE title AS
        WITH acifs AS (
            SELECT cluster_id, reliability_tier, resolution_status, n_grants, orcids
            FROM read_parquet('{ARC_ONLY_PARQUET}')
            WHERE excluded = FALSE
        ),
        candidate_counts AS (
            -- awards_cif_arc_only.parquet's own oax_candidates column is always empty (never
            -- populated pre-OAX-linking) -- the real deduped candidate pool lives in
            -- acif_oax_candidates, built by FilterCandidates.load_clusters_by_size().
            SELECT cluster_id, count(*) AS n_oax_candidates
            FROM prov.acif_oax_candidates
            GROUP BY cluster_id
        ),
        for_counts AS (
            SELECT cluster_id, entry.code AS for_code, entry."name" AS for_name, COUNT(*) AS n
            FROM (
                SELECT cluster_id, unnest(for2020_codes) AS entry
                FROM read_parquet('{ARC_ONLY_PARQUET}')
                WHERE excluded = FALSE
            )
            GROUP BY cluster_id, entry.code, entry."name"
        ),
        for_totals AS (
            SELECT cluster_id, SUM(n) AS total_n FROM for_counts GROUP BY cluster_id
        ),
        for_ranked AS (
            SELECT fc.cluster_id, fc.for_name, fc.n, ft.total_n,
                   -- ORDER BY n DESC alone is non-deterministic on ties (confirmed real,
                   -- 2026-09-18: 5 FOR fields tied at n=1 for one ACIF produced a different
                   -- top-3 pick across two consecutive rebuilds) -- name ASC as a deterministic
                   -- tiebreak, same discipline as this project's other non-determinism fixes.
                   row_number() OVER (
                       PARTITION BY fc.cluster_id ORDER BY fc.n DESC, fc.for_name ASC
                   ) AS rn
            FROM for_counts fc JOIN for_totals ft USING (cluster_id)
        ),
        top_for AS (
            SELECT cluster_id,
                   list({{'name': for_name, 'fraction': round(n * 1.0 / total_n, 3)}}
                        ORDER BY rn) AS top_for_codes
            FROM for_ranked
            WHERE rn <= 3
            GROUP BY cluster_id
        ),
        keeps AS (
            SELECT p.cluster_id, p.oax_id, p.works_count,
                   row_number() OVER (
                       PARTITION BY p.cluster_id ORDER BY p.works_count DESC NULLS LAST
                   ) AS rn,
                   count(*) OVER (PARTITION BY p.cluster_id) AS n_keep
            FROM prov.oax_provenance p
            WHERE p.status = 'keep'
        ),
        selected AS (
            SELECT cluster_id, oax_id, works_count AS oax_works_count, n_keep,
                   try_cast(regexp_extract(oax_id, '[0-9]+$') AS BIGINT) AS author_idx
            FROM keeps WHERE rn = 1
        ),
        oax_sf_counts AS (
            SELECT s.cluster_id, f.subfield_name, f.n
            FROM selected s
            JOIN read_parquet('{OAX_SUBFIELD_FD}') f ON f.author_idx = s.author_idx
        ),
        oax_sf_totals AS (
            SELECT cluster_id, SUM(n) AS total_n FROM oax_sf_counts GROUP BY cluster_id
        ),
        oax_sf_ranked AS (
            SELECT c.cluster_id, c.subfield_name, c.n, t.total_n,
                   row_number() OVER (
                       PARTITION BY c.cluster_id ORDER BY c.n DESC, c.subfield_name ASC
                   ) AS rn
            FROM oax_sf_counts c JOIN oax_sf_totals t USING (cluster_id)
        ),
        top_oax_sf AS (
            SELECT cluster_id,
                   list({{'name': subfield_name, 'fraction': round(n * 1.0 / total_n, 3)}}
                        ORDER BY rn) AS top_oax_subfields
            FROM oax_sf_ranked
            WHERE rn <= 3
            GROUP BY cluster_id
        )
        SELECT
            a.cluster_id,
            a.reliability_tier,
            a.resolution_status,
            a.n_grants,
            a.orcids,
            tf.top_for_codes,
            coalesce(c.n_oax_candidates, 0) AS n_oax_candidates,
            coalesce(s.n_keep, 0) AS n_keep_candidates,
            s.oax_id,
            oax.full_name AS oax_full_name,
            s.oax_works_count,
            tsf.top_oax_subfields,
            CASE
                WHEN s.oax_id IS NOT NULL THEN 'selected'
                WHEN coalesce(c.n_oax_candidates, 0) = 0 THEN 'no_candidates'
                ELSE 'unresolved_no_keep'
            END AS selection_status,
            TIMESTAMP '{now}' AS report_generated_at
        FROM acifs a
        LEFT JOIN top_for tf USING (cluster_id)
        LEFT JOIN candidate_counts c USING (cluster_id)
        LEFT JOIN selected s USING (cluster_id)
        LEFT JOIN top_oax_sf tsf USING (cluster_id)
        LEFT JOIN read_parquet('{OAX_PREP}') oax
            ON oax.author_idx = try_cast(regexp_extract(s.oax_id, '[0-9]+$') AS BIGINT)
    """)
    con.execute("ALTER TABLE title ADD PRIMARY KEY (cluster_id)")
    return con.execute("SELECT count(*) FROM title").fetchone()[0]


def build_oax_candidates_table(con: duckdb.DuckDBPyConnection) -> int:
    """(Re)build the `oax_candidates` table: one row per (ACIF, candidate author_idx) --
    every OAX candidate in AcifOaxLinker's own block() output (data.blk_candidate_pairs,
    src/utils/acif_oax_linker.py + sql/01_blocking_name_keys.sql), NOT the old
    03_link_arc_oax.py/FilterCandidates pipeline (2026-09-17 direct instruction). Author info
    (full_name, works_count*, cited_by_count, h_index, orcid) comes from
    openalex_authors_prep.parquet -- NOT the raw OpenAlex authors dimension table (switched
    2026-09-17: cited_by_count/h_index were added to that prep file's own Phase 1/2, precisely so
    this kind of downstream code has no reason to reach around it to the raw table). Three
    distinct works-count fields, not to be confused (see 00b_extract_oax.py's own module
    docstring for the full derivation):
      - works_count: HEP/AU-context population count -- DISTINCT works with at least one
        HEP-affiliated co-author, counted for this author regardless of THEIR OWN affiliation on
        that work (a purely-overseas collaborator's works still count here).
      - works_count_global: the author's raw total lifetime works_count, unfiltered/worldwide.
      - works_count_au: DISTINCT works where THIS author's own row is Australia-affiliated
        (country_code='AU') -- the tightest, most literal "did they actually work in Australia"
        count. works_count_global - works_count_au is a direct overseas-vs-Australian output
        signal (verified 2026-09-17: Georges Aad, an ATLAS-collaboration physicist with no
        Australian connection of his own, shows works_count=1202/works_count_global=1769/
        works_count_au=0 -- every one of his 1,202 "AU works" only qualifies because a co-author
        was Australian, never him).

    Four evidence columns from AcifOaxLinker's own later stages:
      - orcid: the candidate's own bare ORCID, or 'unknown' if the candidate has none, with a
        trailing '*' when it does NOT equal the ACIF's own ORCID (mirrors block()'s own
        orcid_check logic -- min(orcid) per ACIF as the comparison scalar -- but shows the real
        value instead of a match/mismatch/unknown label, since the ACIF's own report already
        shows its ORCID in the header).
      - subfield_fd: fd_score()'s own blk_fd_scores.subfield_score -- a histogram-intersection
        (0-1) between the ACIF's declared-FOR-code-derived OAX subfield distribution and the
        candidate's actual published-work subfield distribution (see sql/02_fd_score_name_keys.sql).
        High = the candidate's real body of work matches the ACIF's declared field; low = probably
        the wrong person; NULL (shown as 'n/a') when either side has no subfield data to compare.
      - institution_fd: blk_fd_scores.institution_score -- same histogram-intersection mechanism,
        over institutions instead of subfields: the ACIF's own admin_org distribution (single-org
        grants only, mapped to an OAX institution id) vs. the candidate's actual HEP-context
        authorship institution distribution. Same 0-1 range, NULL/'n/a' when nothing to compare.
      - n_corroborating_coauthors: coawardee_corroborate()'s own blk_coauthor_corroboration --
        how many of this candidate's REAL OpenAlex coauthors (exact author_idx, from actual
        co-authorship on a HEP-context work) also appear in one of this ACIF's coawardees' own
        candidate pools (see sql/03_coawardee_coauthor.sql). Always a real integer, 0 included --
        unlike the FD scores, 0 here means "no corroboration found", not "nothing to compare".
      - given_name_check: block()'s own Stage 7 signal (blk_candidate_pairs.given_name_check) --
        'match'/'mismatch'/'not_applicable', an order-agnostic given-name-token overlap test
        between ARC and OAX. Confirmed correct (2026-09-17) but NOT currently a gate anywhere in
        this pipeline -- block() stores it purely as evidence for a future scoring/rating step
        that hasn't been built yet (AcifOaxLinker has no resolve()-equivalent), so a 'mismatch'
        candidate (e.g. "Aaditya" vs "Ajay") still appears here undiminished; this column exists
        so a human reviewing the table can see and act on that evidence directly.
      - provenance: block()'s own match_reason -- exactly one of 'orcid_only'/'name_key_only'/
        'orcid+name_key' per pair (verified: 297,234 rows, 297,234 distinct (arc_id, author_idx)
        pairs, so always a genuine scalar, never a list) -- WHY this candidate pair was generated
        by blocking at all, not a match-quality judgment.

    No `status` (keep/drop) column -- that was oax_provenance's verdict from the now-superseded
    FilterCandidates.resolve(); AcifOaxLinker has no rating/resolve step built yet, so there is
    nothing equivalent to show here for now.

    NOTE (temporary, known inconsistency): the `## ARC-OAX link` identity line in the report
    above this table still comes from the OLD pipeline's oax_provenance 'keep' rows (title
    table, build_title_table()) -- that has not been switched over yet, so the identity line's
    candidate pool and this table's candidate pool can currently disagree. Returns the row
    count."""
    _attach_provenance(con)
    con.execute(f"""
        CREATE OR REPLACE TABLE oax_candidates AS
        WITH arc_orcid AS (
            SELECT cluster_id, list_aggregate(orcids, 'min') AS arc_orcid
            FROM read_parquet('{ARC_ONLY_PARQUET}')
            WHERE excluded = FALSE
        )
        SELECT
            p.arc_id AS cluster_id,
            p.author_idx,
            'https://openalex.org/A' || p.author_idx AS oax_id,
            a.full_name,
            a.works_count,
            a.works_count_global,
            a.works_count_au,
            a.cited_by_count,
            a.h_index,
            CASE
                WHEN a.orcid IS NULL THEN 'unknown'
                WHEN a.orcid = ao.arc_orcid THEN a.orcid
                ELSE a.orcid || '*'
            END AS orcid,
            fd.subfield_score AS subfield_fd,
            fd.institution_score AS institution_fd,
            co.n_corroborating_coauthors,
            p.orcid_check,
            p.given_name_check,
            p.match_reason AS provenance
        FROM prov.blk_candidate_pairs p
        LEFT JOIN arc_orcid ao ON ao.cluster_id = p.arc_id
        LEFT JOIN read_parquet('{OAX_PREP}') a ON a.author_idx = p.author_idx
        LEFT JOIN prov.blk_fd_scores fd ON fd.arc_id = p.arc_id AND fd.author_idx = p.author_idx
        LEFT JOIN prov.blk_coauthor_corroboration co
            ON co.arc_id = p.arc_id AND co.author_idx = p.author_idx
    """)
    con.execute("ALTER TABLE oax_candidates ADD PRIMARY KEY (cluster_id, author_idx)")
    return con.execute("SELECT count(*) FROM oax_candidates").fetchone()[0]


def build_oax_resolve_table(con: duckdb.DuckDBPyConnection) -> int:
    """(Re)build the `oax_resolve` table: the first scoring/acceptance step over
    `oax_candidates`, on AcifOaxLinker's own evidence -- there is no resolve()-equivalent in
    AcifOaxLinker itself yet (block()/fd_score()/coawardee_corroborate() only produce evidence),
    so this is genuinely new logic, not a passthrough of anything already computed elsewhere.
    Design confirmed directly with the user (2026-09-17) before coding, one point per signal:
      - pt_orcid:        1 if orcid_check = 'match'
      - pt_given_name:   1 if given_name_check = 'match'
      - pt_coawardee:    1 if n_corroborating_coauthors >= 2
      - pt_subfield:     1 if subfield_fd > 0.75
      - pt_institution:  1 if institution_fd > 0.3
      - pt_works_au:     1 if works_count_au > 5
    total_score = sum of the six (0-6). NULL FD scores count as failing that point (no evidence
    is not the same as a point) -- same discipline as everywhere else FD scores are used.

    orcid_veto (HARD veto, independent of total_score): orcid_check = 'mismatch' -- both sides
    have a real ORCID and they differ, decisive evidence of a different real person (mirrors
    FilterCandidates.orcid_veto() in the now-superseded old pipeline, kept here deliberately per
    direct instruction -- a pure point total could otherwise still accept a confirmed-different
    person via the other five signals).

    status = 'ejected' if orcid_veto, else 'accepted' if total_score >= 4, else 'ejected'.
    reason: human-readable -- 'orcid_mismatch_veto', or 'score_N_of_6' either way (accepted or
    not), so a review of ejected rows shows exactly which score they missed the bar with, not
    just that they were rejected.

    Explicitly persisted so ejected candidates stay inspectable (direct user requirement,
    2026-09-17: "I will want to look at what is ejected too") -- this is NOT filtered down to
    accepted-only at the table level; that filtering happens only in the report renderer.
    Returns the row count."""
    con.execute("""
        CREATE OR REPLACE TABLE oax_resolve AS
        WITH scored AS (
            SELECT
                cluster_id,
                author_idx,
                full_name,
                orcid_check,
                CASE WHEN orcid_check = 'match' THEN 1 ELSE 0 END AS pt_orcid,
                CASE WHEN given_name_check = 'match' THEN 1 ELSE 0 END AS pt_given_name,
                CASE WHEN n_corroborating_coauthors >= 2 THEN 1 ELSE 0 END AS pt_coawardee,
                CASE WHEN subfield_fd > 0.75 THEN 1 ELSE 0 END AS pt_subfield,
                CASE WHEN institution_fd > 0.3 THEN 1 ELSE 0 END AS pt_institution,
                CASE WHEN works_count_au > 5 THEN 1 ELSE 0 END AS pt_works_au
            FROM oax_candidates
        )
        SELECT
            cluster_id,
            author_idx,
            full_name,
            pt_orcid,
            pt_given_name,
            pt_coawardee,
            pt_subfield,
            pt_institution,
            pt_works_au,
            pt_orcid + pt_given_name + pt_coawardee + pt_subfield + pt_institution + pt_works_au
                AS total_score,
            (orcid_check = 'mismatch') AS orcid_veto,
            CASE
                WHEN orcid_check = 'mismatch' THEN 'ejected'
                WHEN pt_orcid + pt_given_name + pt_coawardee + pt_subfield + pt_institution
                     + pt_works_au >= 4 THEN 'accepted'
                ELSE 'ejected'
            END AS status,
            CASE
                WHEN orcid_check = 'mismatch' THEN 'orcid_mismatch_veto'
                ELSE 'score_' || (pt_orcid + pt_given_name + pt_coawardee + pt_subfield
                                   + pt_institution + pt_works_au)::VARCHAR || '_of_6'
            END AS reason
        FROM scored
    """)
    con.execute("ALTER TABLE oax_resolve ADD PRIMARY KEY (cluster_id, author_idx)")
    return con.execute("SELECT count(*) FROM oax_resolve").fetchone()[0]


def build_arc_table(con: duckdb.DuckDBPyConnection) -> int:
    """(Re)build the `arc` table: one row per (ACIF, grant_code) -- grant/fellowship/amount
    facts. grant_ids on awards_cif_arc_only.parquet are unique_id strings
    ('grant_code_Name'), one per contributing investigator-row, not bare grant codes -- an
    announcement/current name-snapshot pair for the same grant produces two unique_ids for
    one real grant, so this groups by the recovered bare grant_code and takes
    bool_or(is_fellowship)/any_value(role_code) across whichever unique_id rows share it
    (they represent the same person on the same grant, just recorded under two name-forms).
    Returns the row count."""
    hep_crosswalk = _load_hep_crosswalk()
    crosswalk_df = pd.DataFrame(
        list(hep_crosswalk.items()), columns=["admin_org", "hep_code"]
    )
    con.register("_hep_crosswalk_df", crosswalk_df)

    con.execute(f"""
        CREATE OR REPLACE TABLE arc AS
        WITH exploded AS (
            SELECT cluster_id, unnest(grant_ids) AS unique_id
            FROM read_parquet('{ARC_ONLY_PARQUET}')
            WHERE excluded = FALSE
        ),
        with_grant_code AS (
            SELECT cluster_id, unique_id,
                   regexp_replace(unique_id, '_[^_]+$', '') AS grant_code
            FROM exploded
        ),
        per_item AS (
            SELECT g.cluster_id, g.grant_code, i.role_code, i.is_fellowship
            FROM with_grant_code g
            JOIN read_parquet('{INVESTIGATORS_RAW}') i ON i.unique_id = g.unique_id
        ),
        per_grant AS (
            SELECT cluster_id, grant_code,
                   any_value(role_code) AS role_code,
                   bool_or(is_fellowship) AS is_fellowship
            FROM per_item
            GROUP BY cluster_id, grant_code
        )
        SELECT
            pg.cluster_id,
            pg.grant_code,
            pg.role_code,
            pg.is_fellowship,
            gf.funding_commence_year,
            gf.funding_current,
            hc.hep_code
        FROM per_grant pg
        LEFT JOIN read_parquet('{GRANTS_FLAT}') gf ON gf.grant_code = pg.grant_code
        LEFT JOIN _hep_crosswalk_df hc ON hc.admin_org = gf.admin_org
    """)
    con.unregister("_hep_crosswalk_df")
    con.execute("ALTER TABLE arc ADD PRIMARY KEY (cluster_id, grant_code)")
    return con.execute("SELECT count(*) FROM arc").fetchone()[0]


def build_results_db() -> dict:
    """Rebuild results.db from scratch (all tables). Never touches any upstream pipeline
    file -- pure read+join. Returns {table_name: row_count}."""
    con = duckdb.connect(str(RESULTS_DB))
    try:
        n_title = build_title_table(con)
        n_arc = build_arc_table(con)
        n_oax_candidates = build_oax_candidates_table(con)
        n_oax_resolve = build_oax_resolve_table(con)
    finally:
        con.close()
    return {
        "title": n_title, "arc": n_arc,
        "oax_candidates": n_oax_candidates, "oax_resolve": n_oax_resolve,
    }


if __name__ == "__main__":
    counts = build_results_db()
    print(f"Built {RESULTS_DB}")
    for table, n in counts.items():
        print(f"  {table}: {n:,} rows")
