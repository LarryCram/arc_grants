"""
src/04_filter_candidates.py

REBUILD IN PROGRESS (started 2026-09-09) -- the old disambiguation cascade (ORCID match /
institution overlap / field score / probability / works_count dominance, in that fixed order,
picking exactly one winner) was archived to ZARCHIVE/src_archive_20260909/04_resolve_links.py
after tracing several real cases (Isakhan, Tyers, Marcel/Martin Jackson, Zeunert, Amati, Giblin,
Tele Tan) and finding two confirmed defects in it: (1) the given-name "character mismatch"
sanity check (_names_compat) can never actually reject anything, because ARC's own first_names
always contains a self-added bare initial that trivially satisfies its short-circuit condition;
(2) ORCID mismatch is treated as soft, moderate evidence (Splink m=0.15) rather than a hard
veto, even though the failure mode that softness is hedging against (ARC's own ORCID field
being stably-but-wrongly recorded) is empirically rare (~2-3 known cases) and cheaply recoverable
via manual_merges.csv when found -- whereas the false-positive risk it currently permits (two
different real people, each with their own real but different ORCID, both surviving to compete
for the same ARC person) is not.

Built as a class, FilterCandidates, deliberately container-first (2026-09-09, direct user
direction): the class shape is meant to drive out the remaining design decisions (the FD
comparison, the scoring formula) as they're filled in, rather than writing loose functions first
and wrapping them in a class once everything is settled. Same rationale as FetchOrcid
(src/utils/fetch_orcid.py) -- state that's expensive-ish to build but constant across every
candidate comparison (there: a materialized ORCID bulk table; here: the FOR2020-group ->
OAX-subfield lookup) is built once in __init__, not recomputed per call.

Proposed algorithm (from direct conversation, not yet fully implemented -- see method docstrings
for what's real vs. stub):
  a. Sort an ACIF's OAX candidates by works_count descending.
  b. Hard veto: reject any candidate whose own ORCID is non-null AND differs from the ARC
     person's recorded ORCID, regardless of how good its other evidence looks.
  c. For survivors, compute a frequency-distribution (value_counts, rarity-weighted) comparison
     across institution, subfield, and coauthor -- against the ARC person's own institution/FOR/
     coawardees. NOT YET DESIGNED -- "needs a well designed utility" (user's own words).
  d. Combine ORCID-match status (if any) and the FD-comparison result into one score per
     candidate.
  e. Accept the highest-scoring candidate, and any other high-scoring candidates too (fragment-
     merging, e.g. Tele Tan's 1-work "Tele Tan" duplicate) -- UNLESS a first-name clash rules a
     candidate out. The name-clash check itself needs rebuilding on the same informative
     full_name_keys logic already fixed for OAX-side dedup (_oax_names_compat) -- the old
     _names_compat() is confirmed broken (bare-initial short-circuit) and must not be reused as-is.

Still also holds the review/walkthrough capability built first (2026-09-09): loads every ACIF's
deduped OAX candidate pool (reusing populate_oax_candidates() -> dedup_oax_candidates() from
awards_cif.py UNCHANGED -- that pure-noise-removal step is not what's being rebuilt here), and
for the 2-candidate bucket -- sorted by ascending total works_count, smallest/simplest cases
first -- prints each cluster in the ARC / ARC-OAX-links / OAX display format used throughout
this session's case-by-case review.
"""

import sys
from collections import Counter
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, OAX_AUTHORS
from src.utils.for_resolve import for2020_group_name, oax_subfield_name
from src.utils.awards_cif import (
    load_awards_cif,
    populate_oax_candidates,
    dedup_oax_candidates,
    load_grant_for2020_codes,
    _load_institution_oax_crosswalk,
    ARC_ONLY_PARQUET,
)

OAX_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
LINKS = PROCESSED_DATA / "arc_oax_links.parquet"
GRANTS_FLAT = PROCESSED_DATA / "grants_flat.parquet"
AUTHORS = f"{OAX_AUTHORS}/*.parquet"
AUTHORSHIPS_HEP = PROCESSED_DATA / "authorships_hep.parquet"
WORKS_HEP = PROCESSED_DATA / "works_hep.parquet"

# 2026-09-10: durable, row-updatable state for the filter's own keep/drop/uncertain decisions
# per (cluster_id, oax_id) -- must live in a real on-disk DuckDB database, not parquet, because
# parquet is an immutable whole-file format (no row-wise UPDATE); a native DuckDB file gives real
# UPDATE/upsert support plus fast native reads, using the same engine already used everywhere
# else in this pipeline. Once this class's design settles, the table gets exported to parquet
# once (COPY ... TO) to match every other pipeline output's final resting format -- this .duckdb
# file is the mutable working store during development, not the final artifact.
PROVENANCE_DB = PROCESSED_DATA / "oax_provenance.duckdb"


class FilterCandidates:
    """Disambiguates an ACIF's deduped OAX candidate pool down to the correct match(es).

    Holds, as instance state built once in __init__: a DuckDB connection (persistent, backed by
    PROVENANCE_DB -- not the usual in-memory default -- so the oax_provenance table survives
    across separate runs/sessions rather than being rebuilt from scratch each time), and the
    FOR2020-group -> OAX-subfield lookup (small and fixed -- 213 valid FOR2020 groups, confirmed
    by brute-force enumeration via for_resolve.py -- so built once here rather than resolved live
    per candidate).
    """

    def __init__(self, con: duckdb.DuckDBPyConnection | None = None,
                 force_rebuild_caches: bool = False):
        self.con = con or duckdb.connect(str(PROVENANCE_DB))
        self._ensure_provenance_table()
        self._ensure_candidates_table()
        # 2026-09-10 (user-directed, "avoid repeated pre-processing"): both of these were being
        # recomputed from scratch on every single process start -- measured 6.6s
        # (_build_for_subfield_dict, a FIXED 213-entry taxonomy enumeration that basically never
        # changes) + 4.7s (load_grant_for2020_codes, resolving all 30,475 grants' FOR entries,
        # which only changes after a real 00_extract_arc.py rerun) = ~11s of pure setup cost on
        # every interactive call, dwarfing load_clusters_by_size()'s own 2s. Cached in
        # oax_provenance.duckdb, same pattern as acif_oax_candidates -- pass
        # force_rebuild_caches=True after raw ARC data or the FOR taxonomy actually changes (no
        # automatic freshness check yet, matching load_clusters_by_size()'s own explicit-only
        # convention).
        self.for_to_subfield: dict[str, str] = self._ensure_for_subfield_cache(force_rebuild_caches)
        self.grant_for2020: dict[str, list[dict]] = self._ensure_grant_for2020_cache(force_rebuild_caches)
        self.institution_oax_crosswalk: dict[str, str] = _load_institution_oax_crosswalk()
        self._ensure_fd_tables(force_rebuild_caches)

    def _ensure_fd_tables(self, force_rebuild: bool = False) -> None:
        """Precomputed institution/subfield frequency-distribution tables, built once in bulk
        (2026-09-11) rather than recomputed live per fd_compare() call:
          arc_institution_fd / arc_subfield_fd  -- one row per (cluster_id, key), same
              definitions as admin_org_counts()/subfield_counts() (single-org-grant gate,
              FOR2020-code union resolved through for_to_subfield).
          oax_institution_fd / oax_subfield_fd  -- one row per (author_idx, key), same
              definitions as oax_work_institution_counts()/oax_work_subfield_counts()
              (work-count-weighted, COUNT(DISTINCT work_idx)), built over the FULL ~2.78M-author
              HEP-context population -- not just today's arc_oax_links.parquet candidates --
              because a future direct-orcid lookup (bypassing Splink's own blocking) could
              surface an author never in today's candidate pool at all, and its FD needs to
              already exist when that happens, not be computed on first use (2026-09-11,
              user-directed).
          fd_pair_scores -- one row per (arc_id, oax_id) in arc_oax_links.parquet, the actual
              histogram-intersection institution_score/subfield_score for that pair, computed
              via the SAME normalize-then-sum(LEAST(...)) logic as _hist_intersection() but as
              one vectorized SQL join over every pair at once (182,759 pairs in ~1.3s) instead
              of a Python loop -- verified to reproduce _hist_intersection()'s own live-computed
              values exactly (spot-checked against DP0342529_LeoRadom's known
              institution_score=0.7055417700578991 / subfield_score=0.5167439464193715).
              NULL (not 0.0) on either axis when either side of that specific pair has zero FD
              rows at all -- same "absence isn't evidence" distinction _hist_intersection()
              already makes, preserved here rather than collapsed by the join.

        Rebuilt only when empty or force_rebuild=True -- no automatic freshness check yet,
        matching every other cache in this class (pass force_rebuild_caches=True after
        awards_cif_arc_only.parquet, openalex_authors_prep.parquet, or arc_oax_links.parquet
        actually change)."""
        n = self.con.execute("""
            SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'fd_pair_scores'
        """).fetchone()[0]
        if not force_rebuild and n > 0:
            n_rows = self.con.execute("SELECT COUNT(*) FROM fd_pair_scores").fetchone()[0]
            if n_rows > 0:
                return

        self.con.execute(f"""
            CREATE OR REPLACE TABLE oax_subfield_fd AS
            SELECT a.author_idx, w.subfield_name, COUNT(DISTINCT a.work_idx) AS n
            FROM read_parquet('{AUTHORSHIPS_HEP}') a
            JOIN read_parquet('{WORKS_HEP}') w USING (work_idx)
            WHERE w.subfield_name IS NOT NULL
            GROUP BY a.author_idx, w.subfield_name
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE oax_institution_fd AS
            SELECT author_idx,
                   'https://openalex.org/I' || institution_idx::VARCHAR AS institution_id,
                   COUNT(DISTINCT work_idx) AS n
            FROM read_parquet('{AUTHORSHIPS_HEP}')
            WHERE institution_idx IS NOT NULL
            GROUP BY author_idx, institution_idx
        """)

        self.con.execute(f"""
            CREATE OR REPLACE TEMP TABLE cluster_grants AS
            SELECT DISTINCT cluster_id, regexp_replace(gid, '_[^_]*$', '') AS grant_code
            FROM (SELECT cluster_id, unnest(grant_ids) AS gid
                  FROM read_parquet('{ARC_ONLY_PARQUET}') WHERE excluded = FALSE)
        """)
        self.con.execute("""
            CREATE OR REPLACE TABLE arc_subfield_fd AS
            SELECT cg.cluster_id, f.subfield, COUNT(*) AS n
            FROM cluster_grants cg
            JOIN grant_for2020_cache g ON g.grant_code = cg.grant_code
            CROSS JOIN UNNEST(g.codes) AS t(entry)
            JOIN for_subfield_dict f ON f.code = t.entry.code
            GROUP BY cg.cluster_id, f.subfield
        """)
        cw_df = pd.DataFrame(list(self.institution_oax_crosswalk.items()),
                             columns=["admin_org", "institution_id"])
        self.con.register("crosswalk", cw_df)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE arc_institution_fd AS
            SELECT cg.cluster_id, cw.institution_id, COUNT(*) AS n
            FROM cluster_grants cg
            JOIN read_parquet('{GRANTS_FLAT}') g
                ON g.grant_code = cg.grant_code AND g.n_eligible_orgs = 1
            JOIN crosswalk cw ON cw.admin_org = g.admin_org
            GROUP BY cg.cluster_id, cw.institution_id
        """)
        self.con.unregister("crosswalk")

        self.con.execute("""
            CREATE OR REPLACE TEMP TABLE arc_inst_norm AS
            SELECT cluster_id, institution_id, n * 1.0 / SUM(n) OVER (PARTITION BY cluster_id) AS prop
            FROM arc_institution_fd
        """)
        self.con.execute("""
            CREATE OR REPLACE TEMP TABLE arc_sf_norm AS
            SELECT cluster_id, subfield, n * 1.0 / SUM(n) OVER (PARTITION BY cluster_id) AS prop
            FROM arc_subfield_fd
        """)
        self.con.execute("""
            CREATE OR REPLACE TEMP TABLE oax_inst_norm AS
            SELECT author_idx, institution_id, n * 1.0 / SUM(n) OVER (PARTITION BY author_idx) AS prop
            FROM oax_institution_fd
        """)
        self.con.execute("""
            CREATE OR REPLACE TEMP TABLE oax_sf_norm AS
            SELECT author_idx, subfield_name, n * 1.0 / SUM(n) OVER (PARTITION BY author_idx) AS prop
            FROM oax_subfield_fd
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TEMP TABLE pairs AS
            SELECT arc_id, oax_id, match_probability,
                   TRY_CAST(regexp_extract(oax_id, 'A(\\d+)', 1) AS BIGINT) AS author_idx
            FROM read_parquet('{LINKS}')
        """)
        self.con.execute("""
            CREATE OR REPLACE TABLE fd_pair_scores AS
            WITH inst_overlap AS (
                SELECT p.arc_id, p.oax_id, SUM(LEAST(a.prop, o.prop)) AS overlap
                FROM pairs p
                JOIN arc_inst_norm a ON a.cluster_id = p.arc_id
                JOIN oax_inst_norm o ON o.author_idx = p.author_idx AND o.institution_id = a.institution_id
                GROUP BY p.arc_id, p.oax_id
            ),
            sf_overlap AS (
                SELECT p.arc_id, p.oax_id, SUM(LEAST(a.prop, o.prop)) AS overlap
                FROM pairs p
                JOIN arc_sf_norm a ON a.cluster_id = p.arc_id
                JOIN oax_sf_norm o ON o.author_idx = p.author_idx AND o.subfield_name = a.subfield
                GROUP BY p.arc_id, p.oax_id
            ),
            has_data AS (
                SELECT p.arc_id, p.oax_id,
                       EXISTS (SELECT 1 FROM arc_inst_norm a WHERE a.cluster_id = p.arc_id) AS has_arc_inst,
                       EXISTS (SELECT 1 FROM oax_inst_norm o WHERE o.author_idx = p.author_idx) AS has_oax_inst,
                       EXISTS (SELECT 1 FROM arc_sf_norm a WHERE a.cluster_id = p.arc_id) AS has_arc_sf,
                       EXISTS (SELECT 1 FROM oax_sf_norm o WHERE o.author_idx = p.author_idx) AS has_oax_sf
                FROM pairs p
            )
            SELECT p.arc_id, p.oax_id, p.match_probability,
                   CASE WHEN h.has_arc_inst AND h.has_oax_inst THEN COALESCE(io.overlap, 0.0) END AS institution_score,
                   CASE WHEN h.has_arc_sf AND h.has_oax_sf THEN COALESCE(sfo.overlap, 0.0) END AS subfield_score
            FROM pairs p
            JOIN has_data h ON h.arc_id = p.arc_id AND h.oax_id = p.oax_id
            LEFT JOIN inst_overlap io ON io.arc_id = p.arc_id AND io.oax_id = p.oax_id
            LEFT JOIN sf_overlap sfo ON sfo.arc_id = p.arc_id AND sfo.oax_id = p.oax_id
        """)

    def _ensure_provenance_table(self) -> None:
        """oax_provenance: one row per (cluster_id, oax_id) -- the filter's own keep/drop/
        uncertain verdict for that OAX candidate, why (a controlled-vocabulary reason code, not
        free text -- matches this project's own resolved_by-style convention elsewhere), and
        which stage produced it (dedup / orcid_veto / fd_compare / ...). works_count is cached
        here too so the test-1 sort (candidate count ASC, then works DESC) doesn't need to
        re-fetch it from the raw authors table on every run. PRIMARY KEY on (cluster_id, oax_id)
        makes record_provenance()'s upsert well-defined -- re-annotating a candidate replaces its
        prior verdict rather than accumulating duplicate rows."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS oax_provenance (
                cluster_id   VARCHAR,
                oax_id       VARCHAR,
                works_count  BIGINT,
                status       VARCHAR,  -- 'keep' | 'drop' | 'uncertain'
                reason       VARCHAR,  -- controlled-vocabulary code, e.g. 'orcid_mismatch'
                stage        VARCHAR,  -- which check produced this verdict, e.g. 'orcid_veto'
                PRIMARY KEY (cluster_id, oax_id)
            )
        """)

    def _ensure_candidates_table(self) -> None:
        """acif_oax_candidates: the prepared input to 04_ -- one row per (cluster_id, oax_id)
        surviving populate_oax_candidates()/dedup_oax_candidates(), persisted so that expensive
        recomputation (both functions re-derive from arc_oax_links.parquet + OpenAlex prep
        tables) doesn't have to re-run every time load_clusters_by_size() is called -- the
        "this step is repeated many times -- persist as a duckdb database" note in
        design 04_ filter.md, applied to the candidate pool itself, not just provenance
        verdicts about it."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS acif_oax_candidates (
                cluster_id VARCHAR,
                oax_id     VARCHAR,
                PRIMARY KEY (cluster_id, oax_id)
            )
        """)

    def _persist_candidates(self, clusters: list) -> None:
        """Full replace, not upsert -- this table always reflects one complete
        populate_oax_candidates()/dedup_oax_candidates() run over the whole population, never a
        partial/incremental one."""
        rows = [(c.cluster_id, oid) for c in clusters for oid in c.oax_candidates]
        self.con.execute("DELETE FROM acif_oax_candidates")
        if rows:
            self.con.executemany("INSERT INTO acif_oax_candidates VALUES (?, ?)", rows)

    def record_provenance(self, cluster_id: str, oax_id: str, works_count: int,
                           status: str, reason: str, stage: str) -> None:
        """Upsert one (cluster_id, oax_id) verdict -- re-recording an already-annotated
        candidate (e.g. a later stage revisiting an earlier one's call) replaces the row rather
        than duplicating it."""
        assert status in ("keep", "drop", "uncertain"), f"bad status: {status!r}"
        self.con.execute("""
            INSERT INTO oax_provenance VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (cluster_id, oax_id) DO UPDATE SET
                works_count = excluded.works_count,
                status      = excluded.status,
                reason      = excluded.reason,
                stage       = excluded.stage
        """, [cluster_id, oax_id, works_count, status, reason, stage])

    def load_provenance(self, cluster_id: str | None = None) -> "pd.DataFrame":
        """Read back recorded verdicts -- all of them, or just one ACIF's if cluster_id given."""
        if cluster_id is None:
            return self.con.execute("SELECT * FROM oax_provenance").fetchdf()
        return self.con.execute(
            "SELECT * FROM oax_provenance WHERE cluster_id = ?", [cluster_id]
        ).fetchdf()

    @staticmethod
    def _author_idx(oax_id: str) -> int:
        """OpenAlex author_idx -- the native integer key duckdb actually indexes/joins
        authors/works/topics on -- from an oax_id URL string ('https://openalex.org/A<digits>').
        Always convert through this, never inline the string-strip: oax_candidates/oax_id/
        unique_id are stored as URL strings throughout this project's own schema (the correct
        form at the human-facing/output boundary), but any dict keyed for an internal lookup
        against a raw OpenAlex table should be keyed on author_idx directly, not on a
        reconstructed URL string built solely to serve as a Python dict key."""
        return int(oax_id.rsplit("A", 1)[-1])

    def _build_for_subfield_dict(self) -> dict[str, str]:
        """{FOR2020 4-digit group code -> OAX subfield name}, every valid group, built once.
        Enumerated by brute-force testing the code space through for_resolve.py (the package's
        Resolver() has no bulk-listing method) -- confirmed 213 valid codes, all 213 resolve to
        a real OAX subfield, 0 failures."""
        out = {}
        for div in range(1, 100):
            for grp in range(1, 100):
                code = f"{div:02d}{grp:02d}"
                if for2020_group_name(code) is not None:
                    out[code] = oax_subfield_name(code)
        return out

    def _ensure_for_subfield_cache(self, force_rebuild: bool = False) -> dict[str, str]:
        """for_subfield_dict table -- caches _build_for_subfield_dict()'s output (6.6s to
        recompute, a fixed 213-entry taxonomy enumeration that only changes if the
        research_classification package itself is upgraded)."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS for_subfield_dict (
                code VARCHAR PRIMARY KEY, subfield VARCHAR
            )
        """)
        n = self.con.execute("SELECT COUNT(*) FROM for_subfield_dict").fetchone()[0]
        if force_rebuild or n == 0:
            out = self._build_for_subfield_dict()
            self.con.execute("DELETE FROM for_subfield_dict")
            self.con.executemany("INSERT INTO for_subfield_dict VALUES (?, ?)", list(out.items()))
            return out
        return dict(self.con.execute("SELECT code, subfield FROM for_subfield_dict").fetchall())

    def _ensure_grant_for2020_cache(self, force_rebuild: bool = False) -> dict[str, list[dict]]:
        """grant_for2020_cache table -- caches load_grant_for2020_codes()'s output (4.7s to
        recompute, resolving every one of 30,475 grants' FOR entries; only changes after a real
        00_extract_arc.py rerun on fresh ARC data). Nested STRUCT-list column persisted via a
        registered pandas DataFrame, not executemany (DuckDB's Python executemany doesn't
        reliably bind nested list[dict] parameters the way a DataFrame->Arrow insert does)."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS grant_for2020_cache (
                grant_code VARCHAR PRIMARY KEY,
                codes STRUCT(code VARCHAR, "name" VARCHAR, is_primary BOOLEAN, confidence DOUBLE)[]
            )
        """)
        n = self.con.execute("SELECT COUNT(*) FROM grant_for2020_cache").fetchone()[0]
        if force_rebuild or n == 0:
            out = load_grant_for2020_codes()
            self.con.execute("DELETE FROM grant_for2020_cache")
            df = pd.DataFrame([{"grant_code": g, "codes": codes} for g, codes in out.items()])
            self.con.register("_g4020_df", df)
            self.con.execute("INSERT INTO grant_for2020_cache SELECT * FROM _g4020_df")
            self.con.unregister("_g4020_df")
            return out
        df = self.con.execute("SELECT grant_code, codes FROM grant_for2020_cache").fetchdf()
        return {row.grant_code: [dict(c) for c in row.codes] for row in df.itertuples()}

    # ------------------------------------------------------------------
    # ARC-side frequency distributions (item #29) -- the propensity-matching input against an
    # OAX candidate's own institution/subfield distribution. Counter, not list[dict] -- see
    # this session's own design discussion: the destination is a histogram-intersection-style
    # score, which Counter arithmetic (.total(), fractional overlap) supports directly, and a
    # serialized list[dict] form is only ever needed at a print/persist boundary, not as the
    # primary representation.
    # ------------------------------------------------------------------

    @staticmethod
    def _grant_codes(grant_ids: list[str]) -> list[str]:
        """AwardsCIF.grant_ids stores compound unique_id-style strings
        ('DE190100015_ChristophNitsche', i.e. grant_code_Name), one per contributing
        investigator-row -- NOT bare grant_code. Recover the bare code via rsplit("_", 1)[0]
        (matching awards_cif.py's own established convention for this exact operation, e.g.
        `grant_code = gid.rsplit("_", 1)[0]`), deduped: an announcement/current name-snapshot
        pair for the same person on one real grant would otherwise double-count that grant."""
        return sorted({g.rsplit("_", 1)[0] for g in grant_ids})

    def admin_org_counts(self, grant_ids: list[str]) -> Counter:
        """{OAX institution_id: count} across this ACIF's own grants -- one vote per grant,
        ONLY for grants where n_eligible_orgs == 1 (2026-09-10, user-directed). Deliberately
        admin_org only, not the wider eligible_orgs/inst_arr set, AND deliberately restricted
        to single-org grants: ARC's raw data has no field tying a specific investigator to a
        specific organisation, so admin_org is only a trustworthy per-person signal when there
        was no other org on the grant to be ambiguous with -- the same n_eligible_orgs==1 /
        len(inst_arr)==1 single-org gate already established in the 2026-08-25 inst_arr
        widening work (04_resolve_links.py's arc_all_single_org). A multi-org grant contributes
        nothing here rather than a possibly-wrong vote.

        Resolved through institution_oax_crosswalk (admin_orgs.csv organisationName_alias ->
        OAX institution_id) rather than left as a raw admin_org name string -- the SAME id
        space an OAX candidate's own inst_ids use, so this FD is directly comparable to
        section 3's OAX institution story (2026-09-10, user-directed: 'I want to compare with
        the oax inst story'), not just human-eyeballed name-vs-URL. A name with no crosswalk
        entry (an unmapped alias, or a genuinely non-HEP org that slipped past the
        n_eligible_orgs==1 filter) contributes nothing rather than a bare, incomparable
        string."""
        codes = self._grant_codes(grant_ids)
        if not codes:
            return Counter()
        ids_sql = ",".join(f"'{g}'" for g in codes)
        df = self.con.execute(f"""
            SELECT admin_org FROM read_parquet('{GRANTS_FLAT}')
            WHERE grant_code IN ({ids_sql}) AND n_eligible_orgs = 1
        """).fetchdf()
        return Counter(
            self.institution_oax_crosswalk[v] for v in df["admin_org"]
            if v and v in self.institution_oax_crosswalk
        )

    def for2020_counts(self, grant_ids: list[str]) -> Counter:
        """{FOR2020 4-digit group code: count} across this ACIF's own grants -- one vote per
        grant that carries the code (load_grant_for2020_codes() already dedupes within one
        grant by code, so a grant can never contribute more than 1 to any single code's count).
        Unlike admin_org_counts(), not restricted to single-org grants -- a FOR code describes
        the grant's own subject matter, not a specific investigator's institution, so it carries
        no multi-investigator attribution ambiguity."""
        counts = Counter()
        for g in self._grant_codes(grant_ids):
            for entry in self.grant_for2020.get(g, []):
                counts[entry["code"]] += 1
        return counts

    def subfield_counts(self, grant_ids: list[str]) -> Counter:
        """{OAX subfield name: count} -- for2020_counts() resolved through for_to_subfield and
        re-aggregated, so multiple FOR2020 groups mapping to the same OAX subfield combine into
        one entry. This is the unit fd_compare() will actually want to match against an OAX
        candidate's own subfield distribution -- both sides in the same vocabulary from the
        start, not FOR2020 groups on one side and OAX subfields on the other."""
        out = Counter()
        for code, n in self.for2020_counts(grant_ids).items():
            sf = self.for_to_subfield.get(code)
            if sf:
                out[sf] += n
        return out

    @staticmethod
    def oax_subfield_counts(subfield_names: list) -> Counter:
        """{OAX subfield name: count} -- built from a candidate's own subfield_names column
        (openalex_authors_prep.parquet), one entry per OpenAlex topic (undeduped). Approximate:
        OpenAlex caps an author's own topics list at ~25 entries regardless of true publication
        volume, so this counts topic-list SLOTS, not actual works. Superseded by
        oax_work_subfield_counts() below for anything comparing against real publication
        volume -- kept here as the cheap, no-extra-query fallback when only the
        already-fetched openalex_authors_prep.parquet row is available."""
        return Counter(subfield_names)

    def oax_work_subfield_counts(self, author_idx: int) -> Counter:
        """{OAX subfield name: count of DISTINCT works} for one candidate -- the real,
        work-count-weighted counterpart, better than oax_subfield_counts() (2026-09-10,
        user-directed: 'show that instead of the current subfield FD -- it will be better').
        oax_subfield_counts() counts entries in OpenAlex's own capped ~25-topic-per-author
        list, not actual publications, so a person with hundreds of HEP-context works still
        gets at most ~25 votes spread across however many subfields their top topics touch --
        this instead joins authorships_hep.parquet (work_idx, author_idx) to works_hep.parquet's
        real per-work topic rows (work_idx, subfield_name), scoped to this one author_idx, and
        counts COUNT(DISTINCT work_idx) per subfield so a work with 2+ topics in the same
        subfield only counts once. Same live-query-scoped-to-a-handful-of-ids pattern as
        _fetch_candidate_info() -- no population-wide rebuild needed, since this is only ever
        computed for the specific candidates under review at any moment."""
        rows = self.con.execute(f"""
            SELECT w.subfield_name, COUNT(DISTINCT a.work_idx) AS n
            FROM read_parquet('{AUTHORSHIPS_HEP}') a
            JOIN read_parquet('{WORKS_HEP}') w USING (work_idx)
            WHERE a.author_idx = ? AND w.subfield_name IS NOT NULL
            GROUP BY w.subfield_name
            ORDER BY n DESC
        """, [author_idx]).fetchall()
        return Counter(dict(rows))

    def oax_work_count(self, author_idx: int) -> int:
        """This candidate's real, HEP-context work count -- COUNT(DISTINCT work_idx) from
        authorships_hep.parquet, scoped to this one author_idx (2026-09-10, user-directed):
        authors/*.parquet's own works_count is OpenAlex's GLOBAL lifetime figure (every work
        anywhere, any type, any country) -- authorships_hep.parquet is already filtered to
        HEP-linked, eligible-type works, so the two numbers are not the same population and
        showing the global one next to HEP-scoped FDs is misleading (confirmed concretely:
        Deborah Stevenson's real ~2,200 book reviews inflate the global 2,719 to look like a
        mega-author, but none of them appear in authorships_hep.parquet at all -- her real
        HEP-context count is 38). Not the same number as summing oax_work_subfield_counts()'s
        buckets either -- a work touching 2+ subfields is counted once per subfield there, so
        that sum overcounts; this is the true, once-per-work figure."""
        return self.con.execute(f"""
            SELECT COUNT(DISTINCT work_idx) FROM read_parquet('{AUTHORSHIPS_HEP}')
            WHERE author_idx = ?
        """, [author_idx]).fetchone()[0]

    def oax_work_institution_counts(self, author_idx: int) -> Counter:
        """{OAX institution_id: count of DISTINCT works} for one candidate -- the real,
        work-count-weighted institution FD (2026-09-10, user-directed: an external lookup like
        Google Scholar doesn't scale as part of the actual filter design -- this pipeline's own
        institution FD needs to be able to do that corroborating job on its own). inst_ids on
        openalex_authors_prep.parquet is a list_distinct()-deduped SET with no counts at all
        (see this session's own design discussion), so -- same as subfields above -- this goes
        straight to authorships_hep.parquet's own institution_idx (one row per work x author x
        institution), scoped to this one author_idx, COUNT(DISTINCT work_idx) per institution
        (a work naming 2 different institutions for this author counts once for EACH -- a
        genuine cross-appointment signal, never double-counted within one institution).
        institution_idx converted to the OAX institution_id URL form, the SAME id space
        admin_org_counts()'s ARC-side FD already resolves to, so both sides are directly
        comparable by fd_compare() without a human cross-checking IDs by eye."""
        rows = self.con.execute(f"""
            SELECT 'https://openalex.org/I' || institution_idx::VARCHAR AS inst_id,
                   COUNT(DISTINCT work_idx) AS n
            FROM read_parquet('{AUTHORSHIPS_HEP}')
            WHERE author_idx = ? AND institution_idx IS NOT NULL
            GROUP BY institution_idx
            ORDER BY n DESC
        """, [author_idx]).fetchall()
        return Counter(dict(rows))

    # ------------------------------------------------------------------
    # Disambiguation logic -- real pieces vs. stubs, per the algorithm above
    # ------------------------------------------------------------------

    def orcid_veto(self, arc_orcid: str | None, oax_orcid: str | None) -> bool:
        """True if this candidate must be rejected outright: both sides have a recorded ORCID
        and they differ. OpenAlex's own orcid field is a full URL ("https://orcid.org/0000-...")
        while ARC's is bare -- compare via .endswith(), the same fix already applied elsewhere in
        this project (channel_piles(), oeuvre_build.py's Stage 3 ORCID gate,
        test2_orcid_top_candidate_rates() above) for this exact mismatch. A null on either side is
        NOT a veto -- absence of evidence isn't evidence of a different person, only a genuine,
        confirmed mismatch is. Real, implemented (not a stub) -- the logic is fully specified and
        doesn't need the FD-comparison utility."""
        if not arc_orcid or oax_orcid is None or (isinstance(oax_orcid, float) and pd.isna(oax_orcid)):
            return False
        return not str(oax_orcid).endswith(str(arc_orcid))

    @staticmethod
    def _hist_intersection(a: Counter, b: Counter) -> float | None:
        """Histogram-intersection similarity between two frequency distributions, normalized to
        proportions first -- sum(min(fa[k], fb[k]) for k in either) -- bounded [0,1], 1.0 =
        identical distributions, 0.0 = fully disjoint. None (not 0.0) when either side has zero
        total mass -- 'no FD evidence available on this axis' is not the same claim as
        'evidence of a mismatch,' the same absence-isn't-evidence principle already used
        throughout this project (au_signal, middle_match, coinvestigator_match)."""
        ta, tb = sum(a.values()), sum(b.values())
        if not ta or not tb:
            return None
        fa = {k: v / ta for k, v in a.items()}
        fb = {k: v / tb for k, v in b.items()}
        return sum(min(fa.get(k, 0.0), fb.get(k, 0.0)) for k in set(fa) | set(fb))

    def _match_probability(self, cluster_id: str, oax_id: str) -> float | None:
        """This pair's own Splink match_probability from arc_oax_links.parquet, the independent
        signal fd_compare()'s ejection rule combines with FD overlap."""
        row = self.con.execute(f"""
            SELECT match_probability FROM read_parquet('{LINKS}')
            WHERE arc_id = ? AND oax_id = ?
        """, [cluster_id, oax_id]).fetchone()
        return row[0] if row else None

    def _arc_institution_fd(self, cluster_id: str) -> Counter:
        """{OAX institution_id: count} for one ACIF, from the precomputed arc_institution_fd
        table (2026-09-11) -- same definition as admin_org_counts() (single-org grants only,
        resolved through institution_oax_crosswalk), built once in bulk over the whole
        population instead of recomputed per call. See admin_org_counts()'s own docstring for
        why the underlying FD is defined this way; this is purely a faster read of the same
        thing."""
        rows = self.con.execute(
            "SELECT institution_id, n FROM arc_institution_fd WHERE cluster_id = ?",
            [cluster_id],
        ).fetchall()
        return Counter(dict(rows))

    def _arc_subfield_fd(self, cluster_id: str) -> Counter:
        """{OAX subfield name: count} for one ACIF, from the precomputed arc_subfield_fd table
        (2026-09-11) -- same definition as subfield_counts(), built once in bulk instead of
        recomputed per call."""
        rows = self.con.execute(
            "SELECT subfield, n FROM arc_subfield_fd WHERE cluster_id = ?",
            [cluster_id],
        ).fetchall()
        return Counter(dict(rows))

    def _oax_institution_fd(self, author_idx: int) -> Counter:
        """{OAX institution_id: count of DISTINCT works} for one candidate, from the
        precomputed oax_institution_fd table (2026-09-11) -- same definition as
        oax_work_institution_counts(), built once over the full ~2.78M-author HEP-context
        population instead of a live join against authorships_hep.parquet per candidate."""
        rows = self.con.execute(
            "SELECT institution_id, n FROM oax_institution_fd WHERE author_idx = ?",
            [author_idx],
        ).fetchall()
        return Counter(dict(rows))

    def _oax_subfield_fd(self, author_idx: int) -> Counter:
        """{OAX subfield name: count of DISTINCT works} for one candidate, from the precomputed
        oax_subfield_fd table (2026-09-11) -- same definition as oax_work_subfield_counts(),
        built once over the full HEP-context author population."""
        rows = self.con.execute(
            "SELECT subfield_name, n FROM oax_subfield_fd WHERE author_idx = ?",
            [author_idx],
        ).fetchall()
        return Counter(dict(rows))

    def fd_compare(self, cluster, oax_id: str) -> dict:
        """Hard ejection rule (2026-09-10, user-directed -- 'like mismatched orcid, this is the
        final arbiter for ejection', not a soft triage signal): eject a candidate if
        match_probability < 0.9 AND max(institution_score, subfield_score) < 0.8 -- a missing
        score (None, no FD evidence on that axis) counts as 0.0 for this comparison only (it
        can't rescue a candidate here, though it's never treated as a mismatch elsewhere, e.g.
        orcid_veto()). Symmetric in spirit to orcid_veto(): a candidate survives if EITHER an
        independent Splink match_probability >=0.9 OR a strong FD overlap on either axis clears
        0.8 -- only a candidate weak on BOTH independent signals simultaneously gets ejected, a
        deliberately conservative design (never rejects on one weak signal alone). Returns every
        component, not just the verdict, so record_provenance() can log the actual reasoning.

        2026-09-11: reads the precomputed fd_pair_scores table (one vectorized SQL join over
        every arc_oax_links.parquet pair at once, verified to reproduce _hist_intersection()'s
        own live-computed values exactly) instead of computing the histogram intersection fresh
        per call. Falls back to a live computation (via the underlying _arc_*_fd/_oax_*_fd
        lookups, unchanged) for a pair that isn't in arc_oax_links.parquet at all -- e.g. a
        candidate a future direct-orcid lookup surfaces that Splink's own blocking never found,
        which fd_pair_scores structurally can't cover since it's built from that same links
        file."""
        row = self.con.execute("""
            SELECT institution_score, subfield_score, match_probability
            FROM fd_pair_scores WHERE arc_id = ? AND oax_id = ?
        """, [cluster.cluster_id, oax_id]).fetchone()
        if row is not None:
            inst_score, sf_score, match_prob = row
        else:
            arc_inst = self._arc_institution_fd(cluster.cluster_id)
            arc_sf = self._arc_subfield_fd(cluster.cluster_id)
            author_idx = self._author_idx(oax_id)
            oax_inst = self._oax_institution_fd(author_idx)
            oax_sf = self._oax_subfield_fd(author_idx)
            inst_score = self._hist_intersection(arc_inst, oax_inst)
            sf_score = self._hist_intersection(arc_sf, oax_sf)
            match_prob = self._match_probability(cluster.cluster_id, oax_id)
        fd_max = max(inst_score or 0.0, sf_score or 0.0)
        eject = (match_prob or 0.0) < 0.9 and fd_max < 0.8
        # Under the max()-based rule, eject==True always implies BOTH axes are individually
        # below 0.8 (that's what makes the max low) -- record which one(s), not a single
        # generic label, so a human/future caller can see the actual reasoning per axis
        # (2026-09-10, user-directed) rather than just "fd_reject" with no detail.
        reasons = []
        if (inst_score or 0.0) < 0.8:
            reasons.append("fd_inst_low")
        if (sf_score or 0.0) < 0.8:
            reasons.append("fd_for_low")
        return {
            "match_probability": match_prob,
            "institution_score": inst_score,
            "subfield_score": sf_score,
            "fd_max": fd_max,
            "eject": eject,
            "reason": "+".join(reasons) if reasons else None,
        }

    def _oax_orcid(self, author_idx: int) -> str | None:
        """This candidate's own OpenAlex-recorded orcid (full URL form, e.g.
        'https://orcid.org/0000-...') -- the value orcid_veto() compares the ACIF's own orcid
        against."""
        row = self.con.execute(
            f"SELECT orcid FROM read_parquet('{AUTHORS}') WHERE author_idx = ?", [author_idx]
        ).fetchone()
        return row[0] if row else None

    def score(self, cluster, oax_id: str) -> float:
        """Combine orcid_veto() and fd_compare() into one ranking score per candidate. Both are
        hard gates (per their own docstrings), not soft penalties -- a vetoed or fd-ejected
        candidate scores 0.0. A surviving candidate is ranked by its own independent Splink
        match_probability: fd_compare() only ever filters here, it never adds to the score
        (per its own docstring, it's 'the final arbiter for ejection,' not a triage signal)."""
        author_idx = self._author_idx(oax_id)
        arc_orcid = cluster.orcids[0] if cluster.orcids else None
        oax_orcid = self._oax_orcid(author_idx)
        if self.orcid_veto(arc_orcid, oax_orcid):
            return 0.0
        fd = self.fd_compare(cluster, oax_id)
        if fd["eject"]:
            return 0.0
        return fd["match_probability"] or 0.0

    def resolve(self, cluster) -> list[str]:
        """Full pipeline for one ACIF: score() every candidate, accept the top scorer plus any
        other candidate that ALSO clears the high_confidence bar (>=0.9) -- fragment-merging
        (e.g. OAX-side identity split across 2+ author_idx), not a forced single winner, per the
        original design doc. Empty list if nothing survives orcid_veto()/fd_compare()'s gates.

        NOT YET BUILT: the first-name-clash check the design doc also calls for (rebuilt on the
        same informative full_name_keys logic already fixed for OAX-side dedup in
        _oax_names_compat(), explicitly not the old, confirmed-broken _names_compat()).
        Deliberately left out of this pass rather than either blocking on it or reusing the
        broken version -- a future pass should add it as an additional filter narrowing the
        accepted set, not change what's already here."""
        scored = [(oax_id, self.score(cluster, oax_id)) for oax_id in cluster.oax_candidates]
        survivors = [(oid, s) for oid, s in scored if s > 0.0]
        if not survivors:
            return []
        top_score = max(s for _, s in survivors)
        return [oid for oid, s in survivors if s == top_score or s >= 0.9]

    # ------------------------------------------------------------------
    # Review/walkthrough tooling (built first, already working)
    # ------------------------------------------------------------------

    def print_arc_section(self, cluster_id: str) -> None:
        df = self.con.execute(f"""
            SELECT cluster_id, full_names, orcids, first_names, grant_ids
            FROM read_parquet('{ARC_ONLY_PARQUET}') WHERE cluster_id = '{cluster_id}'
        """).fetchdf()
        print("=== 1. ARC ===")
        for c in df.columns:
            v = df.iloc[0][c]
            print(f"  {c}: {list(v) if hasattr(v, '__len__') and not isinstance(v, str) else v}")

        grant_ids = list(df.iloc[0]["grant_ids"])
        admin_fd = self.admin_org_counts(grant_ids)
        for2020_fd = self.for2020_counts(grant_ids)
        subfield_fd = self.subfield_counts(grant_ids)

        print("  admin_org FD (single-org grants only, count desc):",
              admin_fd.most_common())
        print("  for2020 FD (count desc):",
              [(code, for2020_group_name(code), n) for code, n in for2020_fd.most_common()])
        print("  OAX subfield FD (count desc):", subfield_fd.most_common())

    def print_links_section(self, cluster_id: str, oax_ids: list[str]) -> None:
        oid_list = ",".join(f"'{o}'" for o in oax_ids)
        df = self.con.execute(f"""
            SELECT arc_id, oax_id, match_probability, high_confidence
            FROM read_parquet('{LINKS}') WHERE arc_id = '{cluster_id}' AND oax_id IN ({oid_list})
            ORDER BY oax_id
        """).fetchdf()
        print()
        print("=== 2. ARC/OAX ===")
        print(df.to_string(index=False))

    def print_oax_section(self, cluster_id: str, grant_ids: list[str],
                           oax_ids_sorted_by_works_desc: list[str], works_by_id: dict) -> None:
        oid_list = ",".join(f"'{o}'" for o in oax_ids_sorted_by_works_desc)
        df = self.con.execute(f"""
            SELECT unique_id, full_name, full_name_keys, orcid, inst_ids,
                   subfield_names, first_name, family_name_main
            FROM read_parquet('{OAX_PREP}') WHERE unique_id IN ({oid_list})
        """).fetchdf()
        df = df.set_index("unique_id").loc[oax_ids_sorted_by_works_desc].reset_index()
        # Computed once -- doesn't vary per candidate, only the OAX side does.
        arc_inst = self.admin_org_counts(grant_ids)
        arc_sf = self.subfield_counts(grant_ids)
        print()
        print("=== 3. OAX (sorted by works_count desc) ===")
        for _, r in df.iterrows():
            aidx = self._author_idx(r["unique_id"])
            hep_wc = self.oax_work_count(aidx)
            print(f"  -- {r['unique_id']} (HEP-context works_count={hep_wc}  "
                  f"[global: {works_by_id[aidx]}]) --")
            print("     full_name (singular):", repr(r["full_name"]))
            print("     full_name_keys:      ", list(r["full_name_keys"]))
            print("     orcid:", r["orcid"])
            inst_fd = self.oax_work_institution_counts(aidx)
            print("     institution FD (work-count weighted, count desc):", inst_fd.most_common())
            sf_fd = self.oax_work_subfield_counts(aidx)
            print("     subfield FD (work-count weighted, count desc):", sf_fd.most_common())
            print("     first_name:", r["first_name"], "| family_name_main:", r["family_name_main"])
            inst_score = self._hist_intersection(arc_inst, inst_fd)
            sf_score = self._hist_intersection(arc_sf, sf_fd)
            match_prob = self._match_probability(cluster_id, r["unique_id"])
            fd_max = max(inst_score or 0.0, sf_score or 0.0)
            eject = (match_prob or 0.0) < 0.9 and fd_max < 0.8
            def _sig3(x):
                return f"{x:.3g}" if x is not None else "None"
            print(f"     FD scores: match_probability={_sig3(match_prob)}, "
                  f"institution_score={_sig3(inst_score)}, subfield_score={_sig3(sf_score)}, "
                  f"fd_max={_sig3(fd_max)}, eject={eject}")

    def print_case(self, cluster_id: str, grant_ids: list[str], oax_sorted: list[str],
                    total_wc: int, works_by_id: dict, n_grants: int | None = None) -> None:
        print("\n" + "#" * 70)
        grants_note = f"n_grants={n_grants}, " if n_grants is not None else ""
        hep_counts = self._fetch_hep_work_counts([self._author_idx(o) for o in oax_sorted])
        hep_total = sum(hep_counts.values())
        print(f"# {cluster_id}  ({grants_note}total HEP-context works_count across candidates: "
              f"{hep_total}  [global: {total_wc}])")
        print("#" * 70)
        self.print_arc_section(cluster_id)
        self.print_links_section(cluster_id, oax_sorted)
        self.print_oax_section(cluster_id, grant_ids, oax_sorted, works_by_id)

    def load_clusters_by_size(self, force_rebuild: bool = False) -> dict[int, list]:
        """All non-excluded ACIFs, deduped OAX candidate pools populated, bucketed by
        candidate-pool size (len(c.oax_candidates)). Reusable across any n_candidates walk.

        Candidate pools are cached in acif_oax_candidates (see that table's own docstring) --
        populate_oax_candidates()/dedup_oax_candidates() only actually run when the cache is
        empty or force_rebuild=True (e.g. after arc_oax_links.parquet changes upstream; no
        automatic freshness check yet -- pass force_rebuild explicitly when a rerun is known to
        be needed)."""
        print("Loading ACIFs...")
        clusters = load_awards_cif(ARC_ONLY_PARQUET)

        n_cached = self.con.execute("SELECT COUNT(*) FROM acif_oax_candidates").fetchone()[0]
        if force_rebuild or n_cached == 0:
            print("  acif_oax_candidates cache empty (or force_rebuild=True) -- computing "
                  "deduped OAX candidate pools...")
            clusters = populate_oax_candidates(clusters, self.con)
            clusters = dedup_oax_candidates(clusters, self.con)
            self._persist_candidates(clusters)
        else:
            print(f"  loading deduped OAX candidate pools from acif_oax_candidates cache "
                  f"({n_cached:,} rows)...")
            df = self.con.execute("SELECT cluster_id, oax_id FROM acif_oax_candidates").fetchdf()
            by_cluster = df.groupby("cluster_id")["oax_id"].apply(list).to_dict()
            for c in clusters:
                c.oax_candidates = by_cluster.get(c.cluster_id, [])

        by_size: dict[int, list] = {}
        for c in clusters:
            by_size.setdefault(len(c.oax_candidates), []).append(c)

        n_total = sum(len(v) for v in by_size.values())
        print("  candidate-pool-size summary:")
        print(f"    0 candidates: {len(by_size.get(0, [])):,} clusters")
        print(f"    1 candidate:  {len(by_size.get(1, [])):,} clusters")
        print(f"    2+ candidates: {n_total - len(by_size.get(0, [])) - len(by_size.get(1, [])):,} clusters")
        return by_size

    def walk_n_candidate_clusters(self, by_size: dict[int, list], n_candidates: int, limit: int | None = 10) -> None:
        """Walk every ACIF with exactly n_candidates deduped OAX candidates, sorted by
        n_grants DESC then total works_count ASC -- surfaces cases like
        LP160101763_NasimAmiralian (3 grants, only 2 total OAX works across both candidates)
        where a real, more prolific OAX identity is plausibly missing from the candidate pool
        entirely, rather than just small/simple cases (the old sort, total works_count ASC
        alone, couldn't distinguish "genuinely small career" from "prolific person, bad
        candidate pool"). Prints each in the ARC / ARC-OAX-links / OAX display format."""
        cand = by_size.get(n_candidates, [])
        print(f"\n[{n_candidates}-candidate clusters] {len(cand):,} total. Fetching works_count for their OAX ids...")
        all_idx = sorted({self._author_idx(idx) for c in cand for idx in c.oax_candidates})
        idx_sql = ",".join(str(i) for i in all_idx)
        wc_df = self.con.execute(f"SELECT author_idx, works_count FROM read_parquet('{AUTHORS}') WHERE author_idx IN ({idx_sql})").fetchdf()
        works_by_id = {row.author_idx: row.works_count for row in wc_df.itertuples()}

        scored = []
        for c in cand:
            wc = [works_by_id.get(self._author_idx(oid), 0) for oid in c.oax_candidates]
            scored.append((sum(wc), c))
        scored.sort(key=lambda t: (-t[1].n_grants, t[0]))

        print(f"Walking {n_candidates}-candidate clusters, n_grants DESC then total works_count ASC"
              f"{f' (showing first {limit})' if limit else ''}...")
        for total_wc, c in (scored[:limit] if limit else scored):
            oax_sorted = sorted(c.oax_candidates, key=lambda oid: -works_by_id.get(self._author_idx(oid), 0))
            self.print_case(c.cluster_id, c.grant_ids, oax_sorted, total_wc, works_by_id, n_grants=c.n_grants)

    def _fetch_candidate_info(self, clusters: list) -> dict[int, tuple]:
        """author_idx -> (works_count, orcid) for every OAX candidate across the given ACIFs,
        one batched query against the raw OpenAlex authors table. Shared by test2/test3/
        test1-ordering so each doesn't re-derive its own copy of this lookup."""
        all_idx = sorted({self._author_idx(idx) for c in clusters for idx in c.oax_candidates})
        idx_sql = ",".join(str(i) for i in all_idx)
        df = self.con.execute(f"""
            SELECT author_idx, works_count, orcid
            FROM read_parquet('{AUTHORS}') WHERE author_idx IN ({idx_sql})
        """).fetchdf()
        return {row.author_idx: (row.works_count, row.orcid) for row in df.itertuples()}

    def _fetch_hep_work_counts(self, author_idxs: list[int]) -> dict[int, int]:
        """author_idx -> HEP-context work count (COUNT(DISTINCT work_idx) from
        authorships_hep.parquet), batched across many candidates at once -- the correct,
        non-misleading figure for REPORTING (case-banner total, per-candidate label), per direct
        correction (2026-09-10) that both were showing authors/*.parquet's global lifetime
        works_count instead. Deliberately NOT used for candidate SELECTION/sort order
        (_fetch_candidate_info/_top_candidate/_sort_by_candidate_count_then_works, which still
        use the raw global count) -- switching what decides which candidate/case gets surfaced
        is a separate, bigger design question not yet in scope; this only fixes what gets
        displayed once a case is already chosen."""
        if not author_idxs:
            return {}
        idx_sql = ",".join(str(i) for i in sorted(set(author_idxs)))
        rows = self.con.execute(f"""
            SELECT author_idx, COUNT(DISTINCT work_idx) AS n
            FROM read_parquet('{AUTHORSHIPS_HEP}')
            WHERE author_idx IN ({idx_sql})
            GROUP BY author_idx
        """).fetchall()
        return dict(rows)

    def _sort_by_candidate_count_then_works(self, clusters: list, info_by_id: dict) -> list:
        """design 04_ filter.md test 1: sort ACIFs by OAX candidate count ASC, then total
        works_count DESC."""
        def _wc(oid):
            return info_by_id.get(self._author_idx(oid), (0, None))[0]
        return sorted(clusters, key=lambda c: (len(c.oax_candidates), -sum(_wc(o) for o in c.oax_candidates)))

    def _top_candidate(self, c, info_by_id: dict) -> str:
        """This ACIF's top-by-works-count OAX candidate oax_id."""
        def _wc(oid):
            return info_by_id.get(self._author_idx(oid), (0, None))[0]
        return max(c.oax_candidates, key=_wc)

    def _top_candidate_orcid_bucket(self, c, info_by_id: dict) -> str:
        """Classify a single ACIF's top-by-works candidate against its own recorded ARC orcid:
        MATCH / MISMATCH (non-null, differs) / NULL (candidate has no orcid on its own OpenAlex
        record)."""
        top_oid = self._top_candidate(c, info_by_id)
        _, oax_orcid = info_by_id.get(self._author_idx(top_oid), (0, None))
        if pd.isna(oax_orcid):
            return "null"
        return "match" if str(oax_orcid).endswith(c.orcids[0]) else "mismatch"

    def test2_orcid_top_candidate_rates(self, by_size: dict[int, list]) -> dict:
        """design 04_ filter.md test 2: for every ACIF with >=1 deduped OAX candidate AND a
        recorded ARC orcid, classify its top-by-works candidate as MATCH/MISMATCH/NULL against
        the ACIF's own orcid. Population-wide scoping measurement -- not a per-case investigation
        -- of how far ORCID alone could settle the top-by-works pick, before any FD-comparison
        logic exists. Reports overall plus a 1-candidate vs 2+-candidate breakdown, since a
        1-candidate ACIF has no real "pick" to make but the match rate there still measures how
        trustworthy the underlying Splink link is."""
        clusters = [c for size, clist in by_size.items() if size >= 1 for c in clist if c.orcids]
        print(f"\nACIFs with >=1 OAX candidate AND a recorded ARC orcid: {len(clusters):,}")
        info_by_id = self._fetch_candidate_info(clusters)

        results = {"overall": {"match": 0, "mismatch": 0, "null": 0},
                   "1_candidate": {"match": 0, "mismatch": 0, "null": 0},
                   "2plus_candidates": {"match": 0, "mismatch": 0, "null": 0}}
        for c in clusters:
            b = self._top_candidate_orcid_bucket(c, info_by_id)
            results["overall"][b] += 1
            results["1_candidate" if len(c.oax_candidates) == 1 else "2plus_candidates"][b] += 1

        print("Test 2 -- top-candidate ORCID match rate:")
        for group, counts in results.items():
            n = sum(counts.values())
            print(f"  {group} (n={n:,}):")
            for k, v in counts.items():
                pct = 100 * v / n if n else 0.0
                print(f"    {k}: {v:,} ({pct:.1f}%)")
        return results

    def test3_print_first_mismatch(self, by_size: dict[int, list]) -> None:
        """design 04_ filter.md test 3: walking ACIFs in test 1's order (candidate count ASC,
        then works DESC), print the full ACIF/links/OAX data for the first one whose top
        candidate is a MISMATCH (test 2's classification)."""
        clusters = [c for size, clist in by_size.items() if size >= 1 for c in clist if c.orcids]
        info_by_id = self._fetch_candidate_info(clusters)
        works_by_id = {k: v[0] for k, v in info_by_id.items()}
        clusters = self._sort_by_candidate_count_then_works(clusters, info_by_id)

        for c in clusters:
            if self._top_candidate_orcid_bucket(c, info_by_id) != "mismatch":
                continue
            oax_sorted = sorted(c.oax_candidates, key=lambda o: -works_by_id.get(self._author_idx(o), 0))
            total_wc = sum(works_by_id.get(self._author_idx(o), 0) for o in oax_sorted)
            self.print_case(c.cluster_id, c.grant_ids, oax_sorted, total_wc, works_by_id, n_grants=c.n_grants)
            return
        print("No mismatch found.")

    def flag_next_mismatch(self, by_size: dict[int, list]) -> None:
        """design 04_ filter.md's closing line: ">> the first filter is annotate a miss-match as
        that and flag it to be excluded." Walks in test 1's order (candidate count ASC, then
        works DESC), skipping any ACIF whose top-by-works candidate already has a recorded
        oax_provenance verdict from a prior call to this method. For the first unprocessed ACIF
        whose top candidate fails orcid_veto(), records a drop/orcid_mismatch/orcid_veto row for
        it, then prints the case -- so each successive call advances to the next unprocessed
        mismatch, since the one just handled is now recorded and gets skipped on the next run."""
        clusters = [c for size, clist in by_size.items() if size >= 1 for c in clist if c.orcids]
        info_by_id = self._fetch_candidate_info(clusters)
        works_by_id = {k: v[0] for k, v in info_by_id.items()}
        clusters = self._sort_by_candidate_count_then_works(clusters, info_by_id)

        done_df = self.con.execute("SELECT cluster_id, oax_id FROM oax_provenance").fetchdf()
        done = set(zip(done_df["cluster_id"], done_df["oax_id"]))

        for c in clusters:
            top_oid = self._top_candidate(c, info_by_id)
            if (c.cluster_id, top_oid) in done:
                continue
            wc, oax_orcid = info_by_id.get(self._author_idx(top_oid), (0, None))
            if not self.orcid_veto(c.orcids[0], oax_orcid):
                continue
            self.record_provenance(c.cluster_id, top_oid, wc, "drop", "orcid_mismatch", "orcid_veto")
            oax_sorted = sorted(c.oax_candidates, key=lambda o: -works_by_id.get(self._author_idx(o), 0))
            total_wc = sum(works_by_id.get(self._author_idx(o), 0) for o in oax_sorted)
            print(f"Flagged {c.cluster_id} / {top_oid} -> drop / orcid_mismatch / orcid_veto\n")
            self.print_case(c.cluster_id, c.grant_ids, oax_sorted, total_wc, works_by_id, n_grants=c.n_grants)
            return
        print("No unprocessed mismatch found.")

    def flag_mismatches_and_show_next(self, by_size: dict[int, list]) -> None:
        """design 04_ filter.md item 5: "I now expect that for the flow of arc-oax as sorted,
        all the orcid mismatches will be flagged and no longer revealed, and the next print I
        see is NOT an inconsistent orcid arc/oax match." Differs from flag_next_mismatch()
        (which stops and PRINTS the first mismatch it finds, one call per mismatch): this walks
        the whole sorted stream in one call, silently records provenance for EVERY unprocessed
        orcid mismatch it passes (no print per mismatch -- they're understood and handled now,
        not worth re-revealing individually), and prints only the first unprocessed case whose
        top candidate is NOT a mismatch -- the next thing actually worth a human look."""
        clusters = [c for size, clist in by_size.items() if size >= 1 for c in clist if c.orcids]
        info_by_id = self._fetch_candidate_info(clusters)
        works_by_id = {k: v[0] for k, v in info_by_id.items()}
        clusters = self._sort_by_candidate_count_then_works(clusters, info_by_id)

        done_df = self.con.execute("SELECT cluster_id, oax_id FROM oax_provenance").fetchdf()
        done = set(zip(done_df["cluster_id"], done_df["oax_id"]))

        n_flagged = 0
        for c in clusters:
            top_oid = self._top_candidate(c, info_by_id)
            if (c.cluster_id, top_oid) in done:
                continue
            wc, oax_orcid = info_by_id.get(self._author_idx(top_oid), (0, None))
            if self.orcid_veto(c.orcids[0], oax_orcid):
                self.record_provenance(c.cluster_id, top_oid, wc, "drop", "orcid_mismatch", "orcid_veto")
                done.add((c.cluster_id, top_oid))
                n_flagged += 1
                continue
            print(f"Flagged {n_flagged} orcid mismatch(es) along the way (silently dropped).\n")
            oax_sorted = sorted(c.oax_candidates, key=lambda o: -works_by_id.get(self._author_idx(o), 0))
            total_wc = sum(works_by_id.get(self._author_idx(o), 0) for o in oax_sorted)
            self.print_case(c.cluster_id, c.grant_ids, oax_sorted, total_wc, works_by_id, n_grants=c.n_grants)
            return

    def flag_ejections_and_show_next(self, by_size: dict[int, list]) -> None:
        """orcid_veto() + fd_compare()'s ejection rule together, walking the whole sorted
        stream in one call (2026-09-10, user-directed: fd_compare()'s rule is 'like mismatched
        orcid... the final arbiter for ejection', not a soft signal, so it belongs in the same
        walk-and-flag loop as orcid_veto(), not a separate pass). For each unprocessed ACIF's
        top-by-works candidate: orcid_veto() first (only meaningful when the ARC side has a
        recorded orcid), then fd_compare()'s eject rule (always applicable, independent of
        orcid); either firing records a provenance row and moves on WITHOUT printing -- both
        are understood, final verdicts now, not worth re-revealing case by case. Prints only
        the first unprocessed case whose top candidate survives BOTH checks."""
        clusters = [c for size, clist in by_size.items() if size >= 1 for c in clist]
        info_by_id = self._fetch_candidate_info(clusters)
        works_by_id = {k: v[0] for k, v in info_by_id.items()}
        clusters = self._sort_by_candidate_count_then_works(clusters, info_by_id)

        done_df = self.con.execute("SELECT cluster_id, oax_id FROM oax_provenance").fetchdf()
        done = set(zip(done_df["cluster_id"], done_df["oax_id"]))

        n_orcid = n_fd = 0
        for c in clusters:
            top_oid = self._top_candidate(c, info_by_id)
            if (c.cluster_id, top_oid) in done:
                continue
            wc, oax_orcid = info_by_id.get(self._author_idx(top_oid), (0, None))

            if c.orcids and self.orcid_veto(c.orcids[0], oax_orcid):
                self.record_provenance(c.cluster_id, top_oid, wc, "drop", "orcid_mismatch", "orcid_veto")
                done.add((c.cluster_id, top_oid))
                n_orcid += 1
                continue

            fd = self.fd_compare(c, top_oid)
            if fd["eject"]:
                print(f"  fd_reject detail for {c.cluster_id}/{top_oid}: {fd}")
                self.record_provenance(c.cluster_id, top_oid, wc, "drop", fd["reason"], "fd_compare")
                done.add((c.cluster_id, top_oid))
                n_fd += 1
                continue

            self.record_provenance(c.cluster_id, top_oid, wc, "keep", None, "fd_compare")
            print(f"Flagged {n_orcid} orcid mismatch(es) and {n_fd} fd_compare rejection(s) "
                  f"along the way (silently dropped).\n")
            print(f"Surviving top candidate's fd_compare(): {fd}  -- recorded as keep (fall-through, no reason)\n")
            oax_sorted = sorted(c.oax_candidates, key=lambda o: -works_by_id.get(self._author_idx(o), 0))
            total_wc = sum(works_by_id.get(self._author_idx(o), 0) for o in oax_sorted)
            self.print_case(c.cluster_id, c.grant_ids, oax_sorted, total_wc, works_by_id, n_grants=c.n_grants)
            return
        print(f"Flagged {n_orcid} orcid mismatch(es), {n_fd} fd_compare rejection(s). "
              f"No unprocessed survivor found.")

    def run_bulk_pass(self, clusters: list, progress_every: int = 500) -> dict:
        """Process every given ACIF's top-by-works candidate to COMPLETION (unlike
        flag_ejections_and_show_next(), which stops and prints at the first survivor) --
        orcid_veto() then fd_compare(), recording provenance for EVERY one (drop or keep),
        skipping pairs already recorded by a prior run. Returns aggregate counts, the basis for
        reviewing 'a lot of misses' after the fact rather than one case at a time
        (2026-09-10, user-directed)."""
        info_by_id = self._fetch_candidate_info(clusters)
        done_df = self.con.execute("SELECT cluster_id, oax_id FROM oax_provenance").fetchdf()
        done = set(zip(done_df["cluster_id"], done_df["oax_id"]))

        counts = {
            "total": 0, "already_done": 0,
            "orcid_mismatch": 0,
            "fd_reject_inst_only": 0, "fd_reject_for_only": 0, "fd_reject_both": 0,
            "keep": 0,
        }
        for i, c in enumerate(clusters):
            counts["total"] += 1
            top_oid = self._top_candidate(c, info_by_id)
            if (c.cluster_id, top_oid) in done:
                counts["already_done"] += 1
                continue
            wc, oax_orcid = info_by_id.get(self._author_idx(top_oid), (0, None))

            if c.orcids and self.orcid_veto(c.orcids[0], oax_orcid):
                self.record_provenance(c.cluster_id, top_oid, wc, "drop", "orcid_mismatch", "orcid_veto")
                counts["orcid_mismatch"] += 1
                continue

            fd = self.fd_compare(c, top_oid)
            if fd["eject"]:
                self.record_provenance(c.cluster_id, top_oid, wc, "drop", fd["reason"], "fd_compare")
                if fd["reason"] == "fd_inst_low":
                    counts["fd_reject_inst_only"] += 1
                elif fd["reason"] == "fd_for_low":
                    counts["fd_reject_for_only"] += 1
                else:
                    counts["fd_reject_both"] += 1
                continue

            self.record_provenance(c.cluster_id, top_oid, wc, "keep", None, "fd_compare")
            counts["keep"] += 1

            if progress_every and (i + 1) % progress_every == 0:
                print(f"  ...{i + 1}/{len(clusters)} processed", flush=True)
        return counts


def main(n_candidates: int = 2):
    fc = FilterCandidates()
    by_size = fc.load_clusters_by_size()
    fc.walk_n_candidate_clusters(by_size, n_candidates)


if __name__ == "__main__":
    main()
