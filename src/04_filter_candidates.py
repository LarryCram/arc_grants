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
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.utils.for_resolve import for2020_group_name, oax_subfield_name
from src.utils.awards_cif import (
    load_awards_cif,
    populate_oax_candidates,
    dedup_oax_candidates,
    ARC_ONLY_PARQUET,
)

OAX_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
LINKS = PROCESSED_DATA / "arc_oax_links.parquet"
AUTHORS = "/home/lc/m/openalex_jul26/parquet_converted/authors/*.parquet"

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

    def __init__(self, con: duckdb.DuckDBPyConnection | None = None):
        self.con = con or duckdb.connect(str(PROVENANCE_DB))
        self._ensure_provenance_table()
        self._ensure_candidates_table()
        self.for_to_subfield: dict[str, str] = self._build_for_subfield_dict()

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

    def fd_compare(self, cluster, oax_id: str) -> float:
        """Frequency-distribution (rarity-weighted value_counts) comparison of this candidate's
        institution/subfield/coauthor pattern against the ARC person's own institution/FOR/
        coawardees. NOT YET BUILT -- this is the piece explicitly flagged as needing a
        well-designed utility, not yet scoped in detail."""
        raise NotImplementedError("fd_compare: FD-comparison utility not yet designed")

    def score(self, cluster, oax_id: str) -> float:
        """Combine ORCID-match status and fd_compare() into one score per candidate. NOT YET
        BUILT -- depends on fd_compare()."""
        raise NotImplementedError("score: depends on fd_compare()")

    def resolve(self, cluster) -> list[str]:
        """Full pipeline for one ACIF: sort candidates by works_count desc, apply orcid_veto,
        score survivors, accept the highest scorer(s) unless a first-name clash rules one out.
        NOT YET BUILT -- depends on score()."""
        raise NotImplementedError("resolve: depends on score()")

    # ------------------------------------------------------------------
    # Review/walkthrough tooling (built first, already working)
    # ------------------------------------------------------------------

    def print_arc_section(self, cluster_id: str) -> None:
        df = self.con.execute(f"""
            SELECT cluster_id, full_names, orcids, inst_arr, for2020_codes, first_names, grant_ids
            FROM read_parquet('{ARC_ONLY_PARQUET}') WHERE cluster_id = '{cluster_id}'
        """).fetchdf()
        print("=== 1. ARC ===")
        for c in df.columns:
            if c == "for2020_codes":
                continue
            v = df.iloc[0][c]
            print(f"  {c}: {list(v) if hasattr(v, '__len__') and not isinstance(v, str) else v}")
        subfields = sorted({self.for_to_subfield.get(entry["code"]) for entry in df.iloc[0]["for2020_codes"]} - {None})
        print("  mapped OAX subfields (via for_to_subfield):", subfields)

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

    def print_oax_section(self, oax_ids_sorted_by_works_desc: list[str], works_by_id: dict) -> None:
        oid_list = ",".join(f"'{o}'" for o in oax_ids_sorted_by_works_desc)
        df = self.con.execute(f"""
            SELECT unique_id, full_name, full_name_keys, orcid, inst_ids,
                   subfield_names, first_name, family_name_main
            FROM read_parquet('{OAX_PREP}') WHERE unique_id IN ({oid_list})
        """).fetchdf()
        df = df.set_index("unique_id").loc[oax_ids_sorted_by_works_desc].reset_index()
        print()
        print("=== 3. OAX (sorted by works_count desc) ===")
        for _, r in df.iterrows():
            print(f"  -- {r['unique_id']} (works_count={works_by_id[self._author_idx(r['unique_id'])]}) --")
            print("     full_name (singular):", repr(r["full_name"]))
            print("     full_name_keys:      ", list(r["full_name_keys"]))
            print("     orcid:", r["orcid"])
            print("     inst_ids:", list(r["inst_ids"]))
            print("     subfield_names:", list(r["subfield_names"]))
            print("     first_name:", r["first_name"], "| family_name_main:", r["family_name_main"])

    def print_case(self, cluster_id: str, oax_sorted: list[str], total_wc: int, works_by_id: dict, n_grants: int | None = None) -> None:
        print("\n" + "#" * 70)
        grants_note = f"n_grants={n_grants}, " if n_grants is not None else ""
        print(f"# {cluster_id}  ({grants_note}total works_count across candidates: {total_wc})")
        print("#" * 70)
        self.print_arc_section(cluster_id)
        self.print_links_section(cluster_id, oax_sorted)
        self.print_oax_section(oax_sorted, works_by_id)

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
            self.print_case(c.cluster_id, oax_sorted, total_wc, works_by_id, n_grants=c.n_grants)

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
            self.print_case(c.cluster_id, oax_sorted, total_wc, works_by_id, n_grants=c.n_grants)
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
            self.print_case(c.cluster_id, oax_sorted, total_wc, works_by_id, n_grants=c.n_grants)
            return
        print("No unprocessed mismatch found.")


def main(n_candidates: int = 2):
    fc = FilterCandidates()
    by_size = fc.load_clusters_by_size()
    fc.walk_n_candidate_clusters(by_size, n_candidates)


if __name__ == "__main__":
    main()
