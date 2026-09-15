"""
src/utils/acif_oax_linker.py

AcifOaxLinker -- the class-based replacement for 03_link_arc_oax.py (Splink) + 04_filter_
candidates.py (FD scoring/filtering), built container-first per the 2026-09-15 redesign
direction: the class shape drives out the remaining design decisions as they're filled in,
same precedent as FetchOrcid (src/utils/fetch_orcid.py) and FilterCandidates
(src/04_filter_candidates.py) -- state that's expensive-ish to build but constant across every
ACIF/candidate this class will ever process is built ONCE in __init__, not recomputed per call.

Built stepwise, on direct instruction.
  Step 1 (done): preprocessors and importers -- __init__ loads every population-wide reference
    table the later per-ACIF methods will need; print_diagnostics() reports what got loaded.
  Step 2 (this step): block() -- Stage 1 candidate-pair generation (full_name_key + ORCID,
    the sql/01_blocking_name_keys.sql logic already validated earlier this session), plus its
    own print_blocking_diagnostics(). FD scoring/coawardee corroboration/rating are later steps.

SQL lives in sql/04_acif_linker_setup.sql, not inline -- per the 2026-09-15 SQL-file design
decision (separate .sql files are independently testable via `duckdb < file.sql`, keep SQL
readable in its own native comments rather than competing with Python docstring conventions,
and this project's own actual experience today found that loop faster to iterate on). This
class's own Python stays thin: run the file, read the results, report on them.
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import PROCESSED_DATA

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROVENANCE_DB = PROCESSED_DATA / "oax_provenance.duckdb"
_SETUP_SQL = _REPO_ROOT / "sql" / "04_acif_linker_setup.sql"
_BLOCKING_SQL = _REPO_ROOT / "sql" / "01_blocking_name_keys.sql"
_FD_SCORE_SQL = _REPO_ROOT / "sql" / "02_fd_score_name_keys.sql"
_COAWARDEE_SQL = _REPO_ROOT / "sql" / "03_coawardee_coauthor.sql"


class AcifOaxLinker:
    def __init__(self, con: duckdb.DuckDBPyConnection | None = None):
        self.con = con or duckdb.connect()
        self.con.execute(f"ATTACH IF NOT EXISTS '{_PROVENANCE_DB}' AS data")

        # Run the population-wide setup SQL once -- institution crosswalk (in-session TEMP
        # tables) + ARC-side subfield/institution FD (persisted as data.arc_subfield_fd_v2/
        # arc_institution_fd_v2, the _v2 suffix deliberately not colliding with
        # FilterCandidates' own still-live tables of a similar name).
        self.con.execute(_SETUP_SQL.read_text())

        # OAX-side FD: read directly, no computation -- 00b_extract_oax.py's own Phase 4 already
        # persists these population-wide (see that script's own docstring for the 4.2s
        # measurement that justified doing it there instead of scoped-per-run, as
        # 04_filter_candidates.py's own _ensure_fd_tables() used to).
        self._oax_subfield_fd_path = PROCESSED_DATA / "oax_subfield_fd.parquet"
        self._oax_institution_fd_path = PROCESSED_DATA / "oax_institution_fd.parquet"

        # Name-key tables: already persisted (arc_name_keys/oax_name_keys, in oax_provenance.
        # duckdb) -- built and kept fresh across this session's own work, not rebuilt here.
        # AcifOaxLinker reads them as-is; refreshing them (when awards_cif_arc_only.parquet or
        # openalex_authors_prep.parquet changes) is a separate, explicit step, not implicit in
        # construction -- avoids an unexpected multi-second-to-minutes rebuild hiding inside
        # what should be a cheap __init__.

        # ARC-only population itself, for later per-ACIF methods (coawardees, orcids, etc.) --
        # loaded once as a DataFrame, not re-read per call.
        self.arc = pd.read_parquet(PROCESSED_DATA / "awards_cif_arc_only.parquet")

    def print_diagnostics(self) -> dict:
        """Report what actually got loaded -- row counts and basic coverage, not a claim of
        correctness (that's what the later, per-ACIF methods and their own validation will
        need). Returns the same numbers it prints, for a caller that wants to assert on them."""
        d: dict = {}

        d["arc_acifs"] = len(self.arc)
        d["arc_excluded"] = int(self.arc["excluded"].sum())
        d["arc_name_keys_rows"] = self.con.execute(
            "SELECT COUNT(*) FROM data.arc_name_keys"
        ).fetchone()[0]
        d["arc_name_keys_distinct_acifs"] = self.con.execute(
            "SELECT COUNT(DISTINCT acif_id) FROM data.arc_name_keys"
        ).fetchone()[0]

        d["oax_name_keys_rows"] = self.con.execute(
            "SELECT COUNT(*) FROM data.oax_name_keys"
        ).fetchone()[0]
        d["oax_name_keys_distinct_authors"] = self.con.execute(
            "SELECT COUNT(DISTINCT author_idx) FROM data.oax_name_keys"
        ).fetchone()[0]

        d["arc_subfield_fd_rows"] = self.con.execute(
            "SELECT COUNT(*) FROM data.arc_subfield_fd_v2"
        ).fetchone()[0]
        d["arc_subfield_fd_acifs"] = self.con.execute(
            "SELECT COUNT(DISTINCT cluster_id) FROM data.arc_subfield_fd_v2"
        ).fetchone()[0]
        d["arc_institution_fd_rows"] = self.con.execute(
            "SELECT COUNT(*) FROM data.arc_institution_fd_v2"
        ).fetchone()[0]
        d["arc_institution_fd_acifs"] = self.con.execute(
            "SELECT COUNT(DISTINCT cluster_id) FROM data.arc_institution_fd_v2"
        ).fetchone()[0]

        oax_sf = self.con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT author_idx) FROM read_parquet('{self._oax_subfield_fd_path}')"
        ).fetchone()
        d["oax_subfield_fd_rows"], d["oax_subfield_fd_authors"] = oax_sf
        oax_inst = self.con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT author_idx) FROM read_parquet('{self._oax_institution_fd_path}')"
        ).fetchone()
        d["oax_institution_fd_rows"], d["oax_institution_fd_authors"] = oax_inst

        d["for_subfield_dict_rows"] = self.con.execute(
            "SELECT COUNT(*) FROM data.for_subfield_dict"
        ).fetchone()[0]
        d["grant_for2020_cache_rows"] = self.con.execute(
            "SELECT COUNT(*) FROM data.grant_for2020_cache"
        ).fetchone()[0]

        print("=== AcifOaxLinker diagnostics ===")
        print(f"  ARC population:        {d['arc_acifs']:,} ACIFs ({d['arc_excluded']:,} excluded)")
        print(f"  arc_name_keys:         {d['arc_name_keys_rows']:,} rows / "
              f"{d['arc_name_keys_distinct_acifs']:,} distinct ACIFs")
        print(f"  oax_name_keys:         {d['oax_name_keys_rows']:,} rows / "
              f"{d['oax_name_keys_distinct_authors']:,} distinct authors")
        print(f"  arc_subfield_fd_v2:    {d['arc_subfield_fd_rows']:,} rows / "
              f"{d['arc_subfield_fd_acifs']:,} ACIFs")
        print(f"  arc_institution_fd_v2: {d['arc_institution_fd_rows']:,} rows / "
              f"{d['arc_institution_fd_acifs']:,} ACIFs")
        print(f"  oax_subfield_fd:       {d['oax_subfield_fd_rows']:,} rows / "
              f"{d['oax_subfield_fd_authors']:,} authors")
        print(f"  oax_institution_fd:    {d['oax_institution_fd_rows']:,} rows / "
              f"{d['oax_institution_fd_authors']:,} authors")
        print(f"  for_subfield_dict:     {d['for_subfield_dict_rows']:,} rows")
        print(f"  grant_for2020_cache:   {d['grant_for2020_cache_rows']:,} rows")
        return d

    def block(self) -> None:
        """Stage 1: candidate-pair generation. Populates data.blk_candidate_pairs -- see
        sql/01_blocking_name_keys.sql for the actual logic (bidirectional full_name_key match,
        rarity-gated on bare-initial-only keys, plus exact ORCID match), already validated
        against real cases earlier this session (Killcross, the Yan reversal case, Tas/Tokar's
        short surnames). Reads data.arc_name_keys/data.oax_name_keys, both already loaded by
        __init__ -- no re-reading of awards_cif_arc_only.parquet/openalex_authors_prep.parquet
        here."""
        self.con.execute(_BLOCKING_SQL.read_text())

    def print_blocking_diagnostics(self) -> dict:
        """Report block()'s own output -- same discipline as print_diagnostics(): numbers, not
        a correctness claim. Requires block() to have run first."""
        d: dict = {}
        rows = self.con.execute(
            "SELECT match_reason, COUNT(*) FROM data.blk_candidate_pairs GROUP BY 1 ORDER BY 1"
        ).fetchall()
        d["match_reason_counts"] = dict(rows)
        d["total_pairs"] = sum(n for _, n in rows)
        d["acifs_covered"] = self.con.execute(
            "SELECT COUNT(DISTINCT arc_id) FROM data.blk_candidate_pairs"
        ).fetchone()[0]
        d["dropped_by_rarity_gate"] = self.con.execute(
            "SELECT COUNT(*) FROM data.blk_bare_initial_dropped"
        ).fetchone()[0]

        print("=== AcifOaxLinker.block() diagnostics ===")
        for reason, n in rows:
            print(f"  {reason:<16} {n:>10,}")
        print(f"  {'TOTAL':<16} {d['total_pairs']:>10,}")
        print(f"  ACIFs covered:          {d['acifs_covered']:,} / {len(self.arc):,}")
        print(f"  dropped by rarity gate: {d['dropped_by_rarity_gate']:,}")
        return d

    def fd_score(self) -> None:
        """Stage 2: institution/subfield FD scoring over block()'s own candidate pairs.
        Populates data.blk_fd_scores -- see sql/02_fd_score_name_keys.sql (2026-09-15: this file
        replaced an earlier standalone version of itself, archived to
        ZARCHIVE/sql_archive_20260915/ -- see that file's own header for why). That earlier
        version's own Stage 1-3 rebuilt an institution crosswalk and ARC-side FD from scratch,
        both already built once in __init__ (data.arc_subfield_fd_v2/arc_institution_fd_v2),
        and recomputed OAX-side FD scoped to candidates, which 00b_extract_oax.py's own Phase 4
        already makes unnecessary -- the current version reads that population-wide parquet
        directly instead. Requires block() to have run first (reads data.blk_candidate_pairs)."""
        self.con.execute(_FD_SCORE_SQL.read_text())

    def print_fd_diagnostics(self) -> dict:
        """Report fd_score()'s own output -- same discipline as the earlier diagnostics
        methods: numbers, not a correctness claim. Requires fd_score() to have run first."""
        d: dict = {}
        rows = self.con.execute(
            "SELECT priority_level, reason, COUNT(*) FROM data.blk_fd_scores "
            "GROUP BY 1, 2 ORDER BY 1 DESC"
        ).fetchall()
        d["priority_counts"] = [{"priority_level": p, "reason": r, "n": n} for p, r, n in rows]
        d["total_pairs"] = sum(n for _, _, n in rows)

        print("=== AcifOaxLinker.fd_score() diagnostics ===")
        for p, r, n in rows:
            print(f"  {p}  {r:<24} {n:>10,}")
        print(f"     {'TOTAL':<24} {d['total_pairs']:>10,}")
        return d

    def coawardee_corroborate(self) -> None:
        """Stage 3: coauthor/coawardee corroboration over block()'s own candidate pairs.
        Populates data.blk_coauthor_corroboration -- see sql/03_coawardee_coauthor.sql. Used
        as-is, unlike fd_score()'s own file -- nothing here duplicates population-wide state
        __init__ already built; it reads data.blk_candidate_pairs/arc_name_keys (already there)
        and awards_cif_arc_only.parquet/authorships_hep.parquet fresh, same as every other
        stage. Requires block() to have run first."""
        self.con.execute(_COAWARDEE_SQL.read_text())

    def print_coawardee_diagnostics(self) -> dict:
        """Report coawardee_corroborate()'s own output -- same discipline as the earlier
        diagnostics methods: numbers, not a correctness claim. Requires
        coawardee_corroborate() to have run first."""
        d: dict = {}
        row = self.con.execute(
            "SELECT COUNT(*) FILTER (WHERE n_corroborating_coauthors > 0), COUNT(*) "
            "FROM data.blk_coauthor_corroboration"
        ).fetchone()
        d["corroborated_pairs"], d["total_pairs"] = row

        print("=== AcifOaxLinker.coawardee_corroborate() diagnostics ===")
        print(f"  corroborated pairs: {d['corroborated_pairs']:,} / {d['total_pairs']:,}")
        return d


if __name__ == "__main__":
    linker = AcifOaxLinker()
    linker.print_diagnostics()
    linker.block()
    linker.print_blocking_diagnostics()
    linker.fd_score()
    linker.print_fd_diagnostics()
    linker.coawardee_corroborate()
    linker.print_coawardee_diagnostics()
