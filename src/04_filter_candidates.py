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


class FilterCandidates:
    """Disambiguates an ACIF's deduped OAX candidate pool down to the correct match(es).

    Holds, as instance state built once in __init__: a DuckDB connection, and the FOR2020-group
    -> OAX-subfield lookup (small and fixed -- 213 valid FOR2020 groups, confirmed by brute-force
    enumeration via for_resolve.py -- so built once here rather than resolved live per candidate).
    """

    def __init__(self, con: duckdb.DuckDBPyConnection | None = None):
        self.con = con or duckdb.connect()
        self.for_to_subfield: dict[str, str] = self._build_for_subfield_dict()

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
        and they differ. A null on either side is NOT a veto -- absence of evidence isn't
        evidence of a different person, only a genuine, confirmed mismatch is. Real, implemented
        (not a stub) -- the logic is fully specified and doesn't need the FD-comparison utility."""
        if arc_orcid and oax_orcid and arc_orcid != oax_orcid:
            return True
        return False

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
            print(f"  -- {r['unique_id']} (works_count={works_by_id[r['unique_id']]}) --")
            print("     full_name (singular):", repr(r["full_name"]))
            print("     full_name_keys:      ", list(r["full_name_keys"]))
            print("     orcid:", r["orcid"])
            print("     inst_ids:", list(r["inst_ids"]))
            print("     subfield_names:", list(r["subfield_names"]))
            print("     first_name:", r["first_name"], "| family_name_main:", r["family_name_main"])

    def print_case(self, cluster_id: str, oax_sorted: list[str], total_wc: int, works_by_id: dict) -> None:
        print("\n" + "#" * 70)
        print(f"# {cluster_id}  (total works_count across candidates: {total_wc})")
        print("#" * 70)
        self.print_arc_section(cluster_id)
        self.print_links_section(cluster_id, oax_sorted)
        self.print_oax_section(oax_sorted, works_by_id)

    def load_clusters_by_size(self) -> dict[int, list]:
        """All non-excluded ACIFs, deduped OAX candidate pools populated, bucketed by
        candidate-pool size (len(c.oax_candidates)). Reusable across any n_candidates walk."""
        print("Loading ACIFs and deduped OAX candidate pools...")
        clusters = load_awards_cif(ARC_ONLY_PARQUET)
        clusters = populate_oax_candidates(clusters, self.con)
        clusters = dedup_oax_candidates(clusters, self.con)

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
        """Walk every ACIF with exactly n_candidates deduped OAX candidates, smallest total
        works_count first, printing each in the ARC / ARC-OAX-links / OAX display format."""
        cand = by_size.get(n_candidates, [])
        print(f"\n[{n_candidates}-candidate clusters] {len(cand):,} total. Fetching works_count for their OAX ids...")
        all_ids = sorted({idx.replace("https://openalex.org/A", "") for c in cand for idx in c.oax_candidates})
        idx_sql = ",".join(all_ids)
        wc_df = self.con.execute(f"SELECT author_idx, works_count FROM read_parquet('{AUTHORS}') WHERE author_idx IN ({idx_sql})").fetchdf()
        works_by_id = {f"https://openalex.org/A{row.author_idx}": row.works_count for row in wc_df.itertuples()}

        scored = []
        for c in cand:
            wc = [works_by_id.get(oid, 0) for oid in c.oax_candidates]
            scored.append((sum(wc), c))
        scored.sort(key=lambda t: t[0])

        print(f"Walking {n_candidates}-candidate clusters, smallest total works_count first"
              f"{f' (showing first {limit})' if limit else ''}...")
        for total_wc, c in (scored[:limit] if limit else scored):
            oax_sorted = sorted(c.oax_candidates, key=lambda oid: -works_by_id.get(oid, 0))
            self.print_case(c.cluster_id, oax_sorted, total_wc, works_by_id)


def main(n_candidates: int = 2):
    fc = FilterCandidates()
    by_size = fc.load_clusters_by_size()
    fc.walk_n_candidate_clusters(by_size, n_candidates)


if __name__ == "__main__":
    main()
