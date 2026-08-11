"""
analysis/utils/identity_clustering.py

Groups a person's own oeuvre into candidate "identity clusters" -- the computational core of
the oeuvre-decontamination plan. Two works join the same cluster if they share a coauthor
author_idx, or if the candidate person's own institution matches across them; clusters are
formed by transitive closure (union-find) over these discrete, auditable edges.

Deliberately NOT a weighted similarity graph with a clustering algorithm to tune (community
detection, edge-weight thresholds) -- an earlier draft of this design was pushed back on for
exactly that reason: the real structure here is discrete shared facts (works connect only
through concretely shared coauthors/institutions), not a dense graph needing continuous
similarity and threshold tuning. A human reviewer can trace exactly why two works were grouped
("share coauthor X" / "share institution Y"), which a graph-clustering result cannot offer as
directly.

Topic similarity (analysis/04c_subfield_cooccurrence_baseline.py's rarity baseline) is
deliberately NOT a grouping key here -- per the plan, it's a secondary supporting/veto signal
for a later step, not part of the union-find itself.

Runs per-person, on a small set of works (tens to low hundreds) -- no scale concern, so a plain
Python union-find over an edge list built with a simple O(n^2) pairwise scan is the right tool,
not a SQL recursive CTE or a graph library. Takes an already-cleaned work_idx set as input (the
caller's responsibility -- e.g. via create_deduped_works(), same as Dossier construction) rather
than re-deriving it here, and pulls authorship rows only for that small, known set --
filter-small-set-first, per this project's documented 30x-regression lesson (CLAUDE.md,
"OpenAlex Snapshot Migration").
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import duckdb

from config.settings import OPENALEX_COMPACT_DIR

AUTHORSHIPS_GLOB = str(OPENALEX_COMPACT_DIR / "authorships" / "*.parquet")


class UnionFind:
    """Standard disjoint-set with path compression + union by rank."""

    def __init__(self):
        self._parent: dict[int, int] = {}
        self._rank: dict[int, int] = {}

    def add(self, x: int) -> None:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0

    def find(self, x: int) -> int:
        self.add(x)
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[x] != root:  # path compression
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self._rank[ra] < self._rank[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        if self._rank[ra] == self._rank[rb]:
            self._rank[ra] += 1

    def groups(self) -> dict[int, list[int]]:
        out: dict[int, list[int]] = defaultdict(list)
        for x in self._parent:
            out[self.find(x)].append(x)
        return dict(out)


@dataclass(frozen=True)
class ClusterEdge:
    work_a: int
    work_b: int
    reason: str  # "coauthor:<author_idx>" or "institution:<name>" -- auditable, not a bare weight


@dataclass(frozen=True)
class ClusteringResult:
    work_cluster: dict[int, int]  # work_idx -> cluster_id
    clusters: dict[int, list[int]]  # cluster_id -> [work_idx, ...]
    edges: list[ClusterEdge]  # every edge that drove the grouping, for human audit


def _fetch_own_and_coauthor_data(
    own_author_idx: int, con: duckdb.DuckDBPyConnection
) -> tuple[dict[int, set[int]], dict[int, set[str]]]:
    """Returns (work_idx -> set of coauthor author_idx, work_idx -> set of the person's own
    institution names for that work). Assumes build_identity_clusters() has already populated
    the _cluster_work_ids temp table with this person's (already-cleaned) work_idx set."""
    rows = con.execute(f"""
        SELECT work_idx, author_idx, institution_name
        FROM read_parquet('{AUTHORSHIPS_GLOB}')
        WHERE work_idx IN (SELECT work_idx FROM _cluster_work_ids)
    """).fetchall()

    coauthors: dict[int, set[int]] = defaultdict(set)
    own_institutions: dict[int, set[str]] = defaultdict(set)
    for work_idx, author_idx, institution_name in rows:
        if author_idx == own_author_idx:
            if institution_name:
                own_institutions[work_idx].add(institution_name)
        else:
            coauthors[work_idx].add(author_idx)
    return dict(coauthors), dict(own_institutions)


def build_identity_clusters(
    own_author_idx: int, work_idxs: list[int], con: duckdb.DuckDBPyConnection
) -> ClusteringResult:
    """work_idxs: the person's own cleaned oeuvre (e.g. from Dossier.works / create_deduped_works
    output) -- the caller decides what's in scope, this function only groups what it's given."""
    con.execute("CREATE OR REPLACE TEMP TABLE _cluster_work_ids AS SELECT UNNEST(?) AS work_idx", [work_idxs])
    coauthors, own_institutions = _fetch_own_and_coauthor_data(own_author_idx, con)

    uf = UnionFind()
    edges: list[ClusterEdge] = []
    for w in work_idxs:
        uf.add(w)

    for i, wa in enumerate(work_idxs):
        for wb in work_idxs[i + 1:]:
            shared_coauthors = coauthors.get(wa, set()) & coauthors.get(wb, set())
            for author_idx in shared_coauthors:
                uf.union(wa, wb)
                edges.append(ClusterEdge(wa, wb, f"coauthor:{author_idx}"))

            shared_institutions = own_institutions.get(wa, set()) & own_institutions.get(wb, set())
            for inst in shared_institutions:
                uf.union(wa, wb)
                edges.append(ClusterEdge(wa, wb, f"institution:{inst}"))

    groups = uf.groups()
    work_cluster = {w: root for root, members in groups.items() for w in members}
    return ClusteringResult(work_cluster=work_cluster, clusters=groups, edges=edges)
