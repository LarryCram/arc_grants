"""
Tests for analysis/utils/identity_clustering.py.
"""

import duckdb
import pandas as pd
import pytest

from analysis.utils.identity_clustering import (
    UnionFind,
    build_identity_clusters,
    build_identity_clusters_from_data,
)


class TestUnionFind:
    def test_add_creates_own_root(self):
        uf = UnionFind()
        uf.add(1)
        assert uf.find(1) == 1

    def test_union_merges_roots(self):
        uf = UnionFind()
        uf.union(1, 2)
        assert uf.find(1) == uf.find(2)

    def test_transitive_closure(self):
        uf = UnionFind()
        uf.union(1, 2)
        uf.union(2, 3)
        assert uf.find(1) == uf.find(3)

    def test_unrelated_stay_separate(self):
        uf = UnionFind()
        uf.union(1, 2)
        uf.add(3)
        assert uf.find(1) != uf.find(3)

    def test_groups(self):
        uf = UnionFind()
        uf.union(1, 2)
        uf.add(3)
        groups = uf.groups()
        members = sorted(sorted(g) for g in groups.values())
        assert [1, 2] in members
        assert [3] in members


# ---------------------------------------------------------------------------
# build_identity_clusters_from_data -- the in-memory path oeuvre_build.py's
# score_identity_clusters() actually calls.
# ---------------------------------------------------------------------------

class TestBuildIdentityClustersFromData:
    def test_shared_coauthor_groups_two_works(self):
        result = build_identity_clusters_from_data(
            work_idxs=[1, 2],
            coauthor_author_idxs={1: [999], 2: [999]},
            own_institution_idxs={},
        )
        assert result.work_cluster[1] == result.work_cluster[2]
        assert any(e.reason == "coauthor:999" for e in result.edges)

    def test_shared_institution_groups_two_works(self):
        result = build_identity_clusters_from_data(
            work_idxs=[1, 2],
            coauthor_author_idxs={},
            own_institution_idxs={1: [50], 2: [50]},
        )
        assert result.work_cluster[1] == result.work_cluster[2]
        assert any(e.reason == "institution:50" for e in result.edges)

    def test_no_shared_signal_stays_separate(self):
        result = build_identity_clusters_from_data(
            work_idxs=[1, 2],
            coauthor_author_idxs={1: [111], 2: [222]},
            own_institution_idxs={1: [50], 2: [60]},
        )
        assert result.work_cluster[1] != result.work_cluster[2]
        assert result.edges == []

    def test_transitive_closure_across_three_works(self):
        # 1-2 share a coauthor, 2-3 share an institution -> all three in one cluster
        result = build_identity_clusters_from_data(
            work_idxs=[1, 2, 3],
            coauthor_author_idxs={1: [999], 2: [999], 3: []},
            own_institution_idxs={2: [50], 3: [50]},
        )
        assert result.work_cluster[1] == result.work_cluster[2] == result.work_cluster[3]

    def test_singleton_work_forms_its_own_cluster(self):
        result = build_identity_clusters_from_data(
            work_idxs=[1],
            coauthor_author_idxs={},
            own_institution_idxs={},
        )
        assert result.work_cluster == {1: 1}
        assert result.clusters == {1: [1]}

    def test_main_component_vs_small_component(self):
        # {1,2,3} one cluster (shared coauthor chain), {4} its own singleton
        result = build_identity_clusters_from_data(
            work_idxs=[1, 2, 3, 4],
            coauthor_author_idxs={1: [999], 2: [999, 888], 3: [888], 4: []},
            own_institution_idxs={},
        )
        main_root = result.work_cluster[1]
        assert len(result.clusters[main_root]) == 3
        assert result.work_cluster[4] != main_root


# ---------------------------------------------------------------------------
# build_identity_clusters -- the DuckDB-querying path. Real correctness point:
# two of a cluster's OWN candidate author_idx co-occurring on one work must not
# masquerade as an external corroborating coauthor edge.
# ---------------------------------------------------------------------------

class TestBuildIdentityClustersDbPath:
    def _con_with_authorships(self, tmp_path, monkeypatch, rows):
        df = pd.DataFrame(rows, columns=["work_idx", "author_idx", "author_name", "institution_name"])
        parquet_path = tmp_path / "authorships.parquet"
        df.to_parquet(parquet_path)
        import analysis.utils.identity_clustering as ic
        monkeypatch.setattr(ic, "AUTHORSHIPS_GLOB", str(parquet_path))
        return duckdb.connect()

    def test_own_vs_own_cooccurrence_not_treated_as_coauthor_edge(self, tmp_path, monkeypatch):
        # work 1: both of this cluster's own candidates (100, 101) appear as authors, no one else.
        # work 2: only candidate 100, plus an external coauthor 999.
        # Without the fix, 100/101's co-occurrence on work 1 could get mistaken for evidence;
        # with it, work 1 and work 2 share no genuine external coauthor -> stay separate.
        con = self._con_with_authorships(tmp_path, monkeypatch, [
            (1, 100, "A", "Inst X"),
            (1, 101, "B", "Inst X"),
            (2, 100, "A", "Inst Y"),
            (2, 999, "External", "Inst Z"),
        ])
        result = build_identity_clusters({100, 101}, [1, 2], con)
        assert result.work_cluster[1] != result.work_cluster[2]
        assert result.edges == []

    def test_genuine_external_coauthor_still_groups(self, tmp_path, monkeypatch):
        con = self._con_with_authorships(tmp_path, monkeypatch, [
            (1, 100, "A", "Inst X"),
            (1, 999, "External", "Inst Z"),
            (2, 101, "B", "Inst Y"),
            (2, 999, "External", "Inst Z"),
        ])
        result = build_identity_clusters({100, 101}, [1, 2], con)
        assert result.work_cluster[1] == result.work_cluster[2]
        assert any(e.reason == "coauthor:999" for e in result.edges)

    def test_shared_own_institution_groups(self, tmp_path, monkeypatch):
        con = self._con_with_authorships(tmp_path, monkeypatch, [
            (1, 100, "A", "Same Uni"),
            (2, 101, "B", "Same Uni"),
        ])
        result = build_identity_clusters({100, 101}, [1, 2], con)
        assert result.work_cluster[1] == result.work_cluster[2]
        assert any(e.reason == "institution:Same Uni" for e in result.edges)
