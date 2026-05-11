"""
Tests for src/pipeline/models.py — serialisation roundtrips and construction.
"""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from src.pipeline.models import (
    Cluster, ClusterStatus, InvestigatorRecord, NameForm,
    ParsedName, SplitReason, load_clusters, save_clusters,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_parsed_name(**kwargs) -> ParsedName:
    defaults = dict(title="", first="David", middle=(), last="Smith",
                    suffix="", nickname="")
    defaults.update(kwargs)
    return ParsedName(**defaults)


def make_name_form() -> NameForm:
    return NameForm(
        arc=make_parsed_name(first="David", last="Smith"),
        norm_np=make_parsed_name(first="david", last="smith"),
    )


def make_record(**kwargs) -> InvestigatorRecord:
    defaults = dict(grant_code="DP123456", arc_first="David",
                    arc_family="Smith", orcid=None,
                    grant_heps=["UM"], for_2d=["01", "02"],
                    role_code="CI")
    defaults.update(kwargs)
    return InvestigatorRecord(**defaults)


def make_cluster(cluster_id=0) -> Cluster:
    return Cluster(
        cluster_id=cluster_id,
        name_forms=[make_name_form()],
        records=[make_record()],
        orcids=[],
        institutions=["UM"],
        for_2d=["01"],
        co_awardee_cluster_ids=[],
    )


# ── ParsedName ────────────────────────────────────────────────────────────────

class TestParsedName:
    def test_roundtrip(self):
        n = make_parsed_name(middle=("James",))
        assert ParsedName.from_dict(n.to_dict()) == n

    def test_middle_tuple_preserved(self):
        n = make_parsed_name(middle=("James", "Robert"))
        d = n.to_dict()
        assert isinstance(d["middle"], list)       # serialised as list
        n2 = ParsedName.from_dict(d)
        assert isinstance(n2.middle, tuple)        # restored as tuple
        assert n2.middle == ("James", "Robert")

    def test_empty_middle(self):
        n = make_parsed_name(middle=())
        assert ParsedName.from_dict(n.to_dict()).middle == ()


# ── NameForm ──────────────────────────────────────────────────────────────────

class TestNameForm:
    def test_roundtrip(self):
        nf = make_name_form()
        assert NameForm.from_dict(nf.to_dict()) == nf

    def test_json_serialisable(self):
        nf = make_name_form()
        json.dumps(nf.to_dict())   # must not raise


# ── InvestigatorRecord ────────────────────────────────────────────────────────

class TestInvestigatorRecord:
    def test_roundtrip(self):
        r = make_record(orcid="0000-0001-2345-6789")
        assert InvestigatorRecord.from_dict(r.to_dict()) == r

    def test_null_orcid(self):
        r = make_record(orcid=None)
        assert InvestigatorRecord.from_dict(r.to_dict()).orcid is None

    def test_multiple_heps(self):
        r = make_record(grant_heps=["UM", "MON"])
        assert InvestigatorRecord.from_dict(r.to_dict()).grant_heps == ["UM", "MON"]


# ── Cluster ───────────────────────────────────────────────────────────────────

class TestCluster:
    def test_roundtrip(self):
        c = make_cluster()
        assert Cluster.from_dict(c.to_dict()).cluster_id == c.cluster_id

    def test_default_status(self):
        c = make_cluster()
        assert c.status == ClusterStatus.UNRESOLVED.value

    def test_split_reason_none(self):
        c = make_cluster()
        assert c.split_reason is None

    def test_split_reason_roundtrip(self):
        c = make_cluster()
        c.split_reason = SplitReason.ORCID_BOTH_IN_OAX.value
        c2 = Cluster.from_dict(c.to_dict())
        assert c2.split_reason == SplitReason.ORCID_BOTH_IN_OAX.value

    def test_json_serialisable(self):
        c = make_cluster()
        json.dumps(c.to_dict())


# ── save/load roundtrip ───────────────────────────────────────────────────────

class TestIO:
    def test_save_load_roundtrip(self, tmp_path):
        clusters = [make_cluster(0), make_cluster(1)]
        path = tmp_path / "clusters.jsonl"
        save_clusters(clusters, path)
        loaded = load_clusters(path)
        assert len(loaded) == 2
        assert loaded[0].cluster_id == 0
        assert loaded[1].cluster_id == 1

    def test_empty_file(self, tmp_path):
        path = tmp_path / "empty.jsonl"
        save_clusters([], path)
        assert load_clusters(path) == []

    def test_preserves_records(self, tmp_path):
        c = make_cluster()
        c.records[0].orcid = "0000-0001-2345-6789"
        path = tmp_path / "clusters.jsonl"
        save_clusters([c], path)
        loaded = load_clusters(path)
        assert loaded[0].records[0].orcid == "0000-0001-2345-6789"
