"""Tests for src/acif/hand.py: hand ORCIDs, hand merges and keep-apart pairs (hand-built fixtures)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.acif.build import UnionFind, attach_refused_orcids
from src.acif.hand import hand_stage, keep_apart_violations
from src.acif.models import AwardCIFItem, AwardsCIF


def _acif(uid, orcid=None, name="Jan Smith"):
    it = AwardCIFItem(unique_id=uid, grant_code=uid.split("_")[0], first_name=name.split()[0],
                      family_name=name.split()[-1], role_code="CI", orcid=orcid, admin_org=None,
                      institution_oax_id=None, funding_commence_year=None, for_name=None,
                      for_code=None, full_name=name, full_name_keys=[])
    return AwardsCIF(cluster_id=uid, items=[it], cycle_stages=[1])


def test_hand_merge_ignores_names():
    a, b = _acif("G1_frank_pate", name="Frank Pate"), _acif("G2_donald_pate", name="Donald Pate")
    out, rep = hand_stage([a, b], UnionFind(), orcids={}, merges=[("G1_frank_pate", "G2_donald_pate")], distinct=[])
    assert len(out) == 1 and rep["merged_groups"] == 1


def test_hand_orcid_joins_the_acif_holding_it():
    a, b = _acif("G1_jan_smith", orcid="O1"), _acif("G2_jan_smith")
    out, rep = hand_stage([a, b], UnionFind(), orcids={"G2_jan_smith": "O1"}, merges=[], distinct=[])
    assert len(out) == 1 and rep["orcids_applied"] == 1
    assert {it.hand_orcid for it in out[0].items} == {None, "O1"}


def test_hand_orcid_conflict_is_reported_not_applied():
    a = _acif("G1_jan_smith", orcid="O1")
    out, rep = hand_stage([a], UnionFind(), orcids={"G1_jan_smith": "O2"}, merges=[], distinct=[])
    assert rep["orcid_conflicts"] == [("G1_jan_smith", "O2", ["O1"])]
    assert out[0].items[0].hand_orcid is None


def test_orcid_veto_and_keep_apart_refuse():
    a, b = _acif("G1_jan_smith", orcid="O1"), _acif("G2_jan_smith", orcid="O2")
    out, rep = hand_stage([a, b], UnionFind(), orcids={}, merges=[("G1_jan_smith", "G2_jan_smith")], distinct=[])
    assert len(out) == 2 and rep["refused_groups"][0]["reason"] == "orcid_veto"
    c, d = _acif("G3_jan_smith"), _acif("G4_jan_smith")
    out, rep = hand_stage([c, d], UnionFind(), orcids={}, merges=[("G3_jan_smith", "G4_jan_smith")],
                          distinct=[("G3_jan_smith", "G4_jan_smith")])
    assert len(out) == 2 and rep["refused_groups"][0]["reason"] == "keep_apart"


def test_refused_orcid_blocks_hand_orcid_and_merge():
    a, b = attach_refused_orcids([_acif("G1_jan_smith", orcid="O1"), _acif("G2_jan_smith")], {"G2_jan_smith": {"O1"}})
    out, rep = hand_stage([a, b], UnionFind(), orcids={"G2_jan_smith": "O1"}, merges=[], distinct=[])
    assert len(out) == 2 and rep["orcid_conflicts"] == [("G2_jan_smith", "O1", ["refused"])]
    out, rep = hand_stage([a, b], UnionFind(), orcids={}, merges=[("G1_jan_smith", "G2_jan_smith")], distinct=[])
    assert len(out) == 2 and rep["refused_groups"][0]["reason"] == "refused_orcid"


def test_keep_apart_violations():
    a = _acif("G1_jan_smith")
    a.items.append(_acif("G2_jan_smith").items[0])
    assert keep_apart_violations([a], [("G1_jan_smith", "G2_jan_smith")]) == [("G1_jan_smith", "G2_jan_smith")]
