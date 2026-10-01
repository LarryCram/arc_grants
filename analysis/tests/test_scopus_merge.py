"""Tests for analysis/utils/scopus_merge.py (no Scopus or ORCID calls)."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analysis.utils.scopus_merge import decide, names_agree, orcid_record_keys
from src.acif.models import AwardCIFItem, AwardsCIF


def _record(given, family, others=()):
    return {"person": {"name": {"given-names": {"value": given}, "family-name": {"value": family}},
                       "other-names": {"other-name": [{"content": o} for o in others]}}}


def test_orcid_record_keys_include_other_names():
    keys = orcid_record_keys(_record("S. Rachel", "Skinner", others=["Susan Rachel Skinner"]))
    assert {"rachel_skinner", "susan_skinner"} <= keys


def test_names_agree_needs_a_full_given_name_when_the_acif_has_one():
    assert names_agree({"karen_ford", "k_ford"}, {"karen_ford"})
    assert not names_agree({"karen_ford", "k_ford"}, {"k_ford", "kate_ford"})
    assert names_agree({"k_ford"}, {"k_ford", "kate_ford"})   # ACIF has only an initial


def _acif(cid, orcid=None, keys=("jan_smith",)):
    it = AwardCIFItem(unique_id=cid, grant_code=cid.split("_")[0], first_name="Jan", family_name="Smith",
                      role_code="CI", orcid=orcid, admin_org="X", institution_oax_id=None,
                      funding_commence_year=2010, for_name=None, for_code=None, full_name="Jan Smith",
                      full_name_keys=list(keys))
    a = AwardsCIF(cluster_id=cid, items=[it])
    a.orcids = [orcid] if orcid else []
    a.orcid_status = "HAS_ORCID" if orcid else "NO_ORCID"
    return a


def test_decide_rules():
    acifs = [_acif("G1_jan_smith"), _acif("G2_jan_smith"), _acif("G3_jan_smith"),
             _acif("G4_jan_smith"), _acif("G5_jan_smith", orcid="O9")]
    summary = pd.DataFrame({"cluster_id": [a.cluster_id for a in acifs],
                            "status": ["one_orcid", "one_orcid", "several_orcids", "one_orcid", "confirmed"],
                            "n_profiles": [1, 1, 2, 1, 1]})
    profiles = pd.DataFrame({"cluster_id": ["G1_jan_smith", "G2_jan_smith", "G3_jan_smith", "G3_jan_smith",
                                            "G4_jan_smith", "G5_jan_smith"],
                             "scopus_id": ["1", "2", "3", "4", "5", "6"],
                             "orcid": ["O1", "O2", "O3", "O4", "O5", "O9"]})
    cache = {"O1": _record("Jan", "Smith"), "O4": _record("Jan", "Smith"), "O5": _record("Bo", "Lee")}
    d = decide(acifs, summary, profiles, cache).set_index("cluster_id").decision.to_dict()
    assert d == {"G1_jan_smith": "accepted", "G2_jan_smith": "orcid_not_found",
                 "G3_jan_smith": "not_single_profile", "G4_jan_smith": "names_disagree",
                 "G5_jan_smith": "has_arc_orcid"}


def test_decide_blank_orcid_is_no_orcid_and_bulk_names_are_used():
    import math
    acifs = [_acif("G1_jan_smith"), _acif("G2_jan_smith"), _acif("G3_jan_smith")]
    summary = pd.DataFrame({"cluster_id": [a.cluster_id for a in acifs], "status": ["x"] * 3,
                            "n_profiles": [1, 1, 1]})
    profiles = pd.DataFrame({"cluster_id": ["G1_jan_smith", "G2_jan_smith", "G3_jan_smith"],
                             "scopus_id": ["1", "2", "3"], "orcid": [math.nan, "O2", "O3"]})
    d = decide(acifs, summary, profiles, {}, {"O2": ["Jan Smith", "J. Smith"], "O3": ["Bo Lee"]})
    d = d.set_index("cluster_id")
    assert d.decision.to_dict() == {"G1_jan_smith": "no_orcid_on_profile", "G2_jan_smith": "accepted",
                                    "G3_jan_smith": "names_disagree"}
    assert d.loc["G2_jan_smith", "name_source"] == "bulk"


def test_decide_hand_rejection_wins():
    acifs = [_acif("G1_jan_smith")]
    summary = pd.DataFrame({"cluster_id": ["G1_jan_smith"], "status": ["x"], "n_profiles": [1]})
    profiles = pd.DataFrame({"cluster_id": ["G1_jan_smith"], "scopus_id": ["1"], "orcid": ["O1"]})
    d = decide(acifs, summary, profiles, {"O1": _record("Jan", "Smith")}, {}, {"G1_jan_smith": {"O1"}})
    assert d.decision.tolist() == ["rejected_by_hand"]
