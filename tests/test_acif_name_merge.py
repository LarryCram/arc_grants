"""Tests for src/acif/name_merge.py: the name stage merges clean groups only (hand-built fixtures)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.acif.build import UnionFind, attach_refused_orcids
from src.acif.models import AwardCIFItem, AwardsCIF
from src.acif.name_merge import NameMergeInputs, name_merge, year_problems

FOR_A = [{"code": "3401", "name": "Analytical chemistry", "is_primary": True, "confidence": 1.0}]
FOR_B = [{"code": "4704", "name": "Linguistics", "is_primary": True, "confidence": 1.0}]


def _acif(uid, year=2010, orcid=None, for2020=FOR_A, org="Uni A", name="Jan Smith"):
    it = AwardCIFItem(unique_id=uid, grant_code=uid.split("_")[0], first_name=name.split()[0],
                      family_name=name.split()[-1], role_code="CI", orcid=orcid, admin_org=org,
                      admin_orgs=[org], institution_oax_id=None, funding_commence_year=year,
                      for_name=None, for_code=None, full_name=name,
                      full_name_keys=["jan_smith", "j_smith"], for2020_codes=for2020)
    return AwardsCIF(cluster_id=uid, items=[it], cycle_stages=[1])


def _inputs(acifs, for_lift=None):
    uids = [it.unique_id for a in acifs for it in a.items]
    return NameMergeInputs(main_keys={u: "jan_smith" for u in uids}, crosswalk={},
                           hep_names={"Uni A", "Uni B"},
                           single_org={u.split("_")[0] for u in uids},
                           for_lift=for_lift or {}, uni_lift={})


def _run(acifs, distinct=(), for_lift=None):
    return name_merge(acifs, UnionFind(), _inputs(acifs, for_lift), distinct=list(distinct))


def test_clean_group_merges():
    acifs = [_acif("DP1_jan_smith", 2005), _acif("DP2_jan_smith", 2010)]
    out, rep = _run(acifs)
    assert len(out) == 1 and rep["status_counts"] == {"merged": 1}


def test_rare_for_group_is_not_merged():
    acifs = [_acif("DP1_jan_smith", for2020=FOR_A), _acif("DP2_jan_smith", for2020=FOR_B)]
    out, rep = _run(acifs)
    assert len(out) == 2 and rep["status_counts"] == {"flagged": 1}
    assert rep["groups"].iloc[0]["flags"] == ["rare_for"]


def test_for_lift_links_disjoint_fields():
    acifs = [_acif("DP1_jan_smith", for2020=FOR_A), _acif("DP2_jan_smith", for2020=FOR_B)]
    out, _ = _run(acifs, for_lift={("Analytical chemistry", "Linguistics"): 1.5})
    assert len(out) == 1


def test_two_decras_not_merged():
    acifs = [_acif("DE1_jan_smith", 2012), _acif("DE2_jan_smith", 2016)]
    out, rep = _run(acifs)
    assert len(out) == 2 and "two_decras" in rep["groups"].iloc[0]["flags"]


def test_orcid_veto_and_keep_apart():
    out, rep = _run([_acif("DP1_jan_smith", orcid="O1"), _acif("DP2_jan_smith", orcid="O2")])
    assert len(out) == 2 and rep["status_counts"] == {"orcid_veto": 1}
    out, rep = _run([_acif("DP1_jan_smith"), _acif("DP2_jan_smith")],
                    distinct=[("DP1_jan_smith", "DP2_jan_smith")])
    assert len(out) == 2 and rep["status_counts"] == {"kept_apart": 1}


def test_refused_orcid_not_merged_by_name():
    acifs = attach_refused_orcids([_acif("DP1_jan_smith", 2002), _acif("DP2_jan_smith", 2024, orcid="O1")],
                                  {"DP1_jan_smith": {"O1"}})
    out, rep = _run(acifs)
    assert len(out) == 2 and rep["status_counts"] == {"refused_orcid": 1}


def test_interleaved_universities():
    grants = [("DP1", 2002, frozenset({"Uni A"})), ("DP2", 2010, frozenset({"Uni A"})),
              ("DP3", 2004, frozenset({"Uni B"})), ("DP4", 2012, frozenset({"Uni B"}))]
    assert "interleaved_universities" in year_problems(grants)
    moved = [("DP1", 2002, frozenset({"Uni A"})), ("DP2", 2005, frozenset({"Uni A"})),
             ("DP3", 2008, frozenset({"Uni B"})), ("DP4", 2012, frozenset({"Uni B"}))]
    assert "interleaved_universities" not in year_problems(moved)


def test_partial_merge_leaves_odd_part_out():
    acifs = [_acif("DP1_jan_smith", 2005), _acif("DP2_jan_smith", 2008),
             _acif("DP3_jan_smith", 2010, for2020=FOR_B)]
    out, rep = _run(acifs)
    assert len(out) == 2 and rep["status_counts"] == {"partial": 1}
    row = rep["groups"].iloc[0]
    assert row["partial_sets"] == [["DP1_jan_smith", "DP2_jan_smith"]]
    assert row["parts_left_out"] == ["DP3_jan_smith"]


def test_partial_merge_ambiguous_merges_nothing():
    # DP1 links to both; DP2 and DP3 don't link to each other -> two equally large sets
    both = FOR_A + FOR_B
    acifs = [_acif("DP1_jan_smith", for2020=both), _acif("DP2_jan_smith", for2020=FOR_A),
             _acif("DP3_jan_smith", for2020=FOR_B)]
    out, rep = _run(acifs)
    assert len(out) == 3 and rep["status_counts"] == {"flagged": 1}
    assert rep["groups"].iloc[0]["partial_status"] == "ambiguous"
