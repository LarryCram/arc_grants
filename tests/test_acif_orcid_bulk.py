"""Tests for src/acif/orcid_bulk.py (hand-built ACIFs and extract)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.acif.build import UnionFind
from src.acif.models import AwardCIFItem, AwardsCIF
from src.acif.orcid_bulk import OrcidBulkExtract, orcid_bulk_decisions, orcid_bulk_pass


def _acif(uid, orcid=None, keys=("jan_smith", "j_smith"), heps=("UQ",)):
    it = AwardCIFItem(unique_id=uid, grant_code=uid.split("_")[0], first_name="Jan", family_name="Smith",
                      role_code="CI", orcid=orcid, admin_org=None, institution_oax_id=None,
                      funding_commence_year=None, for_name=None, for_code=None, full_name="Jan Smith",
                      full_name_keys=list(keys), hep_codes=list(heps))
    a = AwardsCIF(cluster_id=uid, items=[it], cycle_stages=[1])
    a.orcids, a.orcid_status = ([orcid], "HAS_ORCID") if orcid else ([], "NO_ORCID")
    return a


def _ext(by_key, main_keys, heps, rejected=None, arc_main=None):
    arc_main = arc_main or {u: "jan_smith" for u in ("G1_jan_smith", "G2_jan_smith")}
    return OrcidBulkExtract(by_key=by_key, main_keys=main_keys, hep_codes=heps, rejected=rejected or {},
                            arc_main=arc_main)


def test_needs_name_and_employer_and_one_orcid():
    a = _acif("G1_jan_smith")
    ext = _ext({"jan_smith": {"O1", "O2", "O3"}},
               {"O1": {"jan_smith"}, "O2": {"jan_smith"}, "O3": {"janet_smith"}},
               {"O1": {"UQ"}, "O2": {"ANU"}, "O3": {"UQ"}})
    d = orcid_bulk_decisions([a], ext).iloc[0]
    assert d.decision == "accepted" and d.bulk_orcid == "O1"
    ext.hep_codes["O2"] = {"UQ"}
    assert orcid_bulk_decisions([a], ext).iloc[0].decision == "several"


def test_other_decisions():
    a = _acif("G1_jan_smith")
    assert orcid_bulk_decisions([a], _ext({}, {}, {})).iloc[0].decision == "no_candidate"
    ext = _ext({"jan_smith": {"O1"}}, {"O1": {"jan_smith"}}, {"O1": {"ANU"}})
    assert orcid_bulk_decisions([a], ext).iloc[0].decision == "no_employer_match"
    ext = _ext({"jan_smith": {"O1"}}, {"O1": {"jan_smith"}}, {"O1": {"UQ"}}, rejected={"G1_jan_smith": {"O1"}})
    assert orcid_bulk_decisions([a], ext).iloc[0].decision == "rejected_by_hand"
    assert len(orcid_bulk_decisions([_acif("G2_jan_smith", orcid="O9")], ext)) == 0   # has an ORCID


def test_pass_merges_onto_the_orcid_holder():
    held, frag = _acif("G1_jan_smith", orcid="O1"), _acif("G2_jan_smith")
    ext = _ext({"jan_smith": {"O1"}}, {"O1": {"jan_smith"}}, {"O1": {"UQ"}})
    out, d, mm = orcid_bulk_pass([held, frag], UnionFind(), ext)
    assert len(out) == 1 and mm == [] and {it.bulk_orcid for it in out[0].items} == {None, "O1"}


def test_middle_names_never_match():
    # 'Peter Robert Marks' (main key peter_marks) must not claim Robert Marks
    a = _acif("G1_robert_marks", keys=("robert_marks", "r_marks"))
    ext = _ext({"robert_marks": {"OP"}}, {"OP": {"peter_marks"}}, {"OP": {"UQ"}},
               arc_main={"G1_robert_marks": "robert_marks"})
    assert orcid_bulk_decisions([a], ext).iloc[0].decision == "names_disagree"
    b = _acif("G1_r_marks", keys=("r_marks",))
    ext.arc_main = {}
    assert orcid_bulk_decisions([b], ext).iloc[0].decision == "no_full_given_name"
