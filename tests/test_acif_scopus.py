"""Tests for src/acif/scopus.py (passes one and two) and merge_by_key()'s multi-key groups, on
hand-built ACIFs and a hand-built ScopusExtract -- no Scopus or ORCID calls."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.acif.build import UnionFind, key_components, merge_by_key
from src.acif.models import AwardCIFItem, AwardsCIF
from src.acif.scopus import (ScopusExtract, names_agree, orcid_fits, record_profiles,
                             scopus_orcid_decisions, scopus_pass_one, scopus_pass_two)


def _item(uid, orcid=None, keys=("jan_smith", "j_smith"), name="Jan Smith"):
    return AwardCIFItem(unique_id=uid, grant_code=uid.split("_")[0], first_name="Jan",
                        family_name="Smith", role_code="CI", orcid=orcid, admin_org=None,
                        institution_oax_id=None, funding_commence_year=None, for_name=None,
                        for_code=None, full_name=name, full_name_keys=list(keys))


def _acif(uid, **kw):
    it = _item(uid, **kw)
    a = AwardsCIF(cluster_id=uid, items=[it], cycle_stages=[1])
    a.orcids = [it.orcid] if it.orcid else []
    a.orcid_status = "HAS_ORCID" if it.orcid else "NO_ORCID"
    return a


def _ext(rows, name_keys=None, listed=None, claims=None, rejected=None):
    """rows: cluster_id -> (n_profiles, single profile (scopus_id, orcid) or None)."""
    lookup = pd.DataFrame([{"cluster_id": c, "unique_ids": [c], "status": "x", "n_profiles": n}
                           for c, (n, _) in rows.items()]).set_index("cluster_id")
    name_keys = name_keys or {}
    single = {c: p for c, (n, p) in rows.items() if n == 1}
    return ScopusExtract(
        lookup=lookup, single_profile=single, profile_orcid={sid: o for sid, o in single.values()},
        name_keys=name_keys, name_source={o: "cache" for o in name_keys},
        listed_ids=listed or {}, claims=claims or {}, rejected=rejected or {})


# ── merge_by_key with several keys per ACIF ──────────────────────────────────

def test_key_components_chain_through_shared_keys():
    a, b, c, d = (_acif(f"G{i}_jan_smith") for i in range(1, 5))
    keys = {a.cluster_id: {"S1"}, b.cluster_id: {"S1", "S2"}, c.cluster_id: {"S2"}, d.cluster_id: {"S9"}}
    comps = key_components([d, c, b, a], lambda x: keys[x.cluster_id])
    assert comps == [(["S1", "S2"], [a, b, c])]   # d is alone, so not returned


def test_merge_by_key_check_hook_refuses_a_group():
    a, b = _acif("G1_jan_smith"), _acif("G2_jan_smith")
    out, mm = merge_by_key([a, b], UnionFind(), lambda x: "S1", check=lambda keys, g: "nope")
    assert len(out) == 2 and mm[0]["reason"] == "nope" and mm[0]["keys"] == ["S1"]


# ── names ────────────────────────────────────────────────────────────────────

def test_names_agree_needs_a_full_given_name_when_the_acif_has_one():
    assert names_agree({"karen_ford", "k_ford"}, {"karen_ford"})
    assert not names_agree({"karen_ford", "k_ford"}, {"k_ford", "kate_ford"})
    assert names_agree({"k_ford"}, {"k_ford", "kate_ford"})


# ── pass one ─────────────────────────────────────────────────────────────────

def test_pass_one_decisions():
    acifs = [_acif("G1_jan_smith"), _acif("G2_jan_smith"), _acif("G3_jan_smith"),
             _acif("G4_jan_smith"), _acif("G5_jan_smith"), _acif("G6_jan_smith", orcid="O9"),
             _acif("G7_jan_smith"), _acif("G8_jan_smith")]
    ext = _ext({"G1_jan_smith": (1, ("1", "O1")), "G2_jan_smith": (1, ("2", "O2")),
                "G3_jan_smith": (2, None), "G4_jan_smith": (1, ("4", "O4")),
                "G5_jan_smith": (1, ("5", None)), "G6_jan_smith": (1, ("6", "O9")),
                "G7_jan_smith": (0, None), "G8_jan_smith": (1, ("8", "O8"))},
               name_keys={"O1": {"jan_smith"}, "O4": {"bo_lee"}, "O8": {"jan_smith"}},
               rejected={"G8_jan_smith": {"O8"}})
    d = scopus_orcid_decisions(acifs, ext).set_index("cluster_id").decision.to_dict()
    assert d == {"G1_jan_smith": "accepted", "G2_jan_smith": "orcid_not_found",
                 "G3_jan_smith": "not_single_profile", "G4_jan_smith": "names_disagree",
                 "G5_jan_smith": "no_orcid_on_profile", "G6_jan_smith": "has_orcid",
                 "G7_jan_smith": "no_profile", "G8_jan_smith": "rejected_by_hand"}


def test_pass_one_raises_when_the_extract_is_stale():
    try:
        scopus_orcid_decisions([_acif("G1_jan_smith")], _ext({"G2_jan_smith": (0, None)}))
    except ValueError as e:
        assert "rerun 00d" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_pass_one_merges_a_fragment_onto_an_arc_orcid_acif():
    arc = _acif("G1_jan_smith", orcid="O1")
    frag = _acif("G2_jan_smith")
    ext = _ext({"G1_jan_smith": (1, ("1", "O1")), "G2_jan_smith": (1, ("1", "O1"))},
               name_keys={"O1": {"jan_smith"}})
    out, dec, mm = scopus_pass_one([arc, frag], UnionFind(), ext)
    assert len(out) == 1 and mm == []
    items = {it.unique_id: it for it in out[0].items}
    assert items["G2_jan_smith"].scopus_orcid == "O1" and items["G1_jan_smith"].scopus_orcid is None


# ── pass two ─────────────────────────────────────────────────────────────────

def test_record_profiles_skips_a_refused_profile():
    ext = _ext({"G1_jan_smith": (1, ("7", "O7")), "G2_jan_smith": (1, ("7", "O7"))},
               rejected={"G1_jan_smith": {"O7"}})
    assert record_profiles(ext) == {"G2_jan_smith": "7"}


def test_pass_two_merges_on_a_shared_profile():
    a, b = _acif("G1_jan_smith"), _acif("G2_jan_smith")
    ext = _ext({"G1_jan_smith": (1, ("7", None)), "G2_jan_smith": (1, ("7", None))})
    out, mm, n = scopus_pass_two([a, b], UnionFind(), ext)
    assert len(out) == 1 and mm == [] and n == 0


def test_pass_two_refuses_a_profile_claimed_under_another_name():
    a, b = _acif("G1_jan_smith"), _acif("G2_jan_smith")
    ext = _ext({"G1_jan_smith": (1, ("7", None)), "G2_jan_smith": (1, ("7", None))},
               name_keys={"OX": {"wanyu_lyu"}}, claims={"7": {"OX"}})
    out, mm, _ = scopus_pass_two([a, b], UnionFind(), ext)
    assert len(out) == 2 and mm[0]["reason"] == "profile_claimed_by_another_name"


def test_pass_two_claim_agreeing_gives_the_orcid():
    a, b = _acif("G1_jan_smith"), _acif("G2_jan_smith")
    ext = _ext({"G1_jan_smith": (1, ("7", None)), "G2_jan_smith": (1, ("7", None))},
               name_keys={"OC": {"jan_smith"}}, claims={"7": {"OC"}})
    out, mm, n = scopus_pass_two([a, b], UnionFind(), ext)
    assert len(out) == 1 and n == 1
    assert {it.scopus_orcid for it in out[0].items} == {"OC"}


def test_pass_two_claim_with_a_different_orcid_is_vetoed():
    a, b = _acif("G1_jan_smith", orcid="O1"), _acif("G2_jan_smith")
    ext = _ext({"G1_jan_smith": (1, ("7", None)), "G2_jan_smith": (1, ("7", None))},
               name_keys={"OC": {"jan_smith"}}, claims={"7": {"OC"}})
    out, mm, _ = scopus_pass_two([a, b], UnionFind(), ext)
    assert len(out) == 2 and mm[0]["reason"] == "orcid_veto"


def test_pass_two_orcid_record_naming_another_profile_refuses():
    a, b = _acif("G1_jan_smith", orcid="O1"), _acif("G2_jan_smith")
    ext = _ext({"G1_jan_smith": (1, ("7", None)), "G2_jan_smith": (1, ("7", None))},
               listed={"O1": {"99"}})
    out, mm, _ = scopus_pass_two([a, b], UnionFind(), ext)
    assert len(out) == 2 and mm[0]["reason"] == "orcid_record_names_another_profile"


# ── two-way link (2026-10-03) ────────────────────────────────────────────────

def _will(uid):
    return _acif(uid, keys=("william_featherstone", "w_featherstone"), name="William Featherstone")


def test_orcid_fits_two_way_link_admits_a_nickname():
    ext = _ext({"G1_x": (1, ("7", "OW"))}, name_keys={"OW": {"will_featherstone", "w_featherstone"}},
               listed={"OW": {"7"}})
    keys = {"william_featherstone", "w_featherstone"}
    assert orcid_fits(ext, keys, "OW", "7") == "two_way_link"
    ext.listed_ids["OW"] = set()                       # one-way only: not enough
    assert orcid_fits(ext, keys, "OW", "7") is None
    ext.listed_ids["OW"] = {"7"}
    ext.name_keys["OW"] = {"wanyu_lyu"}                # two-way but another family name
    assert orcid_fits(ext, keys, "OW", "7") is None


def test_pass_one_accepts_a_two_way_link_nickname():
    a = _will("G1_william_featherstone")
    ext = _ext({a.cluster_id: (1, ("7", "OW"))},
               name_keys={"OW": {"will_featherstone", "w_featherstone"}}, listed={"OW": {"7"}})
    d = scopus_orcid_decisions([a], ext).iloc[0]
    assert d.decision == "accepted" and d.accepted_by == "two_way_link"


def test_pass_two_two_way_claim_is_not_a_refusal():
    a, b = _will("G1_william_featherstone"), _will("G2_william_featherstone")
    ext = _ext({a.cluster_id: (1, ("7", "OW")), b.cluster_id: (1, ("7", "OW"))},
               name_keys={"OW": {"will_featherstone", "w_featherstone"}}, listed={"OW": {"7"}},
               claims={"7": {"OW"}})
    out, mm, n = scopus_pass_two([a, b], UnionFind(), ext)
    assert len(out) == 1 and mm == [] and n == 1
