"""Tests for src/oax/orcid_link.py (hand-built frames)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oax.orcid_link import orcid_links, shared_orcids, unmatched


def _acifs():
    return pd.DataFrame([
        {"cluster_id": "DP1_jan_smith", "orcids": ["O1"], "orcid_sources": ["arc"], "full_names": ["Jan Smith"],
         "full_name_keys": ["jan_smith", "j_smith"], "n_records": 2, "first_year": 2005, "last_year": 2010},
        {"cluster_id": "DP2_ann_lee", "orcids": ["O2"], "orcid_sources": ["scopus"], "full_names": ["Ann Lee"],
         "full_name_keys": ["ann_lee", "a_lee"], "n_records": 1, "first_year": 2012, "last_year": 2012},
        {"cluster_id": "DP3_bo_wu", "orcids": [], "orcid_sources": [], "full_names": ["Bo Wu"],
         "full_name_keys": ["bo_wu", "b_wu"], "n_records": 1, "first_year": 2015, "last_year": 2015},
    ])


def _authors():
    return pd.DataFrame([
        {"author_idx": 10, "orcid": "O1", "full_name": "Jan Smith", "full_name_keys": ["jan_smith", "j_smith"],
         "works_count": 90, "works_count_au": 80, "works_count_global": 99},
        {"author_idx": 11, "orcid": "O1", "full_name": "J. Smith", "full_name_keys": ["j_smith"],
         "works_count": 1, "works_count_au": 1, "works_count_global": 1},
        {"author_idx": 20, "orcid": "O3", "full_name": "Ann Lee", "full_name_keys": ["ann_lee"],
         "works_count": 5, "works_count_au": 5, "works_count_global": 5},
    ])


def test_fragments_all_kept_with_shares():
    links = orcid_links(_acifs(), _authors())
    assert list(links.author_idx) == [10, 11] and set(links.n_authors) == {2}
    assert links.works_share.round(2).tolist() == [0.99, 0.01]


def test_name_key_flags():
    links = orcid_links(_acifs(), _authors()).set_index("author_idx")
    assert links.loc[10, "shares_full_name_key"] and links.loc[11, "shares_name_key"]
    assert not links.loc[11, "shares_full_name_key"]


def test_unmatched_and_no_orcid():
    links = orcid_links(_acifs(), _authors())
    miss = unmatched(_acifs(), links)
    assert list(miss.cluster_id) == ["DP2_ann_lee"] and list(miss.orcid) == ["O2"]


def test_shared_orcids():
    a = _acifs()
    a.loc[1, "orcids"] = ["O1"]
    assert sorted(shared_orcids(a).cluster_id) == ["DP1_jan_smith", "DP2_ann_lee"]


def test_apply_overrides_refuses_a_link_and_checks_rows():
    import pytest
    from src.oax.orcid_link import apply_overrides
    a = _authors()
    rej = pd.DataFrame([{"orcid": "O1", "author_idx": 11, "notes": "x"}])
    kept, refused = apply_overrides(a, rej)
    assert list(kept.author_idx) == [10, 20] and list(refused.author_idx) == [11]
    with pytest.raises(SystemExit):
        apply_overrides(a, pd.DataFrame([{"orcid": "O1", "author_idx": 99, "notes": "x"}]))


def test_name_relation_kinds():
    from src.oax.orcid_link import name_relation
    assert name_relation(["majid_ebrahimi warkiani"], ["majid_warkiani", "ebrahimi_warkiani"], "x") == "compound_family"
    assert name_relation(["oscar_oviedo trespalacios"], ["oscar_oviedo-trespalacios"], "x") == "separator"
    assert name_relation(["tuan_hoang"], ["hoang_tuan", "duong_tuan"], "x") == "order_swapped"
    assert name_relation(["per_setterlund"], ["per_zetterlund"], "x") == "family_one_letter"
    assert name_relation(["lan_du"], ["baosheng_yu"], "x") == "unrelated"          # 2 letters: not 'one letter'
    assert name_relation(["dora_marinova"], [], "Дора Маринова") == "non_latin"
    assert name_relation(["william_corcoran"], ["bill_corcoran"], "x") == "same_family"
    assert name_relation(["nanette_bahr"], ["p_bahr"], "x") == "initials_only"


def test_decide_statuses():
    from src.oax.orcid_link import decide
    links = pd.DataFrame([
        {"orcid": "O1", "author_idx": 1, "full_name_keys": ["paul_hagan"], "author_keys": ["bruce_hebblewhite"],
         "author_name": "Bruce Hebblewhite", "works_share": 0.05},
        {"orcid": "O1", "author_idx": 2, "full_name_keys": ["paul_hagan"], "author_keys": ["bruce_hebblewhite"],
         "author_name": "Bruce Hebblewhite", "works_share": 0.5},
        {"orcid": "O2", "author_idx": 3, "full_name_keys": ["william_corcoran"], "author_keys": ["bill_corcoran"],
         "author_name": "Bill Corcoran", "works_share": 1.0},
        {"orcid": "O3", "author_idx": 4, "full_name_keys": ["jan_smith"], "author_keys": ["jan_smith"],
         "author_name": "Jan Smith", "works_share": 1.0},
    ])
    d = decide(links, accepts={("O2", 3)})
    assert list(d.status) == ["reject_unrelated", "review_unrelated", "accept_hand", "accept_name_key"]
