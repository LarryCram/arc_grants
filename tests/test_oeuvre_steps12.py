"""Tests for src/oeuvre/records.py and authorships.py (hand-built frames, a tiny parquet source)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oeuvre.authorships import connect, pull_authorships
from src.oeuvre.records import records


def _links():
    return pd.DataFrame([
        {"cluster_id": "C1", "author_idx": 10, "author_name": "A", "orcid": "O1", "status": "accept_name_key", "in_pool": True, "works_count_global": 90},
        {"cluster_id": "C1", "author_idx": 11, "author_name": "A", "orcid": "O1", "status": "accept_name_form", "in_pool": True, "works_count_global": 10},
        {"cluster_id": "C1", "author_idx": 12, "author_name": "B", "orcid": "O1", "status": "reject_unrelated", "in_pool": True, "works_count_global": 5},
        {"cluster_id": "C2", "author_idx": 20, "author_name": "C", "orcid": "O2", "status": "accept_hand", "in_pool": False, "works_count_global": 3},
    ])


def test_records_keeps_accepted_ranks_and_shares():
    r = records(_links())
    assert list(r.author_idx) == [10, 11, 20]
    assert list(r.record_rank) == [1, 2, 1] and list(r.n_records) == [2, 2, 1]
    assert r.works_share.round(2).tolist() == [0.9, 0.1, 1.0]


def test_pull_authorships_filters_and_dedups(tmp_path):
    src = tmp_path / "authorships"
    src.mkdir()
    pd.DataFrame([
        {"work_idx": 1, "author_idx": 10, "author_name": "A", "institution_idx": 7, "institution_name": "U", "ror": None, "country_code": "AU"},
        {"work_idx": 1, "author_idx": 10, "author_name": "A", "institution_idx": 7, "institution_name": "U", "ror": None, "country_code": "AU"},
        {"work_idx": 2, "author_idx": 11, "author_name": "A", "institution_idx": None, "institution_name": None, "ror": None, "country_code": None},
        {"work_idx": 3, "author_idx": 99, "author_name": "Z", "institution_idx": 8, "institution_name": "V", "ror": None, "country_code": "US"},
    ]).to_parquet(src / "part.parquet")
    out = tmp_path / "a.parquet"
    n = pull_authorships(records(_links()), out, connect(), source=src)
    a = pd.read_parquet(out)
    assert n == 2 and sorted(a.work_idx) == [1, 2] and set(a.cluster_id) == {"C1"}
