"""Tests for src/oax/manual_links.py (record-keyed hand links), with small temporary files."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.oax import manual_links as ml


def _files(tmp_path):
    (tmp_path / "authors").mkdir()
    pd.DataFrame({"author_idx": [10, 20]}).to_parquet(tmp_path / "authors" / "a.parquet")
    pd.DataFrame({"unique_id": ["DP1_ann_lee", "DP2_ann_lee", "DP3_bo_wu"],
                  "cluster_id": ["DP1_ann_lee", "DP1_ann_lee", "DP3_bo_wu"]}).to_parquet(tmp_path / "rec.parquet")
    f = tmp_path / "manual_links.csv"
    f.write_text("action,unique_id,oax_author,source,notes\n"
                 "link,DP2_ann_lee,A10,s,record keyed: reaches the ACIF named by DP1\n"
                 "unlink,DP3_bo_wu,,s,checked: not in OpenAlex\n"
                 "link,DP9_gone,A20,s,record not in this build\n"
                 "reject,DP1_ann_lee,A20,s,a namesake found by a stage\n", encoding="utf-8")
    return f


def test_resolve_links_unlinks_and_inert(tmp_path):
    f = _files(tmp_path)
    links, unlinked, inert, rejected = ml.resolve(ml.load(f), {"DP1_ann_lee", "DP3_bo_wu"}, tmp_path / "rec.parquet", tmp_path / "authors")
    assert links[["cluster_id", "author_idx"]].values.tolist() == [["DP1_ann_lee", 10]]
    assert unlinked == {"DP3_bo_wu"} and inert.unique_id.tolist() == ["DP9_gone"]
    assert rejected == {("DP1_ann_lee", 20)}


def test_unknown_author_stops(tmp_path):
    f = _files(tmp_path)
    f.write_text(f.read_text() + "link,DP1_ann_lee,A99,s,typo\n", encoding="utf-8")
    with pytest.raises(SystemExit):
        ml.resolve(ml.load(f), {"DP1_ann_lee", "DP3_bo_wu"}, tmp_path / "rec.parquet", tmp_path / "authors")
