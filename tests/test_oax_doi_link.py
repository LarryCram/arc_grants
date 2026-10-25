"""Tests for src/oax/doi_link.py decide() (hand-built evidence)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.oax import doi_link as dl


def test_decide():
    ev = pd.DataFrame([("A", 1, 10), ("A", 2, 3), ("B", 5, 4), ("B", 6, 4), ("C", 7, 1), ("D", 8, 9), ("E", 9, 9),
                       ("F", 11, 9), ("G", 12, 3)], columns=["cluster_id", "author_idx", "n"])
    found = pd.Series({"A": 20, "B": 8, "C": 1, "D": 9, "E": 9, "F": 9, "G": 30})
    taken = {8: "OTHER"}
    rec_orcid = {9: "0000-OTHER", 11: "0000-FREE"}
    held = {"0000-OTHER": "Z"}
    d, lk = dl.decide(list("ABCDEFGH"), ev, found, "accept_x", taken, rec_orcid, held, {}, min_share=0.2)
    s = dict(zip(d.cluster_id, d.status))
    assert s == {"A": "accept_x", "B": "tied", "C": "below_threshold", "D": "record_linked_elsewhere",
                 "E": "record_other_orcid", "F": "accept_x", "G": "below_threshold", "H": "no_evidence"}
    assert set(zip(lk.cluster_id, lk.author_idx)) == {("A", 1), ("F", 11)}
    assert lk.set_index("cluster_id").loc["F", "record_orcid"] == "0000-FREE"
    # without the share rule G passes (3 works, next 0)
    d2, _ = dl.decide(["G"], ev, found, "accept_x", {}, {}, {}, {})
    assert d2.status.iloc[0] == "accept_x"
