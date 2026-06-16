"""
Cluster quality checks shared between 01_prepare_arc.py and 01a_diagnose.py.
"""
import csv
from pathlib import Path

import pandas as pd

_CONFIG = Path(__file__).resolve().parents[2] / "config"
FOR_DIVISIONS_CSV = _CONFIG / "for_divisions.csv"
FOR_ADJACENT_CSV  = _CONFIG / "for_adjacent_divisions.csv"

RARE_NAME_TF = 5e-5


def load_for_divisions() -> tuple[dict[str, str], set[frozenset]]:
    div: dict[str, str] = {}
    with open(FOR_DIVISIONS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            div[row["for_name"]] = row["division"]
    adj: set[frozenset] = set()
    with open(FOR_ADJACENT_CSV, newline="") as f:
        for row in csv.DictReader(f):
            adj.add(frozenset([row["div_a"], row["div_b"]]))
    return div, adj


def division_mismatch(for_names: list, div_map: dict, adj: set) -> bool:
    divs = list({div_map.get(n, "?") for n in for_names if n and div_map.get(n)})
    if len(divs) <= 1:
        return False

    parent = list(range(len(divs)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(len(divs)):
        for j in range(i + 1, len(divs)):
            if divs[i] == divs[j] or frozenset([divs[i], divs[j]]) in adj:
                ri, rj = find(i), find(j)
                if ri != rj:
                    parent[ri] = rj

    return len({find(i) for i in range(len(divs))}) > 1


def is_case_a(full_names: list) -> bool:
    """Single distinct first-name token across all name forms in the cluster."""
    firsts = set()
    for name in full_names:
        parts = name.strip().split()
        if parts:
            firsts.add(parts[0].lower())
    return len(firsts) <= 1


def is_suspicious(
    row: pd.Series,
    div_map: dict,
    adj: set,
    tf_lookup: dict,
) -> bool:
    """True if cluster warrants manual review: common name, no ORCID, cross-division FOR."""
    if len(row["orcids"]) > 0:
        return False
    fnk = row.get("full_name_key")
    if fnk and tf_lookup.get(fnk, 1.0) < RARE_NAME_TF:
        return False
    if not division_mismatch(row["for_names"], div_map, adj):
        return False
    return True
