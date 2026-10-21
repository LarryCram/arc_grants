"""
OpenAlex-evidence stage of the ACIF build (2026-10-21): applies the merge sets that
src/02b_shared_record_merges.py chose from the linker's output (ACIFs whose accepted OpenAlex links
share a record; rules in src/oax/shared_records.py). Last merge stage of build_acifs(), after the
name stage. The sets are keyed on record ids (unique_ids), so they hold across rebuilds; each set
joins the current ACIFs holding its records, unless the join would put 2+ different ORCIDs in one
ACIF, join a record to an ORCID it refuses, or put a keep-apart pair (manual_confirmed_distinct.csv)
in one ACIF -- then that set is refused and reported. Records of a set no longer in scope are
ignored. Without the file (a first build, before any linking) the stage does nothing.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config.settings import OAX_LINK_DIR
from src.acif.build import UnionFind, _item_orcids, apply_unions, refusal_hits
from src.acif.models import AwardsCIF

SETS = OAX_LINK_DIR / "shared_record_merges.parquet"


def oax_merge_stage(acifs: list[AwardsCIF], uf: UnionFind, path: Path = SETS, distinct=None):
    """Returns (acifs, report)."""
    report = {"sets": 0, "merged_sets": 0, "already_one": 0, "refused": []}
    if not Path(path).exists():
        return acifs, report
    if distinct is None:
        from src.acif.hand import load_hand_distinct
        distinct = load_hand_distinct()
    apart = [frozenset(p) for p in distinct]
    sets = pd.read_parquet(path)
    where = {it.unique_id: a.cluster_id for a in acifs for it in a.items}
    by_id = {a.cluster_id: a for a in acifs}
    to_merge = []
    for s in sets.itertuples():
        report["sets"] += 1
        ids = sorted({where[u] for u in s.unique_ids if u in where})
        if len(ids) < 2:
            report["already_one"] += 1
            continue
        group = [by_id[i] for i in ids]
        held = set().union(*(_item_orcids(g) for g in group))
        recs = {it.unique_id for g in group for it in g.items}
        reason = ("orcid_veto" if len(held) > 1 else "refused_orcid" if refusal_hits(group, held)
                  else "kept_apart" if any(p <= recs for p in apart) else None)
        if reason:
            report["refused"].append({"reason": reason, "acifs": ids, "names": list(s.names)})
            continue
        to_merge += [(ids[0], x) for x in ids[1:]]
        report["merged_sets"] += 1
    return apply_unions(acifs, uf, to_merge), report
