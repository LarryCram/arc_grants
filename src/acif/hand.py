"""
Hand-confirmed decisions from the old pipeline, applied to the ACIF build after the Scopus passes
(2026-10-03, user). The files predate the new build but were rekeyed to its record ids
(unique_id = grant_code + "_" + NameParser key) on 2026-09-30; a row whose id is no longer an
in-scope record is inert and counted.

  data_persisted/manual_orcids.csv            cluster_id, orcid -- a verified ORCID for the record
                                              `cluster_id`, put on it as AwardCIFItem.hand_orcid;
                                              then the record's ACIF is joined to every ACIF
                                              already carrying that ORCID (ARC or Scopus).
  data_persisted/manual_merges.csv            cluster_keep, cluster_drop, reason -- the two
                                              records are one person: their ACIFs are joined.
  data_persisted/manual_confirmed_distinct.csv cluster_id_1, cluster_id_2, notes -- the two records
                                              are different people: never in one ACIF.

Hand decisions are not tested on names (they are the evidence). Joins are grouped transitively and
a group is left unmerged, and reported, when its ACIFs carry 2+ different ORCIDs (ARC, Scopus or
hand), when one of its records refuses an ORCID the group holds (AwardCIFItem.refused_orcids),
or when it would put a keep-apart pair in one ACIF. A hand ORCID that differs from an ORCID the
record already carries, or that the record refuses, is reported and not applied.
"""

from __future__ import annotations

import csv
from dataclasses import replace

from src.acif.build import DATA_PERSISTED
from src.acif.build import UnionFind, _item_orcids, apply_unions, refusal_hits
from src.acif.models import AwardsCIF

MANUAL_ORCIDS = DATA_PERSISTED / "manual_orcids.csv"
MANUAL_MERGES = DATA_PERSISTED / "manual_merges.csv"
MANUAL_DISTINCT = DATA_PERSISTED / "manual_confirmed_distinct.csv"


def _rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_hand_orcids(path=MANUAL_ORCIDS) -> dict[str, str]:
    return {r["cluster_id"].strip(): r["orcid"].strip() for r in _rows(path) if r["orcid"].strip()}


def load_hand_merges(path=MANUAL_MERGES) -> list[tuple[str, str]]:
    return [(r["cluster_keep"].strip(), r["cluster_drop"].strip()) for r in _rows(path)]


def load_hand_distinct(path=MANUAL_DISTINCT) -> list[tuple[str, str]]:
    return [(r["cluster_id_1"].strip(), r["cluster_id_2"].strip()) for r in _rows(path)]


def keep_apart_violations(acifs, distinct) -> list[tuple[str, str]]:
    """Keep-apart pairs whose two records are in one ACIF."""
    where = {it.unique_id: a.cluster_id for a in acifs for it in a.items}
    return [(a, b) for a, b in distinct if a in where and b in where and where[a] == where[b]]


def hand_stage(acifs: list[AwardsCIF], uf: UnionFind, orcids=None, merges=None, distinct=None):
    """Apply hand ORCIDs, then hand merges (and the joins hand ORCIDs imply), under the ORCID veto
    and the keep-apart pairs. Returns (acifs, report)."""
    orcids = load_hand_orcids() if orcids is None else orcids
    merges = load_hand_merges() if merges is None else merges
    distinct = load_hand_distinct() if distinct is None else distinct
    report = {"orcids_applied": 0, "orcids_already_held": 0, "orcid_conflicts": [],
              "inert_rows": 0, "merged_groups": 0, "refused_groups": [],
              "keep_apart_already_together": keep_apart_violations(acifs, distinct)}

    # 1. hand ORCIDs onto their records
    where = {it.unique_id: a.cluster_id for a in acifs for it in a.items}
    out = []
    for a in acifs:
        items = []
        for it in a.items:
            o = orcids.get(it.unique_id)
            held = {x for x in (it.orcid, it.scopus_orcid) if x}
            if o and o in it.refused_orcids:
                report["orcid_conflicts"].append((it.unique_id, o, ["refused"]))
            elif o and not held:
                it = replace(it, hand_orcid=o)
                report["orcids_applied"] += 1
            elif o and held == {o}:
                report["orcids_already_held"] += 1
            elif o:
                report["orcid_conflicts"].append((it.unique_id, o, sorted(held)))
            items.append(it)
        b = AwardsCIF(cluster_id=a.cluster_id, items=items, cycle_stages=list(a.cycle_stages),
                      orcids=list(a.orcids), orcid_status=a.orcid_status)
        out.append(b)
    acifs = out
    report["inert_rows"] += sum(1 for u in orcids if u not in where)

    # 2. joins: hand merges, and every ACIF sharing a hand ORCID with another
    edges = []
    for keep, drop in merges:
        if keep in where and drop in where:
            edges.append((where[keep], where[drop]))
        else:
            report["inert_rows"] += 1
    by_orcid: dict[str, list[str]] = {}
    hand_set = set(orcids.values())
    for a in acifs:
        for o in _item_orcids(a) & hand_set:
            by_orcid.setdefault(o, []).append(a.cluster_id)
    for ids in by_orcid.values():
        edges += [(ids[0], x) for x in ids[1:]]

    comp = UnionFind()
    for x, y in edges:
        comp.union(x, y)
    groups: dict[str, list[str]] = {}
    for x in {i for e in edges for i in e}:
        groups.setdefault(comp.find(x), []).append(x)
    by_id = {a.cluster_id: a for a in acifs}
    pairs = set(map(frozenset, distinct))
    to_merge = []
    for ids in sorted(groups.values(), key=min):
        ids = sorted(ids)
        group = [by_id[i] for i in ids]
        held = set().union(*(_item_orcids(g) for g in group))
        recs = {it.unique_id for g in group for it in g.items}
        names = sorted({it.full_name for g in group for it in g.items})
        if len(held) > 1:
            report["refused_groups"].append({"reason": "orcid_veto", "acifs": ids, "names": names,
                                             "orcids": sorted(held)})
            continue
        if refusal_hits(group, held):
            report["refused_groups"].append({"reason": "refused_orcid", "acifs": ids, "names": names,
                                             "orcids": sorted(held)})
            continue
        if any(p <= recs for p in pairs):
            report["refused_groups"].append({"reason": "keep_apart", "acifs": ids, "names": names})
            continue
        to_merge += [(ids[0], x) for x in ids[1:]]
        report["merged_groups"] += 1
    return apply_unions(acifs, uf, to_merge), report
