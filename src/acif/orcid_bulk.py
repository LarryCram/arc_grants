"""
ORCID-bulk pass of the ACIF build (2026-10-06, user: "roll out the additional arc orcid parts"):
after the hand stage, an ACIF that carries no ORCID of any kind takes one from the ORCID bulk file
when exactly one bulk ORCID passes both tests:
    - names: the ACIF's main name keys (first given name + family, arc_names full_name_key; the
      name stage's keys) share one with the ORCID record's main keys (00e main_keys: first given
      name + family of each of its name forms). Middle names never count on either side: matching
      them let 'Peter Robert Marks' claim Robert Marks and 'Karen Anne Hamnet Green' claim Anne
      Green (2026-10-06). An ACIF with no full given name takes no ORCID here. And
    - employer: the record lists employment at an ARC HEP that is on one of the ACIF's grants
      (the ACIF's records' hep_codes: administering, announcement-administering and eligible
      organisations).
No reject_scopus row (arc_name_overrides.csv) and no enrichment_blocklist.csv row may refuse that
ORCID for one of the ACIF's records (both say the record must not take the ORCID). Reads only src/00e_extract_orcid_bulk.py's
outputs and the Scopus extract's rejections.

Decision per ORCID-less ACIF: accepted / several (2+ ORCIDs pass) / names_disagree (a candidate
with an ARC-university employer, but no main keys agree) / no_employer_match (candidates by name
key, none employed at the ACIF's universities) / no_candidate / no_full_given_name /
rejected_by_hand.

The accepted ORCID goes on the records (AwardCIFItem.bulk_orcid) and ACIFs are merged by ORCID
through build.merge_by_key() (names must link; the ORCID veto holds). When that merge is refused
because names don't link, the bulk ORCID is taken off again as in Scopus pass one
(scopus.drop_unlinked_scopus_orcids(field="bulk_orcid")) and the merge runs again.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

import pandas as pd

import csv

from config.settings import ORCID_BULK_EXTRACT_DIR, SCOPUS_EXTRACT_DIR
from src.acif.build import DATA_PERSISTED, UnionFind, _item_orcids, merge_by_key
from src.acif.models import AwardsCIF
from src.acif.scopus import _single_orcid, drop_unlinked_scopus_orcids


@dataclass
class OrcidBulkExtract:
    by_key: dict[str, set[str]]          # full_name_key -> ORCIDs carrying it (bulk keys)
    main_keys: dict[str, set[str]]       # orcid -> its name forms' main keys (current parser)
    hep_codes: dict[str, set[str]]       # orcid -> ARC HEP codes it was employed at
    rejected: dict[str, set[str]]        # unique_id -> ORCIDs it must not take (hand rows)
    arc_main: dict[str, str]             # unique_id -> the record's main name key (arc_names)


def load_orcid_bulk_extract(d=ORCID_BULK_EXTRACT_DIR, scopus_dir=SCOPUS_EXTRACT_DIR,
                            blocklist=DATA_PERSISTED / "enrichment_blocklist.csv") -> OrcidBulkExtract:
    from src.acif.name_merge import main_keys as arc_main_keys
    ko = pd.read_parquet(d / "key_orcids.parquet")
    facts = pd.read_parquet(d / "orcid_facts.parquet")
    rej = pd.read_parquet(scopus_dir / "scopus_rejections.parquet")
    by_key: dict[str, set[str]] = {}
    for k, o in ko[["full_name_key", "orcid"]].itertuples(index=False):
        by_key.setdefault(k, set()).add(o)
    rejected: dict[str, set[str]] = {}
    for u, o in rej[["unique_id", "orcid"]].itertuples(index=False):
        rejected.setdefault(u, set()).add(o)
    with open(blocklist, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rejected.setdefault(r["cluster_id"].strip(), set()).add(r["orcid"].strip())
    return OrcidBulkExtract(
        by_key=by_key,
        main_keys={r.orcid: set(r.main_keys) for r in facts.itertuples()},
        hep_codes={r.orcid: set(r.hep_codes) for r in facts.itertuples()},
        rejected=rejected, arc_main=arc_main_keys())


def _acif_heps(acif: AwardsCIF) -> set[str]:
    return {h for it in acif.items for h in (it.hep_codes or [])}


def orcid_bulk_decisions(acifs: list[AwardsCIF], ext: OrcidBulkExtract) -> pd.DataFrame:
    """One row per ACIF without any ORCID: candidates found, the ORCIDs passing each test, and the
    decision (module docstring)."""
    rows = []
    for a in acifs:
        if _item_orcids(a):
            continue
        keys = {ext.arc_main[it.unique_id] for it in a.items if it.unique_id in ext.arc_main}
        cands = set().union(*(ext.by_key.get(k, set()) for k in keys)) if keys else set()
        heps = _acif_heps(a)
        employed = {o for o in cands if ext.hep_codes.get(o, set()) & heps}
        named = {o for o in employed if ext.main_keys.get(o, set()) & keys}
        refused = {o for o in named for it in a.items if o in ext.rejected.get(it.unique_id, ())}
        ok = named - refused
        if not keys:
            decision = "no_full_given_name"
        elif not cands:
            decision = "no_candidate"
        elif not employed:
            decision = "no_employer_match"
        elif not named:
            decision = "names_disagree"
        elif len(ok) > 1:
            decision = "several"
        elif not ok:
            decision = "rejected_by_hand"
        else:
            decision = "accepted"
        rows.append({"cluster_id": a.cluster_id, "n_candidates": len(cands), "n_employed": len(employed),
                     "passing": sorted(ok), "decision": decision,
                     "bulk_orcid": next(iter(ok)) if decision == "accepted" else None})
    cols = ["cluster_id", "n_candidates", "n_employed", "passing", "decision", "bulk_orcid"]
    return pd.DataFrame(rows, columns=cols)


def _with_bulk_orcid(acif: AwardsCIF, orcid: str) -> AwardsCIF:
    items = [it if (it.orcid or it.scopus_orcid or it.hand_orcid) else replace(it, bulk_orcid=orcid)
             for it in acif.items]
    return AwardsCIF(cluster_id=acif.cluster_id, items=items, cycle_stages=list(acif.cycle_stages),
                     orcids=list(acif.orcids), orcid_status=acif.orcid_status)


def orcid_bulk_pass(acifs: list[AwardsCIF], uf: UnionFind, ext: OrcidBulkExtract):
    """Returns (acifs, decisions, mismatches)."""
    decisions = orcid_bulk_decisions(acifs, ext)
    acc = decisions[decisions.decision == "accepted"]
    accepted = dict(zip(acc.cluster_id, acc.bulk_orcid))
    acifs = [_with_bulk_orcid(a, accepted[a.cluster_id]) if a.cluster_id in accepted else a for a in acifs]
    merged, mismatches = merge_by_key(acifs, uf, _single_orcid)
    merged, dropped = drop_unlinked_scopus_orcids(merged, mismatches, ext.main_keys, field="bulk_orcid")
    if dropped:
        decisions.loc[decisions.cluster_id.isin(dropped), "decision"] = "dropped_names_do_not_link"
        merged, mismatches = merge_by_key(merged, uf, _single_orcid)
    return merged, decisions, mismatches
