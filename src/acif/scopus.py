"""
Scopus merge passes for the ACIF build, run after the ARC ORCID merge (build.merge_by_orcid()).
Reads only src/00d_extract_scopus.py's outputs (SCOPUS_EXTRACT_DIR); never calls Scopus or reads
the ORCID sources itself. Rules settled 2026-10-01 (CLAUDE.md, "Scopus full run ..." and the
entries after it).

Order of trust: ARC ORCID > ORCID found through Scopus (pass one) > shared Scopus profile (pass
two) > names. ACIFs are never merged on names alone, and every merge goes through
build.merge_by_key(): names must link, and the ORCID veto (ARC and Scopus ORCIDs together) holds.

Pass one -- an ORCID found through Scopus. An ACIF with no ARC ORCID takes the ORCID of its Scopus
search's result only when all of these hold:
    1. the search found exactly one profile            (else: not_single_profile / no_profile)
    2. that profile carries an ORCID                   (else: no_orcid_on_profile)
    3. no reject_scopus row refuses that ORCID for one of its records   (else: rejected_by_hand)
    4. the ORCID's names are known (ORCID cache, else bulk file)        (else: orcid_not_found)
    5. the ORCID fits the ACIF (orcid_fits()), either
       - names: its names agree with the ACIF's -- a shared full_name_key with a full given name
         (an initial-only key when the ACIF has nothing else), or
       - two_way_link (2026-10-03): the Scopus link runs both ways -- the profile carries the ORCID
         and the ORCID's own record lists that profile -- and the family name agrees. This admits
         nicknames and other given-name forms (Will/William Featherstone, Ken/Kenneth Beagley,
         Margaret/Leigh Ackland), which rule 5's names test alone refused.
                                                                        (else: names_disagree)
`accepted_by` records which.
The accepted ORCID is put on the ACIF's records (AwardCIFItem.scopus_orcid) and ACIFs are then
merged by ORCID, ARC's or Scopus's. When that merge is refused because the ACIFs' names don't link
(2026-10-06, user), the Scopus ORCID is taken off again: it stays only on the name-linked part(s)
of the group that hold it from ARC (or by hand), or -- when none does -- on the part(s) whose names
agree with the ORCID record's own names; every other part loses it -- the names show that at
least one acceptance was wrong (cases: Robert Young and David McKnight, whose
Scopus searches matched the middle names of Ian Robert Young and Anthony David Blake McKnight).
Such ACIFs get decision "dropped_names_do_not_link", and the ORCID merge runs again without
those ORCIDs (the refused group had held its other parts apart too).

Pass two -- a shared Scopus profile. Each record takes the Scopus author id of its search's one
profile when the search found exactly one (none when it found 0 or 2+, or when a reject_scopus row
refuses that profile's ORCID for the record). ACIFs sharing a profile id -- or chained by shared
ids -- form a group (build.key_components()). Different Scopus ids are not evidence of different
people. Besides the name and ORCID-veto tests, a group is left unmerged when:
    - profile_claimed_by_another_name: an ORCID record lists one of the group's profiles as its own
      Scopus id and the ORCID doesn't fit the group (orcid_fits(): neither its names agree nor a
      two-way link with an agreeing family name);
    - orcid_veto: that claiming ORCID differs from an ORCID the group already carries;
    - orcid_record_names_another_profile: an ORCID the group carries lists Scopus ids of its own,
      none of them a profile of the group.
A group that merges on a profile claimed by exactly one ORCID record that fits it takes that ORCID
as the Scopus ORCID of its records that have none.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

import pandas as pd

from config.settings import SCOPUS_EXTRACT_DIR
from src.acif.build import UnionFind, _full_name_keys, _item_orcids, merge_by_key
from src.acif.models import AwardsCIF


@dataclass
class ScopusExtract:
    lookup: pd.DataFrame                      # 00d's summary, indexed by cluster_id
    single_profile: dict[str, tuple[str, str | None]]  # lookup cluster_id -> (scopus_id, its ORCID)
    profile_orcid: dict[str, str | None]      # scopus_id -> the ORCID Scopus shows on that profile
    name_keys: dict[str, set[str] | None]     # orcid -> its names' full_name_keys (None: unknown)
    name_source: dict[str, str | None]
    listed_ids: dict[str, set[str]]           # orcid -> Scopus ids its own record lists
    claims: dict[str, set[str]]               # scopus_id -> ORCIDs whose record lists it
    rejected: dict[str, set[str]]             # unique_id -> ORCIDs refused for it by hand


def _orcid_or_none(x) -> str | None:
    return x.strip() if isinstance(x, str) and x.strip() else None


def load_scopus_extract(d=SCOPUS_EXTRACT_DIR) -> ScopusExtract:
    summary = pd.read_parquet(d / "scopus_acif_summary.parquet").set_index("cluster_id")
    profiles = pd.read_parquet(d / "scopus_acif_profiles.parquet")
    facts = pd.read_parquet(d / "scopus_orcid_facts.parquet")
    claims = pd.read_parquet(d / "scopus_profile_claims.parquet")
    rej = pd.read_parquet(d / "scopus_rejections.parquet")
    single = {}
    ones = set(summary.index[summary.n_profiles == 1])
    for r in profiles[profiles.cluster_id.isin(ones)].itertuples():
        single[r.cluster_id] = (str(r.scopus_id), _orcid_or_none(r.orcid))
    profile_orcid = {str(r.scopus_id): _orcid_or_none(r.orcid) for r in profiles.itertuples()}
    cl: dict[str, set[str]] = {}
    for r in claims.itertuples():
        cl.setdefault(str(r.scopus_id), set()).add(r.orcid)
    rj: dict[str, set[str]] = {}
    for r in rej.itertuples():
        rj.setdefault(r.unique_id, set()).add(r.orcid)
    return ScopusExtract(
        lookup=summary, single_profile=single, profile_orcid=profile_orcid,
        name_keys={r.orcid: (set(r.name_keys) if isinstance(r.name_source, str) else None)
                   for r in facts.itertuples()},
        name_source={r.orcid: (r.name_source if isinstance(r.name_source, str) else None)
                     for r in facts.itertuples()},
        listed_ids={r.orcid: set(r.listed_scopus_ids) for r in facts.itertuples()},
        claims=cl, rejected=rj,
    )


def _full(keys) -> set[str]:
    return {k for k in keys if len(k.split("_", 1)[0]) > 1}


def names_agree(acif_keys: set[str], orcid_keys: set[str]) -> bool:
    """A shared key with a full given name; only when the ACIF has no such key, any shared key."""
    full = _full(acif_keys)
    if full:
        return bool(full & _full(orcid_keys))
    return bool(set(acif_keys) & set(orcid_keys))


def _family(keys) -> set[str]:
    return {k.split("_", 1)[1] for k in keys if "_" in k}


def two_way_link(ext: ScopusExtract, orcid: str, scopus_id: str) -> bool:
    """Scopus shows `orcid` on profile `scopus_id` and the ORCID's own record lists that profile."""
    return ext.profile_orcid.get(scopus_id) == orcid and scopus_id in ext.listed_ids.get(orcid, set())


def orcid_fits(ext: ScopusExtract, acif_keys: set[str], orcid: str, scopus_id: str) -> str | None:
    """'names' if the ORCID's names agree with the ACIF's; 'two_way_link' if the Scopus link runs
    both ways and the family name agrees; None otherwise (or when the ORCID's names are unknown)."""
    okeys = ext.name_keys.get(orcid)
    if okeys is None:
        return None
    if names_agree(acif_keys, okeys):
        return "names"
    if two_way_link(ext, orcid, scopus_id) and _family(acif_keys) & _family(okeys):
        return "two_way_link"
    return None


def _refused(ext: ScopusExtract, unique_ids, orcid: str | None) -> bool:
    return bool(orcid) and any(orcid in ext.rejected.get(u, ()) for u in unique_ids)


# ── Pass one ────────────────────────────────────────────────────────────────

def scopus_orcid_decisions(acifs: list[AwardsCIF], ext: ScopusExtract) -> pd.DataFrame:
    """One row per ACIF: lookup status, n_profiles, the single profile's id and ORCID, where
    the ORCID's names came from, and the decision (pass-one rules in the module docstring).
    The ACIFs must be the ones 00d searched (same records): raises otherwise."""
    rows = []
    for a in acifs:
        uids = sorted(it.unique_id for it in a.items)
        if a.cluster_id not in ext.lookup.index or sorted(ext.lookup.loc[a.cluster_id, "unique_ids"]) != uids:
            raise ValueError(f"{a.cluster_id} is not one of the ACIFs 00d searched -- rerun 00d_extract_scopus.py")
        r = ext.lookup.loc[a.cluster_id]
        orcids = _item_orcids(a)
        row = {"cluster_id": a.cluster_id, "lookup_status": r.status, "n_profiles": int(r.n_profiles),
               "scopus_id": None, "scopus_orcid": None, "name_source": None, "accepted_by": None}
        if orcids:
            row["decision"] = "has_orcid"
        elif r.n_profiles != 1:
            row["decision"] = "not_single_profile" if r.n_profiles > 1 else "no_profile"
        else:
            sid, orcid = ext.single_profile[a.cluster_id]
            row["scopus_id"], row["scopus_orcid"] = sid, orcid
            okeys = ext.name_keys.get(orcid) if orcid else None
            row["name_source"] = ext.name_source.get(orcid) if orcid else None
            if not orcid:
                row["decision"] = "no_orcid_on_profile"
            elif _refused(ext, uids, orcid):
                row["decision"] = "rejected_by_hand"
            elif okeys is None:
                row["decision"] = "orcid_not_found"
            elif not (fit := orcid_fits(ext, _full_name_keys(a), orcid, sid)):
                row["decision"] = "names_disagree"
            else:
                row["decision"], row["accepted_by"] = "accepted", fit
        rows.append(row)
    return pd.DataFrame(rows)


def _with_scopus_orcid(acif: AwardsCIF, orcid: str) -> AwardsCIF:
    """The ACIF with `orcid` as the Scopus ORCID of each record that has no ORCID of either kind."""
    items = [it if (it.orcid or it.scopus_orcid) else replace(it, scopus_orcid=orcid) for it in acif.items]
    return AwardsCIF(cluster_id=acif.cluster_id, items=items, cycle_stages=list(acif.cycle_stages),
                     orcids=list(acif.orcids), orcid_status=acif.orcid_status)


def _single_orcid(acif: AwardsCIF) -> str | None:
    os = _item_orcids(acif)
    return next(iter(os)) if len(os) == 1 else None


def scopus_pass_one(acifs: list[AwardsCIF], uf: UnionFind, ext: ScopusExtract):
    """Pass one: put each accepted Scopus ORCID on its ACIF's records, then merge by ORCID (ARC or
    Scopus). Returns (acifs, decisions, mismatches)."""
    decisions = scopus_orcid_decisions(acifs, ext)
    accepted = dict(zip(decisions.loc[decisions.decision == "accepted", "cluster_id"],
                        decisions.loc[decisions.decision == "accepted", "scopus_orcid"]))
    acifs = [_with_scopus_orcid(a, accepted[a.cluster_id]) if a.cluster_id in accepted else a for a in acifs]
    merged, mismatches = merge_by_key(acifs, uf, _single_orcid)
    merged, dropped = drop_unlinked_scopus_orcids(merged, mismatches, ext.name_keys)
    if dropped:
        decisions.loc[decisions.cluster_id.isin(dropped), "decision"] = "dropped_names_do_not_link"
        # the refused groups held their remaining parts apart too: merge again without the dropped ORCIDs
        merged, mismatches = merge_by_key(merged, uf, _single_orcid)
    return merged, decisions, mismatches


def drop_unlinked_scopus_orcids(acifs: list[AwardsCIF], mismatches: list[dict], name_keys=None):
    """Take the Scopus ORCID off ACIFs in ORCID groups refused because names don't link (module
    docstring). `name_keys`: orcid -> the ORCID record's own name keys (ScopusExtract.name_keys).
    Returns (acifs, cluster_ids that lost it)."""
    name_keys = name_keys or {}
    by_id = {a.cluster_id: a for a in acifs}
    drop: dict[str, str] = {}
    for m in mismatches:
        if m["reason"] != "names_do_not_link":
            continue
        orcid = m["orcid"]
        subs = [[by_id[c] for c in sub if c in by_id] for sub in m["groups"]]  # each linked by names

        def firm(parts):
            return any(o == orcid for a in parts for it in a.items for o in (it.orcid, it.hand_orcid))

        def agrees(parts):
            okeys = name_keys.get(orcid)
            return okeys is not None and names_agree(set().union(*(_full_name_keys(a) for a in parts)), okeys)

        keep = [p for p in subs if firm(p)] or [p for p in subs if agrees(p)]
        for parts in subs:
            if any(parts is k for k in keep):
                continue
            for a in parts:
                if any(it.scopus_orcid == orcid for it in a.items):
                    drop[a.cluster_id] = orcid
    out = []
    for a in acifs:
        if a.cluster_id in drop:
            items = [replace(it, scopus_orcid=None) if it.scopus_orcid == drop[a.cluster_id] else it
                     for it in a.items]
            a = AwardsCIF(cluster_id=a.cluster_id, items=items, cycle_stages=list(a.cycle_stages),
                          orcids=list(a.orcids), orcid_status=a.orcid_status)
        out.append(a)
    return out, sorted(drop)


# ── Pass two ────────────────────────────────────────────────────────────────

def record_profiles(ext: ScopusExtract) -> dict[str, str]:
    """unique_id -> the Scopus id of its search's one profile (searches that found exactly one,
    less those whose profile ORCID a reject_scopus row refuses for one of the searched records)."""
    out = {}
    for cid, (sid, orcid) in ext.single_profile.items():
        uids = ext.lookup.loc[cid, "unique_ids"]
        if _refused(ext, uids, orcid):
            continue
        for u in uids:
            out[u] = sid
    return out


def _claimers(ext: ScopusExtract, keys) -> set[str]:
    return {o for k in keys for o in ext.claims.get(k, ())}


def pass_two_check(ext: ScopusExtract):
    """The extra tests for a shared-profile group (module docstring); for merge_by_key(check=)."""
    def check(keys, group) -> str | None:
        names = set().union(*(_full_name_keys(a) for a in group))
        claimers = _claimers(ext, keys)
        for o in sorted(claimers):
            if ext.name_keys.get(o) is None:
                continue
            claimed = [k for k in keys if o in ext.claims.get(k, ())]
            if not any(orcid_fits(ext, names, o, k) for k in claimed):
                return "profile_claimed_by_another_name"
        held = set().union(*(_item_orcids(a) for a in group))
        if len(held | claimers) > 1:
            return "orcid_veto"
        for o in sorted(held):
            listed = ext.listed_ids.get(o, set())
            if listed and not (listed & set(keys)):
                return "orcid_record_names_another_profile"
        return None
    return check


def scopus_pass_two(acifs: list[AwardsCIF], uf: UnionFind, ext: ScopusExtract):
    """Pass two: merge ACIFs that share a single Scopus profile. Returns (acifs, mismatches,
    n_orcid_from_claims) -- the last counts merged ACIFs given a Scopus ORCID by a profile claim."""
    prof = record_profiles(ext)

    def key_of(a):
        return {prof[it.unique_id] for it in a.items if it.unique_id in prof}

    before = {it.unique_id: a.cluster_id for a in acifs for it in a.items}
    merged, mismatches = merge_by_key(acifs, uf, key_of, check=pass_two_check(ext))
    out, n_claimed = [], 0
    for a in merged:
        if len({before[it.unique_id] for it in a.items}) > 1:
            keys = key_of(a)
            claimers = _claimers(ext, keys)
            if len(claimers) == 1:
                o = next(iter(claimers))
                claimed = [k for k in keys if o in ext.claims.get(k, ())]
                fits = any(orcid_fits(ext, _full_name_keys(a), o, k) for k in claimed)
                if fits and _item_orcids(a) <= {o}:
                    if any(not (it.orcid or it.scopus_orcid) for it in a.items):
                        a = _with_scopus_orcid(a, o)
                        n_claimed += 1
        out.append(a)
    return out, mismatches, n_claimed
