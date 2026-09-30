"""
Scopus merge pass one (what-if, analysis only): use ORCIDs found through Scopus the way ARC's own
ORCIDs are used by src/acif/build.py::merge_by_orcid().

Input: the ACIFs after the ARC ORCID merge, and analysis/16_scopus_lookup.py's output for them
(PROCESSED_DATA/scopus/acif_scopus_summary.parquet + acif_scopus_profiles.parquet), which must be
from the same build (same cluster_ids).

An ACIF with no ARC ORCID is given a Scopus ORCID only when all of these hold:
  1. its Scopus search found exactly one profile            (else: not_single_profile)
  2. that profile carries an ORCID                          (else: no_orcid_on_profile)
  3. the ORCID record is in the ORCID cache                 (else: orcid_not_in_cache)
  4. the ORCID record's own names agree with the ACIF's keys, through NameParser() only: a shared
     full_name_key with a full given name (an initial-only key when the ACIF has nothing else)
                                                            (else: names_disagree)
ACIFs that carry an ARC ORCID keep it; Scopus is not used for them (their lookup status --
confirmed / other_orcid / ... -- is reported, and an ARC ORCID always outranks a Scopus one).

Merge: group every ACIF by its ORCID -- the ARC one, or the accepted Scopus one -- and merge
through build.merge_by_key() (names must link; groups whose names don't link are reported, not
merged). One ORCID per group, so the ORCID veto holds by construction.
"""

from __future__ import annotations

import pandas as pd

from src.utils.names import HumanNameParser

_P = HumanNameParser()


def _full(keys) -> set[str]:
    return {k for k in keys if len(k.split("_", 1)[0]) > 1}


def orcid_record_keys(record: dict) -> set[str]:
    """full_name_keys of an ORCID record's own names: given+family as a pair, plus the credit
    name and every other-name as single strings -- all through NameParser()."""
    person = (record or {}).get("person") or {}
    name = person.get("name") or {}
    given = (name.get("given-names") or {}).get("value") or ""
    family = (name.get("family-name") or {}).get("value") or ""
    keys = set(_P.parse((given, family)).full_name_keys) if (given or family) else set()
    others = [(name.get("credit-name") or {}).get("value")]
    others += [o.get("content") for o in (person.get("other-names") or {}).get("other-name") or []]
    for o in others:
        if o:
            keys |= set(_P.parse(o).full_name_keys)
    return keys


def names_agree(acif_keys: set[str], orcid_keys: set[str]) -> bool:
    full = _full(acif_keys)
    if full:
        return bool(full & _full(orcid_keys))
    return bool(set(acif_keys) & set(orcid_keys))


def decide(acifs, summary: pd.DataFrame, profiles: pd.DataFrame, orcid_cache) -> pd.DataFrame:
    """One row per ACIF: arc_orcid, lookup status, n_profiles, scopus_orcid (the single profile's)
    and decision (accepted or the reason it was not)."""
    s = summary.set_index("cluster_id")
    single = profiles.groupby("cluster_id").first()
    rows = []
    for a in acifs:
        if a.cluster_id not in s.index:
            raise ValueError(f"{a.cluster_id} not in the Scopus lookup output -- rerun 16_scopus_lookup.py")
        r = s.loc[a.cluster_id]
        arc = a.orcids[0] if a.orcid_status == "HAS_ORCID" else None
        row = {"cluster_id": a.cluster_id, "orcid_status": a.orcid_status, "arc_orcid": arc,
               "lookup_status": r.status, "n_profiles": int(r.n_profiles), "scopus_orcid": None,
               "scopus_id": None}
        if a.orcid_status != "NO_ORCID":
            row["decision"] = "has_arc_orcid"
        elif r.n_profiles != 1:
            row["decision"] = "not_single_profile" if r.n_profiles > 1 else "no_profile"
        else:
            p = single.loc[a.cluster_id]
            row["scopus_id"], row["scopus_orcid"] = p.scopus_id, p.orcid
            rec = orcid_cache.get(p.orcid) if p.orcid else None
            if not p.orcid:
                row["decision"] = "no_orcid_on_profile"
            elif not rec or "_error" in rec:
                row["decision"] = "orcid_not_in_cache"
            elif not names_agree({k for it in a.items for k in it.full_name_keys}, orcid_record_keys(rec)):
                row["decision"] = "names_disagree"
            else:
                row["decision"] = "accepted"
        rows.append(row)
    return pd.DataFrame(rows)
