"""
Scopus merge pass one (what-if, analysis only): use ORCIDs found through Scopus the way ARC's own
ORCIDs are used by src/acif/build.py::merge_by_orcid().

Input: the ACIFs after the ARC ORCID merge, and analysis/16_scopus_lookup.py's output for them
(PROCESSED_DATA/scopus/acif_scopus_summary.parquet + acif_scopus_profiles.parquet), which must be
from the same build (same cluster_ids).

An ACIF with no ARC ORCID is given a Scopus ORCID only when all of these hold:
  1. its Scopus search found exactly one profile            (else: not_single_profile / no_profile)
  2. that profile carries an ORCID                          (else: no_orcid_on_profile)
  3. the ORCID's names can be read: from the ORCID record cache, or failing that from
     /home/lc/s/orcid/orcid_bulk.parquet (Oct-2023 dump: name + aliases)
                                                            (else: orcid_not_found)
  4. those names agree with the ACIF's keys, through NameParser() only: a shared full_name_key
     with a full given name (an initial-only key when the ACIF has nothing else)
                                                            (else: names_disagree)
A hand `reject_scopus` row in data_persisted/arc_name_overrides.csv (grant, ARC's raw names, the
refused ORCID) overrides all of this for that record: decision rejected_by_hand. These are records
whose single Scopus profile is a namesake (David Price LP0211991, Peter Love LP0348071, Peter Taylor
DP130100077) -- the name check can't catch them because the names agree.
`name_source` records whether the names came from the cache or the bulk file. (2026-10-01 fix: a
profile with no ORCID arrives as NaN from parquet, and was being counted as "not in cache".)
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
ORCID_BULK_PARQUET = "/home/lc/s/orcid/orcid_bulk.parquet"


def _orcid_or_none(x) -> str | None:
    """A profile's ORCID, or None when it has none (None, NaN or empty from parquet)."""
    return x.strip() if isinstance(x, str) and x.strip() else None


def load_bulk_names(orcids, path: str = ORCID_BULK_PARQUET) -> dict[str, list[str]]:
    """orcid -> [name, *aliases] from the ORCID bulk file, for just these ORCIDs."""
    import duckdb
    orcids = sorted({o for o in orcids if o})
    if not orcids:
        return {}
    con = duckdb.connect()
    con.register("wanted", pd.DataFrame({"orcid": orcids}))
    df = con.execute(f"SELECT o.orcid, o.name, o.aliases FROM read_parquet('{path}') o JOIN wanted USING (orcid)").fetchdf()
    return {r.orcid: [n for n in [r.name, *(list(r.aliases) if r.aliases is not None else [])] if n]
            for r in df.itertuples()}


def bulk_name_keys(names: list[str]) -> set[str]:
    """full_name_keys of the bulk file's name strings, each parsed by NameParser()."""
    keys: set[str] = set()
    for n in names:
        keys |= set(_P.parse(n).full_name_keys)
    return keys


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


def load_scopus_rejections(inv: pd.DataFrame) -> dict[str, set[str]]:
    """unique_id -> ORCIDs refused for it, from the reject_scopus rows of arc_name_overrides.csv.
    Rows are keyed on grant + ARC's raw names; matched here against investigators_raw.parquet's
    names (inv) or, for a corrected record, its pre-correction form. Raises if a row matches no
    record."""
    import importlib
    ov = importlib.import_module("src.00a_extract_arc").load_name_overrides()
    out: dict[str, set[str]] = {}
    for (grant, first, family), (orcid, _note) in ov.reject_scopus.items():
        hit = inv[(inv.grant_code == grant) & (inv.first_name == first) & (inv.family_name == family)]
        if len(hit) != 1:
            raise ValueError(f"reject_scopus row {grant} {first!r} {family!r} matches {len(hit)} records")
        out.setdefault(hit.unique_id.iloc[0], set()).add(orcid)
    return out


def decide(acifs, summary: pd.DataFrame, profiles: pd.DataFrame, orcid_cache,
           bulk_names: dict[str, list[str]] | None = None,
           rejected: dict[str, set[str]] | None = None) -> pd.DataFrame:
    """One row per ACIF: arc_orcid, lookup status, n_profiles, scopus_orcid (the single profile's),
    name_source (cache / bulk) and decision (accepted or the reason it was not). bulk_names:
    load_bulk_names() for ORCIDs the cache lacks. rejected: load_scopus_rejections()."""
    bulk_names = bulk_names or {}
    rejected = rejected or {}
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
               "scopus_id": None, "name_source": None}
        if a.orcid_status != "NO_ORCID":
            row["decision"] = "has_arc_orcid"
        elif r.n_profiles != 1:
            row["decision"] = "not_single_profile" if r.n_profiles > 1 else "no_profile"
        else:
            p = single.loc[a.cluster_id]
            orcid = _orcid_or_none(p.orcid)
            row["scopus_id"], row["scopus_orcid"] = p.scopus_id, orcid
            rec = orcid_cache.get(orcid) if orcid else None
            if rec and "_error" not in rec:
                row["name_source"], okeys = "cache", orcid_record_keys(rec)
            elif orcid in bulk_names:
                row["name_source"], okeys = "bulk", bulk_name_keys(bulk_names[orcid])
            else:
                okeys = None
            if not orcid:
                row["decision"] = "no_orcid_on_profile"
            elif any(orcid in rejected.get(it.unique_id, ()) for it in a.items):
                row["decision"] = "rejected_by_hand"
            elif okeys is None:
                row["decision"] = "orcid_not_found"
            elif not names_agree({k for it in a.items for k in it.full_name_keys}, okeys):
                row["decision"] = "names_disagree"
            else:
                row["decision"] = "accepted"
        rows.append(row)
    return pd.DataFrame(rows)
