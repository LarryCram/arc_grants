"""
The ARC-stage list of people: ACIF-level and record-level rows from the finished build
(build_acifs()), written by src/01_build_arc_acifs.py (2026-10-06). The OpenAlex additions will be
a later, separate persisted stage.

Every ACIF-level field is recomputed here from the ACIF's current records -- nothing is carried
over from an earlier stage.
"""

from __future__ import annotations

import importlib
from collections import defaultdict

import pandas as pd

from src.acif.build import INDIGENOUS_DIVISION_PREFIX
from src.acif.models import AwardsCIF


def _scheme(grant_code: str) -> str:
    return "".join(ch for ch in grant_code[:2] if ch.isalpha())


def for2020_union(acif: AwardsCIF) -> list[dict]:
    """Every FOR2020 group on the ACIF's grants, one entry per code: is_primary if primary on any
    grant, highest confidence; ordered primary-first then by name. A kept (not excluded) ACIF's
    division-45 codes are left out (the old pipeline's second Indigenous-research step)."""
    by_code: dict[str, dict] = {}
    for it in acif.items:
        for e in it.for2020_codes or []:
            d = by_code.setdefault(e["code"], {"code": e["code"], "name": e["name"],
                                               "is_primary": False, "confidence": 0.0})
            d["is_primary"] = d["is_primary"] or bool(e["is_primary"])
            d["confidence"] = max(d["confidence"], float(e["confidence"]))
    out = list(by_code.values())
    if not acif.excluded:
        out = [e for e in out if not e["code"].startswith(INDIGENOUS_DIVISION_PREFIX)]
    return sorted(out, key=lambda e: (not e["is_primary"], e["name"].lower()))


def _single_org_universities():
    """(grant codes with one eligible organisation, organisation alias -> canonical name, HEP
    canonical names) -- the same sources the name stage uses."""
    from src.acif.name_merge import single_org_grants
    x00c = importlib.import_module("src.00c_extract_propensities")
    crosswalk, hep_names = x00c._load_institution_name_crosswalk()
    return single_org_grants(), crosswalk, hep_names


def acif_rows(acifs: list[AwardsCIF], single=None, crosswalk=None, hep_names=None) -> pd.DataFrame:
    """One row per ACIF."""
    if single is None:
        single, crosswalk, hep_names = _single_org_universities()
    acif_of = {it.unique_id: a.cluster_id for a in acifs for it in a.items}
    on_grant = defaultdict(set)
    for u, c in acif_of.items():
        on_grant[u.split("_", 1)[0]].add(c)

    rows = []
    for a in acifs:
        items = sorted(a.items, key=lambda it: it.unique_id)
        grants = sorted({it.grant_code for it in items})
        years = [it.funding_commence_year for it in items if it.funding_commence_year]
        orcids = sorted({o for it in items for o in (it.orcid, it.scopus_orcid, it.hand_orcid) if o})
        sources = sorted({src for it in items for src, o in
                          (("arc", it.orcid), ("scopus", it.scopus_orcid), ("hand", it.hand_orcid)) if o})
        unis = sorted({crosswalk.get(o, o) for it in items if it.grant_code in single
                       for o in (it.admin_orgs or [it.admin_org]) if o} & hep_names)
        rows.append({
            "cluster_id": a.cluster_id,
            "unique_ids": [it.unique_id for it in items],
            "grant_codes": grants,
            "n_records": len(items),
            "n_grants": len(grants),
            "first_year": min(years) if years else None,
            "last_year": max(years) if years else None,
            "schemes": sorted({_scheme(g) for g in grants}),
            "full_names": sorted({it.full_name for it in items}),
            "full_name_keys": sorted({k for it in items for k in it.full_name_keys}),
            "orcids": orcids,
            "orcid_sources": sources,
            "for2020_codes": for2020_union(a),
            "hep_codes": sorted({h for it in items for h in it.hep_codes}),
            "inst_ids": sorted({i for it in items for i in it.inst_ids}),
            "single_org_universities": unis,
            "coawardee_acif_ids": sorted({c for g in grants for c in on_grant[g]} - {a.cluster_id}),
            "excluded": bool(a.excluded),
            "excluded_reason": a.excluded_reason,
        })
    return pd.DataFrame(rows).sort_values("cluster_id", ignore_index=True)


def record_rows(acifs: list[AwardsCIF]) -> pd.DataFrame:
    """One row per record (grant x investigator): its ACIF and its own facts."""
    rows = [{
        "unique_id": it.unique_id, "cluster_id": a.cluster_id, "grant_code": it.grant_code,
        "full_name": it.full_name, "role_code": it.role_code, "is_fellowship": it.is_fellowship,
        "funding_commence_year": it.funding_commence_year, "admin_org": it.admin_org,
        "admin_orgs": list(it.admin_orgs), "orcid": it.orcid, "scopus_orcid": it.scopus_orcid,
        "hand_orcid": it.hand_orcid,
    } for a in acifs for it in a.items]
    return pd.DataFrame(rows).sort_values("unique_id", ignore_index=True)
