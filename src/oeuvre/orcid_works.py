"""
DOIs each ACIF's people list on their own ORCID records (2026-10-08), from the ORCID record cache
(DISKCACHE_DIR/orcid_records_authenticated, full /record responses). A cached record whose own path
differs from its key (a deprecated ORCID answered with the record it now redirects to) is ignored,
as in src/00d_extract_scopus.py. Used by step 5 of the oeuvre extractor: a reference-work entry on
the person's own ORCID list is kept as a real work.
"""

from __future__ import annotations

import re

import pandas as pd

from config.settings import DISKCACHE_DIR

_DOI_URL = re.compile(r"^https?://(dx\.)?doi\.org/")


def record_dois(rec: dict) -> set[str]:
    """Lower-case DOIs in an ORCID /record's works summary."""
    out = set()
    for g in ((rec.get("activities-summary") or {}).get("works") or {}).get("group", []):
        for e in (g.get("external-ids") or {}).get("external-id", []):
            if e.get("external-id-type") == "doi":
                v = ((e.get("external-id-normalized") or {}).get("value") or e.get("external-id-value") or "").lower()
                v = _DOI_URL.sub("", v.strip())
                if v:
                    out.add(v)
    return out


def claimed_dois(acifs: pd.DataFrame, cache=None) -> pd.DataFrame:
    """(cluster_id, doi) for every DOI on the cached ORCID records of each ACIF's ORCIDs."""
    if cache is None:
        import diskcache
        cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    rows = []
    for cid, orcids in zip(acifs.cluster_id, acifs.orcids):
        for o in orcids if orcids is not None else []:
            rec = cache.get(o)
            if not rec or (rec.get("orcid-identifier") or {}).get("path") not in (None, o):
                continue
            rows += [(cid, d) for d in record_dois(rec)]
    return pd.DataFrame(rows, columns=["cluster_id", "doi"]).drop_duplicates()
