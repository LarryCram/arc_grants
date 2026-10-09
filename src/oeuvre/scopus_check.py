"""
The person's own Scopus profile as evidence for step 5 of the oeuvre extractor (2026-10-08, user:
Scopus replaces paid Gemini for the works still to judge). A TRUSTED profile is one 00d's Author
Search found for the ACIF that carries the ACIF's own ORCID; 00f fetched its documents.

Measured 2026-10-09 on 10,487 ACIFs with a fetched trusted profile (works since 2000 with a DOI): of
the core's works (tied to the grants) 83% of articles and 93% of reviews are on the profile, but only
43% of book chapters and 38% of books; of the 'fits core' works 14% of articles; of works still
pending 4.5%.
"""

from __future__ import annotations

import pandas as pd

from config.settings import SCOPUS_EXTRACT_DIR
from src.oax.scopus_link import searched_profiles

DOCS = SCOPUS_EXTRACT_DIR / "scopus_profile_documents.parquet"


def trusted_profiles(acifs: pd.DataFrame) -> pd.DataFrame:
    """(cluster_id, scopus_id) for kept ACIFs whose 00d search found a profile carrying the ACIF's
    own ORCID. acifs: cluster_id, orcids, excluded."""
    a = acifs[~acifs.excluded]
    orc = {c: set(o) for c, o in zip(a.cluster_id, a.orcids)}
    p = searched_profiles(a.cluster_id)
    p = p[[isinstance(o, str) and o in orc.get(c, set()) for c, o in zip(p.cluster_id, p.profile_orcid)]]
    return p[["cluster_id", "scopus_id"]].reset_index(drop=True)


def trusted_profile_dois(acifs: pd.DataFrame, docs=DOCS) -> tuple[pd.DataFrame, pd.DataFrame]:
    """((cluster_id, doi) on the ACIFs' fetched trusted profiles, (cluster_id) of ACIFs with one)."""
    tp = trusted_profiles(acifs)
    d = pd.read_parquet(docs, columns=["scopus_id", "doi"]).astype({"scopus_id": str})
    tp = tp[tp.scopus_id.isin(set(d.scopus_id))]
    dois = tp.merge(d[d.doi.notna()], on="scopus_id")[["cluster_id", "doi"]]
    dois = dois.assign(doi=dois.doi.str.lower()).drop_duplicates().reset_index(drop=True)
    return dois, pd.DataFrame({"cluster_id": sorted(set(tp.cluster_id))})
