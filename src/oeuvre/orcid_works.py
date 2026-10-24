"""
DOIs each ACIF's people list on their own ORCID records (2026-10-08), from the ORCID record cache
(DISKCACHE_DIR/orcid_records_authenticated, full /record responses). A cached record whose own path
differs from its key (a deprecated ORCID answered with the record it now redirects to) is ignored,
as in src/00d_extract_scopus.py. Used by step 5 of the oeuvre extractor: a reference-work entry on
the person's own ORCID list is kept as a real work.

Unattributed works (2026-10-24, user: "build the linking for unattributed works"): OpenAlex leaves some
authorships on no author record (author_idx NULL; 72M in the Oct26 snapshot). A person's own works can
sit there -- whole careers when OpenAlex put the person's ORCID on another person's record (Tony Clarke,
QUT: 136 of the 139 DOIs on his ORCID record). unattributed_authorships() gives step 1 those authorships:
a work whose DOI is on the ACIF's own ORCID record, carrying an authorship with no author record whose
printed name fits the ACIF's names (printed_name_fits). Left out: works the ACIF already reaches through
a linked record; works with 2+ fitting unattributed authorships for the ACIF (which one is the person is
unknown); authorships fitting 2+ ACIFs that both list the DOI. No author_idx is minted: the ACIF is the
identity, the row's author_idx stays NULL and its link_status is UNATTRIBUTED_STATUS.
"""

from __future__ import annotations

import re
from collections import Counter

import pandas as pd

from config.settings import DISKCACHE_DIR, OPENALEX_COMPACT_DIR
from src.oax.orcid_hint_link import names_compatible
from src.utils.names import HumanNameParser

UNATTRIBUTED_STATUS = "orcid_doi_unattributed"
_PARSER = HumanNameParser()

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


def printed_name_fits(acif_keys, name: str, cache: dict) -> bool:
    """A printed byline name can be the ACIF's: compatible parser keys (orcid_hint_link.names_compatible), or
    an initials-only name whose first initial is the first letter of one of the ACIF's full given names under
    the same family name ("A. R. Clarke" for Anthony Clarke)."""
    if name not in cache:
        p = _PARSER.parse(name) if isinstance(name, str) and name.strip() else None
        cache[name] = ([], []) if p is None else (
            list(p.full_name_keys),
            [(set(p.family_names), f) for f in [set(p.given_tokens) - set(p.middle_tokens)]
             if f and all(len(t) == 1 for t in f)])
    keys, inits = cache[name]
    if names_compatible(list(acif_keys), keys):
        return True
    first = {}
    for k in acif_keys:
        if "_" in k and len(k.split("_", 1)[0]) > 1:
            first.setdefault(k.split("_", 1)[1], set()).add(k[0])
    return any(f & first.get(fam, set()) for fams, f in inits for fam in fams)


def unattributed_authorships(con, acifs: pd.DataFrame, links: pd.DataFrame, cache=None,
                             authorships=OPENALEX_COMPACT_DIR / "authorships",
                             works=OPENALEX_COMPACT_DIR / "works") -> tuple[pd.DataFrame, dict]:
    """Rows for step 1 (module docstring): cluster_id, link_status, author_idx (NULL), work_idx, author_name,
    institution_idx, institution_name, country_code -- one per authorship row (an authorship with two
    institutions gives two rows), as step 1 reads them from the authorships table. `acifs`: cluster_id,
    orcids, full_name_keys of the kept ACIFs; `links`: (cluster_id, author_idx) accepted links."""
    cd = claimed_dois(acifs, cache)
    counts = Counter(acifs_with_dois=cd.cluster_id.nunique(), dois=len(cd))
    cols = ["cluster_id", "link_status", "author_idx", "work_idx", "author_name", "institution_idx",
            "institution_name", "country_code"]
    if cd.empty:
        return pd.DataFrame(columns=cols), dict(counts)
    con.register("uw_dois", cd)
    cw = con.execute(f"""SELECT d.cluster_id, w.work_idx FROM read_parquet('{works}/*.parquet') w
                         JOIN uw_dois d ON lower(w.doi) = d.doi""").df().drop_duplicates()
    counts["claimed_works"] = len(cw)
    con.register("uw_cw", cw[["work_idx"]].drop_duplicates())
    au = con.execute(f"""SELECT work_idx, author_idx, author_name, institution_idx, institution_name, country_code
                         FROM read_parquet('{authorships}/*.parquet')
                         WHERE work_idx IN (SELECT work_idx FROM uw_cw)""").df().drop_duplicates()
    # works the ACIF already reaches through one of its linked records
    lk = links[["cluster_id", "author_idx"]].astype({"author_idx": "int64"})
    reached = cw.merge(au[au.author_idx.notna()][["work_idx", "author_idx"]].astype({"author_idx": "int64"}),
                       on="work_idx").merge(lk, on=["cluster_id", "author_idx"])[["cluster_id", "work_idx"]]
    x = cw.merge(au[au.author_idx.isna()], on="work_idx")
    x = x.merge(reached.drop_duplicates().assign(_r=True), on=["cluster_id", "work_idx"], how="left")
    counts["works_already_reached"] = int(x[x._r.notna()][["cluster_id", "work_idx"]].drop_duplicates().shape[0])
    x = x[x._r.isna()].drop(columns="_r")
    keys = dict(zip(acifs.cluster_id, acifs.full_name_keys))
    names: dict = {}
    pairs = x[["cluster_id", "author_name"]].drop_duplicates()
    pairs = pairs[[printed_name_fits(keys[c], n, names) for c, n in zip(pairs.cluster_id, pairs.author_name)]]
    x = x.merge(pairs, on=["cluster_id", "author_name"])
    n_names = x.groupby(["cluster_id", "work_idx"]).author_name.nunique()
    ambiguous = set(n_names[n_names > 1].index)
    counts["works_two_fitting_authorships"] = len(ambiguous)
    x = x[[(c, w) not in ambiguous for c, w in zip(x.cluster_id, x.work_idx)]]
    n_acifs = x.groupby(["work_idx", "author_name"]).cluster_id.nunique()
    shared = set(n_acifs[n_acifs > 1].index)
    counts["authorships_fitting_two_acifs"] = len(shared)
    x = x[[(w, n) not in shared for w, n in zip(x.work_idx, x.author_name)]]
    x = x.assign(link_status=UNATTRIBUTED_STATUS, author_idx=pd.array([pd.NA] * len(x), dtype="Int64"))
    counts["works_added"] = int(x[["cluster_id", "work_idx"]].drop_duplicates().shape[0])
    counts["acifs_added"] = int(x.cluster_id.nunique())
    counts["acifs_added_unlinked"] = int(len(set(x.cluster_id) - set(lk.cluster_id)))
    x = x.astype({"work_idx": "int64", "institution_idx": "Int64"})
    return x[cols].reset_index(drop=True), dict(counts)
