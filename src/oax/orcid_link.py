"""
Step 1 of the ARC<->OpenAlex linker (2026-10-06): link each ACIF to every OpenAlex author that
carries the ACIF's ORCID. An exact identity link -- no names are used to find it. Every author
found is kept: OpenAlex often splits one person into a large record plus small fragments.

Sides:
  ACIFs    acifs_arc.parquet, kept ACIFs only (excluded=False); `orcids` holds at most one ORCID
           (ARC, Scopus-found or hand; the build's ORCID veto), `orcid_sources` says which.
  authors  openalex_authors_prep.parquet (00b): the HEP-context pool, ORCID already bare.
           Authors outside that pool are looked up in the full OpenAlex authors table only to
           explain ACIF ORCIDs not found in the pool.

Name keys are not used to link; `shares_name_key` / `shares_full_name_key` only report whether
the two sides' parser keys agree (a check on the link, and input to step 2's design).
"""

from __future__ import annotations

import duckdb
import pandas as pd

from config.settings import ACIFS_ARC, OAX_AUTHORS, PROCESSED_DATA

AUTHORS_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
ACIF_COLUMNS = ["cluster_id", "orcids", "orcid_sources", "full_names", "full_name_keys",
                "n_records", "first_year", "last_year"]
AUTHOR_COLUMNS = ["author_idx", "orcid", "full_name", "full_name_keys", "works_count",
                  "works_count_au", "works_count_global"]


def load_acifs(path=ACIFS_ARC) -> pd.DataFrame:
    """Kept ACIFs (excluded=False)."""
    a = pd.read_parquet(path, columns=ACIF_COLUMNS + ["excluded"])
    return a[~a.excluded].drop(columns="excluded").reset_index(drop=True)


def load_authors(orcids, path=AUTHORS_PREP) -> pd.DataFrame:
    """Pool authors whose ORCID is in `orcids`."""
    con = duckdb.connect()
    con.register("want", pd.DataFrame({"orcid": sorted(set(orcids))}))
    return con.execute(f"""
        SELECT {', '.join(AUTHOR_COLUMNS)} FROM read_parquet('{path}')
        WHERE orcid IN (SELECT orcid FROM want)
    """).fetchdf()


def _full_keys(keys) -> set[str]:
    """Keys with a full (multi-letter) given name."""
    return {k for k in keys if len(k.split("_", 1)[0]) > 1}


def orcid_links(acifs: pd.DataFrame, authors: pd.DataFrame) -> pd.DataFrame:
    """One row per (ACIF, author) sharing an ORCID: the ACIF's side, the author's side, the
    number of authors found for that ACIF, the author's share of their works, and whether the
    two sides share a name key (any / one with a full given name)."""
    a = acifs[acifs.orcids.map(len) > 0].copy()
    a["orcid"] = a.orcids.map(lambda x: x[0])
    au = authors.rename(columns={"full_name": "author_name", "full_name_keys": "author_keys"})
    m = a.drop(columns="orcids").merge(au, on="orcid", how="inner")
    if m.empty:
        return m.assign(n_authors=pd.Series(dtype=int), works_share=pd.Series(dtype=float),
                        shares_name_key=pd.Series(dtype=bool), shares_full_name_key=pd.Series(dtype=bool))
    ak = [set(k) for k in m.full_name_keys]
    ok = [set(k) for k in m.author_keys]
    m["shares_name_key"] = [bool(x & y) for x, y in zip(ak, ok)]
    m["shares_full_name_key"] = [bool(_full_keys(x) & y) for x, y in zip(ak, ok)]
    m["n_authors"] = m.groupby("cluster_id").author_idx.transform("size")
    total = m.groupby("cluster_id").works_count_global.transform("sum")
    m["works_share"] = (m.works_count_global / total.where(total > 0)).fillna(0.0)
    return m.sort_values(["cluster_id", "works_count_global"], ascending=[True, False],
                         ignore_index=True)


def unmatched(acifs: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    """Kept ACIFs with an ORCID that no pool author carries."""
    a = acifs[acifs.orcids.map(len) > 0].copy()
    a["orcid"] = a.orcids.map(lambda x: x[0])
    return a[~a.cluster_id.isin(set(links.cluster_id))].drop(columns="orcids").reset_index(drop=True)


def outside_pool(orcids) -> pd.DataFrame:
    """Authors in the full OpenAlex authors table carrying any of `orcids` (bare form)."""
    con = duckdb.connect()
    con.register("want", pd.DataFrame({"orcid": sorted(set(orcids))}))
    return con.execute(f"""
        SELECT author_idx, replace(orcid, 'https://orcid.org/', '') AS orcid, display_name, works_count
        FROM read_parquet('{OAX_AUTHORS}/*.parquet')
        WHERE replace(orcid, 'https://orcid.org/', '') IN (SELECT orcid FROM want)
    """).fetchdf()


def shared_orcids(acifs: pd.DataFrame) -> pd.DataFrame:
    """ORCIDs held by 2+ kept ACIFs (the build refused to merge them on names)."""
    a = acifs[acifs.orcids.map(len) > 0].copy()
    a["orcid"] = a.orcids.map(lambda x: x[0])
    g = a.groupby("orcid").filter(lambda d: len(d) > 1)
    return g[["orcid", "cluster_id", "full_names", "orcid_sources"]].sort_values(["orcid", "cluster_id"],
                                                                                ignore_index=True)
