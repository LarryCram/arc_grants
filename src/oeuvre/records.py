"""
Step 1: the OpenAlex author records of each ACIF -- the accepted links of linker stage 1
(processed/oax_link/orcid_links.parquet, status accept_*). Every accepted record counts as the
person's (user, 2026-10-06: no main/extra distinction yet); rank and share are kept so the later
steps can report by record kind.
"""

from __future__ import annotations

import pandas as pd

from config.settings import OAX_LINK_DIR

COLUMNS = ["cluster_id", "author_idx", "author_name", "orcid", "status", "in_pool", "works_count_global"]


def records(links: pd.DataFrame) -> pd.DataFrame:
    """One row per (ACIF, accepted author record): n_records (accepted records of the ACIF),
    record_rank (1 = most works), works_share (share of the ACIF's accepted records' works)."""
    r = links.loc[links.status.str.startswith("accept"), COLUMNS].copy()
    r["author_idx"] = r.author_idx.astype("int64")
    r["works_count_global"] = r.works_count_global.fillna(0).astype("int64")
    r = r.sort_values(["cluster_id", "works_count_global", "author_idx"], ascending=[True, False, True])
    r["n_records"] = r.groupby("cluster_id").author_idx.transform("size")
    r["record_rank"] = r.groupby("cluster_id").cumcount() + 1
    total = r.groupby("cluster_id").works_count_global.transform("sum")
    r["works_share"] = (r.works_count_global / total.where(total > 0)).fillna(0.0)
    return r.reset_index(drop=True)


def load_records(path=OAX_LINK_DIR / "orcid_links.parquet") -> pd.DataFrame:
    return records(pd.read_parquet(path))
