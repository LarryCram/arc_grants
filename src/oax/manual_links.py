"""
Hand links keyed on ARC record ids (2026-10-23, user: "build the updated manual_linker as you go"):
data_persisted/manual_links.csv -- action (link | unlink), unique_id, oax_author, source, notes.

Seeded from the old pipeline's manual_resolutions.csv after reviewing every row against the Oct26 snapshot
(analysis/34_review_manual_resolutions.py): a row is kept when its record still has works and its names fit
the ACIF's (or the note documents the name form), or, for a record OpenAlex emptied, when an Oct26 author
holding its Jul26 works was found. The rest wait in processed/manual_resolutions_to_review.csv.

Rows are keyed on a record (unique_id), not a cluster_id, so they survive the cluster relabels of later
builds. A row whose record is not in the current build (its grant left scope, e.g. not taken up) is inert
and reported; a row naming an OpenAlex author that does not exist stops the run.
  link     the record's ACIF takes the OpenAlex author (stage "hand", status accept_manual_link)
  unlink   the record's ACIF takes no OpenAlex author from any stage (a person checked by hand and found
           absent from OpenAlex)
  reject   the record's ACIF never takes this OpenAlex author, whichever stage found it (e.g. an ORCID-hint
           namesake: Yan Yan's Dalian Maritime record)
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pandas as pd

from config.settings import ACIF_ARC_RECORDS, OAX_AUTHORS

MANUAL_LINKS = Path(__file__).resolve().parents[2] / "data_persisted" / "manual_links.csv"
ACTIONS = {"link", "unlink", "reject"}


def load(path=MANUAL_LINKS) -> pd.DataFrame:
    if not Path(path).exists():
        return pd.DataFrame(columns=["action", "unique_id", "author_idx", "source", "notes"])
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    bad = sorted({r["action"] for r in rows} - ACTIONS)
    if bad:
        raise SystemExit(f"{Path(path).name}: unknown action(s) {bad}")
    return pd.DataFrame([{"action": r["action"], "unique_id": r["unique_id"].strip(),
                          "author_idx": int(r["oax_author"].strip().lstrip("Aa")) if r["oax_author"].strip() else None,
                          "source": r["source"], "notes": r["notes"]} for r in rows],
                        columns=["action", "unique_id", "author_idx", "source", "notes"])


def resolve(rows: pd.DataFrame, acif_ids, records=ACIF_ARC_RECORDS, authors=OAX_AUTHORS) -> tuple[pd.DataFrame, set, pd.DataFrame, set]:
    """(links: cluster_id, author_idx, status; unlinked: cluster_ids; inert rows; rejected (cluster_id,
    author_idx) pairs). A link row naming an OpenAlex author that does not exist stops the run."""
    rec = pd.read_parquet(records, columns=["unique_id", "cluster_id"])
    of = dict(zip(rec.unique_id, rec.cluster_id))
    r = rows.assign(cluster_id=rows.unique_id.map(of))
    keep = r.cluster_id.isin(set(acif_ids))
    inert, r = r[~keep], r[keep]
    lk = r[r.action == "link"]
    if len(lk):
        con = duckdb.connect()
        con.register("want", lk[["author_idx"]].astype("int64"))
        have = {x[0] for x in con.execute(f"SELECT author_idx FROM read_parquet('{authors}/*.parquet') "
                                          "WHERE author_idx IN (SELECT author_idx FROM want)").fetchall()}
        missing = sorted(set(lk.author_idx.astype("int64")) - have)
        if missing:
            raise SystemExit(f"manual_links.csv link rows name OpenAlex authors that do not exist: {missing}")
    links = pd.DataFrame({"cluster_id": lk.cluster_id.values, "author_idx": lk.author_idx.astype("int64").values,
                          "status": "accept_manual_link"}).drop_duplicates(["cluster_id", "author_idx"])
    rj = r[r.action == "reject"]
    rejected = {(c, int(a)) for c, a in zip(rj.cluster_id, rj.author_idx)}
    return links, set(r.loc[r.action == "unlink", "cluster_id"]), inert, rejected
