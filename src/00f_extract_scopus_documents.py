"""
00f_extract_scopus_documents.py

PURPOSE (2026-10-08, user decision: Scopus replaces paid Gemini for the oeuvre works still to judge):
    Every document in each TRUSTED Scopus author profile of the ACIFs that still need it -- trusted
    meaning the profile, found by 00d's per-ACIF Author Search, carries the ACIF's own ORCID. Step 5
    of the oeuvre extractor then accepts a work on the person's own profile and can reject an article
    or review missing from it (see the plan file).

    One Scopus Search per profile: AU-ID(<scopus_id>), STANDARD view (DOI, title, date, type,
    source), subscriber cursor paging. pybliometrics caches each query (SCOPUS_CACHE_DIR), so a rerun
    or a resumed run spends nothing on profiles already fetched; a profile that failed is not cached
    and is fetched next time.

    Throttling: pybliometrics paces ScopusSearch at 9 requests a second per process (the workers here
    are threads in one process, so they share it) and retries server errors (5xx) briefly, but on a
    429 it raises at once. Here a 429 pauses every worker and the same profile is retried after
    BACKOFF waits (1, 2, 4, 8, 16, 30 minutes); if the last wait still fails the run stops cleanly,
    logging why. Other errors are logged and the profile skipped (retried on the next run).

TARGETS (default): kept ACIFs with works still pending or accepted only by the liberal 'fits core'
    rule (processed/oeuvre/acif_works_classified.parquet) that have a trusted profile.
    --all-trusted: every kept ACIF with a trusted profile.

OUTPUT (SCOPUS_EXTRACT_DIR):
    scopus_document_targets.parquet   (cluster_id, scopus_id)
    scopus_profile_documents.parquet  (scopus_id, eid, doi, year, subtype, title, source) -- profiles
                                      fetched so far (rewritten at the end of every run)
    scopus_documents_run.log          progress, waits, failures, stop reason

Usage: .venv/bin/python src/00f_extract_scopus_documents.py [--workers 4] [--all-trusted] [--limit N]
"""

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, OEUVRE_DIR, SCOPUS_EXTRACT_DIR
from src.utils.scopus import init_scopus

BACKOFF = [60, 120, 240, 480, 960, 1800]
LOG = SCOPUS_EXTRACT_DIR / "scopus_documents_run.log"


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def trusted_profiles() -> pd.DataFrame:
    """(cluster_id, scopus_id) for kept ACIFs whose 00d search found a profile carrying the ACIF's
    own ORCID. 00d searched per ACIF of an earlier stage; its records map to the current ACIFs."""
    acifs = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "orcids", "excluded"])
    acifs = acifs[~acifs.excluded]
    orc = {c: set(o) for c, o in zip(acifs.cluster_id, acifs.orcids)}
    rec = dict(pd.read_parquet(ACIF_ARC_RECORDS, columns=["unique_id", "cluster_id"]).values)
    summ = pd.read_parquet(SCOPUS_EXTRACT_DIR / "scopus_acif_summary.parquet", columns=["cluster_id", "unique_ids"])
    m = summ.explode("unique_ids").assign(acif=lambda d: d.unique_ids.map(rec)).dropna(subset=["acif"])
    m = m[["cluster_id", "acif"]].drop_duplicates().rename(columns={"cluster_id": "search_id"})
    prof = pd.read_parquet(SCOPUS_EXTRACT_DIR / "scopus_acif_profiles.parquet", columns=["cluster_id", "scopus_id", "orcid"])
    p = prof.rename(columns={"cluster_id": "search_id"}).merge(m, on="search_id")
    p = p[[isinstance(o, str) and o in orc.get(a, set()) for o, a in zip(p.orcid, p.acif)]]
    return (p[["acif", "scopus_id"]].rename(columns={"acif": "cluster_id"}).astype({"scopus_id": str})
            .drop_duplicates().sort_values(["cluster_id", "scopus_id"]).reset_index(drop=True))


def open_oeuvre_acifs() -> set:
    k = pd.read_parquet(OEUVRE_DIR / "acif_works_classified.parquet", columns=["cluster_id", "rule", "decision"])
    return set(k.loc[(k.decision == "pending") | (k.rule == "fits core"), "cluster_id"])


class Fetcher:
    def __init__(self):
        from pybliometrics.scopus import ScopusSearch
        from pybliometrics.exception import Scopus429Error
        self.search, self.e429 = ScopusSearch, Scopus429Error
        self.go = threading.Event()
        self.go.set()
        self.lock = threading.Lock()
        self.stop = None
        self.rows, self.done, self.failed, self.waits = {}, 0, [], 0
        self.checkpoint = None

    def one(self, sid: str) -> None:
        for attempt in range(len(BACKOFF) + 1):
            if self.stop:
                return
            self.go.wait()
            try:
                q = self.search(f"AU-ID({sid})", view="STANDARD", subscriber=True, refresh=False)
                rows = [(sid, r.eid, (r.doi or "").lower() or None, (r.coverDate or "")[:4] or None, r.subtype,
                         r.title, r.publicationName) for r in (q.results or [])]
                with self.lock:
                    self.rows[sid] = rows
                    self.done += 1
                    if self.done % 200 == 0:
                        log(f"profiles done {self.done:,}; documents {sum(len(v) for v in self.rows.values()):,}")
                    if self.done % 1000 == 0 and self.checkpoint:
                        self.checkpoint(self.rows)
                return
            except self.e429 as e:
                if attempt == len(BACKOFF):
                    with self.lock:
                        self.stop = f"429 after {len(BACKOFF)} waits: {e}"
                    log(f"STOP {self.stop}")
                    return
                with self.lock:
                    if self.go.is_set():          # the first worker to see the 429 pauses everyone
                        self.go.clear()
                        self.waits += 1
                        wait = BACKOFF[attempt]
                        log(f"429 on {sid} ({e}); all workers pause {wait} s (wait {attempt + 1}/{len(BACKOFF)})")
                        threading.Timer(wait, self.go.set).start()
            except Exception as e:  # other errors: skip this profile now, it is retried on the next run
                with self.lock:
                    self.failed.append((sid, repr(e)[:200]))
                log(f"error on {sid}: {repr(e)[:200]}")
                return


def write_documents(rows: dict) -> pd.DataFrame:
    """Write the documents fetched so far (also every 1,000 profiles, so a stopped run leaves them)."""
    docs = pd.DataFrame([r for v in list(rows.values()) for r in v],
                        columns=["scopus_id", "eid", "doi", "year", "subtype", "title", "source"])
    docs.to_parquet(SCOPUS_EXTRACT_DIR / "scopus_profile_documents.parquet", index=False)
    return docs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--all-trusted", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    SCOPUS_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    targets = trusted_profiles()
    if not args.all_trusted:
        targets = targets[targets.cluster_id.isin(open_oeuvre_acifs())]
    targets.to_parquet(SCOPUS_EXTRACT_DIR / "scopus_document_targets.parquet", index=False)
    sids = sorted(set(targets.scopus_id))[: args.limit]
    log(f"start: {targets.cluster_id.nunique():,} ACIFs, {len(sids):,} profiles, {args.workers} workers")
    init_scopus()
    f = Fetcher()
    f.checkpoint = write_documents
    t = time.time()
    with ThreadPoolExecutor(args.workers) as ex:
        list(ex.map(f.one, sids))
    docs = write_documents(f.rows)
    log(f"end: {f.done:,}/{len(sids):,} profiles, {len(docs):,} documents, {len(f.failed)} failed, "
        f"{f.waits} throttling pauses, {time.time() - t:.0f} s; stop reason: {f.stop or 'none'}")


if __name__ == "__main__":
    main()
