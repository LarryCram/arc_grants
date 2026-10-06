"""
src/02_link_arc_oax.py -- the ARC<->OpenAlex linker of the rebuild (2026-10-06), built one stage
at a time; each stage writes its own table and report section so its effect can be seen alone.

Stages so far:
  1. ORCID links (src/oax/orcid_link.py): every OpenAlex author carrying the ACIF's ORCID -- in
     the HEP-context pool, or, for an ORCID no pool author carries, anywhere in OpenAlex
     (in_pool=False) -- less the links refused in data_persisted/oax_link_overrides.csv; each
     link gets a name_relation and a status (accept_* / review_* / reject_unrelated; see
     src/oax/orcid_link.py).

Inputs: acifs_arc.parquet (src/01_build_arc_acifs.py), openalex_authors_prep.parquet (00b).
Outputs (OAX_LINK_DIR = processed/oax_link/):
    orcid_links.parquet       one row per (ACIF, author) sharing an ORCID: in_pool, name_relation, status
    orcid_rejected.parquet    links refused by hand (oax_link_overrides.csv), with the reason
    orcid_unmatched.parquet   ACIFs with an ORCID and no candidate link
    report.md                 statistics and examples per stage

Usage: .venv/bin/python src/02_link_arc_oax.py
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import OAX_LINK_DIR
from src.oax.orcid_link import (MINOR_SHARE, apply_overrides, decide, load_acifs, load_authors,
                                load_outside_authors, load_overrides, orcid_links, shared_orcids, unmatched)


def _names(x) -> str:
    return ", ".join(x)


def orcid_section(acifs, links, miss, rejected, shared) -> list[str]:
    with_orcid = acifs[acifs.orcids.map(len) > 0]
    src = Counter(s for ss in with_orcid.orcid_sources for s in ss)
    per = links.groupby("cluster_id").size()
    dist = Counter(per.tolist())
    dist[0] = len(miss)
    pool = links[links.in_pool]
    frag = links[links.n_authors > 1]
    top_share = frag.groupby("cluster_id").works_share.max()
    nokey = links[~links.shares_name_key]
    L = ["## Stage 1: ORCID links", "",
         f"- Kept ACIFs: {len(acifs):,}; with an ORCID: {len(with_orcid):,} "
         f"(ORCID sources: " + ", ".join(f"{k} {v:,}" for k, v in sorted(src.items())) + ")",
         f"- ACIF-author pairs sharing an ORCID: {len(links):,}; ACIFs linked: {links.cluster_id.nunique():,}; "
         f"OpenAlex authors linked: {links.author_idx.nunique():,}",
         f"- in the HEP-context pool: {len(pool):,} pairs ({pool.cluster_id.nunique():,} ACIFs); outside it: "
         f"{int((~links.in_pool).sum()):,} pairs ({links.loc[~links.in_pool, 'cluster_id'].nunique():,} ACIFs)",
         f"- refused by hand (oax_link_overrides.csv): {len(rejected):,}",
         f"- **accepted links: {int(links.status.str.startswith('accept').sum()):,}; ACIFs with an accepted link: "
         f"{links.loc[links.status.str.startswith('accept'), 'cluster_id'].nunique():,}**", "",
         "Status of the links (names compared by parser keys; a minor record holds < "
         f"{MINOR_SHARE:.0%} of the ACIF's linked works):", "",
         "| status | links | ACIFs |", "|---|---|---|"]
    for st, sub in links.groupby("status"):
        L.append(f"| {st} | {len(sub):,} | {sub.cluster_id.nunique():,} |")
    acc_ids = set(links.loc[links.status.str.startswith("accept"), "cluster_id"])
    L += ["", f"ACIFs whose links are all review or reject (no accepted link yet): "
          f"{links.loc[~links.cluster_id.isin(acc_ids), 'cluster_id'].nunique():,}", "",
          "Name relation of links that share no name key:", ""]
    for rel, n in links.loc[links.name_relation != "shared_key", "name_relation"].value_counts().items():
        L.append(f"- {rel}: {n:,}")
    for st in ("review_unrelated", "review_given_name", "reject_unrelated", "accept_hand"):
        sub = links[links.status == st].sort_values("works_count_global", ascending=False)
        if len(sub):
            L += ["", f"### {st} ({len(sub):,})", ""]
            for r in sub.itertuples():
                L.append(f"- {r.cluster_id} ({_names(r.full_names)}) ~ A{r.author_idx} {r.author_name} "
                         f"({r.works_count_global} works, {r.works_share:.0%} of the ACIF's linked works; "
                         f"ORCID {r.orcid}, source {'+'.join(r.orcid_sources)})")
    L += ["",
         "OpenAlex authors linked per ACIF (all candidate links):", "",
         "| authors | ACIFs |", "|---|---|"]
    for k in sorted(dist):
        L.append(f"| {k} | {dist[k]:,} |")
    L += ["", "By ORCID source (ACIFs with an ORCID / linked):", ""]
    for s_, sub in with_orcid.groupby(with_orcid.orcid_sources.map(lambda x: "+".join(x))):
        L.append(f"- {s_}: {len(sub):,} / {int(sub.cluster_id.isin(set(links.cluster_id)).sum()):,}")
    rej_ids = set(rejected.cluster_id) if len(rejected) else set()
    L += ["", f"### ACIFs with an ORCID but no candidate link: {len(miss):,}", "",
          f"- ORCID not in OpenAlex: {int((~miss.cluster_id.isin(rej_ids)).sum()):,}",
          f"- only links refused by hand: {int(miss.cluster_id.isin(rej_ids).sum()):,}", "",
          "Examples not in OpenAlex (latest grants first):", ""]
    for r in miss[~miss.cluster_id.isin(rej_ids)].sort_values("last_year", ascending=False).head(10).itertuples():
        L.append(f"- {r.cluster_id} ({_names(r.full_names)}, {r.orcid}, grants {int(r.first_year)}-{int(r.last_year)})")
    L += ["", "Links refused by hand:", ""]
    for r in rejected.itertuples():
        L.append(f"- {r.cluster_id} ({_names(r.full_names)}) ~ A{r.author_idx} {r.author_name}: {r.notes[:140]}")
    L += ["", "Links outside the pool (accepted):", ""]
    for r in links[~links.in_pool].sort_values("cluster_id").itertuples():
        L.append(f"- {r.cluster_id} ({_names(r.full_names)}) ~ A{r.author_idx} {r.author_name} ({r.works_count_global} works)")
    L += ["", f"### Fragments: {frag.cluster_id.nunique():,} ACIFs with 2+ OpenAlex authors", "",
          "Share of works in the largest author: "
          + ", ".join(f"p{q}: {top_share.quantile(q / 100):.3f}" for q in (10, 50, 90)), ""]
    big = frag.groupby("cluster_id").size().sort_values(ascending=False).head(6).index
    for cid in big:
        sub = frag[frag.cluster_id == cid]
        L.append(f"- {cid} ({_names(sub.full_names.iloc[0])}): "
                 + ", ".join(f"{r.author_name} ({r.works_count_global})" for r in sub.itertuples()))
    even = top_share[top_share < 0.8].index
    L += ["", f"ACIFs whose largest author holds < 80% of works: {len(even):,}", ""]
    for cid in list(even)[:12]:
        sub = frag[frag.cluster_id == cid]
        L.append(f"- {cid} ({_names(sub.full_names.iloc[0])}): "
                 + ", ".join(f"{r.author_name} ({r.works_count_global})" for r in sub.itertuples()))
    L += ["", f"### Name keys: {int(links.shares_name_key.sum()):,} of {len(links):,} pairs share a key "
          f"({int(links.shares_full_name_key.sum()):,} share one with a full given name); "
          f"{len(nokey):,} share none", "",
          "Pairs sharing no key (largest authors first):", ""]
    for r in nokey.sort_values("works_count_global", ascending=False).head(25).itertuples():
        L.append(f"- {r.cluster_id} ({_names(r.full_names)}) ~ A{r.author_idx} {r.author_name} "
                 f"({r.works_count_global} works)")
    L += ["", f"### ORCIDs held by two ACIFs: {shared.orcid.nunique():,}", ""]
    for orcid, sub in shared.groupby("orcid"):
        L.append(f"- {orcid}: " + "; ".join(f"{r.cluster_id} ({_names(r.full_names)})" for r in sub.itertuples()))
    return L + [""]


def main():
    OAX_LINK_DIR.mkdir(parents=True, exist_ok=True)
    acifs = load_acifs()
    orcids = {o for os in acifs.orcids for o in os}
    pool = load_authors(orcids)
    authors = pd.concat([pool, load_outside_authors(orcids - set(pool.orcid))], ignore_index=True)
    rejects = load_overrides()
    authors, refused = apply_overrides(authors, rejects)
    accepts = set(zip(rejects.loc[rejects.action == "accept_link", "orcid"],
                      rejects.loc[rejects.action == "accept_link", "author_idx"]))
    links = decide(orcid_links(acifs, authors), accepts)
    miss = unmatched(acifs, links)
    a = acifs[acifs.orcids.map(len) > 0].assign(orcid=lambda d: d.orcids.map(lambda x: x[0]))
    rejected = (a[["cluster_id", "orcid", "full_names"]]
                .merge(refused.rename(columns={"full_name": "author_name"})[["orcid", "author_idx", "author_name"]], on="orcid")
                .merge(rejects[rejects.action == "reject_link"], on=["orcid", "author_idx"]))
    shared = shared_orcids(acifs)

    links.to_parquet(OAX_LINK_DIR / "orcid_links.parquet", index=False)
    rejected.to_parquet(OAX_LINK_DIR / "orcid_rejected.parquet", index=False)
    miss.to_parquet(OAX_LINK_DIR / "orcid_unmatched.parquet", index=False)
    text = "\n".join(["# ARC<->OpenAlex linking", ""] + orcid_section(acifs, links, miss, rejected, shared))
    (OAX_LINK_DIR / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
