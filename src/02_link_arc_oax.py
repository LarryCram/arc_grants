"""
src/02_link_arc_oax.py -- the ARC<->OpenAlex linker of the rebuild (2026-10-06), built one stage
at a time; each stage writes its own table and report section so its effect can be seen alone.

Stages so far:
  1. ORCID links (src/oax/orcid_link.py): every OpenAlex author carrying the ACIF's ORCID.

Inputs: acifs_arc.parquet (src/01_build_arc_acifs.py), openalex_authors_prep.parquet (00b).
Outputs (OAX_LINK_DIR = processed/oax_link/):
    orcid_links.parquet       one row per (ACIF, author) sharing an ORCID
    orcid_unmatched.parquet   ACIFs whose ORCID no pool author carries, with any full-OpenAlex hit
    report.md                 statistics and examples per stage

Usage: .venv/bin/python src/02_link_arc_oax.py
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import OAX_LINK_DIR
from src.oax.orcid_link import load_acifs, load_authors, orcid_links, outside_pool, shared_orcids, unmatched


def _names(x) -> str:
    return ", ".join(x)


def orcid_section(acifs, links, miss, outside, shared) -> list[str]:
    with_orcid = acifs[acifs.orcids.map(len) > 0]
    src = Counter(s for ss in with_orcid.orcid_sources for s in ss)
    per = links.groupby("cluster_id").size()
    dist = Counter(per.tolist())
    dist[0] = len(miss)
    found_outside = set(outside.orcid)
    frag = links[links.n_authors > 1]
    top_share = frag.groupby("cluster_id").works_share.max()
    nokey = links[~links.shares_name_key]
    L = ["## Stage 1: ORCID links", "",
         f"- Kept ACIFs: {len(acifs):,}; with an ORCID: {len(with_orcid):,} "
         f"(ORCID sources: " + ", ".join(f"{k} {v:,}" for k, v in sorted(src.items())) + ")",
         f"- ACIF-author pairs sharing an ORCID: {len(links):,}; ACIFs linked: {links.cluster_id.nunique():,}; "
         f"OpenAlex authors linked: {links.author_idx.nunique():,}", "",
         "OpenAlex authors found per ACIF (HEP-context pool):", "",
         "| authors | ACIFs |", "|---|---|"]
    for k in sorted(dist):
        L.append(f"| {k} | {dist[k]:,} |")
    L += ["", "By ORCID source (ACIFs with an ORCID / linked):", ""]
    for s, sub in with_orcid.groupby(with_orcid.orcid_sources.map(lambda x: "+".join(x))):
        L.append(f"- {s}: {len(sub):,} / {int(sub.cluster_id.isin(set(links.cluster_id)).sum()):,}")
    L += ["", f"### ORCID not in the pool: {len(miss):,} ACIFs", "",
          f"- in full OpenAlex, outside the HEP-context pool: {int(miss.orcid.isin(found_outside).sum()):,}",
          f"- not in OpenAlex at all: {int((~miss.orcid.isin(found_outside)).sum()):,}", "",
          "Examples in full OpenAlex (largest first):", ""]
    o = outside.sort_values("works_count", ascending=False).drop_duplicates("orcid")
    m = miss.merge(o, on="orcid")
    for r in m.head(10).itertuples():
        L.append(f"- {r.cluster_id} ({_names(r.full_names)}, grants {int(r.first_year)}-{int(r.last_year)}) -> "
                 f"A{r.author_idx} {r.display_name}, {r.works_count} works")
    L += ["", "Examples not in OpenAlex:", ""]
    for r in miss[~miss.orcid.isin(found_outside)].sort_values("last_year", ascending=False).head(10).itertuples():
        L.append(f"- {r.cluster_id} ({_names(r.full_names)}, {r.orcid}, grants {int(r.first_year)}-{int(r.last_year)})")
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
    for cid in list(even)[:8]:
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
    links = orcid_links(acifs, load_authors(orcids))
    miss = unmatched(acifs, links)
    outside = outside_pool(miss.orcid)
    shared = shared_orcids(acifs)

    links.to_parquet(OAX_LINK_DIR / "orcid_links.parquet", index=False)
    miss.merge(outside, on="orcid", how="left").to_parquet(OAX_LINK_DIR / "orcid_unmatched.parquet", index=False)
    text = "\n".join(["# ARC<->OpenAlex linking", ""] + orcid_section(acifs, links, miss, outside, shared))
    (OAX_LINK_DIR / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
