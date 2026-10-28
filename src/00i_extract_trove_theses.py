"""
src/00i_extract_trove_theses.py -- every Australian thesis Trove holds for chosen decades, with all its library
records (2026-10-26, user: "download all the 2010 theses and see what we get ... some of the theses will be for
CIs who never have fellowships"). Purpose: PhD year, university and (where a repository lists them) supervisors
for ARC investigators; a test of Trove against ORCID PhD years found 87% of Australian PhDs, 94% of them within
a year.

Source: Trove API v3, category 'research' (theses are there, not in 'book'), limit l-format=Thesis,
l-australian=y (user, 2026-10-26: Trove's raw year counts are inflated by overseas theses held in Australian
libraries -- 1990: 15,910 theses, 6,800 Australian content; Trove only helps for Australian PhDs) and l-decade,
q='thesis' (matches every Thesis-format work), reclevel=full, include=workversions -- each work comes with every
version's Dublin Core record: creator (author; the national catalogue record adds the university and faculty),
contributor (supervisors and school, on some university repository records), type (sometimes the degree), date,
identifier, metadataSource (library code, e.g. SAU:IR = Adelaide repository). Paging: bulkHarvest=true with the
nextStart cursor, n=100.
Harvested by decade, not by year (user, 2026-10-26: "only download multi year records once"): a record dated
over a range ("1964-1995") is counted by Trove in every year of the range, so a year-by-year harvest fetches it
again each year; by decade it comes once per decade it touches. The API cannot exclude ids already held (a
page always carries full records), so the rest of the de-duplication is in the cache: each work is stored once.

Cache (diskcache, DISKCACHE_DIR/trove_theses; the Bibliometric_disparities cache at /home/lc/k/era/.trove is not
used): 'page:au:{decade}:{cursor}' -> {'ids': [...], 'next': cursor or None, 'total'}; 'work:{id}' -> the whole
work record as returned (first copy kept). A rerun or an interrupted run resumes from the cache without calling
Trove for pages already held. (A first trial, 2026-10-26, harvested 2010 alone without the Australian limit:
pages 'page:2010:{cursor}', 21,058 works, all kept in the same cache.)

Output: processed/trove_theses/theses_au_{decade}0s.parquet, one row per work: work_id, title, issued,
year_start / year_end (first and last year in 'issued'), single_year (start == end), authors (the work's
contributor list), creators / contributors / types / sources (unions over versions), extent, degree (doctoral /
masters / honours / unknown from the version types and the short descriptions -- abstracts are not read),
n_versions. Token: trove_token in .env (never printed).

Usage: .venv/bin/python src/00i_extract_trove_theses.py 199 200 201 202   (decades; default 201)
"""

import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import diskcache
import pandas as pd
import requests
from dotenv import load_dotenv

from config.settings import DISKCACHE_DIR, PROCESSED_DATA

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
API = "https://api.trove.nla.gov.au/v3/result"
OUT_DIR = PROCESSED_DATA / "trove_theses"
PAUSE = 0.5
DOCTORAL = re.compile(r"ph\.?\s?d|doctor of philosophy|doctora|d\.\s?phil", re.I)
MASTERS = re.compile(r"\bmaster|\bm\.?\s?(phil|res|sc|a|ed|eng)\b\.?", re.I)
HONOURS = re.compile(r"honou?rs|bachelor|\bb\.?\s?(a|sc)\b\.?\s*\(?hons", re.I)


def fetch_page(decade: str, cursor: str) -> dict:
    params = {"category": "research", "encoding": "json", "n": 100, "q": "thesis", "l-format": "Thesis",
              "l-australian": "y", "l-decade": decade, "reclevel": "full", "include": "workversions",
              "bulkHarvest": "true", "s": cursor}
    for attempt in range(6):        # Trove is slow and sometimes times out: retry with back-off
        try:
            r = requests.get(API, params=params, headers={"X-API-KEY": os.environ["trove_token"]}, timeout=180)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()["category"][0]["records"]
        except requests.RequestException as e:
            if attempt == 5:
                raise
            print(f"  retry {attempt + 1} after {e}", flush=True)
            time.sleep(5 * 2 ** attempt)


def harvest(decade: str, cache) -> list[str]:
    """All work ids for the decade, fetching only pages not already cached; each work stored once."""
    ids, cursor, n_pages, n_new = [], "*", 0, 0
    while cursor:
        key = f"page:au:{decade}:{cursor}"
        page = cache.get(key)
        if page is None:
            rec = fetch_page(decade, cursor)
            works = rec.get("work", [])
            for w in works:
                cache.add(f"work:{w['id']}", w)     # add: keeps the copy already held
            page = {"ids": [w["id"] for w in works], "next": rec.get("nextStart") if works else None,
                    "total": rec.get("total")}
            cache.set(key, page)
            n_new += 1
            time.sleep(PAUSE)
        ids += page["ids"]
        cursor = page["next"]
        n_pages += 1
        if n_pages % 50 == 0:
            print(f"  {decade}0s: {n_pages} pages, {len(ids):,} works (of {page.get('total')}), {n_new} fetched",
                  flush=True)
    return ids


def _vals(items, key):
    out = []
    for x in items or []:
        v = x.get(key) if isinstance(x, dict) else x
        if v:
            out.append(str(v))
    return out


def work_years(w: dict) -> list[int]:
    """Years in the work's 'issued', without Trove's 1900 placeholder (2026-10-29): a catalogue date of '19uu'
    (decade unknown) is issued as 1900, so 1,020 works start at 1900 ('1900-2017', Ton Otto's 1991 ANU thesis).
    The years are those the versions were issued in (their union is the work's 'issued' range); a version's 1900 is
    dropped when its own date has a 'u' or it has no date. The versions' dc dates are not used otherwise: they are
    sparse and mix in record time stamps and subject periods. No version years: the work's 'issued'."""
    year = r"(1[89]\d\d|20\d\d)"
    ys = []
    for v in w.get("version", []) or []:
        vy = [int(y) for i in v.get("issued") or [] for y in re.findall(year, str(i))]
        dates = [d for rr in v.get("record") or [] if isinstance(rr, dict)
                 for d in _vals(((rr.get("metadata") or {}).get("dc") or {}).get("date"), "value")]
        if not dates or any("u" in d.lower() for d in dates):
            vy = [y for y in vy if y != 1900]
        ys += vy
    return ys or [int(y) for y in re.findall(year, str(w.get("issued") or ""))]


def summarise(w: dict) -> dict:
    creators, contributors, types, sources, descs = set(), set(), set(), set(), []
    for v in w.get("version", []) or []:
        for rr in v.get("record") or []:
            if not isinstance(rr, dict):
                continue
            dc = (rr.get("metadata") or {}).get("dc") or {}
            creators.update(_vals(dc.get("creator"), "name"))
            contributors.update(_vals(dc.get("contributor"), "value"))
            types.update(_vals(dc.get("type"), "value"))
            descs += [d for d in _vals(dc.get("description"), "value") if len(d) < 300]   # notes, not abstracts
            src = rr.get("metadataSource")
            if isinstance(src, dict) and src.get("value"):
                sources.add(src["value"])
    text = " | ".join(sorted(types) + descs)
    degree = ("doctoral" if DOCTORAL.search(text) else "masters" if MASTERS.search(text)
              else "honours" if HONOURS.search(text) else "unknown")
    ys = work_years(w)
    return {"work_id": w["id"], "title": w.get("title"), "issued": w.get("issued"),
            "year_start": min(ys) if ys else None, "year_end": max(ys) if ys else None,
            "single_year": bool(ys) and min(ys) == max(ys),
            "authors": list(w.get("contributor") or []), "creators": sorted(creators),
            "contributors": sorted(contributors), "types": sorted(types), "sources": sorted(sources),
            "extent": w.get("extent"), "degree": degree, "degree_text": text[:500],
            "n_versions": len(w.get("version", []) or [])}


def main():
    decades = sys.argv[1:] or ["201"]
    if not all(re.fullmatch(r"1[89]\d|20\d", d) for d in decades):
        raise SystemExit("give decades as 3 digits, e.g. 199 200 201 202")
    cache = diskcache.Cache(str(DISKCACHE_DIR / "trove_theses"), size_limit=2 ** 34)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for decade in decades:
        t = time.time()
        ids = harvest(decade, cache)
        df = pd.DataFrame([summarise(cache.get(f"work:{i}")) for i in dict.fromkeys(ids)])
        out = OUT_DIR / f"theses_au_{decade}0s.parquet"
        df.to_parquet(out, index=False)
        print(f"{decade}0s: {len(df):,} theses in {time.time() - t:.0f} s -> {out}; single year "
              f"{df.single_year.sum():,}; degree: {df.degree.value_counts().to_dict()}", flush=True)


if __name__ == "__main__":
    main()
