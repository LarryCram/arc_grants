"""
analysis/30_trove_trial.py -- a first trial of the National Library of Australia's Trove API as a source of
works (mostly books) for ACIFs OpenAlex leaves unlinked (2026-10-22, user: "run a test using 8 of the
earliest unlinked ACIFs as name searches in Trove; show me the distilled response").

Per ACIF: one search of category 'book' (Books & Libraries) for the FULL name (user, 2026-10-22: a
family-name search buries the ARC person in the haystack): creator:"Family, Given" OR creator:"Given
Family" for every full given name of the ACIF's name keys, up to MAX_RECORDS records (cached as JSON in
processed/trove_cache/, so a rerun calls nothing). Each record's contributors are parsed with NameParser;
a record is a name match when one contributor shares a full_name_key with a full given name (len > 1) with
the ACIF -- initial-only keys don't count. Then two plain filters, reported
side by side, not applied as decisions: the record's year within the career window (first grant - 25 ..
last grant + 15) and the record's subjects against the ACIF's FOR2020 field names (shared word).
Writes processed/trove_trial.md. Token: trove_token in .env (never printed).

Usage: .venv/bin/python analysis/30_trove_trial.py [N]   (N earliest unlinked ACIFs, default 8)
"""

import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import requests
from dotenv import load_dotenv

from config.settings import ACIFS_ARC, PROCESSED_DATA
from src.utils.names import HumanNameParser

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
API = "https://api.trove.nla.gov.au/v3/result"
CACHE = PROCESSED_DATA / "trove_cache"
OUT = PROCESSED_DATA / "trove_trial.md"
MAX_RECORDS = 300
STOP = {"and", "of", "the", "in", "for", "studies", "other", "excl", "not", "elsewhere", "classified", "sciences",
        "science", "history", "general"}


def search(q: str) -> list[dict]:
    CACHE.mkdir(parents=True, exist_ok=True)
    f = CACHE / f"book_{re.sub(r'[^a-z0-9]+', '_', q.lower())[:150]}.json"
    if f.exists():
        return json.loads(f.read_text())
    out, start = [], "*"
    while len(out) < MAX_RECORDS:
        for attempt in range(5):        # Trove is slow and sometimes times out: retry with back-off
            try:
                r = requests.get(API, params={"category": "book", "q": q, "encoding": "json", "n": 100,
                                              "s": start, "reclevel": "full", "bulkHarvest": "true"},
                                 headers={"X-API-KEY": os.environ["trove_token"]}, timeout=90)
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                break
            except requests.RequestException:
                if attempt == 4:
                    raise
                time.sleep(5 * 2 ** attempt)
        r.raise_for_status()
        rec = r.json()["category"][0]["records"]
        out += rec.get("work", [])
        start = rec.get("nextStart")
        if not start or not rec.get("work"):
            break
        time.sleep(0.5)
    f.write_text(json.dumps(out))
    return out


def words(s) -> set[str]:
    return {w for w in re.findall(r"[a-z]+", (s or "").lower()) if len(w) > 3 and w not in STOP}


def year(issued) -> int | None:
    m = re.search(r"(1[89]\d\d|20\d\d)", str(issued or ""))
    return int(m.group(1)) if m else None


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    P = PROCESSED_DATA
    u = pd.read_parquet(P / "unlinked_composition.parquet").sort_values(["last_year", "first_year", "cluster_id"]).head(n)
    a = pd.read_parquet(ACIFS_ARC).set_index("cluster_id")
    g = pd.read_parquet(P / "grants_flat.parquet", columns=["grant_code", "grant_summary", "admin_org"]).set_index("grant_code")
    parser = HumanNameParser()
    L = [f"# Trove trial: {n} earliest unlinked ACIFs (category 'book', creator = full name)", ""]
    for r in u.itertuples():
        x = a.loc[r.cluster_id]
        keys = {k for k in x.full_name_keys if len(k.split("_", 1)[0]) > 1}
        family = parser.parse(list(x.full_names)[0]).family_name_main or list(x.full_names)[0].split()[-1]
        fam_raw = list(x.full_names)[0].split()[-1]
        givens = sorted({k.split("_", 1)[0].capitalize() for k in keys})
        q = " OR ".join(f'creator:"{fam_raw}, {gv}" OR creator:"{gv} {fam_raw}"' for gv in givens)
        fields = {f["name"] for f in x.for2020_codes}
        fw = set().union(*(words(f) for f in fields)) if fields else set()
        recs = search(q)
        lo, hi = int(r.first_year) - 25, int(r.last_year) + 15
        rows = []
        for w in recs:
            cs = w.get("contributor") or []
            hit = None
            for c in cs:
                k = set(parser.parse(c).full_name_keys)
                if k & keys:
                    hit = c
                    break
            if not hit:
                continue
            y = year(w.get("issued"))
            subj = w.get("subject") or []
            rows.append({"title": re.sub(r"\s*/\s*[^/]*$", "", w.get("title", ""))[:110], "contributor": hit, "year": y,
                         "type": "/".join((w.get("type") or [])[:1]), "in_window": y is not None and lo <= y <= hi,
                         "field_word": bool(fw & set().union(*(words(s) for s in subj))) if subj else None,
                         "subjects": "; ".join(subj[:2])[:90], "holdings": w.get("holdingsCount", 0)})
        d = pd.DataFrame(rows)
        grants = "; ".join(f"{gc} {int(r.first_year) if gc else ''} {g.loc[gc, 'admin_org'] if gc in g.index else ''}" for gc in x.grant_codes)
        summ = g.loc[x.grant_codes[0], "grant_summary"][:220] if x.grant_codes[0] in g.index else ""
        L += [f"## {list(x.full_names)[0]} -- {r.cluster_id}", "",
              f"- ARC: {grants}; fields: {', '.join(sorted(fields))}",
              f"- grant: {summ}...",
              f"- Trove query `{q}`: {len(recs):,} records{' (capped)' if len(recs) >= MAX_RECORDS else ''}; "
              f"a contributor matching the ARC name: {len(d)}; of these in the career window {lo}-{hi}: "
              f"{int(d.in_window.sum()) if len(d) else 0}; subject shares a word with the ARC fields: "
              f"{int((d.field_word == True).sum()) if len(d) else 0}", ""]
        if len(d):
            d = d.sort_values(["in_window", "field_word", "year"], ascending=[False, False, True])
            L += ["| year | title | as | type | window | field word | subjects |", "|---|---|---|---|---|---|---|"]
            for t in d.head(12).itertuples():
                L.append(f"| {t.year or ''} | {t.title} | {t.contributor} | {t.type} | {'yes' if t.in_window else 'no'} | "
                         f"{'yes' if t.field_word else '-' if t.field_word is None else 'no'} | {t.subjects} |")
            if len(d) > 12:
                L.append(f"| | ... {len(d) - 12} more | | | | | |")
        L.append("")
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
