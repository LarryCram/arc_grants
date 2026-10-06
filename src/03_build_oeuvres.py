"""
src/03_build_oeuvres.py -- the oeuvre extractor (2026-10-06): works of each ACIF from its accepted
OpenAlex author records (linker stage 1). Built one step at a time; each step writes its table to
OEUVRE_DIR and a section of report.md. Plan: /home/lc/.claude/plans/sunny-sauteeing-peach.md.

Steps so far:
  1. records.parquet      accepted author records per ACIF (src/oeuvre/records.py)
  2. authorships.parquet  every authorship of those records, full OpenAlex (src/oeuvre/authorships.py)

Usage: .venv/bin/python src/03_build_oeuvres.py
"""

import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import OEUVRE_DIR
from src.oeuvre.authorships import connect, pull_authorships
from src.oeuvre.records import load_records


def records_section(r: pd.DataFrame) -> list[str]:
    per = r.groupby("cluster_id").size()
    L = ["## Step 1: author records per ACIF", "",
         f"- ACIFs: {r.cluster_id.nunique():,}; author records: {r.author_idx.nunique():,} "
         f"({len(r):,} ACIF-record rows); outside the HEP-context pool: {int((~r.in_pool).sum()):,}",
         f"- works on the records (OpenAlex works_count): {int(r.works_count_global.sum()):,}", "",
         "| records per ACIF | ACIFs |", "|---|---|"]
    for k, v in sorted(Counter(per.clip(upper=6)).items()):
        L.append(f"| {k if k < 6 else '6+'} | {v:,} |")
    L += ["", "Link status of the records: " + ", ".join(f"{k} {v:,}" for k, v in r.status.value_counts().items()), ""]
    return L


def authorships_section(a: pd.DataFrame, r: pd.DataFrame, seconds: float) -> list[str]:
    w = a.drop_duplicates(["cluster_id", "author_idx", "work_idx"])
    per_rec = w.groupby(["cluster_id", "author_idx"]).size().rename("n").reset_index().merge(r, on=["cluster_id", "author_idx"])
    per_acif = w.drop_duplicates(["cluster_id", "work_idx"]).groupby("cluster_id").size()
    shared = w.groupby(["cluster_id", "work_idx"]).author_idx.nunique()
    no_rows = set(zip(r.cluster_id, r.author_idx)) - set(zip(per_rec.cluster_id, per_rec.author_idx))
    gap = (per_rec.n - per_rec.works_count_global)
    L = ["## Step 2: authorships (full OpenAlex)", "",
         f"- pulled in {seconds:,.0f} s: {len(a):,} rows (one per institution on an authorship); "
         f"{len(w):,} (ACIF, record, work) authorships; {w.work_idx.nunique():,} distinct works",
         f"- records with no authorship at all: {len(no_rows):,}",
         f"- works reached through 2+ records of the same ACIF: {int((shared > 1).sum()):,}",
         f"- authorships with no institution: {int(a.institution_idx.isna().sum()):,}; "
         f"with an Australian institution: {int((a.country_code == 'AU').sum()):,} rows", "",
         "Works per ACIF (distinct): " + ", ".join(f"p{q}: {per_acif.quantile(q / 100):,.0f}" for q in (10, 50, 90, 99))
         + f", max {per_acif.max():,} ({per_acif.idxmax()})", "",
         "Authorships per record vs OpenAlex's works_count (pulled minus counted): "
         + ", ".join(f"p{q}: {gap.quantile(q / 100):,.0f}" for q in (1, 10, 50, 90, 99)), ""]
    big = per_rec.assign(d=gap.abs()).sort_values("d", ascending=False).head(8)
    L += ["Largest differences from works_count:", ""]
    L += [f"- {x.cluster_id} A{x.author_idx} {x.author_name}: pulled {x.n:,}, works_count {x.works_count_global:,}"
          for x in big.itertuples()]
    return L + [""]


def main():
    OEUVRE_DIR.mkdir(parents=True, exist_ok=True)
    r = load_records()
    r.to_parquet(OEUVRE_DIR / "records.parquet", index=False)
    t = time.time()
    pull_authorships(r, OEUVRE_DIR / "authorships.parquet", connect())
    secs = time.time() - t
    a = pd.read_parquet(OEUVRE_DIR / "authorships.parquet")
    text = "\n".join(["# Oeuvres", ""] + records_section(r) + authorships_section(a, r, secs))
    (OEUVRE_DIR / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
