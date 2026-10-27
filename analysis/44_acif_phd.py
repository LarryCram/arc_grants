"""
analysis/44_acif_phd.py -- one PhD record per kept ACIF (2026-10-27, user: "OK and leave the phd year None when we don't
know it"), from the evidence analysis/43_trove_phd_match.py gathers. An analysis table, not a pipeline stage; run after
analysis/43.

The first source that has the fact wins, in this order:
  hand              data_persisted/manual_phd.csv, status confirmed
  orcid             a doctoral education entry with an end year on the ACIF's ORCID record (earliest)
  trove             the thesis analysis/43 accepted (Australian by construction)
  oax_dissertation  an OpenAlex dissertation on the person's own accepted or unsure works with an institution (earliest)
  hand_probable     data_persisted/manual_phd.csv, status probable
  early_share       no other source: the early Australian share (analysis/43) is 0 -> overseas_probable, 1 ->
                    australian_probable; country only
phd_year is None unless a source gives it -- no estimate from the publication record (user).
status: australian / overseas (a source with a country), australian_probable / overseas_probable, unknown.
year_conflict: two dated sources (hand, ORCID, Trove, OpenAlex dissertation) more than a year apart.

Writes processed/trove_theses/acif_phd.parquet and acif_phd.md.
"""

import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

m = importlib.import_module("analysis.43_trove_phd_match")
OUT = m.TH_DIR / "acif_phd.parquet"
REPORT = m.TH_DIR / "acif_phd.md"
STATUS_ORDER = ["australian", "overseas", "australian_probable", "overseas_probable", "unknown"]


def _year(v):
    return None if v is None or pd.isna(v) else int(v)


def main():
    a = m.acifs()
    _, _, early = m.own_work_heps(m.hep_names())
    a["early_au_share"] = a.cluster_id.map(early)
    th = pd.read_parquet(m.OUT)
    acc = th[th.accepted].drop_duplicates("cluster_id").set_index("cluster_id")
    ot = m.own_titles()
    diss = ot[(ot.type == "dissertation") & ot.countries.map(len).gt(0)].sort_values("year").drop_duplicates("cluster_id")
    diss = diss.set_index("cluster_id")

    rows = []
    for r in a.itertuples():
        c = r.cluster_id
        cand = []   # (source, year, university, country, title)
        if r.hand_phd_status == "confirmed":
            cand.append(("hand", _year(r.hand_phd_year), r.hand_phd_university, r.hand_phd_country, r.hand_phd_thesis_title))
        if not pd.isna(r.orcid_phd_year):
            cand.append(("orcid", _year(r.orcid_phd_year), r.orcid_phd_org, r.orcid_phd_country, None))
        if c in acc.index:
            t = acc.loc[c]
            cand.append(("trove", _year(t.year), t.uni if isinstance(t.uni, str) else None, "AU", t.title))
        if c in diss.index:
            d = diss.loc[c]
            ctry = list(d.countries)
            cand.append(("oax_dissertation", _year(d.year), None, "AU" if "AU" in ctry else ctry[0], d.title))
        if r.hand_phd_status == "probable":
            cand.append(("hand_probable", _year(r.hand_phd_year), r.hand_phd_university,
                         r.hand_phd_country if isinstance(r.hand_phd_country, str) else None, None))
        years = [y for s, y, *_ in cand if y is not None and s != "hand_probable"]
        conflict = bool(years) and max(years) - min(years) > 1
        first = lambda i: next((x[i] for x in cand if x[i] is not None and not (isinstance(x[i], float) and pd.isna(x[i]))), None)
        src = cand[0][0] if cand else None
        year, uni, country, title = first(1), first(2), first(3), first(4)
        if src in ("hand", "orcid", "trove", "oax_dissertation"):
            status = "australian" if country == "AU" else "overseas" if country else "unknown"
        elif src == "hand_probable":
            status = "overseas_probable" if r.hand_phd_abroad == "yes" else "australian_probable"
        elif r.early_au_share == 0:
            src, status = "early_share", "overseas_probable"
        elif r.early_au_share == 1:
            src, status = "early_share", "australian_probable"
        else:
            status = "unknown"
        rows.append((c, r.group, status, src, year, uni, country, title, len(cand), conflict, r.early_au_share,
                     "; ".join(f"{s}:{y}" for s, y, *_ in cand)))
    p = pd.DataFrame(rows, columns=["cluster_id", "group", "status", "source", "phd_year", "phd_university",
                                    "phd_country", "phd_title", "n_sources", "year_conflict", "early_au_share",
                                    "sources"])
    p["phd_year"] = p.phd_year.astype("Int64")
    p.to_parquet(OUT, index=False)

    L = ["# PhD per ACIF", "", f"Kept ACIFs {len(p):,}. phd_year is left empty when no source gives it.", "",
         "## Status by group", "", "| group | ACIFs | " + " | ".join(STATUS_ORDER) + " | year known |",
         "|---|---|" + "---|" * (len(STATUS_ORDER) + 1)]
    for g, x in list(p.groupby("group")) + [("all", p)]:
        vc = x.status.value_counts()
        L.append(f"| {g} | {len(x):,} | " + " | ".join(f"{vc.get(s, 0):,} ({vc.get(s, 0) / len(x):.0%})" for s in STATUS_ORDER)
                 + f" | {x.phd_year.notna().sum():,} ({x.phd_year.notna().mean():.0%}) |")
    L += ["", "## Source of the record", "", "| source | ACIFs | with year |", "|---|---|---|"]
    for s, x in p.groupby(p.source.fillna("none")):
        L.append(f"| {s} | {len(x):,} | {x.phd_year.notna().sum():,} |")
    L += ["", f"Dated sources more than a year apart: {int(p.year_conflict.sum()):,} ACIFs (the first source's year is used).",
          "", "## PhD year relative to the first fellowship announcement (fellows with a year)", ""]
    fa = a.set_index("cluster_id").first_fellow_award
    q = p[p.phd_year.notna()].assign(rel=lambda d: d.phd_year - d.cluster_id.map(fa))
    q = q[q.rel.notna()]
    L += ["| group | n | median | 10% | 90% |", "|---|---|---|---|---|"]
    for g, x in q.groupby("group"):
        L.append(f"| {g} | {len(x):,} | {x.rel.median():.0f} | {x.rel.quantile(.1):.0f} | {x.rel.quantile(.9):.0f} |")
    REPORT.write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
