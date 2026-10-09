"""
analysis/22_overall_status.py -- where the build stands (2026-10-09, user: "where are we overall? ...
arc names in uncertain arc acifs, ... acifs without works, especially in terms of years and fields").
Reads only persisted outputs (01, 02, 03); writes processed/status_report.md.

Sections:
  1. Funnel: ARC records -> ACIFs -> linked (by linker stage) -> works (accept / reject / unsure).
  2. Uncertain ARC ACIFs: the name groups the ACIF build did not merge (flagged; partly merged;
     ORCID veto with no-ORCID parts), their names, and what the OpenAlex links now say -- whether
     unmerged parts of one group were linked to the same OpenAlex record.
  3. ACIFs without accepted works: why (not linked / linked, no works / works but none accepted),
     by last grant year and by main FOR2020 division, against all kept ACIFs.

Usage: .venv/bin/python analysis/22_overall_status.py
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import ACIFS_ARC, OAX_LINK_DIR, OEUVRE_DIR, PROCESSED_DATA
from src.utils.for_resolve import Resolver

OUT = PROCESSED_DATA / "status_report.md"
_res = Resolver()


def division_name(code: str) -> str:
    try:
        return _res.resolve(code, "FOR2020", "FOR2020").label.capitalize()
    except Exception:
        return code


def main_division(codes) -> str | None:
    """The most frequent 2-digit division among the ACIF's primary FOR2020 codes (else all codes)."""
    codes = list(codes) if codes is not None else []
    prim = [c["code"][:2] for c in codes if c.get("is_primary")] or [c["code"][:2] for c in codes]
    return Counter(prim).most_common(1)[0][0] if prim else None


def year_band(y) -> str:
    if pd.isna(y):
        return "unknown"
    y = int(y)
    return "2001-2005" if y <= 2005 else "2006-2010" if y <= 2010 else "2011-2015" if y <= 2015 else \
        "2016-2020" if y <= 2020 else "2021-2026"


def table(df: pd.DataFrame) -> list[str]:
    cols = list(df.columns)
    fmt = {c: (lambda v: f"{v:.1%}") if df[c].dtype.kind == "f" else (lambda v: f"{int(v):,}") for c in cols}
    out = ["| " + " | ".join([df.index.name or ""] + cols) + " |", "|" + "---|" * (len(cols) + 1)]
    for i, r in df.iterrows():
        out.append("| " + " | ".join([str(i)] + [fmt[c](r[c]) for c in cols]) + " |")
    return out


def main():
    a = pd.read_parquet(ACIFS_ARC)
    kept = a[~a.excluded].copy()
    kept["division"] = kept.for2020_codes.map(main_division)
    kept["band"] = kept.last_year.map(year_band)
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    stage = acc.groupby("cluster_id").stage.agg(lambda s: "orcid" if "orcid" in set(s) else s.iloc[0])
    kept["link_stage"] = kept.cluster_id.map(stage).fillna("not linked")
    k = pd.read_parquet(OEUVRE_DIR / "acif_works_classified.parquet", columns=["cluster_id", "decision", "rule"])
    per = k.groupby(["cluster_id", "decision"]).size().unstack(fill_value=0)
    for d in ("accept", "reject", "unsure"):
        kept[d] = kept.cluster_id.map(per[d] if d in per else {}).fillna(0).astype(int)
    kept["works"] = kept.accept + kept.reject + kept.unsure

    # unlinked reasons: last decision row per ACIF
    sdec = pd.read_parquet(OAX_LINK_DIR / "scopus_decisions.parquet").set_index("cluster_id").status
    kept["why_unlinked"] = kept.cluster_id.map(sdec).where(kept.link_stage == "not linked")

    L = ["# Status of the ARC -> OpenAlex build", "",
         "## 1. Funnel", "",
         f"- ARC records in scope: {int(a.n_records.sum()):,}; ACIFs (people): {len(a):,}; set aside (Indigenous research): "
         f"{int(a.excluded.sum()):,}; kept: {len(kept):,}",
         f"- with an ORCID (ARC, Scopus, hand, ORCID bulk): {int((kept.orcids.map(len) > 0).sum()):,}", ""]
    t = kept.groupby("link_stage").agg(acifs=("cluster_id", "size"), accept=("accept", "sum"), reject=("reject", "sum"),
                                       unsure=("unsure", "sum"))
    t = t.reindex(["orcid", "name", "works", "scopus", "not linked"])
    t.index.name = "linked by"
    L += table(t) + ["", "Not linked, by the last step's verdict (Scopus DOI bridge):", ""]
    L += [f"- {s}: {n:,}" for s, n in kept.loc[kept.link_stage == "not linked", "why_unlinked"].fillna("(not reached)").value_counts().items()]

    # 2. uncertain ARC ACIFs
    g = pd.read_parquet(PROCESSED_DATA / "acif_arc_name_groups.parquet")
    rec = pd.read_parquet(PROCESSED_DATA / "acif_arc_records.parquet", columns=["unique_id", "cluster_id"])
    rec_map = dict(zip(rec.unique_id, rec.cluster_id))
    links = acc.groupby("cluster_id").author_idx.apply(set).to_dict()
    unc = g[g.status.isin(["flagged", "partial", "orcid_veto", "kept_apart"])].copy()
    rows = []
    for r in unc.itertuples():
        acifs = sorted({rec_map.get(p, p) for p in r.parts})       # parts are ACIF ids of the name stage's input
        acifs = sorted({c for c in acifs if c in set(kept.cluster_id)})
        recs = [links.get(c, set()) for c in acifs]
        linked = [x for x in recs if x]
        shared = any(recs[i] & recs[j] for i in range(len(recs)) for j in range(i + 1, len(recs)))
        rows.append((r.first_id, r.status, len(acifs), len(linked), shared, ", ".join(r.names), list(r.flags) if r.flags is not None else []))
    u = pd.DataFrame(rows, columns=["group", "status", "acifs", "acifs_linked", "share_a_record", "names", "flags"])
    u = u[u.acifs > 1]
    L += ["", "## 2. Uncertain ARC ACIFs (name groups the build did not fully merge)", "",
          "A name group is ACIFs sharing a main name (first given + family). The build merges a group only when no "
          "check flags it; it never merges across two different ORCIDs. Below: groups still split into 2+ kept ACIFs.", "",
          "| status | groups | ACIFs | groups with all parts linked to OpenAlex | groups where 2+ parts link to the same OpenAlex record |",
          "|---|---|---|---|---|"]
    for st, sub in u.groupby("status"):
        L.append(f"| {st} | {len(sub):,} | {int(sub.acifs.sum()):,} | {int((sub.acifs_linked == sub.acifs).sum()):,} | "
                 f"{int(sub.share_a_record.sum()):,} |")
    fl = Counter(f for fs in u["flags"] for f in fs)
    L += ["", "Flags raised (a group can have several): " + ", ".join(f"{k} {v:,}" for k, v in fl.most_common()), "",
          "Most frequent names among these groups (ACIFs):", ""]
    nm = u.groupby("names").acifs.sum().sort_values(ascending=False)
    L.append(", ".join(f"{n} ({c})" for n, c in nm.head(30).items()))
    L += ["", "Groups where 2+ unmerged ACIFs link to the same OpenAlex record (OpenAlex sees one person -- or has merged two):", ""]
    for r in u[u.share_a_record].itertuples():
        L.append(f"- {r.names} ({r.status}; {r.acifs} ACIFs; flags {', '.join(r.flags) or '-'})")

    # 3. ACIFs without accepted works
    kept["works_state"] = [("not linked" if s == "not linked" else "linked, no works" if w == 0
                            else "works, none accepted" if acc_ == 0 else "has accepted works")
                           for s, w, acc_ in zip(kept.link_stage, kept.works, kept.accept)]
    no = kept[kept.works_state != "has accepted works"]
    L += ["", "## 3. ACIFs without accepted works", "",
          f"- kept ACIFs without an accepted work: {len(no):,} of {len(kept):,} ({len(no) / len(kept):.1%})", ""]
    L += [f"- {s}: {n:,}" for s, n in no.works_state.value_counts().items()]
    L += ["", "By last grant year (share = of all kept ACIFs in that band):", ""]
    t = kept.groupby("band").agg(acifs=("cluster_id", "size"),
                                 not_linked=("works_state", lambda s: int((s == "not linked").sum())),
                                 no_accepted_works=("works_state", lambda s: int((s != "has accepted works").sum())))
    t["share_without"] = t.no_accepted_works / t.acifs
    t.index.name = "last grant"
    L += table(t)
    t = kept.groupby("division").agg(acifs=("cluster_id", "size"),
                                     not_linked=("works_state", lambda s: int((s == "not linked").sum())),
                                     no_accepted_works=("works_state", lambda s: int((s != "has accepted works").sum())))
    t["share_without"] = t.no_accepted_works / t.acifs
    t = t.sort_values("share_without", ascending=False)
    t.index = [f"{d} {division_name(d)}" for d in t.index]
    t.index.name = "main FOR2020 division"
    L += ["", "By main FOR2020 division (most frequent division among the ACIF's primary codes):", ""] + table(t)
    old = kept[kept.last_year <= 2010]
    t = old.groupby("division").agg(acifs=("cluster_id", "size"),
                                    no_accepted_works=("works_state", lambda s: int((s != "has accepted works").sum())))
    t["share_without"] = t.no_accepted_works / t.acifs
    t = t[t.acifs >= 50].sort_values("share_without", ascending=False).head(8)
    t.index = [f"{d} {division_name(d)}" for d in t.index]
    t.index.name = "division, last grant <= 2010"
    L += ["", "Highest shares without works among ACIFs whose last grant was 2010 or earlier (divisions with 50+):", ""] + table(t)
    L += ["", "Unsure works per linked ACIF (works that need a decision):", "",
          f"- ACIFs with 1+ unsure work: {int((kept.unsure > 0).sum()):,}; with 10+: {int((kept.unsure >= 10).sum()):,}; "
          f"unsure works {int(kept.unsure.sum()):,}"]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
