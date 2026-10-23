"""
analysis/36_grant_ack_test.py -- a read-only test of grant acknowledgements as linking evidence
(2026-10-23, user: "run a tester").

Chain: ACIF -> its ARC grants -> OpenAlex awards (ARC funder F4320334704; the award's funder_award_id holds the
grant code, sometimes several: "DE140100550 and DP150101425") -> works acknowledging the award
(compact/work_awards) -> the authorships on those works -> the authors whose names fit the ACIF's
(orcid_hint_link.names_compatible, or a bare first initial matching under the same family name).

Measures:
  1 coverage   kept ACIFs with >= 1 acknowledging work, by linked / unlinked and last-grant era
  2 agreement  ACIFs linked by their ORCID: is the fitting author with most acknowledging works one of the
               ACIF's ORCID-linked records?
  3 additions  unlinked ACIFs with one clear fitting candidate; linked records by stage with no
               acknowledging works although the ACIF has 3+
Writes processed/grant_ack_test.md and processed/grant_ack_candidates.parquet.
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from config.settings import ACIFS_ARC, OAX_LINK_DIR, OPENALEX_COMPACT_DIR, OPENALEX_DIR, PROCESSED_DATA
from src.oax.orcid_hint_link import names_compatible
from src.utils.names import HumanNameParser

ARC_FUNDER = "F4320334704"
OUT = PROCESSED_DATA / "grant_ack_test.md"
P = HumanNameParser()


def fits(acif_keys, name: str, cache: dict) -> bool:
    if name not in cache:
        p = P.parse(name) if isinstance(name, str) and name else None
        cache[name] = ([], []) if p is None else (list(p.full_name_keys),
                                                  [(set(p.family_names), f) for f in [set(p.given_tokens) - set(p.middle_tokens)]
                                                   if f and all(len(t) == 1 for t in f)])
    keys, inits = cache[name]
    if names_compatible(list(acif_keys), keys):
        return True
    first = {}
    for k in acif_keys:
        if "_" in k and len(k.split("_", 1)[0]) > 1:
            first.setdefault(k.split("_", 1)[1], set()).add(k[0])
    return any(f & first.get(fam, set()) for fams, f in inits for fam in fams)


def main():
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    acifs = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "grant_codes", "full_name_keys", "last_year", "excluded"])
    acifs = acifs[~acifs.excluded]
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet").astype({"author_idx": "int64"})
    linked = set(acc.cluster_id)

    aw = con.execute(f"""SELECT id, funder_award_id FROM read_parquet('{OPENALEX_DIR}/awards.parquet')
                         WHERE funder.id LIKE '%{ARC_FUNDER}'""").df()
    aw["award_idx"] = aw.id.str.extract(r"G(\d+)")[0].astype("int64")
    aw["codes"] = aw.funder_award_id.fillna("").str.upper().map(lambda s: re.findall(r"[A-Z]{2}\d{6,9}", s))
    ag = aw.explode("codes").dropna(subset=["codes"]).rename(columns={"codes": "grant_code"})[["award_idx", "grant_code"]]
    g = acifs[["cluster_id", "grant_codes"]].explode("grant_codes").rename(columns={"grant_codes": "grant_code"})
    ga = g.merge(ag, on="grant_code")
    con.register("ga_awards", ga[["award_idx"]].drop_duplicates())
    wa = con.execute(f"""SELECT DISTINCT work_idx, award_idx FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_awards/*.parquet')
                         WHERE award_idx IN (SELECT award_idx FROM ga_awards)""").df()
    acw = ga.merge(wa, on="award_idx")[["cluster_id", "work_idx"]].drop_duplicates()
    con.register("acw_w", acw[["work_idx"]].drop_duplicates())
    au = con.execute(f"""SELECT DISTINCT work_idx, author_idx, author_name FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
                         WHERE work_idx IN (SELECT work_idx FROM acw_w)""").df()
    x = acw.merge(au, on="work_idx")
    keys = dict(zip(acifs.cluster_id, acifs.full_name_keys))
    cache = {}
    pairs = x[["cluster_id", "author_name"]].drop_duplicates()
    pairs["fit"] = [fits(keys[c], n, cache) for c, n in zip(pairs.cluster_id, pairs.author_name)]
    x = x.merge(pairs, on=["cluster_id", "author_name"])
    fx = x[x.fit]
    cand = (fx[fx.author_idx.notna()].groupby(["cluster_id", "author_idx"]).work_idx.nunique()
            .reset_index(name="ack_works").sort_values(["cluster_id", "ack_works"], ascending=[True, False]))
    cand["author_idx"] = cand.author_idx.astype("int64")
    cand.to_parquet(PROCESSED_DATA / "grant_ack_candidates.parquet", index=False)

    L = ["# Grant acknowledgements as linking evidence (test, 2026-10-23)", "",
         f"- ARC awards in OpenAlex: {len(aw):,}; matching a kept ACIF's grant: {ga.award_idx.nunique():,} awards, "
         f"{ga.grant_code.nunique():,} grants; acknowledging works: {acw.work_idx.nunique():,}", ""]
    nack = acw.groupby("cluster_id").size()
    a = acifs.assign(ack=acifs.cluster_id.map(nack).fillna(0).astype(int), linked=acifs.cluster_id.isin(linked),
                     own_fit=acifs.cluster_id.map(fx.groupby("cluster_id").work_idx.nunique()).fillna(0).astype(int))
    a["era"] = pd.cut(a.last_year, [2000, 2008, 2014, 2020, 2030], labels=["2001-08", "2009-14", "2015-20", "2021-"])
    L += ["## 1 Coverage (kept ACIFs)", "", "| | ACIFs | with >= 1 acknowledging work | with >= 1 work whose byline fits the ACIF |",
          "|---|---|---|---|"]
    for name, s in [("all", a), ("linked", a[a.linked]), ("unlinked", a[~a.linked])] + \
                   [(f"last grant {e}", a[a.era == e]) for e in a.era.cat.categories]:
        L.append(f"| {name} | {len(s):,} | {int((s.ack > 0).sum()):,} ({(s.ack > 0).mean():.0%}) | {int((s.own_fit > 0).sum()):,} ({(s.own_fit > 0).mean():.0%}) |")

    orc = acc[acc.stage == "orcid"]
    ok = orc.groupby("cluster_id").author_idx.apply(set).to_dict()
    top = cand.groupby("cluster_id").head(2).groupby("cluster_id").agg(top=("author_idx", "first"), n1=("ack_works", "first"),
                                                                       n2=("ack_works", lambda s: s.iloc[1] if len(s) > 1 else 0))
    top["unique"] = top.n1 > top.n2
    k = top[top.index.isin(ok)]
    k = k.assign(right=[t in ok[c] for c, t in zip(k.index, k.top)])
    ku = k[k.unique]
    L += ["", "## 2 Agreement on ORCID-linked ACIFs", "",
          f"- ACIFs linked by ORCID with a fitting author on acknowledging works: {len(k):,}; top author one of the ACIF's "
          f"ORCID records: {int(k.right.sum()):,} ({k.right.mean():.1%})",
          f"- when the top author is unique (more acknowledging works than the next): {len(ku):,}; right {int(ku.right.sum()):,} "
          f"({ku.right.mean():.1%})"]
    for m in (2, 3, 5):
        s = ku[ku.n1 >= m]
        L.append(f"- unique top with >= {m} acknowledging works: {len(s):,}; right {int(s.right.sum()):,} ({s.right.mean():.1%})")

    un = top[~top.index.isin(linked) & top.unique]
    L += ["", "## 3 What it would add", "",
          f"- unlinked ACIFs with a unique top fitting author: {len(un):,} (>= 2 works: {int((un.n1 >= 2).sum()):,}; "
          f">= 3: {int((un.n1 >= 3).sum()):,})", "",
          "Linked records with no acknowledging works although the ACIF has 3+ acknowledging works with a fitting byline:", "",
          "| stage | linked records (ACIF has 3+ fitting acknowledging works) | records with none of them |", "|---|---|---|"]
    has = set(zip(cand.cluster_id, cand.author_idx))
    rich = set(a[a.own_fit >= 3].cluster_id)
    for st, s in acc[acc.cluster_id.isin(rich)].groupby("stage"):
        miss = [(c, int(r)) not in has for c, r in zip(s.cluster_id, s.author_idx)]
        L.append(f"| {st} | {len(s):,} | {sum(miss):,} ({sum(miss) / max(len(s), 1):.0%}) |")
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))


if __name__ == "__main__":
    main()
