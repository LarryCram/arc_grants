"""
analysis/28_unlinked_composition.py -- who are the kept ACIFs the linker leaves unlinked, and what route
is left for each (2026-10-22, user: "is there anything left to do in terms of linking? report on the
composition of the unlinked ACIFs"). A report only.

Reads acifs_arc.parquet and processed/oax_link/ (accepted_links, orcid_links, orcid_unmatched,
name_decisions, works_decisions, scopus_decisions, scopus_evidence) and 00d's Scopus search summary.
Writes processed/unlinked_composition.md.
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import ACIFS_ARC, OAX_LINK_DIR, PROCESSED_DATA, SCOPUS_EXTRACT_DIR

OUT = PROCESSED_DATA / "unlinked_composition.md"


def table(s: pd.Series, total: int, label: str) -> list[str]:
    out = [f"| {label} | ACIFs | share |", "|---|---|---|"]
    for k, n in s.items():
        out.append(f"| {k} | {n:,} | {n / total:.1%} |")
    return out + [""]


def division(codes) -> str:
    c = Counter()
    for f in codes:
        c[(f["code"][:2], f.get("name"))] += 2 if f.get("is_primary") else 1
    if not c:
        return "(none)"
    (d, _), _ = c.most_common(1)[0]
    return d


DIVISIONS = {"30": "Agricultural, veterinary, food", "31": "Biological", "32": "Biomedical, clinical",
             "33": "Built environment, design", "34": "Chemical", "35": "Commerce, management",
             "36": "Creative arts, writing", "37": "Earth", "38": "Economics", "39": "Education",
             "40": "Engineering", "41": "Environmental", "42": "Health", "43": "History, heritage, archaeology",
             "44": "Human society", "45": "Indigenous", "46": "Information and computing",
             "47": "Language, communication, culture", "48": "Law", "49": "Mathematical", "50": "Philosophy, religion",
             "51": "Physical", "52": "Psychology"}


def main():
    a = pd.read_parquet(ACIFS_ARC)
    a = a[~a.excluded]
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    u = a[~a.cluster_id.isin(set(acc.cluster_id))].copy()
    n, N = len(u), len(a)
    d = {f: pd.read_parquet(OAX_LINK_DIR / f"{f}_decisions.parquet").set_index("cluster_id")
         for f in ("name", "works", "scopus")}
    u["name"] = u.cluster_id.map(d["name"].status)
    u["works"] = u.cluster_id.map(d["works"].status)
    u["scopus"] = u.cluster_id.map(d["scopus"].status)
    u["scopus_id"] = u.cluster_id.map(d["scopus"].scopus_id)
    u["n_cand"] = u.cluster_id.map(d["name"].n_candidates).fillna(0).astype(int)
    u["has_orcid"] = u.orcids.map(len) > 0
    ol = pd.read_parquet(OAX_LINK_DIR / "orcid_links.parquet")
    review = set(ol.loc[ol.status.str.startswith("review"), "cluster_id"])
    in_oax = set(ol.cluster_id)
    u["orcid_state"] = ["no ORCID" if not h else "ORCID, links only for review" if c in review
                        else "ORCID, links refused" if c in in_oax else "ORCID not in OpenAlex"
                        for c, h in zip(u.cluster_id, u.has_orcid)]
    u["div"] = [DIVISIONS.get(division(f), division(f)) for f in u.for2020_codes]
    a["div"] = [DIVISIONS.get(division(f), division(f)) for f in a.for2020_codes]

    # one route left for each ACIF, first that applies
    def route(r):
        if r.orcid_state == "ORCID, links only for review":
            return "A. hand review of a stage-1 ORCID link"
        if r.scopus == "no_profile" and isinstance(r.scopus_id, str):
            return "B. one Scopus profile, its documents not fetched (00f --targets unlinked)"
        if r.scopus in ("record_name_differs", "record_linked_elsewhere", "record_other_orcid", "several_records",
                        "weak_shared_record"):
            return f"C. Scopus bridge found a record but refused it ({r.scopus})"
        if r.name == "several_pass" or r.works in ("several_university", "several_coinvestigator"):
            return "D. 2+ OpenAlex records pass (common name, no deciding evidence)"
        if r.scopus == "several_profiles":
            return "E. 2+ Scopus profiles, no OpenAlex record passes"
        if r.name == "no_candidate" and r.works == "no_candidate":
            return "F. no same-name OpenAlex record in the Australian-context pool"
        if r.name == "no_single_institution_grant":
            return "G. no single-institution grant to test a record on, no works evidence"
        return "H. same-name records exist, none at the grant university in the grant years"
    u["route"] = [route(r) for r in u.itertuples()]

    last = pd.cut(u.last_year, [2000, 2005, 2010, 2015, 2020, 2027], labels=["2001-05", "2006-10", "2011-15", "2016-20", "2021-26"])
    last_all = pd.cut(a.last_year, [2000, 2005, 2010, 2015, 2020, 2027], labels=["2001-05", "2006-10", "2011-15", "2016-20", "2021-26"])
    L = [f"# The {n:,} unlinked ACIFs (of {N:,} kept; Oct26)", "",
         "## Route left for each (first that applies)", ""] + table(u.route.value_counts().sort_index(), n, "route")
    L += ["## When: last grant year (share of all kept ACIFs in that band left unlinked)", "",
          "| last grant | unlinked | of all kept in band |", "|---|---|---|"]
    for b, k in last.value_counts().sort_index().items():
        L.append(f"| {b} | {k:,} | {k / (last_all == b).sum():.1%} |")
    L += ["", "## Field (main FOR2020 division, primary codes counted double)", "",
          "| division | unlinked | of all kept in division |", "|---|---|---|"]
    for v, k in u["div"].value_counts().items():
        L.append(f"| {v} | {k:,} | {k / (a['div'] == v).sum():.1%} |")
    L += ["", "## Size and schemes", ""]
    L += table(u.n_grants.clip(upper=4).map(lambda x: f"{x}" if x < 4 else "4+").value_counts().sort_index(), n, "grants")
    sch = Counter(s for ss in u.schemes for s in set(ss))
    L += ["| scheme (ACIFs holding one) | ACIFs |", "|---|---|"] + [f"| {k} | {v:,} |" for k, v in sch.most_common()] + [""]
    L += ["## ORCID", ""] + table(u.orcid_state.value_counts(), n, "ORCID")
    L += ["## Linker decisions", ""] + table(u.name.value_counts(), n, "name + institution stage") \
         + table(u.works.value_counts(), n, "works-first stage") + table(u.scopus.value_counts(), n, "Scopus bridge")
    L += ["## Same-name OpenAlex records in the pool (name stage candidates)", ""] + table(
        pd.cut(u.n_cand, [-1, 0, 1, 3, 10, 10**6], labels=["0", "1", "2-3", "4-10", "11+"]).value_counts().sort_index(), n, "candidates")
    for r, sub in u.groupby("route"):
        L += [f"### {r}: examples", ""]
        for x in sub.sort_values("n_grants", ascending=False).head(8).itertuples():
            L.append(f"- {x.cluster_id} ({list(x.full_names)[0]}; {x.n_grants} grants {x.first_year}-{x.last_year}; {x.div}; "
                     f"name {x.name}, works {x.works}, Scopus {x.scopus}, candidates {x.n_cand})")
        L.append("")
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    u[["cluster_id", "route", "orcid_state", "name", "works", "scopus", "n_cand", "div", "first_year", "last_year",
       "n_grants"]].to_parquet(PROCESSED_DATA / "unlinked_composition.parquet", index=False)
    print("\n".join(L[:80]))


if __name__ == "__main__":
    main()
