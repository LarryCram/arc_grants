"""
src/02_link_arc_oax.py -- the ARC<->OpenAlex linker of the rebuild (2026-10-06), built one stage
at a time; each stage writes its own table and report section so its effect can be seen alone.

Stages so far:
  1. ORCID links (src/oax/orcid_link.py): every OpenAlex author carrying the ACIF's ORCID -- in
     the HEP-context pool, or, for an ORCID no pool author carries, anywhere in OpenAlex
     (in_pool=False) -- less the links refused in data_persisted/oax_link_overrides.csv; each
     link gets a name_relation and a status (accept_* / review_* / reject_unrelated; see
     src/oax/orcid_link.py).
  2. Name + institution-in-time links (src/oax/name_link.py), for kept ACIFs stage 1 did not link:
     candidates sharing a full given name (then, only if none passes, a not-incompatible name) whose
     OpenAlex affiliations show 2+ years at a single-institution grant university near the grant;
     accepted when exactly one passes.
  3. Works-first links (src/oax/works_link.py), for ACIFs stage 2 left open: the same candidates'
     works, linked on co-investigator co-authorship or works at a grant university in grant years.

Inputs: acifs_arc.parquet (src/01_build_arc_acifs.py), openalex_authors_prep.parquet (00b).
Outputs (OAX_LINK_DIR = processed/oax_link/):
    orcid_links.parquet       one row per (ACIF, author) sharing an ORCID: in_pool, name_relation, status
    orcid_rejected.parquet    links refused by hand (oax_link_overrides.csv), with the reason
    orcid_unmatched.parquet   ACIFs with an ORCID and no candidate link
    name_links.parquet        stage 2: every name candidate with tier, years at a grant university, passes
    name_decisions.parquet    stage 2: one row per ACIF not linked by stage 1 (status, tier, author_idx)
    works_evidence.parquet    works-first: every candidate with its works / co-investigator / university counts
    works_decisions.parquet   works-first: one row per ACIF left open by stage 2
    works_links.parquet       works-first: accepted (ACIF, author) links
    calibration_works_evidence.parquet   (--calibrate) works-first evidence on ORCID-linked ACIFs, with
                              `correct` = the record is the ACIF's accepted ORCID link
    report.md                 statistics and examples per stage

Usage: .venv/bin/python src/02_link_arc_oax.py
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from config.settings import ACIFS_ARC, DUCKDB_TMP_DIR, OAX_LINK_DIR
from src.oax.name_link import (MIN_YEARS, WINDOW_AFTER, WINDOW_BEFORE, acif_main_keys, calibrate, name_links,
                                single_institution_windows)
from src.oax import works_link as wl
from src.oax.orcid_link import (MINOR_SHARE, apply_overrides, decide, load_acifs, load_authors,
                                load_name_evidence, load_outside_authors, load_overrides, orcid_links,
                                shared_orcids, unmatched)


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
    for st in ("review_initial_only", "review_unrelated", "review_given_name", "reject_orcid_names",
               "reject_unrelated", "accept_hand"):
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


def calibration_section(cal: dict) -> list[str]:
    n = cal["acifs"]
    return ["### Stage 2 calibration on ORCID-linked ACIFs (true record known, nothing taken)", "",
            f"- testable ACIFs: {n:,}; only the correct record passes {cal['only_correct'] / n:.1%}; correct and another "
            f"{cal['correct_and_other'] / n:.1%}; only others {cal['only_others'] / n:.1%}",
            f"- accepted by the rule: {cal['accepted']:,}; of which the ORCID-linked record: {cal['accepted_correct']:,} "
            f"({cal['accepted_correct'] / max(cal['accepted'], 1):.1%})", ""]


def works_section(acifs, ev, wdec, wlinks, cal) -> list[str]:
    a = acifs.set_index("cluster_id")
    d = wdec.join(a[["full_names", "last_year"]], on="cluster_id")
    L = ["## Stage 2, works-first: co-investigator and university anchors", "",
         f"Rule: the one candidate record with the most works co-authored by a linked OpenAlex author of one of the "
         f"ACIF's ARC co-investigators is linked, when it has >= {wl.MIN_COINV_WORKS} such works and no other record ties "
         f"it; otherwise a record is linked when it is the only one with works at an administering university of one "
         f"of the ACIF's grants in >= {wl.MIN_UNI_YEARS} distinct years, from {wl.WINDOW_BEFORE} year before the grant "
         f"to {wl.WINDOW_AFTER} after its funded years.", "",
         f"- ACIFs left open by the first kind: {len(wdec):,}; candidate records: {len(ev):,}",
         f"- **linked ACIFs: {int(wdec.status.str.startswith('accept').sum()):,}** ({len(wlinks):,} records)", "",
         "| status | ACIFs | last grant < 2015 | >= 2015 | records linked |", "|---|---|---|---|---|"]
    for st, sub in d.groupby("status"):
        L.append(f"| {st} | {len(sub):,} | {int((sub.last_year < 2015).sum()):,} | {int((sub.last_year >= 2015).sum()):,} | "
                 f"{int(sub.n_linked.sum()):,} |")
    if cal:
        L += ["", "Calibration on ORCID-linked ACIFs (true records known, nothing taken):", ""]
        for st, v in cal.items():
            if st != "decisions":
                L.append(f"- {st}: {v['acifs']:,} ACIFs, {v['records']:,} records linked, {v['correct_records']:,} of them the "
                         f"ORCID-linked record ({v['correct_records'] / max(v['records'], 1):.1%}); ACIFs whose correct record is "
                         f"among those linked {v['acifs_with_correct'] / max(v['acifs'], 1):.1%}")
        L.append(f"- decisions: {cal['decisions']}")
    e = ev.set_index(["cluster_id", "author_idx"])
    def ex(st, n):
        out = []
        sub = d[d.status == st]
        for r in sub.sample(min(n, len(sub)), random_state=5).itertuples():
            rows = wlinks[wlinks.cluster_id == r.cluster_id] if st.startswith("accept") else \
                ev[(ev.cluster_id == r.cluster_id) & (ev.uni_years >= wl.MIN_UNI_YEARS)]
            out.append(f"- {r.cluster_id} ({', '.join(r.full_names)}): " + "; ".join(
                f"A{int(x.author_idx)} [{e.loc[(r.cluster_id, x.author_idx), 'n_works']} works, "
                f"{e.loc[(r.cluster_id, x.author_idx), 'coinv_works']} co-inv, {e.loc[(r.cluster_id, x.author_idx), 'uni_years']} uni yrs]"
                for x in rows.itertuples()))
        return out
    for st in ("accept_coinvestigator", "accept_university", "several_university"):
        L += ["", f"Examples, {st}:", ""] + ex(st, 10)
    return L + [""]


def name_section(acifs, pairs, dec) -> list[str]:
    a = acifs.set_index("cluster_id")
    d = dec.join(a[["full_names", "last_year", "full_name_keys"]], on="cluster_id")
    d["era"] = d.last_year.map(lambda y: "last grant < 2015" if y < 2015 else "last grant >= 2015")
    d["initial_only"] = d.full_name_keys.map(lambda ks: all(len(k.split("_", 1)[0]) <= 1 for k in ks))
    L = ["## Stage 2: name + institution-in-time links", "",
         f"Rule: a candidate shares a main name (first given + family) with the ACIF (or, only if no such candidate "
         f"passes, a not-incompatible first given name: equal, or one an initial of the other); it passes when its OpenAlex affiliations show >= {MIN_YEARS} distinct years at a "
         f"single-institution grant university, from {WINDOW_BEFORE} year before to {WINDOW_AFTER} after the "
         f"grant's commencement; the ACIF is linked when exactly one candidate passes.", "",
         f"- ACIFs not linked by stage 1: {len(dec):,}; candidate pairs: {len(pairs):,}; passing: {int(pairs.passes.sum()):,}",
         f"- **accepted: {int((dec.status == 'accept').sum()):,}** (full name {int(((dec.status == 'accept') & (dec.tier == 'full')).sum()):,}, "
         f"not-incompatible name {int(((dec.status == 'accept') & (dec.tier == 'loose')).sum()):,})", "",
         "| status | ACIFs | last grant < 2015 | >= 2015 | initial-only ARC names |", "|---|---|---|---|---|"]
    for st, sub in d.groupby("status"):
        L.append(f"| {st} | {len(sub):,} | {int((sub.era == 'last grant < 2015').sum()):,} | "
                 f"{int((sub.era == 'last grant >= 2015').sum()):,} | {int(sub.initial_only.sum()):,} |")
    acc = d[d.status == "accept"]
    shared = acc.groupby("author_idx").cluster_id.apply(list)
    shared = shared[shared.map(len) > 1]
    L += ["", f"OpenAlex authors accepted for 2+ ACIFs (possible fragments of one person; reported only): {len(shared):,}", ""]
    for aid, cids in shared.head(10).items():
        L.append(f"- A{int(aid)}: " + "; ".join(f"{c} ({', '.join(a.loc[c, 'full_names'])})" for c in cids))
    pn = pairs.set_index(["cluster_id", "author_idx"])
    def ex(sub, n):
        out = []
        for r in sub.sample(min(n, len(sub)), random_state=3).itertuples():
            ps = pairs[(pairs.cluster_id == r.cluster_id) & pairs.passes]
            out.append(f"- {r.cluster_id} ({', '.join(r.full_names)}): " + "; ".join(
                f"A{int(p.author_idx)} {p.author_name} [{p.tier}, {p.years_at_grant_university} yrs]" for p in ps.itertuples()))
        return out
    L += ["", "Examples, accepted:", ""] + ex(acc, 15)
    L += ["", "Examples, accepted on a not-incompatible name:", ""] + ex(acc[acc.tier == "loose"], 10)
    L += ["", "Examples, several pass:", ""] + ex(d[d.status == "several_pass"], 10)
    return L + [""]


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--calibrate", action="store_true", help="also score stage 2 on the ORCID-linked ACIFs (~3 min)")
    args = ap.parse_args()
    OAX_LINK_DIR.mkdir(parents=True, exist_ok=True)
    acifs = load_acifs()
    orcids = {o for os in acifs.orcids for o in os}
    pool = load_authors(orcids)
    authors = pd.concat([pool, load_outside_authors(orcids - set(pool.orcid))], ignore_index=True)
    rejects = load_overrides()
    authors, refused = apply_overrides(authors, rejects)
    accepts = set(zip(rejects.loc[rejects.action == "accept_link", "orcid"],
                      rejects.loc[rejects.action == "accept_link", "author_idx"]))
    links = orcid_links(acifs, authors)
    links = decide(links, accepts, *load_name_evidence(links))
    miss = unmatched(acifs, links)
    a = acifs[acifs.orcids.map(len) > 0].assign(orcid=lambda d: d.orcids.map(lambda x: x[0]))
    rejected = (a[["cluster_id", "orcid", "full_names"]]
                .merge(refused.rename(columns={"full_name": "author_name"})[["orcid", "author_idx", "author_name"]], on="orcid")
                .merge(rejects[rejects.action == "reject_link"], on=["orcid", "author_idx"]))
    shared = shared_orcids(acifs)

    linked = set(links.loc[links.status.str.startswith("accept"), "cluster_id"])
    todo = acifs[~acifs.cluster_id.isin(linked)].reset_index(drop=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{DUCKDB_TMP_DIR}'")
    pairs, dec = name_links(con, todo, set(links.loc[links.status.str.startswith("accept"), "author_idx"].astype("int64")),
                            single_institution_windows(todo.cluster_id), acif_main_keys(todo.cluster_id))

    cal_lines = []
    if args.calibrate:
        acc_links = links[links.status.str.startswith("accept")]
        known = acifs[acifs.cluster_id.isin(set(acc_links.cluster_id))].reset_index(drop=True)
        cp, cd = name_links(con, known, set(), single_institution_windows(known.cluster_id), acif_main_keys(known.cluster_id))
        cal_lines = calibration_section(calibrate(cp, cd, acc_links[["cluster_id", "author_idx"]]))

    # works-first, for ACIFs the first kind left open
    open_ids = dec.loc[dec.status != "accept", "cluster_id"]
    coaw = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "coawardee_acif_ids"])
    linked_all = pd.concat([links.loc[links.status.str.startswith("accept"), ["cluster_id", "author_idx"]],
                            dec.loc[dec.status == "accept", ["cluster_id", "author_idx"]]]).astype({"author_idx": "int64"})
    coinv = wl.coinvestigator_authors(coaw, linked_all)
    ev = wl.works_evidence(con, pairs[pairs.cluster_id.isin(set(open_ids))], coinv[coinv.cluster_id.isin(set(open_ids))],
                           wl.grant_windows(open_ids))
    wdec, wlinks = wl.decide(open_ids, ev)
    wcal = None
    if args.calibrate:
        known_coinv = coinv[coinv.cluster_id.isin(set(known.cluster_id))]
        kev = wl.works_evidence(con, cp, known_coinv, wl.grant_windows(known.cluster_id))
        kdec, klinks = wl.decide(known.cluster_id, kev)
        wcal = wl.calibrate(kdec, klinks, acc_links[["cluster_id", "author_idx"]])
        ok = set(zip(acc_links.cluster_id, acc_links.author_idx.astype("int64")))
        kev.assign(correct=[(c, int(a)) in ok for c, a in zip(kev.cluster_id, kev.author_idx)]).to_parquet(
            OAX_LINK_DIR / "calibration_works_evidence.parquet", index=False)

    links.to_parquet(OAX_LINK_DIR / "orcid_links.parquet", index=False)
    ev.to_parquet(OAX_LINK_DIR / "works_evidence.parquet", index=False)
    wdec.to_parquet(OAX_LINK_DIR / "works_decisions.parquet", index=False)
    wlinks.to_parquet(OAX_LINK_DIR / "works_links.parquet", index=False)
    pairs.to_parquet(OAX_LINK_DIR / "name_links.parquet", index=False)
    dec.to_parquet(OAX_LINK_DIR / "name_decisions.parquet", index=False)
    rejected.to_parquet(OAX_LINK_DIR / "orcid_rejected.parquet", index=False)
    miss.to_parquet(OAX_LINK_DIR / "orcid_unmatched.parquet", index=False)
    text = "\n".join(["# ARC<->OpenAlex linking", ""] + orcid_section(acifs, links, miss, rejected, shared)
                     + name_section(acifs, pairs, dec) + cal_lines + works_section(acifs, ev, wdec, wlinks, wcal))
    (OAX_LINK_DIR / "report.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
