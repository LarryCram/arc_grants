"""
analysis/35_assess_manual_review.py -- a preliminary assessment of the manual_resolutions.csv rows left for
review by analysis/34 (2026-10-23, user: "the number you want me to review is too large. can you run a
preliminary assessment over them?").

For each row (classes differs, unlink_conflict, name_differs, add held back) the old hand record and every
record the new linker accepted for the ACIF are scored on the same evidence:
  works         its works (Oct26 authorships)
  uni_works     works with an affiliation at one of the ACIF's grant institutions (acifs_arc.inst_ids)
  coinv_works   works co-authored with a record linked to one of the ACIF's co-investigators
  orcid         the record's ORCID, and whether it is the ACIF's
Proposed verdicts (a proposal for the user, not applied):
  differs         ("fits" = has uni or co-investigator works)
                  replace_current  the hand record fits and no current link does -> the current link is likely
                                   wrong (e.g. an ORCID-hint namesake) and the hand record right
                  keep_both        both fit and the hand record carries no other ORCID -> another fragment
                  look             both fit but the hand record carries another ORCID (two people in the ACIF, or
                                   two ORCID accounts), or neither fits
                  drop_hand        the current link fits and the hand record does not
  unlink_conflict keep_link (current link carries the ACIF's ORCID, or has uni + co-investigator works) ;
                  keep_unlink (current link has neither) ; look
  name_differs    drop_hand unless the hand record has 2+ co-investigator works (then look: ARC name error?)
  add (held back) add_with_care when the record has uni or co-investigator works, else drop_hand
Writes processed/manual_review_assessment.csv.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from config.settings import ACIFS_ARC, OAX_AUTHORS, OAX_LINK_DIR, OPENALEX_COMPACT_DIR, PROCESSED_DATA

IN = PROCESSED_DATA / "manual_resolutions_to_review.csv"
OUT = PROCESSED_DATA / "manual_review_assessment.csv"
CLASSES = ["differs", "unlink_conflict", "name_differs", "add"]


def evidence(con, pairs: pd.DataFrame, inst: dict, coinv: dict) -> pd.DataFrame:
    """(cluster_id, author_idx) -> works, uni_works, coinv_works, years, top institution, top field, orcid."""
    con.register("pp", pairs[["cluster_id", "author_idx"]].drop_duplicates())
    au = con.execute(f"""SELECT DISTINCT p.cluster_id, a.author_idx, a.work_idx, a.institution_idx, a.institution_name
        FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet') a JOIN pp p USING (author_idx)""").df()
    con.register("aw", au[["work_idx"]].drop_duplicates())
    yr = con.execute(f"SELECT work_idx, publication_year AS y FROM read_parquet('{OPENALEX_COMPACT_DIR}/works/*.parquet') "
                     "WHERE work_idx IN (SELECT work_idx FROM aw)").df()
    tp = con.execute(f"""SELECT work_idx, field_name FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_topics/*.parquet')
        WHERE work_idx IN (SELECT work_idx FROM aw) QUALIFY row_number() OVER (PARTITION BY work_idx ORDER BY score DESC) = 1""").df()
    allco = sorted(set().union(*coinv.values())) if coinv else []
    con.register("co", pd.DataFrame({"author_idx": pd.Series(allco, dtype="int64")}))
    cw = con.execute(f"""SELECT DISTINCT work_idx, author_idx FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
        WHERE author_idx IN (SELECT author_idx FROM co) AND work_idx IN (SELECT work_idx FROM aw)""").df()
    on_work = cw.groupby("work_idx").author_idx.apply(set).to_dict()
    orc = con.execute(f"""SELECT author_idx, replace(orcid, 'https://orcid.org/', '') AS orcid, display_name
        FROM read_parquet('{OAX_AUTHORS}/*.parquet') WHERE author_idx IN (SELECT author_idx FROM pp)""").df().set_index("author_idx")
    au = au.merge(yr, on="work_idx", how="left").merge(tp, on="work_idx", how="left")
    rows = []
    for (cid, aid), d in au.groupby(["cluster_id", "author_idx"]):
        w = d.drop_duplicates("work_idx")
        rows.append({"cluster_id": cid, "author_idx": int(aid), "works": len(w),
                     "uni_works": int(d[d.institution_idx.isin(inst.get(cid, []))].work_idx.nunique()),
                     "coinv_works": int(sum(1 for x in w.work_idx if on_work.get(x, set()) & coinv.get(cid, set()))),
                     "years": f"{int(w.y.min())}-{int(w.y.max())}" if w.y.notna().any() else "",
                     "top_inst": "; ".join(d.institution_name.dropna().value_counts().head(2).index),
                     "top_field": "; ".join(w.field_name.dropna().value_counts().head(2).index)})
    e = pd.DataFrame(rows)
    if len(e):
        e["orcid"] = e.author_idx.map(orc.orcid)
        e["name"] = e.author_idx.map(orc.display_name)
    return e


def main():
    rv = pd.read_csv(IN)
    rv = rv[rv["class"].isin(CLASSES)].copy()
    acifs = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "inst_ids", "coawardee_acif_ids", "orcids", "full_names"]).set_index("cluster_id")
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet").astype({"author_idx": "int64"})
    cids = set(rv.cluster_id)
    inst = {c: [int(i.rsplit("I", 1)[1]) for i in acifs.loc[c, "inst_ids"]] for c in cids}
    coinv = {c: set(acc[acc.cluster_id.isin(acifs.loc[c, "coawardee_acif_ids"])].author_idx) for c in cids}
    cur = acc[acc.cluster_id.isin(cids)][["cluster_id", "author_idx", "stage"]]
    hand = rv[rv.author_idx.notna()][["cluster_id", "author_idx"]].astype({"author_idx": "int64"})
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    ev = evidence(con, pd.concat([hand, cur[["cluster_id", "author_idx"]]]), inst, coinv).set_index(["cluster_id", "author_idx"])

    def get(c, a):
        return ev.loc[(c, a)] if (c, a) in ev.index else None

    def fits(e) -> bool:
        return e is not None and (e.uni_works > 0 or e.coinv_works > 0)

    out = []
    for r in rv.to_dict("records"):
        c = r["cluster_id"]
        own = (list(acifs.loc[c, "orcids"]) or [None])[0]
        h = get(c, int(r["author_idx"])) if pd.notna(r["author_idx"]) else None
        links = cur[cur.cluster_id == c]
        le = [(int(x.author_idx), x.stage, get(c, int(x.author_idx))) for x in links.itertuples()]
        best = max(le, key=lambda t: (t[2].uni_works + t[2].coinv_works) if t[2] is not None else -1, default=None)
        cls = r["class"]
        h_other_orcid = h is not None and isinstance(h.orcid, str) and h.orcid and own and h.orcid != own
        if cls == "differs":
            cs = best is not None and fits(best[2])
            verdict = ("replace_current" if fits(h) and not cs else
                       ("look" if h_other_orcid else "keep_both") if fits(h) else
                       "drop_hand" if cs else "look")
        elif cls == "unlink_conflict":
            orcid_link = any(t[1] == "orcid" for t in le)
            ok = best is not None and best[2] is not None and best[2].uni_works > 0 and best[2].coinv_works > 0
            verdict = "keep_link" if orcid_link or ok else ("keep_unlink" if best is None or not fits(best[2]) else "look")
        elif cls == "name_differs":
            verdict = "look" if h is not None and h.coinv_works >= 2 else "drop_hand"
        else:
            verdict = "add_with_care" if fits(h) else "drop_hand"
        fmt = lambda e: "" if e is None else (f"{e['name']} | {e.works} works, uni {e.uni_works}, coinv {e.coinv_works}, "
                                               f"{e.years}, {e.top_inst} | {e.top_field} | ORCID {e.orcid if isinstance(e.orcid, str) else '-'}")
        out.append({"verdict": verdict, "class": cls, "cluster_id": c, "acif_name": "; ".join(acifs.loc[c, "full_names"]),
                    "acif_orcid": own or "", "hand_record": f"A{int(r['author_idx'])}" if pd.notna(r["author_idx"]) else "",
                    "hand_evidence": fmt(h),
                    "current_links": " || ".join(f"A{a} [{s}] {fmt(e)}" for a, s, e in le),
                    "review_reason": r.get("review_reason", ""), "old_note": r["note"]})
    d = pd.DataFrame(out).sort_values(["class", "verdict", "cluster_id"])
    d.to_csv(OUT, index=False)
    print(pd.crosstab(d["class"], d.verdict).to_string())
    print(OUT)


if __name__ == "__main__":
    main()
