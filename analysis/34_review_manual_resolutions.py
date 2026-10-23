"""
analysis/34_review_manual_resolutions.py -- review the old pipeline's hand decisions
(data_persisted/manual_resolutions.csv: 424 resolve + 31 unlink rows, made on the Feb26/Jul26 OpenAlex
snapshots for the archived 04_resolve_links.py) against the Oct26 snapshot and the new linker's output
(2026-10-23, user: "is manual_resolutions still the right place to start or should it be reviewed against
the new oax authors?" ... "yes - and build the updated manual_linker as you go").

The new linker never read that file. Each row is treated as evidence, not as a decision:
  - the row's record id (arc_id) is mapped to its current ACIF (acif_arc_records.parquet);
  - the OpenAlex record is checked in Oct26: its works (own authorships), and whether its names (display
    name + alternatives, NameParser) are compatible with the ACIF's (orcid_hint_link.names_compatible, or
    an initials-only record matching the first initial);
  - its Jul26 work count is compared (the frozen Jul26 snapshot), to see records OpenAlex emptied or split;
  - a record with no Oct26 works gets a successor: the Oct26 author, with a compatible name, holding the
    most of the record's Jul26 works.
Class per row:
  agrees              the new linker already links the ACIF to this record
  add                 ACIF unlinked; record sound (works, compatible name) -> a manual link
  add_successor       ACIF unlinked; record emptied, successor found -> a manual link to the successor
  differs             ACIF linked to other records; this record sound -> review (both shown)
  record_gone         record emptied, no successor
  name_differs        record's names incompatible with the ACIF's -> review
  out_of_scope        the record's grant is not taken up (leaves scope at the next 00a/01 run)
  no_record           arc_id is not a current record
  unlink_agrees / unlink_conflict   unlink rows: ACIF unlinked / linked now
Writes processed/manual_resolutions_review.csv (every row) and processed/manual_links_draft.csv (the
add / add_successor rows and the unlink_agrees rows, in data_persisted/manual_links.csv's format).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, OAX_AUTHORS, OAX_LINK_DIR, OPENALEX_COMPACT_DIR, PROCESSED_DATA
from src.oax.orcid_hint_link import names_compatible
from src.utils.names import HumanNameParser

JUL26_AUTHORSHIPS = Path("/home/lc/m/openalex_jul26/parquet_converted/compact/authorships")
OUT = PROCESSED_DATA / "manual_resolutions_review.csv"
DRAFT = PROCESSED_DATA / "manual_links_draft.csv"
P = HumanNameParser()


def record_keys(con, ids) -> tuple[dict, dict, dict]:
    """(author_idx -> parser keys of its display name and alternatives, author_idx -> display name,
    author_idx -> [(family names, first-name initials)] when its DISPLAY name's first name is a bare initial:
    given tokens less middle tokens all single letters -- "J. Grant Collins", "A.J. Underwood". Alternatives
    are not used for this: "Jack Corbett" also appears as "J. Corbett", which would fit any J. Corbett.)"""
    con.register("rk", pd.DataFrame({"author_idx": sorted(ids)}))
    raw = con.execute(f"""SELECT author_idx, display_name, display_name_alternatives AS alt FROM read_parquet('{OAX_AUTHORS}/*.parquet')
                          WHERE author_idx IN (SELECT author_idx FROM rk)""").fetchdf()
    keys, inits = {}, {}
    for r in raw.itertuples():
        ps = [P.parse(n) for n in [r.display_name, *(list(r.alt) if r.alt is not None else [])] if isinstance(n, str) and n]
        keys[int(r.author_idx)] = sorted({k for p in ps for k in p.full_name_keys})
        d = ps[:1] if isinstance(r.display_name, str) and r.display_name else []
        inits[int(r.author_idx)] = [(set(p.family_names), f) for p in d
                                    if (f := set(p.given_tokens) - set(p.middle_tokens)) and all(len(t) == 1 for t in f)]
    return keys, dict(zip(raw.author_idx.astype("int64"), raw.display_name)), inits


def fits(acif_keys, rec_keys, rec_inits=()) -> bool:
    """The record's names fit the ACIF's: orcid_hint_link.names_compatible (shared full key, nickname or
    separator variant), or one of the record's names has a bare-initial first name matching the first letter
    of an ACIF full given name under the same family name ("J. Grant Collins" for John Collins, "A.J.
    Underwood" for Antony Underwood). A different full first name ("Jack Corbett" for Jennifer) never fits."""
    if names_compatible(list(acif_keys), list(rec_keys)):
        return True
    first = {}
    for k in acif_keys:
        if "_" in k and len(k.split("_", 1)[0]) > 1:
            first.setdefault(k.split("_", 1)[1], set()).add(k[0])
    return any(f & first.get(fam, set()) for fams, f in rec_inits for fam in fams)


def main():
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    m = pd.read_csv("data_persisted/manual_resolutions.csv")
    m["author_idx"] = m.oax_id.str.extract(r"A(\d+)")[0].astype("Int64")
    rec = pd.read_parquet(ACIF_ARC_RECORDS, columns=["unique_id", "cluster_id", "grant_code"])
    m = m.merge(rec, left_on="arc_id", right_on="unique_id", how="left")
    acifs = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "full_name_keys", "full_names"]).set_index("cluster_id")
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    links = acc.groupby("cluster_id").author_idx.apply(lambda s: sorted(s.astype("int64"))).to_dict()
    g = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")
    ntu = set(g[g.declined | (g.project_start_date.notna() & (g.project_start_date == g.anticipated_end_date))
                | ((g.funding_current == 0) & (g.funding_announced > 0))].grant_code)

    ids = set(m.author_idx.dropna().astype("int64"))
    con.register("ids", pd.DataFrame({"author_idx": sorted(ids)}))
    oct_w = dict(con.execute(f"""SELECT author_idx, count(DISTINCT work_idx) FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
                                 WHERE author_idx IN (SELECT author_idx FROM ids) GROUP BY 1""").fetchall())
    jul = con.execute(f"""SELECT DISTINCT author_idx, work_idx FROM read_parquet('{JUL26_AUTHORSHIPS}/*.parquet')
                          WHERE author_idx IN (SELECT author_idx FROM ids)""").fetchdf()
    jul_w = jul.groupby("author_idx").size().to_dict()

    # successors of emptied records: the Oct26 author holding most of their Jul26 works
    empty = {a for a in ids if oct_w.get(a, 0) == 0}
    je = jul[jul.author_idx.isin(empty)]
    con.register("je", je.rename(columns={"author_idx": "old"}))
    succ = con.execute(f"""SELECT je.old, a.author_idx AS new, count(DISTINCT a.work_idx) AS n
        FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet') a JOIN je USING (work_idx)
        WHERE a.author_idx IS NOT NULL GROUP BY 1, 2""").fetchdf()
    keys, names, inits = record_keys(con, ids | set(succ.new.astype("int64")))
    succ_of = {}
    for old, d in succ.groupby("old"):
        succ_of[int(old)] = [(int(r.new), int(r.n)) for r in d.sort_values("n", ascending=False).itertuples()]
    oct_w.update(dict(con.execute(f"""SELECT author_idx, count(DISTINCT work_idx) FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
                                      WHERE author_idx IN ({','.join(str(int(x)) for x in succ.new.unique()) or '0'}) GROUP BY 1""").fetchall()))

    rows = []
    for r in m.itertuples():
        cid, aid = r.cluster_id, (None if pd.isna(r.author_idx) else int(r.author_idx))
        out = dict(arc_id=r.arc_id, action=r.action, cluster_id=cid, author_idx=aid, record_name=names.get(aid),
                   jul26_works=jul_w.get(aid, 0) if aid else None, oct26_works=oct_w.get(aid, 0) if aid else None,
                   current_links=";".join(f"A{x}" for x in links.get(cid, [])) if isinstance(cid, str) else "",
                   successor=None, successor_name=None, successor_shared=None, note=r.note)
        if not isinstance(cid, str):
            out["class"] = "no_record"
        elif r.grant_code in ntu:
            out["class"] = "out_of_scope"
        elif r.action == "unlink":
            out["class"] = "unlink_conflict" if cid in links else "unlink_agrees"
        else:
            akeys = acifs.loc[cid, "full_name_keys"]
            linked = cid in links
            if linked and aid in links[cid]:
                out["class"] = "agrees"
            elif oct_w.get(aid, 0) == 0:
                cands = [(n, k) for n, k in succ_of.get(aid, []) if fits(akeys, keys.get(n, []), inits.get(n, []))]
                if cands:
                    out.update(successor=cands[0][0], successor_name=names.get(cands[0][0]), successor_shared=cands[0][1])
                    out["class"] = ("agrees" if linked and cands[0][0] in links[cid] else
                                    "differs" if linked else "add_successor")
                else:
                    out["class"] = "record_gone"
            elif not fits(akeys, keys.get(aid, []), inits.get(aid, [])):
                out["class"] = "name_differs"
            else:
                out["class"] = "differs" if linked else "add"
        rows.append(out)
    d = pd.DataFrame(rows)
    d.to_csv(OUT, index=False)
    print(d["class"].value_counts().to_string())

    draft = []
    for r in d[d["class"].isin(["add", "add_successor", "unlink_agrees"])].to_dict("records"):
        cls = r["class"]
        target = r["successor"] if cls == "add_successor" else r["author_idx"]
        draft.append({"action": "unlink" if cls == "unlink_agrees" else "link", "unique_id": r["arc_id"],
                      "oax_author": "" if cls == "unlink_agrees" else f"A{int(target)}",
                      "source": "manual_resolutions.csv" + (f" (Oct26 successor of A{int(r['author_idx'])})" if cls == "add_successor" else ""),
                      "notes": str(r["note"])})
    pd.DataFrame(draft, columns=["action", "unique_id", "oax_author", "source", "notes"]).to_csv(DRAFT, index=False)
    print(OUT, DRAFT, len(draft))


if __name__ == "__main__":
    main()
