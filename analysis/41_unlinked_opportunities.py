"""
analysis/41_unlinked_opportunities.py -- what evidence is still unused for the ACIFs the linker left unlinked
(2026-10-24, user: "reexamine the unlinked cases to summarise them and advise on any opportunities to follow
them further").

Read-only. For each unlinked kept ACIF (processed/unlinked_acifs.csv, written by analysis/38) three sources the
linker does not use, each reduced to authorships whose printed name fits the ACIF's names (analysis/36's fits():
compatible parser keys, or a bare first initial under the same family name):
  ack       works acknowledging one of the ACIF's ARC grants (OpenAlex awards, ARC funder)
  orcid     works on the ACIF's ORCID record (DOIs from the ORCID record cache), matched to OpenAlex by DOI
  au_null   authorships with no OpenAlex author record (author_idx NULL) at an Australian institution, between 6
            years before the first grant and 6 years after the last
For ack and orcid a fitting authorship is either on an author record (a candidate to link: how many records,
how many works the top one holds) or unattributed (author_idx NULL: works that exist in OpenAlex but sit on no
record, as for Chen Chen, Tom Cochrane, Shuaishuai Sun).
Writes processed/unlinked_opportunities.md and processed/unlinked_opportunities.csv (one row per ACIF).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import diskcache
import duckdb
import pandas as pd

from config.settings import ACIFS_ARC, DISKCACHE_DIR, OPENALEX_COMPACT_DIR, OPENALEX_DIR, PROCESSED_DATA

sys.path.insert(0, str(Path(__file__).resolve().parent))
from importlib import import_module  # noqa: E402

fits = import_module("36_grant_ack_test").fits
ARC_FUNDER = "F4320334704"
OUT_MD = PROCESSED_DATA / "unlinked_opportunities.md"
OUT_CSV = PROCESSED_DATA / "unlinked_opportunities.csv"
WINDOW = 6


def orcid_dois(orcid: str, cache) -> set[str]:
    r = cache.get(orcid)
    if not isinstance(r, dict) or (r.get("path") or orcid).strip("/") != orcid:   # a redirected record is another ORCID
        return set()
    out = set()
    for g in (r.get("activities-summary") or {}).get("works", {}).get("group", []) or []:
        for e in (g.get("external-ids") or {}).get("external-id", []) or []:
            if e.get("external-id-type") == "doi" and e.get("external-id-value"):
                out.add(e["external-id-value"].strip().lower().removeprefix("https://doi.org/"))
    return out


def summarise(x: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Per ACIF: works with a fitting authorship, records among them, top record and its works, unattributed works."""
    if x.empty:
        return pd.DataFrame(columns=["cluster_id"])
    att = x[x.author_idx.notna()]
    rec = att.groupby(["cluster_id", "author_idx"]).work_idx.nunique().reset_index(name="n").sort_values(
        ["cluster_id", "n"], ascending=[True, False])
    top = rec.groupby("cluster_id").head(2).groupby("cluster_id").agg(
        top=("author_idx", "first"), top_n=("n", "first"), second_n=("n", lambda s: s.iloc[1] if len(s) > 1 else 0))
    out = pd.DataFrame({f"{prefix}_works": x.groupby("cluster_id").work_idx.nunique(),
                        f"{prefix}_records": rec.groupby("cluster_id").size(),
                        f"{prefix}_unattributed": x[x.author_idx.isna()].groupby("cluster_id").work_idx.nunique()})
    out = out.join(top.rename(columns={c: f"{prefix}_{c}" for c in top.columns}))
    return out.reset_index()


def main():
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    con.execute("SET threads=16")
    u = pd.read_csv(PROCESSED_DATA / "unlinked_acifs.csv")
    a = pd.read_parquet(ACIFS_ARC).set_index("cluster_id").loc[u.cluster_id]
    keys = a.full_name_keys.to_dict()
    cache_names = {}

    def fitting(df):
        if df.empty:
            return df
        p = df[["cluster_id", "author_name"]].drop_duplicates()
        p["fit"] = [fits(keys[c], n, cache_names) for c, n in zip(p.cluster_id, p.author_name)]
        return df.merge(p[p.fit][["cluster_id", "author_name"]], on=["cluster_id", "author_name"])

    def authorships_of(works: pd.DataFrame) -> pd.DataFrame:
        con.register("ww", works[["work_idx"]].drop_duplicates())
        au = con.execute(f"""SELECT DISTINCT work_idx, author_idx, author_name FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
                             WHERE work_idx IN (SELECT work_idx FROM ww)""").df()
        return fitting(works.merge(au, on="work_idx"))

    # ack: works acknowledging the ACIF's grants
    aw = con.execute(f"SELECT id, funder_award_id FROM read_parquet('{OPENALEX_DIR}/awards.parquet') "
                     f"WHERE funder.id LIKE '%{ARC_FUNDER}'").df()
    aw["award_idx"] = aw.id.str.extract(r"G(\d+)")[0].astype("int64")
    aw["grant_code"] = aw.funder_award_id.fillna("").str.upper().str.findall(r"[A-Z]{2}\d{6,9}")
    ag = aw.explode("grant_code").dropna(subset=["grant_code"])[["award_idx", "grant_code"]]
    g = a.reset_index()[["cluster_id", "grant_codes"]].explode("grant_codes").rename(columns={"grant_codes": "grant_code"})
    ga = g.merge(ag, on="grant_code")
    con.register("gaw", ga[["award_idx"]].drop_duplicates())
    wa = con.execute(f"SELECT DISTINCT work_idx, award_idx FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_awards/*.parquet') "
                     "WHERE award_idx IN (SELECT award_idx FROM gaw)").df()
    ack = authorships_of(ga.merge(wa, on="award_idx")[["cluster_id", "work_idx"]].drop_duplicates())

    # orcid: works on the ACIF's ORCID record, by DOI
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    rows = [(cid, d) for cid, ors in a.orcids.items() for o in ors for d in orcid_dois(o, cache)]
    od = pd.DataFrame(rows, columns=["cluster_id", "doi"])
    n_dois = od.groupby("cluster_id").doi.nunique()
    con.register("od", od[["doi"]].drop_duplicates())
    wd = con.execute(f"SELECT work_idx, lower(doi) AS doi FROM read_parquet('{OPENALEX_COMPACT_DIR}/works/*.parquet') "
                     "WHERE lower(doi) IN (SELECT doi FROM od)").df()
    orc = authorships_of(od.merge(wd, on="doi")[["cluster_id", "work_idx"]].drop_duplicates())

    # au_null: unattributed Australian authorships in the grant window
    fams = {k.split("_", 1)[1] for ks in keys.values() for k in ks if "_" in k}
    nul = con.execute(f"""SELECT a.work_idx, a.author_name, w.publication_year AS y
        FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet') a
        JOIN read_parquet('{OPENALEX_COMPACT_DIR}/works/*.parquet') w USING (work_idx)
        WHERE a.author_idx IS NULL AND a.country_code = 'AU'""").df()
    nul["tok"] = nul.author_name.fillna("").str.lower().str.replace(r"[^a-z' -]", " ", regex=True).str.split()
    nul = nul[[any(t in fams for t in ts) or " ".join(ts[-2:]) in fams for ts in nul.tok]]
    fam_of = {}
    for cid, ks in keys.items():
        for k in ks:
            if "_" in k:
                fam_of.setdefault(k.split("_", 1)[1], set()).add(cid)
    pairs = [(cid, w, n, y) for w, n, y, ts in zip(nul.work_idx, nul.author_name, nul.y, nul.tok)
             for f in set(ts) | {" ".join(ts[-2:])} for cid in fam_of.get(f, ())]
    an = pd.DataFrame(pairs, columns=["cluster_id", "work_idx", "author_name", "y"]).drop_duplicates()
    an = an.merge(u[["cluster_id", "first_year", "last_year"]], on="cluster_id")
    an = an[(an.y >= an.first_year - WINDOW) & (an.y <= an.last_year + WINDOW)]
    an = fitting(an)
    au_null = an.groupby("cluster_id").work_idx.nunique().rename("au_null_works").reset_index()

    t = u.merge(summarise(ack, "ack"), on="cluster_id", how="left").merge(
        summarise(orc, "orcid"), on="cluster_id", how="left").merge(au_null, on="cluster_id", how="left")
    t["orcid_dois"] = t.cluster_id.map(n_dois).fillna(0).astype(int)
    t["has_orcid"] = t.cluster_id.map(a.orcids.map(len)) > 0
    num = [c for c in t.columns if c.endswith(("_works", "_records", "_unattributed", "_n"))]
    t[num] = t[num].fillna(0).astype(int)
    # what each ACIF offers, first that applies
    def offer(r):
        if r.orcid_top_n >= 2 and r.orcid_top_n > r.orcid_second_n:
            return "orcid_works_one_record"
        if r.ack_top_n >= 2 and r.ack_top_n > r.ack_second_n:
            return "ack_one_record"
        if r.orcid_records or r.ack_records:
            return "orcid_or_ack_records_tied_or_single_work"
        if r.orcid_unattributed or r.ack_unattributed:
            return "works_unattributed_only"
        if r.au_null_works >= 2:
            return "au_unattributed_name_matches"
        return "nothing_found"
    t["offer"] = t.apply(offer, axis=1)
    t.to_csv(OUT_CSV, index=False)

    n = len(t)
    L = ["# Unlinked ACIFs: unused evidence (2026-10-24)", "",
         f"Unlinked kept ACIFs: {n:,} ({int(t.has_orcid.sum()):,} with an ORCID; {int((t.orcid_dois > 0).sum()):,} whose "
         "ORCID record lists DOIs). Method in the script's docstring.", "",
         "## What each source finds", "",
         "| source | ACIFs with a fitting authorship | on an author record | unattributed only |", "|---|---|---|---|"]
    for p in ("ack", "orcid"):
        has = t[f"{p}_works"] > 0
        rec = t[f"{p}_records"] > 0
        L.append(f"| {p} | {int(has.sum()):,} | {int(rec.sum()):,} | {int((has & ~rec).sum()):,} |")
    L.append(f"| au_null (2+ works) | {int((t.au_null_works >= 2).sum()):,} | - | {int((t.au_null_works >= 2).sum()):,} |")
    L += ["", "## Best offer per ACIF (first that applies)", "", "| offer | ACIFs |", "|---|---|"]
    L += [f"| {k} | {v:,} |" for k, v in t.offer.value_counts().items()]
    L += ["", "By linker reason:", "", pd.crosstab(t.reason, t.offer).to_markdown(), ""]
    L += ["## By last grant year", "", pd.crosstab(pd.cut(t.last_year, [2000, 2005, 2010, 2015, 2020, 2030],
          labels=["2001-05", "2006-10", "2011-15", "2016-20", "2021-26"]), t.offer).to_markdown(), ""]
    for off in ("orcid_works_one_record", "ack_one_record", "orcid_or_ack_records_tied_or_single_work",
                "works_unattributed_only", "au_unattributed_name_matches"):
        s = t[t.offer == off].sort_values(["orcid_top_n", "ack_top_n", "au_null_works"], ascending=False).head(15)
        cols = ["cluster_id", "reason", "first_year", "last_year", "orcid_dois", "orcid_works", "orcid_top", "orcid_top_n",
                "orcid_second_n", "orcid_unattributed", "ack_works", "ack_top", "ack_top_n", "ack_second_n",
                "ack_unattributed", "au_null_works"]
        L += [f"## Examples: {off}", "", s[cols].to_markdown(index=False) if len(s) else "(none)", ""]
    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT_MD)


if __name__ == "__main__":
    main()
