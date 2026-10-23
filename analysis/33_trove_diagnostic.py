"""
analysis/33_trove_diagnostic.py -- Trove as a probe of the linker (2026-10-22, user: "the Trove response may not
be usable but it is diagnostic"; "only do the unlinked ones; start with another 100, chosen to have early grant award
years; prefer ACIFs with more than one grant, or with a fellowship").

ACIFs (--set): `unlinked` -- kept, unlinked (not in accepted_links.parquet), not among analysis/30's trial; those
with 2+ grants or a fellowship record first, then by first grant year; the first N (default all). `baseline` (user,
2026-10-22: "the sample you need to get a baseline from matches") -- N random ORCID-linked ACIFs (stage 1; seed 7)
whose first grant is in 2010 or before, so the true record is known; their own accepted records are class own_record.
Trove queries are fetched first, 3 in parallel (cached; analysis/30's search with retries).

Per ACIF: analysis/30's Trove search (category 'book', creator = each full given name + family; cached in
processed/trove_cache/) and its NameParser name test (a contributor shares a full-given-name key with the ACIF).
Each name-matched Trove item's title is then looked up in OpenAlex (compact works): same normalised main title (text
before the first ':' / ' / ', lower case, letters and digits only, 12+ characters), on a work with an authorship
whose name is compatible with the ACIF's (src/oax/orcid_hint_link.names_compatible on parser keys). Each such
authorship is classified:
  unattributed            no author record (the Oct26 cleanup orphans)
  outside_pool            an author record not in openalex_authors_prep (works without an Australian institution)
  linked_elsewhere        a record accepted for another ACIF
  pool_candidate          a pool record the linker's name stage considered for this ACIF (its stage-2 decision shown)
  pool_not_candidate      a pool record the name stage did not reach (name forms)
Items with no OpenAlex title match count as not_in_openalex.
Writes processed/trove_diagnostic.md and trove_diagnostic.parquet (one row per Trove item x OpenAlex authorship).
"""

import re
import runpy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, OAX_LINK_DIR, OPENALEX_COMPACT_DIR, PROCESSED_DATA
from src.oax.orcid_hint_link import names_compatible
from src.utils.names import HumanNameParser

T30 = runpy.run_path(str(Path(__file__).with_name("30_trove_trial.py")), run_name="t30")
OUT = PROCESSED_DATA / "trove_diagnostic"
P = HumanNameParser()


def main_title(t) -> str:
    t = re.split(r"\s+/\s+|:", str(t or ""), maxsplit=1)[0]
    return re.sub(r"[^a-z0-9]", "", t.lower())


def pick(n, which="unlinked") -> pd.DataFrame:
    a = pd.read_parquet(ACIFS_ARC)
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    fell = set(pd.read_parquet(ACIF_ARC_RECORDS, columns=["cluster_id", "is_fellowship"]).query("is_fellowship").cluster_id)
    if which == "baseline":
        b = a[~a.excluded & a.cluster_id.isin(set(acc.loc[acc.stage == "orcid", "cluster_id"])) & (a.first_year <= 2010)].copy()
        b = b.sample(min(n or 300, len(b)), random_state=7)
        b["priority"] = (b.n_grants > 1) | b.cluster_id.isin(fell)
        b["fellowship"] = b.cluster_id.isin(fell)
        return b
    u = a[~a.excluded & ~a.cluster_id.isin(set(acc.cluster_id))].copy()
    trial = set(pd.read_parquet(PROCESSED_DATA / "unlinked_composition.parquet")
                .sort_values(["last_year", "first_year", "cluster_id"]).head(8).cluster_id)
    u = u[~u.cluster_id.isin(trial)]
    u["priority"] = (u.n_grants > 1) | u.cluster_id.isin(fell)
    u["fellowship"] = u.cluster_id.isin(fell)
    u = u.sort_values(["priority", "first_year", "cluster_id"], ascending=[False, True, True])
    return u.head(n) if n else u


def query(x) -> str | None:
    keys = {k for k in x.full_name_keys if len(k.split("_", 1)[0]) > 1}
    fam_raw = list(x.full_names)[0].split()[-1]
    givens = sorted({k.split("_", 1)[0].capitalize() for k in keys})
    return " OR ".join(f'creator:"{fam_raw}, {g}" OR creator:"{g} {fam_raw}"' for g in givens) if givens else None


def prefetch(u: pd.DataFrame) -> None:
    from concurrent.futures import ThreadPoolExecutor
    qs = [q for q in (query(x) for x in u.itertuples()) if q]
    with ThreadPoolExecutor(3) as ex:
        for k, _ in enumerate(ex.map(T30["search"], qs), 1):
            if k % 50 == 0:
                print(f"  Trove {k}/{len(qs)}", flush=True)


def trove_items(u: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for x in u.itertuples():
        keys = {k for k in x.full_name_keys if len(k.split("_", 1)[0]) > 1}
        q = query(x)
        if not q:
            continue
        lo, hi = int(x.first_year) - 25, int(x.last_year) + 15
        for w in T30["search"](q):
            hit = next((c for c in (w.get("contributor") or []) if set(P.parse(c).full_name_keys) & keys), None)
            if not hit:
                continue
            y = T30["year"](w.get("issued"))
            rows.append({"cluster_id": x.cluster_id, "trove_title": re.sub(r"\s*/\s*[^/]*$", "", w.get("title", "")),
                         "trove_contributor": hit, "trove_year": y, "in_window": y is not None and lo <= y <= hi,
                         "trove_type": "/".join((w.get("type") or [])[:1]), "key": main_title(w.get("title"))})
    return pd.DataFrame(rows)


def openalex_matches(con, items: pd.DataFrame, u: pd.DataFrame) -> pd.DataFrame:
    """Works whose normalised main title equals a Trove item's (computed in DuckDB, so only matching works and
    their authorships reach memory -- an earlier version pulled every authorship whose name contained one of the
    ACIFs' family names and ran out of memory on 64 GB), then the authorships with a compatible name."""
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    con.execute("SET memory_limit='24GB'")
    keys_df = items.loc[items.key.str.len() >= 12, ["key"]].drop_duplicates()
    con.register("tk", keys_df)
    w = con.execute(f"""
        WITH w AS (SELECT work_idx, title, publication_year, type,
                          regexp_replace(lower(split_part(regexp_replace(coalesce(title, ''), '\\s+/\\s+', ':', 'g'), ':', 1)),
                                         '[^a-z0-9]', '', 'g') AS key
                   FROM read_parquet('{OPENALEX_COMPACT_DIR}/works/*.parquet'))
        SELECT * FROM w WHERE key IN (SELECT key FROM tk)""").df()
    con.register("mw", w[["work_idx"]].drop_duplicates())
    au = con.execute(f"""SELECT work_idx, author_idx, author_name FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
                         WHERE work_idx IN (SELECT work_idx FROM mw)""").df()
    m = items[items.key.str.len() >= 12].merge(w, on="key").merge(au, on="work_idx")
    keys = dict(zip(u.cluster_id, u.full_name_keys))
    fams = {c: {k.split("_", 1)[1] for k in ks} for c, ks in keys.items()}
    m = m[[any(f in str(n).lower() for f in fams[c]) for c, n in zip(m.cluster_id, m.author_name)]]   # cheap pre-filter
    m = m.assign(name_ok=[names_compatible(keys[c], P.parse(n).full_name_keys) for c, n in zip(m.cluster_id, m.author_name)])
    return m[m.name_ok]


def classify(m: pd.DataFrame) -> pd.DataFrame:
    prep = set(pd.read_parquet(PROCESSED_DATA / "openalex_authors_prep.parquet", columns=["author_idx"]).author_idx)
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    taken = dict(zip(acc.author_idx.astype("int64"), acc.cluster_id))
    nl = pd.read_parquet(OAX_LINK_DIR / "name_links.parquet")
    cand = set(zip(nl.cluster_id, nl.author_idx.astype("int64")))
    nd = pd.read_parquet(OAX_LINK_DIR / "name_decisions.parquet").set_index("cluster_id").status
    hd = pd.read_parquet(OAX_LINK_DIR / "orcid_hint_decisions.parquet").set_index("cluster_id").status

    own = set(zip(acc.cluster_id, acc.author_idx.astype("int64")))

    def cls(c, a):
        if pd.isna(a):
            return "unattributed"
        a = int(a)
        if (c, a) in own:
            return "own_record"
        if a in taken:
            return "linked_elsewhere"
        if a not in prep:
            return "outside_pool"
        return "pool_candidate" if (c, a) in cand else "pool_not_candidate"
    m = m.copy()
    m["category"] = [cls(c, a) for c, a in zip(m.cluster_id, m.author_idx)]
    m["linked_to"] = [taken.get(int(a)) if not pd.isna(a) else None for a in m.author_idx]
    m["name_stage"] = m.cluster_id.map(nd)
    m["hint_stage"] = m.cluster_id.map(hd)
    return m


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("n", nargs="?", type=int, default=0)
    ap.add_argument("--set", default="unlinked", choices=["unlinked", "baseline"])
    args = ap.parse_args()
    global OUT
    OUT = PROCESSED_DATA / ("trove_diagnostic" if args.set == "unlinked" else "trove_baseline")
    u = pick(args.n, args.set)
    prefetch(u)
    items = trove_items(u)
    con = duckdb.connect()
    m = classify(openalex_matches(con, items, u))
    m.to_parquet(OUT.with_suffix(".parquet"), index=False)
    found = set(zip(m.cluster_id, m.trove_title))
    items["in_openalex"] = [(c, t) in found for c, t in zip(items.cluster_id, items.trove_title)]
    g = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet", columns=["grant_code", "grant_summary"]).set_index("grant_code")
    title = ("unlinked ACIFs (2+ grants or a fellowship first, then early grants)" if args.set == "unlinked"
             else "ORCID-linked ACIFs, first grant <= 2010 (baseline: the true record is known)")
    L = [f"# Trove as a probe of the linker: {len(u)} {title}", "",
         f"- ACIFs: {len(u)} ({int(u.priority.sum())} with 2+ grants or a fellowship; {int(u.fellowship.sum())} fellowships); "
         f"first grant years {int(u.first_year.min())}-{int(u.first_year.max())}",
         f"- with a name-matched Trove item: {items.cluster_id.nunique()}; items {len(items):,} (in the career window "
         f"{int(items.in_window.sum()):,}); found in OpenAlex by title with a compatible author name: "
         f"{int(items.in_openalex.sum()):,} items for {m.cluster_id.nunique()} ACIFs", "",
         "## What the OpenAlex matches show (ACIFs with at least one item in the category)", "",
         "| category | ACIFs | items |", "|---|---|---|"]
    for k, s in m.groupby("category"):
        L.append(f"| {k} | {s.cluster_id.nunique()} | {s.drop_duplicates(['cluster_id', 'trove_title']).shape[0]} |")
    L += ["", "## Per ACIF", ""]
    for x in u.itertuples():
        it = items[items.cluster_id == x.cluster_id]
        mm = m[m.cluster_id == x.cluster_id]
        summ = str(g.loc[x.grant_codes[0], "grant_summary"]).split(". ")[0][:100] if x.grant_codes[0] in g.index else ""
        L.append(f"### {list(x.full_names)[0]} -- {x.cluster_id} ({x.n_grants} grants {int(x.first_year)}-{int(x.last_year)}"
                 f"{', fellowship' if x.fellowship else ''}): {summ}")
        L.append(f"- Trove: {len(it)} name-matched items ({int(it.in_window.sum()) if len(it) else 0} in window); in OpenAlex: "
                 f"{int(it.in_openalex.sum()) if len(it) else 0}")
        for r in mm.drop_duplicates(["trove_title", "author_idx"]).head(6).itertuples():
            rec = "no author record" if pd.isna(r.author_idx) else f"A{int(r.author_idx)}"
            extra = f" -> {r.linked_to}" if r.category == "linked_elsewhere" else (f" (name stage {r.name_stage})" if r.category == "pool_candidate" else "")
            L.append(f"  - [{r.category}] {r.trove_year or ''} {r.trove_title[:80]} | OpenAlex '{r.author_name}' {rec}{extra}")
        for r in it[~it.in_openalex & it.in_window].head(3).itertuples():
            L.append(f"  - [not_in_openalex] {r.trove_year or ''} {r.trove_title[:80]} ({r.trove_type})")
        L.append("")
    OUT.with_suffix(".md").write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT.with_suffix(".md"))
    print("\n".join(L[:14]))


if __name__ == "__main__":
    main()
