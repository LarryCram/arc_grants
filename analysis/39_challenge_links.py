"""
analysis/39_challenge_links.py -- a challenge to the linked ACIFs (2026-10-24, user: "challenge the linked acifs.
Report at least 10 problematic linkages in these categories - mislinked grants, mislinked arc names, mislinked oax
names, mislinked oax author_idx, mislinked works").

Read-only; each test flags candidates for review, ranked by a score, not decisions:
  1 grants      a grant that does not fit the person's linked publication record: the grant began 5+ years before
                the person's first accepted work, or none of the person's accepted works lies in an OpenAlex field
                of the grant's FOR codes (persons with 20+ accepted works). For a grant 5+ years early, free same-name
                records with Australian works in the grant's years are listed (an earlier-career fragment the linker
                missed); a grant so explained, whose fields the accepted works do reach, ranks lower
  2 arc names   two records in one ACIF whose ARC names conflict: full first given names that are neither equal,
                separator / one-letter spelling variants, nicknames / clipped forms (names.given_names_related) nor
                one the other's middle name; an initials-only record whose initials fit none of the other's given
                names; or family names with nothing in common. Then, weaker: two given forms that differ only as
                nickname / clipped form / initials, in an ACIF with no ORCID (the join rests on names and grant
                evidence alone). Not flagged (2026-10-24, the 9 cases reviewed were all one person): a pair joined by
                a hand merge; a pair whose records carry the same ARC ORCID; a pair ARC itself ties together (one
                record of the ACIF carries a name form of each -- both names on one grant); and a weak pair whose
                given names are in the nicknames list (Ray/Raymond) or whose records share an administering
                university or a co-investigator
  3 oax names   an accepted OpenAlex record whose name shares no parser key with the ACIF's names (initial keys
                included), or whose full first given name conflicts with every ACIF full given name while its
                family name is none of the ACIF's (hand links left out)
  4 author_idx  an accepted record none of whose works carries an Australian affiliation on the person's
                authorship (listed first), or whose works in the person's oeuvre are mostly rejected by the works
                filter (10+ works)
  5 works       an accepted work whose printed byline for the person conflicts with the ACIF's names (the ACIF's
                family name absent, or no given initial / related given name in common) when such bylines are a
                minority (< 20%) of the person's accepted works -- a byline used on most works is the person's
                publishing name; or a work that appeared 40+ years before the person's first grant
Names are compared only through the parser's output (arc_names.parquet for ARC records, openalex_authors_prep for
OpenAlex records, HumanNameParser for printed bylines). Writes processed/linkage_challenges.md and one CSV per test
in processed/linkage_challenges/.
"""

import csv
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, OAX_LINK_DIR, OEUVRE_DIR, OPENALEX_COMPACT_DIR, PROCESSED_DATA
from src.acif.build import DATA_PERSISTED, load_grant_for2020_codes
from src.utils.for_resolve import oax_field_name
from src.utils.names import HumanNameParser, given_names_related

OUT = PROCESSED_DATA / "linkage_challenges.md"
CSV_DIR = PROCESSED_DATA / "linkage_challenges"
TOP = 15
P = HumanNameParser()


def full_given(keys) -> set[str]:
    """Full (2+ letter) given parts of parser keys."""
    return {k.split("_", 1)[0] for k in keys if "_" in k and len(k.split("_", 1)[0]) > 1}


def families(keys) -> set[str]:
    return {k.split("_", 1)[1] for k in keys if "_" in k}


def related_any(a: set[str], b: set[str]) -> bool:
    return any(given_names_related(x, y) for x in a for y in b)


def md_table(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return ["(none)"]
    return [df.to_markdown(index=False)]


def load():
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    a = pd.read_parquet(ACIFS_ARC)
    a = a[~a.excluded].set_index("cluster_id")
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    acc = acc[acc.cluster_id.isin(a.index)]
    rec = pd.read_parquet(ACIF_ARC_RECORDS)
    rec = rec[rec.cluster_id.isin(set(acc.cluster_id))]
    names = pd.read_parquet(PROCESSED_DATA / "arc_names.parquet").set_index("unique_id")
    con.register("lk", acc[["cluster_id"]].drop_duplicates())
    w = con.execute(f"""
        SELECT k.cluster_id, k.work_idx, k.decision, s.publication_year AS y, s.title, s.authors_count, s.dominant_field,
               [x.author_idx FOR x IN s.authorships] AS own_idx,
               [x.printed_name FOR x IN s.authorships] AS printed,
               list_contains(flatten([[i.country FOR i IN x.institutions] FOR x IN s.authorships]), 'AU') AS au
        FROM read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') k
        JOIN read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s USING (cluster_id, work_idx)
        WHERE k.cluster_id IN (SELECT cluster_id FROM lk)""").df()
    con.register("ax", acc[["author_idx"]].drop_duplicates())
    oax = con.execute(f"""SELECT author_idx, full_name, full_name_keys, orcid FROM read_parquet('{PROCESSED_DATA}/openalex_authors_prep.parquet')
                          WHERE author_idx IN (SELECT author_idx FROM ax)""").df().set_index("author_idx")
    return a, acc, rec, names, w, oax


def who(a, cid) -> str:
    r = a.loc[cid]
    return f"{'; '.join(r.full_names)} ({r.n_grants} grants {int(r.first_year)}-{int(r.last_year)})"


def test_grants(a, rec, w) -> pd.DataFrame:
    acc_w = w[w.decision == "accept"]
    first = acc_w[acc_w.y >= 1950].groupby("cluster_id").y.min()
    n_acc = acc_w.groupby("cluster_id").size()
    fields = acc_w.groupby(["cluster_id", "dominant_field"]).size().rename("n").reset_index()
    fmap = {c: dict(zip(g.dominant_field, g.n)) for c, g in fields.groupby("cluster_id")}
    g4 = load_grant_for2020_codes()
    rows = []
    for r in rec.itertuples():
        n = int(n_acc.get(r.cluster_id, 0))
        if n < 10 or pd.isna(r.funding_commence_year):
            continue
        gy = int(r.funding_commence_year)
        gap = int(first.get(r.cluster_id, gy)) - gy
        gfields = {oax_field_name(e["code"]) for e in g4.get(r.grant_code, [])} - {None}
        in_fields = sum(fmap.get(r.cluster_id, {}).get(f, 0) for f in gfields)
        no_field = bool(gfields) and n >= 20 and in_fields == 0
        if gap >= 5 or no_field:
            rows.append({"cluster_id": r.cluster_id, "grant": r.grant_code, "grant_year": gy, "role": r.role_code,
                         "admin_org": r.admin_org, "first_work": int(first.get(r.cluster_id, 0)), "years_before_first_work": gap,
                         "grant_fields": "; ".join(sorted(gfields)), "accepted_works": n,
                         "works_in_grant_fields": in_fields, "score": max(gap, 0) + 10 * no_field})
    return pd.DataFrame(rows).sort_values("score", ascending=False) if rows else pd.DataFrame()


def early_fragments(a, acc, flags: pd.DataFrame) -> pd.DataFrame:
    """For grants flagged as 5+ years before the first accepted work: same-name OpenAlex pool records accepted by no
    ACIF that hold works with an Australian affiliation from 6 years before to 3 after the grant year -- an
    earlier-career fragment of the person that the linker missed explains the gap better than a wrong link (user,
    2026-10-24: Yabsley's record is right; his earlier papers sit on another record). Same name = the record's
    family name is one of the ACIF's and its first given name is one of the ACIF's full given names, or a bare
    initial matching one."""
    f = flags[flags.years_before_first_work >= 5]
    if f.empty:
        return pd.DataFrame(columns=["cluster_id", "grant", "fragments", "fragment_works"])
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    con.execute("SET preserve_insertion_order=false")
    pool = con.execute(f"SELECT author_idx, full_name, family_name_main, first_name_canonical "
                       f"FROM read_parquet('{PROCESSED_DATA}/openalex_authors_prep.parquet')").df()
    taken = set(acc.author_idx)
    cand = []
    for cid in f.cluster_id.unique():
        keys = set(a.loc[cid].full_name_keys)
        full, fams = full_given(keys), families(keys)
        ini = {k.split("_", 1)[0][0] for k in keys if "_" in k and k.split("_", 1)[0]}
        p = pool[pool.family_name_main.isin(fams) & ~pool.author_idx.isin(taken)]
        p = p.loc[[(g in full) or (isinstance(g, str) and len(g) == 1 and g in ini) for g in p.first_name_canonical]]
        cand += [(cid, x, n) for x, n in zip(p.author_idx, p.full_name)]
    c = pd.DataFrame(cand, columns=["cluster_id", "author_idx", "full_name"])
    con.register("c", c[["author_idx"]].drop_duplicates())
    con.execute(f"""CREATE TEMP TABLE au AS SELECT DISTINCT author_idx, work_idx, country_code = 'AU' AS au
                    FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet') WHERE author_idx IN (SELECT author_idx FROM c)""")
    yw = con.execute(f"""SELECT au.author_idx, au.work_idx, bool_or(au.au) AS au, any_value(w.publication_year) AS y
                         FROM au JOIN read_parquet('{OPENALEX_COMPACT_DIR}/works/*.parquet') w USING (work_idx)
                         GROUP BY 1, 2""").df()
    rows = []
    for r in f.itertuples():
        ids = c[c.cluster_id == r.cluster_id]
        x = yw[yw.author_idx.isin(ids.author_idx) & yw.au & yw.y.between(r.grant_year - 6, r.grant_year + 3)]
        n = x.groupby("author_idx").work_idx.nunique().sort_values(ascending=False)
        n = n[n >= 2]
        nm = dict(zip(ids.author_idx, ids.full_name))
        rows.append({"cluster_id": r.cluster_id, "grant": r.grant, "fragments": len(n),
                     "fragment_works": "; ".join(f"A{i} '{nm[i]}' {k}" for i, k in n.head(3).items())})
    return pd.DataFrame(rows)


def _same_spelling(x: str, y: str) -> bool:
    """Separator or one-letter spelling variants of one given name (Qing-Hua/Qinghua, Jeffrey/Jeffery)."""
    x, y = x.replace("-", "").replace(" ", ""), y.replace("-", "").replace(" ", "")
    return x == y or (min(len(x), len(y)) >= 4 and SequenceMatcher(None, x, y).ratio() >= 0.85)


def test_arc_names(a, rec, names) -> pd.DataFrame:
    hand = set()
    with open(DATA_PERSISTED / "manual_merges.csv", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            hand.add(frozenset((r["cluster_keep"].strip(), r["cluster_drop"].strip())))
    allrec = pd.read_parquet(ACIF_ARC_RECORDS, columns=["unique_id", "cluster_id", "grant_code", "admin_org", "orcid"])
    by_grant = allrec.groupby("grant_code").cluster_id.apply(set).to_dict()
    info = {u: (o if isinstance(o, str) and o else None, adm, by_grant.get(g, set()) - {c})
            for u, c, g, adm, o in zip(allrec.unique_id, allrec.cluster_id, allrec.grant_code, allrec.admin_org, allrec.orcid)}

    def nickname_listed(x: str, y: str) -> bool:
        return len(x) > 1 and len(y) > 1 and given_names_related(x, y, min_clipped=99)

    rows = []
    for cid, g in rec.groupby("cluster_id"):
        if len(g) < 2:
            continue
        nm = names.reindex(g.unique_id)
        forms = [(u, set(x.given_tokens) if x.given_tokens is not None else set(), x.first_name_canonical or "",
                  set(x.family_names) if x.family_names is not None else set(), x.name_forms[0])
                 for u, x in nm.iterrows() if x.name_forms is not None and len(x.name_forms)]
        rforms = {u: set(x.name_forms) for u, x in nm.iterrows() if x.name_forms is not None}
        worst = None
        for i in range(len(forms)):
            for j in range(i + 1, len(forms)):
                u1, gt1, f1, fam1, n1 = forms[i]
                u2, gt2, f2, fam2, n2 = forms[j]
                o1, adm1, co1 = info.get(u1, (None, None, set()))
                o2, adm2, co2 = info.get(u2, (None, None, set()))
                tied = (frozenset((u1, u2)) in hand or (o1 is not None and o1 == o2)
                        or any(fs & rforms[u1] and fs & rforms[u2] for fs in rforms.values()))
                if tied:
                    continue
                if len(f1) > 1 and len(f2) > 1:
                    given_conflict = not (given_names_related(f1, f2) or _same_spelling(f1, f2)
                                          or f1 in gt2 or f2 in gt1)
                else:   # an initials-only record: its initials against the other's given names
                    given_conflict = bool(gt1 and gt2) and not ({t[0] for t in gt1} & {t[0] for t in gt2})
                family_conflict = not (fam1 & fam2) and not any(_same_spelling(x, y) for x in fam1 for y in fam2)
                # weaker: the two given forms differ (nickname, clipped form, initials only) and no ORCID backs the ACIF
                weak = (not given_conflict and not family_conflict and f1 != f2 and not _same_spelling(f1, f2)
                        and not len(a.loc[cid].orcids)
                        and not nickname_listed(f1, f2) and adm1 != adm2 and not (co1 & co2))
                sc = 2 * given_conflict + 2 * family_conflict + weak
                if sc and (worst is None or sc > worst[0]):
                    worst = (sc, u1, u2, n1, n2, given_conflict, family_conflict, weak)
        if worst:
            sc, u1, u2, n1, n2, gc, fc, wk = worst
            rows.append({"cluster_id": cid, "record_1": u1, "name_1": n1, "record_2": u2, "name_2": n2,
                         "given_conflict": gc, "family_conflict": fc, "differs_without_orcid": wk,
                         "orcid_sources": "; ".join(a.loc[cid].orcid_sources), "score": sc + gc * fc})
    cols = ["cluster_id", "record_1", "name_1", "record_2", "name_2", "given_conflict", "family_conflict",
            "differs_without_orcid", "orcid_sources", "score"]
    return pd.DataFrame(rows, columns=cols).sort_values(["score", "cluster_id"], ascending=[False, True])


def test_oax_names(a, acc, oax) -> pd.DataFrame:
    rows = []
    for r in acc[acc.stage != "hand"].itertuples():
        if r.author_idx not in oax.index:
            continue
        o = oax.loc[r.author_idx]
        ak, ok = set(a.loc[r.cluster_id].full_name_keys), set(o.full_name_keys) if o.full_name_keys is not None else set()
        no_key = not (ak & ok)
        ag, og = full_given(ak), full_given(ok)
        given_conflict = bool(ag and og) and not related_any(ag, og) and not (families(ak) & families(ok))
        if no_key or given_conflict:
            rows.append({"cluster_id": r.cluster_id, "acif_names": "; ".join(a.loc[r.cluster_id].full_names),
                         "author_idx": f"A{r.author_idx}", "oax_name": o.full_name, "stage": r.stage, "status": r.status,
                         "works": int(r.works_count_global or 0), "no_shared_key": no_key, "given_conflict": given_conflict,
                         "score": 2 * no_key + given_conflict})
    return pd.DataFrame(rows).sort_values(["score", "works"], ascending=False) if rows else pd.DataFrame()


def test_author_idx(a, acc, w, oax) -> pd.DataFrame:
    x = w[["cluster_id", "work_idx", "decision", "au", "own_idx"]].explode("own_idx").rename(columns={"own_idx": "author_idx"})
    x = x.dropna(subset=["author_idx"]).astype({"author_idx": "int64"})
    x = x.merge(acc[["cluster_id", "author_idx", "stage"]], on=["cluster_id", "author_idx"])
    g = x.groupby(["cluster_id", "author_idx", "stage"]).agg(n=("work_idx", "nunique"),
                                                             reject=("decision", lambda s: (s == "reject").mean()),
                                                             au=("au", "mean")).reset_index()
    g = g[(g.n >= 10) & ((g.reject >= 0.5) | (g.au == 0))].copy()
    g["oax_name"] = g.author_idx.map(oax.full_name)
    g["acif"] = g.cluster_id.map(lambda c: who(a, c))
    g["no_au"] = g.au == 0
    g["score"] = g.n * g.reject + 10_000 * g.no_au
    g["author_idx"] = "A" + g.author_idx.astype(str)
    return g.sort_values("score", ascending=False).round({"reject": 2, "au": 2})


def _tokens(keys) -> tuple[set[str], set[str]]:
    """(family parts, given parts) of parser keys, families split on spaces and hyphens and also compacted."""
    fam, giv = set(), set()
    for k in keys:
        if "_" not in k:
            continue
        g, f = k.split("_", 1)
        fam |= set(re.split(r"[ -]", f)) | {re.sub(r"[ -]", "", f)}
        giv |= set(re.split(r"[ -]", g))
    return fam - {""}, giv - {""}


def test_works(a, w) -> pd.DataFrame:
    acc_w = w[w.decision == "accept"].copy()
    cache = {}

    def keys(name):
        if name not in cache:
            cache[name] = set(P.parse(name).full_name_keys) if isinstance(name, str) and name.strip() else set()
        return cache[name]
    afam = {c: _tokens(k) for c, k in a.full_name_keys.items()}

    def conflict(cid, printed) -> bool:
        ks = set().union(*(keys(p) for p in printed)) if printed is not None and len(printed) else set()
        if not ks:
            return False
        fam, giv = afam[cid]
        bf, bg = _tokens(ks)
        btok = bf | bg
        if not (btok & fam):
            return True                                   # the ACIF's family name is not on the byline
        rest = btok - fam
        if not rest or not giv:
            return False
        if {t[0] for t in rest} & {t[0] for t in giv}:
            return False
        return not any(given_names_related(x, y) for x in rest if len(x) > 1 for y in giv if len(y) > 1)
    acc_w["conflict"] = [conflict(c, p) for c, p in zip(acc_w.cluster_id, acc_w.printed)]
    share = acc_w.groupby("cluster_id").conflict.mean()
    first = a.first_year
    acc_w["early"] = acc_w.y < acc_w.cluster_id.map(first) - 40
    acc_w["minority_conflict"] = acc_w.conflict & (acc_w.cluster_id.map(share) < 0.2)
    df = acc_w[acc_w.minority_conflict | acc_w.early].copy()
    df["acif_names"] = df.cluster_id.map(lambda c: "; ".join(a.loc[c].full_names))
    df["printed_name"] = df.printed.map(lambda p: "; ".join(p))
    df["work_idx"] = "W" + df.work_idx.astype(str)
    df["title"] = df.title.astype(str).str[:80]
    df["acif_conflict_share"] = df.cluster_id.map(share).round(3)
    per = df.groupby("cluster_id").size()
    df["acif_flagged_works"] = df.cluster_id.map(per)
    cols = ["cluster_id", "acif_names", "work_idx", "y", "title", "printed_name", "authors_count",
            "minority_conflict", "early", "acif_flagged_works", "acif_conflict_share"]
    return df[cols].rename(columns={"y": "year"}).sort_values(["acif_flagged_works", "cluster_id", "year"],
                                                              ascending=[False, True, True])


def main():
    a, acc, rec, names, w, oax = load()
    CSV_DIR.mkdir(exist_ok=True)
    grants = test_grants(a, rec, w)
    if len(grants):
        grants = grants.merge(early_fragments(a, acc, grants), on=["cluster_id", "grant"], how="left")
        grants["fragments"] = grants.fragments.fillna(0).astype(int)
        # a grant explained by a free same-name record with Australian works in its years ranks below the rest
        grants["score"] -= 5 * (grants.fragments > 0) * (grants.works_in_grant_fields > 0)
        grants = grants.sort_values("score", ascending=False)
    tests = [("1 Mislinked grants", "grants", grants),
             ("2 Mislinked ARC names", "arc_names", test_arc_names(a, rec, names)),
             ("3 Mislinked OpenAlex names", "oax_names", test_oax_names(a, acc, oax)),
             ("4 Mislinked OpenAlex records (author_idx)", "author_idx", test_author_idx(a, acc, w, oax)),
             ("5 Mislinked works", "works", test_works(a, w))]
    L = ["# Challenge to the linked ACIFs (2026-10-24)", "",
         f"Linked kept ACIFs: {acc.cluster_id.nunique():,}; accepted links: {len(acc):,}. Each section lists the "
         f"top {TOP} flags by score (all flags in `{CSV_DIR.name}/<test>.csv`). Flags are candidates for review, "
         "not decisions; see the script's docstring for each test.", ""]
    for title, key, df in tests:
        df.to_csv(CSV_DIR / f"{key}.csv", index=False)
        n_acif = df.cluster_id.nunique() if len(df) else 0
        L += [f"## {title}", "", f"Flags: {len(df):,} rows, {n_acif:,} ACIFs.", ""]
        top = df.head(TOP) if key != "works" else df.groupby("cluster_id").head(3).head(TOP)
        if key in ("grants", "arc_names") and len(top):
            top = top.assign(acif=top.cluster_id.map(lambda c: who(a, c)))
        L += md_table(top.drop(columns=[c for c in ("score",) if c in top.columns])) + [""]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
