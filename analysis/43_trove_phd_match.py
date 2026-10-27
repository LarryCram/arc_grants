"""
analysis/43_trove_phd_match.py -- PhD thesis (year, university) for ARC investigators from the Trove thesis
harvest (src/00i_extract_trove_theses.py), checked against the PhD years people give on their ORCID records
(2026-10-27, user: "do it" -- after the 1990s-2020s harvest). A test and a first table, not a pipeline stage.

Theses: every Australian-content thesis Trove holds for the 1990s-2020s (487,031 distinct). Cleaned:
  - Deakin repository-only records dropped (VDU:IR, 132,010: staff research outputs typed "monograph | thesis");
  - works with an article / journal / conference / book type, or 2+ authors, dropped;
  - degree from the records' type and short notes (00i's degree: doctoral / masters / honours / unknown);
    masters and honours kept in the table but never matched;
  - thesis year = year_start (the earliest date on the work; a range is the spread of its library records'
    dates, e.g. a 1987 thesis digitised in 2026).
University of a thesis: (a) an organisation among the record's creators ("University of Wollongong. Faculty of
Engineering"): the text before the first full stop, normalised as in 00e (norm_org) and looked up in 00e's HEP
name table (hep_names.parquet); (b) a repository code (metadataSource, e.g. SAU:IR): the HEP its theses name
through (a) on 90%+ of 20+ theses -- derived here, listed in the report -- plus codes confirmed by the user in
data_persisted/trove_source_hep.csv (2026-10-27). Old university names (James Cook University of North Queensland,
UWS Nepean, Ballarat University College, ...) were added to HEP_concordances.xlsx Variants the same day.
Names (name_pairs()): every person-name form of the thesis author, parsed with NameParser, against the ACIF under a
shared family name -- 'key' (a full-given-name key in common), 'related' (nickname or clipped given name, or given
names equal without spaces; user, 2026-10-27: the exact key was too strict -- Jan/Janice Turbill, Tim/Timothy Slade,
Han Lin/Hanlin Shang) or 'initials' (thesis author with initials only, first initial matching; needs own-works
confirmation).
Evidence (beside the name): the ACIF's own accepted works name the thesis university in the 4 years before
the thesis year to 1 after (OpenAlex institution -> ROR -> 00e's HEP crosswalk), or the name is carried by no other
ACIF and gives only one candidate thesis. One passing thesis is accepted; among 2+ passing, the one the person's own
works confirm, then a doctoral label, then the later year (accepted only if strictly ahead of the next).
Truth: the ACIF's ORCID record (cache) -- a doctoral education entry with an end year; an Australian one for the
recall test (Trove only holds Australian theses).
Timing: thesis year -2..+8 from the start of the ACIF's main run of accepted works (leading works 5+ years
before the next skipped: main_run_start()), for everyone; against grants, none for CIs (user, 2026-10-26: a CI need not hold a PhD before a grant); for fellows the thesis is no
later than the first fellowship's announcement year + 1, and for early-career fellows no earlier than 8 years
before it (APD ~2, DECRA ~4 years after the PhD; career interruptions allowed).
Later the same day (user: "yes", after the 2014 DECRA cohort review): own works count when accepted or unsure (the
early works of thinly covered people are unsure); an own work within 6 years whose title echoes the thesis title
confirms it like the university does; OpenAlex dissertations on the person's own works join the ORCID thesis titles in
the title route, and one with only non-Australian institutions is overseas evidence, with an ORCID PhD abroad and an
early Australian share of 0; for early-career fellows the eligibility window replaces the gap test unless there is
overseas evidence (Haroro Ingram, Tom Murray: theses 3 and 8 years before their first accepted work); duplicate Trove
works of one thesis are merged (they tied with themselves).
Hand PhD findings (user, 2026-10-27): data_persisted/manual_phd.csv (unique_id, status confirmed/probable, abroad yes/no,
university, country, year, thesis_title, source, notes), keyed on an ARC record id; an abroad row is overseas evidence,
and a confirmed row overrules -- abroad takes no Trove thesis, Australian with a year takes only one within a year. The
early Australian share starts at the first year whose works carry an institution.

Writes processed/trove_theses/acif_trove_phd.parquet (one row per ACIF x candidate thesis, with the features and
the decision) and processed/trove_theses/trove_phd_match.md.
"""

import glob
import importlib
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import diskcache
import pandas as pd

from config.scope import FELLOWSHIP_TIER
from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, DISKCACHE_DIR, PROCESSED_DATA
from src.utils.names import HumanNameParser, given_names_related

TH_DIR = PROCESSED_DATA / "trove_theses"
OUT = TH_DIR / "acif_trove_phd.parquet"
REPORT = TH_DIR / "trove_phd_match.md"
SOURCE_HEP = Path(__file__).resolve().parents[1] / "data_persisted" / "trove_source_hep.csv"
MANUAL_PHD = Path(__file__).resolve().parents[1] / "data_persisted" / "manual_phd.csv"
ARTICLE_TYPES = {"article", "journal article", "Journal contribution", "Conference paper", "conference paper", "paper"}
THESIS_TYPE = re.compile(r"thesis|doctor|ph\.?d", re.I)
NOT_THESIS = {"article", "journal article", "Journal contribution", "Conference paper", "conference paper", "paper",
              "Book", "book", "still image", "speaking", "spoken word"}
ECR_BACK = 8
OWN_BACK, OWN_AHEAD = 4, 1     # own accepted works naming the thesis university, thesis year -4 .. +1
NO_CLIPPED = 99                # nicknames list only (clipped forms paired Chinese syllables: Ling/Lingxue)
RELATION_RANK = {"key": 0, "related": 1, "compound": 2, "initials": 3, "title": 4}
TITLE_WORDS = 8                # ORCID thesis title vs Trove title: first 8 normalised words
GAP_MIN, GAP_MAX = -2, 8
ISOLATED = 5                   # a leading work 5+ years before the next is not the start of the main run       # thesis year - first accepted work (ORCID-dated PhDs: median +2..+3, user's pppp/ddd model)
EARLY_YEARS = 4                # early Australian share: the first 4 years of the main run
TITLE_SHARED = 4               # own work title vs thesis title: 4+ shared content words
OWN_TITLE_BACK, OWN_TITLE_AHEAD = 6, 6   # own works within 6 years of the thesis year (papers from it, the book of it)
norm_org = importlib.import_module("src.00e_extract_orcid_bulk").norm_org
P = HumanNameParser()
PHD = re.compile(r"\b(ph\.?\s?d|doctor|doctorate|dphil|d\.phil|dr\.?\s?rer|doktor|doctorat|dottorato|doctoral)\b", re.I)


def full(keys) -> set[str]:
    return {k for k in keys if "_" in k and len(k.split("_", 1)[0]) > 1}


def load_theses() -> pd.DataFrame:
    d = pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(str(TH_DIR / "theses_au_*.parquet")))])
    d = d.drop_duplicates("work_id").reset_index(drop=True)
    d["deakin_only"] = d.sources.map(lambda s: set(s) == {"VDU:IR"})
    # a book / still-image type alone does not make a record a non-thesis when a version is typed thesis (2026-10-27:
    # Cameron Holley's ANU PhD shares a Trove work with the book of it; theses with figures carry "still image")
    d["not_thesis_type"] = d.types.map(lambda t: bool(set(t) & ARTICLE_TYPES)
                                       or (bool(set(t) & NOT_THESIS) and not any(THESIS_TYPE.search(x) for x in t)))
    d["n_authors"] = d.authors.map(len)
    d["clean"] = ~d.deakin_only & ~d.not_thesis_type & (d.n_authors == 1)
    d["year"] = d.year_start
    return d


def hep_names() -> pd.DataFrame:
    """00e's HEP crosswalk (key, kind name|ror, hep_code), built in process so that new HEP_concordances.xlsx
    Variants count without rerunning 00e."""
    import duckdb
    return importlib.import_module("src.00e_extract_orcid_bulk").hep_crosswalk(duckdb.connect())


def thesis_universities(d: pd.DataFrame, names: pd.DataFrame) -> tuple[pd.Series, pd.DataFrame]:
    by_name = names[names.kind == "name"].groupby("key").hep_code.agg(lambda s: sorted(set(s)))
    by_name = {k: v[0] for k, v in by_name.items() if len(v) == 1}
    def from_creators(cr):
        hits = {by_name.get(norm_org(re.split(r"\.(?:\s|$)", c)[0])) for c in cr} - {None}
        return hits.pop() if len(hits) == 1 else None
    via_creator = d.creators.map(from_creators)
    # repository code -> HEP, from theses carrying both
    co = Counter()
    tot = Counter()
    for srcs, h in zip(d.sources, via_creator):
        if pd.isna(h):
            continue
        for s in srcs:
            if ":" in s:
                co[(s, h)] += 1
                tot[s] += 1
    rows = []
    for (s, h), n in co.items():
        rows.append((s, h, n, tot[s], n / tot[s]))
    m = pd.DataFrame(rows, columns=["source", "hep_code", "n", "n_source", "share"]).sort_values(["source", "n"], ascending=[True, False])
    src_map = {r.source: r.hep_code for r in m.itertuples() if r.share >= 0.9 and r.n_source >= 20}
    hand = pd.read_csv(SOURCE_HEP)
    src_map.update(dict(zip(hand.source, hand.hep_code)))   # user-confirmed codes for the rest
    def from_sources(srcs):
        hits = {src_map.get(s) for s in srcs} - {None}
        return hits.pop() if len(hits) == 1 else None
    uni = via_creator.where(via_creator.notna(), d.sources.map(from_sources))
    m = m.assign(used=m.source.map(src_map).eq(m.hep_code) & m.source.isin(src_map), by_hand=False)
    hand_rows = hand[~hand.source.isin(m[m.used].source)].assign(n=0, n_source=0, share=float("nan"), used=True, by_hand=True)
    return uni, pd.concat([m, hand_rows[m.columns]], ignore_index=True)


def orcid_phd(orcids, cache):
    for o in (list(orcids) if orcids is not None else []):
        r = cache.get(o)
        if not r or (r.get("orcid-identifier") or {}).get("path") not in (None, o):
            continue
        ys = []
        for g in (((r.get("activities-summary") or {}).get("educations") or {}).get("affiliation-group") or []):
            for s in g.get("summaries") or []:
                e = s.get("education-summary") or {}
                end = ((e.get("end-date") or {}).get("year") or {}).get("value")
                if PHD.search(e.get("role-title") or "") and end:
                    ys.append((int(end), ((e.get("organization") or {}).get("address") or {}).get("country")))
        if ys:
            return min(ys)
    return (None, None)


def own_work_heps(names: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """(cluster_id, year, hep_code) for every ARC university on the ACIF's own authorships of its accepted and unsure
    works (OpenAlex institution -> ROR -> 00e's HEP crosswalk, which takes faculties and institutes through the
    lineage; unsure counted since 2026-10-27: thinly covered people's early works are unsure -- Haroro Ingram's
    2006 Monash paper, Charlotte Greenhalgh's Oxford ones), the start of each ACIF's main run of accepted works, and
    the early Australian share: over the accepted and unsure works of the first EARLY_YEARS years of their main run,
    the mean share of the ACIF's own institutions that are Australian (works with no institution left out)."""
    import duckdb
    from config.settings import OEUVRE_DIR, OPENALEX_DIR
    ror = importlib.import_module("src.00e_extract_orcid_bulk")._ror
    ror_hep = dict(zip(names[names.kind == "ror"].key, names[names.kind == "ror"].hep_code))
    con = duckdb.connect()
    inst = con.execute(f"SELECT institution_idx, ror FROM read_parquet('{OPENALEX_DIR}/institutions.parquet') "
                       "WHERE ror IS NOT NULL").df()
    inst["hep_code"] = inst.ror.map(lambda r: ror_hep.get(ror(r)))
    con.register("ih", inst.dropna(subset=["hep_code"])[["institution_idx", "hep_code"]])
    own = f"""SELECT s.cluster_id, s.work_idx, s.publication_year AS y, k.decision,
                     flatten(list_transform(list_filter(s.authorships, a -> a.author_idx IS NOT NULL
                                                        OR a.link_status IS NOT NULL), a -> a.institutions)) AS inst
              FROM read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s
              JOIN read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') k USING (cluster_id, work_idx)
              WHERE k.decision IN ('accept', 'unsure') AND s.publication_year IS NOT NULL"""
    w = con.execute(f"""
        WITH i AS (SELECT cluster_id, y, unnest(inst).institution_idx AS institution_idx FROM ({own}))
        SELECT DISTINCT i.cluster_id, i.y AS year, ih.hep_code FROM i JOIN ih USING (institution_idx)""").df()
    years = con.execute(f"""
        SELECT cluster_id, list(DISTINCT y ORDER BY y) FILTER (WHERE decision = 'accept') AS ys,
               list(DISTINCT y ORDER BY y) FILTER (WHERE len(list_filter(list_transform(inst, i -> i.country),
                                                                   x -> x IS NOT NULL)) > 0) AS ys_inst
        FROM ({own}) GROUP BY 1""").df()
    first = pd.Series({c: main_run_start(list(ys)) for c, ys in zip(years.cluster_id, years.ys) if ys is not None and len(ys)})
    # the early window starts at the first year with an institution (2026-10-27: Margaretha van der Merwe's 1989-91 works
    # carry none, which left her share empty though her early institutions are all South African)
    yi = years[years.ys_inst.map(lambda ys: not isinstance(ys, type(pd.NA)) and ys is not None and len(ys) > 0)]
    start_all = pd.DataFrame({"cluster_id": yi.cluster_id, "y0": [main_run_start(list(ys)) for ys in yi.ys_inst]})
    con.register("st", start_all)
    sh = con.execute(f"""
        WITH o AS (SELECT cluster_id, work_idx, y, list_transform(inst, i -> i.country) AS c FROM ({own}))
        SELECT o.cluster_id, avg(len(list_filter(c, x -> x = 'AU')) / len(c)) AS au_share
        FROM o JOIN st USING (cluster_id)
        WHERE len(list_filter(c, x -> x IS NOT NULL)) > 0 AND o.y BETWEEN st.y0 AND st.y0 + {EARLY_YEARS - 1}
        GROUP BY 1""").df()
    return w, first, pd.Series(sh.au_share.values, index=sh.cluster_id)


def own_titles() -> pd.DataFrame:
    """(cluster_id, year, title, type, countries) of every accepted or unsure work of each ACIF -- for the title
    evidence and the OpenAlex dissertations (2026-10-27)."""
    import duckdb
    from config.settings import OEUVRE_DIR
    return duckdb.connect().execute(f"""
        SELECT s.cluster_id, s.publication_year AS year, s.title, s.type,
               list_distinct(list_filter(list_transform(flatten(list_transform(list_filter(s.authorships,
                   a -> a.author_idx IS NOT NULL OR a.link_status IS NOT NULL), a -> a.institutions)), i -> i.country),
                   x -> x IS NOT NULL)) AS countries
        FROM read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s
        JOIN read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') k USING (cluster_id, work_idx)
        WHERE k.decision IN ('accept', 'unsure') AND s.title IS NOT NULL""").df()


STOP = {"the", "and", "for", "from", "with", "into", "their", "this", "that", "study", "analysis", "role", "case",
        "towards", "toward", "between", "within", "through", "using", "under", "among"}


def content_words(t) -> set[str]:
    """Content words (4+ letters, not STOP) of the first 12 words of a title, normalised as in title_key."""
    import unicodedata
    t = re.split(r" / ", str(t))[0]
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode().lower()
    return {x for x in re.sub(r"[^a-z0-9 ]+", " ", t).split()[:12] if len(x) >= 4 and x not in STOP}


def titles_similar(a: set[str], b: set[str]) -> bool:
    """Two titles share TITLE_SHARED+ content words making up 70%+ of the shorter one ("The transformative charisma
    phenomenon in Islamic radicalism and militancy" -- Ingram's 2006 paper -- and his 2011 thesis on "Islamist
    radicalism and militancy")."""
    n = len(a & b)
    return n >= TITLE_SHARED and n >= 0.7 * min(len(a), len(b))


def main_run_start(years: list[int]) -> int:
    """First year of the main run of works (2026-10-27): leading works more than ISOLATED years before the next are
    skipped -- OpenAlex's 1970 date placeholder (Matt Allen's 2010 paper dated 1970), stray or another person's early
    papers, and pre-PhD papers before the PhD run."""
    i = 0
    while i + 1 < len(years) and years[i + 1] - years[i] >= ISOLATED:
        i += 1
    return years[i]


def manual_phd(rec: pd.DataFrame) -> pd.DataFrame:
    """data_persisted/manual_phd.csv (2026-10-27, user: PhD findings added by hand, from CVs, staff pages and web
    searches), keyed on an ARC record id so it survives ACIF relabels, indexed here by the record's current ACIF.
    status confirmed / probable; abroad yes / no. A row whose record id is not in scope stops the run."""
    h = pd.read_csv(MANUAL_PHD, dtype={"year": "Int64"})
    cid = dict(zip(rec.unique_id, rec.cluster_id))
    missing = sorted(set(h.unique_id) - set(cid))
    if missing:
        raise SystemExit(f"manual_phd.csv: record ids not in acif_arc_records: {missing}")
    bad = h[~h.status.isin(["confirmed", "probable"]) | ~h.abroad.isin(["yes", "no"])]
    if len(bad):
        raise SystemExit(f"manual_phd.csv: status must be confirmed/probable and abroad yes/no: {bad.unique_id.tolist()}")
    h["cluster_id"] = h.unique_id.map(cid)
    if h.cluster_id.duplicated().any():
        raise SystemExit(f"manual_phd.csv: 2+ rows for one ACIF: {h[h.cluster_id.duplicated(keep=False)].unique_id.tolist()}")
    return h.set_index("cluster_id")


def acifs() -> pd.DataFrame:
    a = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "full_name_keys", "full_names", "orcids", "hep_codes", "first_year",
                                            "excluded"])
    a = a[~a.excluded].copy()
    rec = pd.read_parquet(ACIF_ARC_RECORDS)
    f = rec[rec.is_fellowship.fillna(False) & ~rec.not_taken_up.fillna(False) & rec.role_code.isin(FELLOWSHIP_TIER)]
    f = f.assign(tier=f.role_code.map(FELLOWSHIP_TIER), award=f.funding_commence_year - 1)
    first = f.sort_values("award").drop_duplicates("cluster_id").set_index("cluster_id")
    a["first_fellow_award"] = a.cluster_id.map(first.award)
    a["first_fellow_tier"] = a.cluster_id.map(first.tier)
    a["group"] = a.first_fellow_tier.fillna("CI only")
    hand = manual_phd(rec)
    for c in ["status", "abroad", "university", "country", "year"]:
        a[f"hand_phd_{c}"] = a.cluster_id.map(hand[c])
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    op = a.orcids.map(lambda o: orcid_phd(o, cache))
    a["orcid_phd_year"] = op.map(lambda x: x[0])
    a["orcid_phd_country"] = op.map(lambda x: x[1])
    a["orcid_thesis_titles"] = a.orcids.map(lambda o: orcid_thesis_titles(o, cache))
    return a


def orcid_thesis_titles(orcids, cache) -> list[str]:
    """Titles of the works typed dissertation / dissertation-thesis on the ACIF's ORCID records (first summary of each
    work group). 9% of kept ACIFs with an ORCID list one (2026-10-27)."""
    out = []
    for o in (list(orcids) if orcids is not None else []):
        r = cache.get(o)
        if not r or (r.get("orcid-identifier") or {}).get("path") not in (None, o):
            continue
        for g in ((r.get("activities-summary") or {}).get("works") or {}).get("group", []) or []:
            for ws in (g.get("work-summary", []) or [])[:1]:
                if ws.get("type") in ("dissertation-thesis", "dissertation"):
                    t = (((ws.get("title") or {}).get("title") or {}).get("value"))
                    if t:
                        out.append(t)
    return out


def title_key(t) -> str | None:
    """First TITLE_WORDS words of a title, normalised (catalogue titles carry " / author" and subtitles); None for
    titles under 4 words."""
    import unicodedata
    t = re.split(r" / ", str(t))[0]
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode().lower()
    w = re.sub(r"[^a-z0-9 ]+", " ", t).split()
    return " ".join(w[:TITLE_WORDS]) if len(w) >= 4 else None


def title_pairs(cand: pd.DataFrame, a: pd.DataFrame) -> pd.DataFrame:
    """(work_id, cluster_id) where a thesis title on the ACIF's ORCID record matches the Trove thesis title (title_key)
    and the thesis author shares a family name with the ACIF (user, 2026-10-27: the title route)."""
    idx = defaultdict(list)
    for wid, t in zip(cand.work_id, cand.title):
        k = title_key(t)
        if k:
            idx[k].append(wid)
    auth = dict(zip(cand.work_id, cand.authors))
    cache: dict = {}
    out = []
    for cid, titles, keys in zip(a.cluster_id, a.orcid_thesis_titles, a.full_name_keys):
        fams = {k.split("_", 1)[1] for k in (keys if keys is not None else []) if "_" in k}
        for t in titles:
            for wid in idx.get(title_key(t), []):
                if _parsed(auth[wid][0], cache)[0] & fams:
                    out.append((wid, cid))
    return pd.DataFrame(out, columns=["work_id", "cluster_id"]).drop_duplicates()


def _compact(tokens) -> str:
    return "".join(tokens)


def _parsed(name: str, cache: dict):
    """(family names, full given tokens in order, first initial, initials only) of a name, cached."""
    if name not in cache:
        try:
            pn = P.parse(name)
            giv = [t for t in pn.given_tokens if len(t) > 1]
            first = pn.given_tokens[0][0] if pn.given_tokens else None
            cache[name] = (set(pn.family_names), giv, first, bool(pn.given_tokens) and not giv)
        except Exception:
            cache[name] = (set(), [], None, False)
    return cache[name]


def name_pairs(cand: pd.DataFrame, a: pd.DataFrame) -> pd.DataFrame:
    """(work_id, cluster_id, name_relation) for theses whose author can be the ACIF (user, 2026-10-27: the exact-key
    match was too strict). The thesis author's name forms: the first author string plus every creator string on its
    records that shares that author's family name (fuller forms: "Turonek, Lucyna A. J. (Lucyna Anne Jadwiga)").
    Under a shared family name, the relation is the first that holds:
      key       a full given name key in common (jan_smith), as before;
      related   the first given names nickname-related (names.given_names_related with the nicknames list only --
                Jan/Janice, Tim/Timothy, Jill/Jillian; no clipped forms: they paired Chinese syllables, Ling/Lingxue,
                Jian/Jiangtao), or the whole given names equal once spaces and hyphens are removed (Han Lin / Hanlin);
      compound  the thesis author's family name is one part of the ACIF's compound family name and a full given
                name is shared (Louise Olsen-Kettle's UQ thesis as Louise Marie Kettle; 2026-10-27);
      initials  the thesis author has initials only ("Grimmer, KA") and the first initial is the first letter of one
                of the ACIF's full given names (accepted later only with own-works confirmation)."""
    cache: dict = {}
    acif_fam = defaultdict(list)
    acif_part = defaultdict(list)   # one part of a compound family name (Olsen-Kettle -> olsen, kettle)
    for cid, keys, fnames in zip(a.cluster_id, a.full_name_keys, a.full_names):
        keys = set(keys if keys is not None else [])
        fams = {k.split("_", 1)[1] for k in keys if "_" in k}
        givs = {k.split("_", 1)[0] for k in full(keys)}
        parsed = [_parsed(n, cache)[1] for n in (fnames if fnames is not None else [])]
        compacts = {_compact(g) for g in parsed} - {""}
        firsts = {g[0] for g in parsed if g}
        rec = (cid, full(keys), givs, compacts, firsts)
        for f in fams:
            acif_fam[f].append(rec)
        for f in {x for f in fams for x in re.split(r"[- ]", f) if len(x) > 2} - fams:
            acif_part[f].append(rec)
    rel_cache: dict = {}
    out = []
    for wid, authors, creators in zip(cand.work_id, cand.authors, cand.creators):
        fam0, _, _, _ = _parsed(authors[0], cache)
        if not fam0:
            continue
        forms = [authors[0]] + [c for c in (creators if creators is not None else []) if c != authors[0]
                                and _parsed(c, cache)[0] & fam0]
        best = {}
        for form in forms:
            fams, giv, first, init_only = _parsed(form, cache)
            tkeys = {f"{g}_{f}" for g in giv for f in fams}
            tcomp = _compact(giv)
            for f in fams:
                for cid, akeys, agivs, acomps, afirsts in acif_fam.get(f, []):
                    rk = (form, cid)
                    if rk not in rel_cache:
                        if tkeys & akeys:
                            r = "key"
                        elif (tcomp and (tcomp in acomps or tcomp in agivs)) or \
                                (giv and any(given_names_related(giv[0], h, NO_CLIPPED) for h in afirsts)):
                            r = "related"
                        elif init_only and first and any(h[0] == first for h in agivs):
                            r = "initials"
                        else:
                            r = None
                        rel_cache[rk] = r
                    r = rel_cache[rk]
                    if r and RELATION_RANK[r] < RELATION_RANK.get(best.get(cid), 9):
                        best[cid] = r
                for cid, akeys, agivs, acomps, afirsts in acif_part.get(f, []):
                    if set(giv) & agivs and RELATION_RANK["compound"] < RELATION_RANK.get(best.get(cid), 9):
                        best[cid] = "compound"
        out += [(wid, cid, r) for cid, r in best.items()]
    return pd.DataFrame(out, columns=["work_id", "cluster_id", "name_relation"])


def main():
    d = load_theses()
    names = hep_names()
    d["uni"], src_map = thesis_universities(d, names)
    cand = d[d.clean & ~d.degree.isin(["masters", "honours"]) & d.year.notna()]
    # one thesis held as 2+ Trove works (2026-10-27: Paul Stanwix's and Muhammad Hossain's UWA theses, one record with
    # and one without " / author" in the title, tied with themselves and were refused): same normalised title, year
    # and author family name -> one candidate (doctoral label, then a university found, then the lowest work_id)
    pc: dict = {}
    cand = cand.assign(_t=cand.title.map(title_key),
                       _f=cand.authors.map(lambda x: " ".join(sorted(_parsed(x[0], pc)[0]))),
                       _r=list(zip(~cand.degree.eq("doctoral"), cand.uni.isna(), cand.work_id)))
    dup = cand._t.notna() & cand.duplicated(["_t", "year", "_f"], keep=False)
    keep = cand[dup].sort_values("_r").drop_duplicates(["_t", "year", "_f"])
    cand = pd.concat([cand[~dup], keep]).drop(columns=["_t", "_f", "_r"])
    n_dup = int(dup.sum() - len(keep))
    a = acifs()
    ot = own_titles()
    # OpenAlex dissertations on the person's own works (2026-10-27): their titles join the ORCID thesis titles in the
    # title route; one with only non-Australian institutions is overseas-PhD evidence (Greenhalgh: Oxford 2012)
    diss = ot[ot.type == "dissertation"]
    dtitles = diss.groupby("cluster_id").title.agg(list)
    a["oax_dissertation_titles"] = a.cluster_id.map(dtitles).map(lambda x: x if isinstance(x, list) else [])
    a["orcid_thesis_titles"] = [list(o) + list(x) for o, x in zip(a.orcid_thesis_titles, a.oax_dissertation_titles)]
    abroad = diss[diss.countries.map(lambda c: len(c) > 0 and "AU" not in list(c))]
    a["oax_dissertation_abroad"] = a.cluster_id.isin(abroad.cluster_id)
    pairs = name_pairs(cand, a)
    tp = title_pairs(cand, a).assign(title_match=True)
    p = pairs.merge(tp, on=["work_id", "cluster_id"], how="outer")
    p["name_relation"] = p.name_relation.fillna("title")      # title-matched, family name only
    p["title_match"] = p.title_match.fillna(False).astype(bool)
    p = p.merge(cand[["work_id", "title", "year", "single_year", "degree", "uni", "extent", "sources"]], on="work_id")
    p = p.merge(a[["cluster_id", "group", "hep_codes", "first_year", "first_fellow_award", "first_fellow_tier",
                   "orcid_phd_year", "orcid_phd_country", "oax_dissertation_abroad", "hand_phd_status", "hand_phd_abroad",
                   "hand_phd_year"]], on="cluster_id")
    # candidates are counted among pairs of the same or a stronger name relation, so weaker pairs (nicknames,
    # initials) do not take a stronger pair's "only candidate" status away
    p["rel_rank"] = p.name_relation.map(RELATION_RANK)
    for col, grp, other in [("n_acifs_for_thesis", "work_id", "cluster_id"), ("n_theses_for_acif", "cluster_id", "work_id")]:
        p[col] = 0
        for r in sorted(RELATION_RANK.values()):
            sub = p[p.rel_rank <= r].groupby(grp)[other].nunique()
            m = p.rel_rank == r
            p.loc[m, col] = p.loc[m, grp].map(sub).values
    # a thesis university can be 2+ HEPs ("UA|UNISA": a repository code shared by Adelaide and UniSA theses); any counts
    p["uni_match"] = [bool(set(u.split("|")) & set(h if h is not None else [])) if isinstance(u, str) else None
                      for u, h in zip(p.uni, p.hep_codes)]
    ow, first_work, early_au = own_work_heps(names)
    ow_set = set(zip(ow.cluster_id, ow.year, ow.hep_code))
    p["works_uni"] = [None if pd.isna(u) else any((c, y + k, h) in ow_set for k in range(-OWN_BACK, OWN_AHEAD + 1)
                                                  for h in u.split("|"))
                      for c, y, u in zip(p.cluster_id, p.year, p.uni)]
    # own-work title evidence (2026-10-27): an accepted or unsure work of the person within 6 years of the thesis year
    # whose title shares most of its content words with the thesis title (papers from the thesis, the book of it)
    otw = defaultdict(list)
    for c, y, t in zip(ot.cluster_id, ot.year, ot.title):
        otw[c].append((y, content_words(t)))
    p["own_title"] = [any(not pd.isna(oy) and -OWN_TITLE_BACK <= oy - y <= OWN_TITLE_AHEAD and titles_similar(tw, ow_)
                          for oy, ow_ in otw.get(c, [])) if (tw := content_words(t)) else False
                      for c, y, t in zip(p.cluster_id, p.year, p.title)]
    p["first_work"] = p.cluster_id.map(first_work)
    p["early_au_share"] = p.cluster_id.map(early_au)
    fellow = p.first_fellow_award.notna()
    late = fellow & (p.year > p.first_fellow_award + 1)
    ecr = p.first_fellow_tier == "Early-career"
    early = ecr & (p.year < p.first_fellow_award - ECR_BACK)
    p["in_window"] = ~(late | early)
    p["unique_name"] = (p.n_acifs_for_thesis == 1)
    p["gap"] = p.year - p.first_work
    p["gap_ok"] = p.gap.between(GAP_MIN, GAP_MAX)
    # overseas-PhD evidence (2026-10-27): ORCID education abroad, an OpenAlex dissertation abroad, or early works
    # with no Australian institution
    p["overseas_hint"] = ((p.orcid_phd_country.notna() & p.orcid_phd_country.ne("AU")) | p.oax_dissertation_abroad
                          | p.early_au_share.eq(0) | p.hand_phd_abroad.eq("yes"))
    confirmed = p.works_uni.eq(True) | p.own_title
    sole = p.unique_name & (p.n_theses_for_acif == 1) & ~p.name_relation.isin(["initials", "compound"])  # those need confirmation
    # decision (2026-10-27, calibrated on ORCID PhD years): name pair + timing (fellows) + thesis within GAP of the
    # person's main run + (own works naming the thesis university around the thesis year or echoing its title, or a
    # name only this ACIF carries with one candidate thesis); exactly one passing thesis is accepted.
    # Early-career fellows (user, 2026-10-27): the eligibility window replaces the gap test, unless the PhD looks overseas
    gap_or_ecr = p.gap_ok | (ecr & p.in_window & ~p.overseas_hint)
    p["passes"] = p.in_window & gap_or_ecr & (confirmed | sole)
    p["n_pass_for_acif"] = p.cluster_id.map(p[p.passes].groupby("cluster_id").work_id.nunique()).fillna(0).astype(int)
    # 2+ passing theses (user, 2026-10-27): prefer the stronger name relation, then the one the person's own works
    # confirm, then a doctoral label,
    # then the later year; accepted only when the best is strictly ahead of the next on (confirmed, doctoral, year)
    p["rank_key"] = list(zip(-p.rel_rank, confirmed, p.degree.eq("doctoral"), p.year))
    best = {}
    for cid, g in p[p.passes].groupby("cluster_id"):
        keys = sorted(g.rank_key, reverse=True)
        if len(keys) == 1 or keys[0] > keys[1]:
            best[cid] = g.loc[g.rank_key == keys[0], "work_id"].iloc[0]
    p["accepted"] = p.passes & (p.cluster_id.map(best) == p.work_id)
    p["accepted_by"] = None
    p.loc[p.accepted & (p.n_pass_for_acif == 1), "accepted_by"] = "only passing"
    p.loc[p.accepted & (p.n_pass_for_acif > 1), "accepted_by"] = "tie-break"
    # title route first (user, 2026-10-27): a Trove thesis whose title matches a thesis title on the ACIF's ORCID record
    # is taken outright; with 2+, the doctoral label, then the later year
    tbest = {}
    for cid, g in p[p.title_match].groupby("cluster_id"):
        tbest[cid] = g.sort_values(["degree", "year"], key=lambda s: s.eq("doctoral") if s.name == "degree" else s,
                                   ascending=False).work_id.iloc[0]
    has_title = p.cluster_id.isin(tbest)
    p.loc[has_title, "accepted"] = p.loc[has_title, "cluster_id"].map(tbest) == p.loc[has_title, "work_id"]
    p.loc[has_title, "accepted_by"] = None
    p.loc[has_title & p.accepted, "accepted_by"] = "thesis title"
    # hand PhD findings (manual_phd.csv) overrule: a confirmed PhD abroad takes no Trove thesis; a confirmed
    # Australian PhD with a year takes only a thesis within a year of it
    hc = p.hand_phd_status.eq("confirmed")
    refuse = hc & (p.hand_phd_abroad.eq("yes") | (p.hand_phd_year.notna() & ((p.year - p.hand_phd_year).abs() > 1)))
    p["refused_by_hand"] = p.accepted & refuse.fillna(False).astype(bool)
    p.loc[p.refused_by_hand, "accepted"] = False
    p.loc[p.refused_by_hand, "accepted_by"] = None
    p = p.drop(columns=["rank_key", "rel_rank"])
    p["orcid_agree"] = (p.year - p.orcid_phd_year).abs() <= 1
    p.drop(columns=["hep_codes"]).to_parquet(OUT, index=False)

    L = ["# Trove PhD theses for ARC investigators", "",
         f"Theses (1990s-2020s, Australian content): {len(d):,}; clean {d.clean.sum():,} (Deakin repository-only "
         f"{d.deakin_only.sum():,}, non-thesis type {d.not_thesis_type.sum():,}, 2+ authors {(d.n_authors > 1).sum():,} removed); "
         f"degree of clean theses: {d[d.clean].degree.value_counts().to_dict()}",
         f"University found for {d[d.clean].uni.notna().mean():.0%} of clean theses "
         f"({len(src_map[src_map.used]):,} repository codes mapped).", "",
         f"Duplicate Trove works of one thesis (same title, year, author family) merged: {n_dup:,}.",
         f"Kept ACIFs {len(a):,}; with a name-paired thesis {p.cluster_id.nunique():,}; pairs {len(p):,}.", ""]
    # truth sets
    t = a[a.orcid_phd_year.notna()]
    tau = t[t.orcid_phd_country == "AU"]
    L.append(f"ORCID PhD year known: {len(t):,} ACIFs ({len(tau):,} Australian).")
    L += ["", "## Agreement with ORCID (pairs whose ACIF has an ORCID PhD year; agree = within a year)", "",
          "| pairs | n | agree |", "|---|---|---|"]
    q = p[p.orcid_phd_year.notna()]
    for lab, m in [("all name pairs", q.index == q.index), ("in window (fellows)", q.in_window),
                   ("thesis -2..+8 years from start of main run", q.gap_ok),
                   ("grant university", q.uni_match.eq(True)), ("own works name the thesis university", q.works_uni.eq(True)),
                   ("own work title echoes the thesis title", q.own_title), ("overseas hint", q.overseas_hint),
                   ("unique name, one candidate", q.unique_name & (q.n_theses_for_acif == 1)),
                   ("doctoral label", q.degree.eq("doctoral")), ("passes rule", q.passes), ("accepted (only passing)", q.accepted)]:
        L.append(f"| {lab} | {m.sum():,} | {q[m].orcid_agree.mean():.0%} |")
    L += ["", "## By name relation (pairs with an ORCID PhD year)", "", "| relation | pairs | agree | accepted | accepted agree |",
          "|---|---|---|---|---|"]
    for r, x in q.groupby("name_relation"):
        L.append(f"| {r} | {len(x):,} | {x.orcid_agree.mean():.0%} | {x.accepted.sum():,} | {x[x.accepted].orcid_agree.mean():.0%} |")
    L += ["", "## By group (accepted pairs with an ORCID PhD year)", "", "| group | accepted | agree |", "|---|---|---|"]
    for g, x in q[q.accepted].groupby("group"):
        L.append(f"| {g} | {len(x):,} | {x.orcid_agree.mean():.0%} |")
    rec_au = tau.cluster_id.isin(p[p.accepted & p.orcid_agree].cluster_id).mean()
    L += ["", f"Recall on ACIFs with an Australian ORCID PhD: accepted thesis within a year for {rec_au:.0%} of {len(tau):,}.",
          f"Overseas ORCID PhDs with an accepted thesis (wrong or a second degree): "
          f"{t[t.orcid_phd_country != 'AU'].cluster_id.isin(p[p.accepted].cluster_id).mean():.0%} of {(t.orcid_phd_country != 'AU').sum():,}."]
    acc = p[p.accepted].drop_duplicates("cluster_id")
    L += ["", "## Coverage (accepted theses)", "", "| group | ACIFs | with accepted thesis |", "|---|---|---|"]
    for g, x in a.groupby("group"):
        L.append(f"| {g} | {len(x):,} | {x.cluster_id.isin(acc.cluster_id).sum():,} ({x.cluster_id.isin(acc.cluster_id).mean():.0%}) |")
    L += ["", "## Repository codes mapped to universities", "", "| source | HEP | theses with a creator university | share |",
          "|---|---|---|---|"]
    for r in src_map[src_map.used].itertuples():
        L.append(f"| {r.source} | {r.hep_code} | " + ("confirmed by user | |" if r.by_hand else f"{r.n_source:,} | {r.share:.0%} |"))
    c = d[d.clean & d.uni.isna()]
    un = Counter(norm_org(re.split(r"\.(?:\s|$)", x)[0]) for cr in c.creators for x in cr if "niversit" in x or "ollege" in x)
    L += ["", "## University-like creator names not matched to a HEP (top 30; candidates for HEP_concordances Variants)", "",
          "| name | theses |", "|---|---|"] + [f"| {k} | {v:,} |" for k, v in un.most_common(30)]
    L += ["", f"Accepted through a thesis title (ORCID thesis works, OpenAlex dissertations): {(p.accepted_by == 'thesis title').sum():,} ACIFs."]
    h = a[a.hand_phd_status.notna()]
    ok = p[p.accepted].set_index("cluster_id").year
    hau = h[h.hand_phd_abroad.eq("no") & h.hand_phd_year.notna()]
    L += ["", f"Hand PhD findings (manual_phd.csv): {len(h):,} ACIFs ({h.hand_phd_status.eq('confirmed').sum():,} confirmed, "
          f"{h.hand_phd_abroad.eq('yes').sum():,} abroad); accepted theses refused by them {int(p.refused_by_hand.sum()):,}; "
          f"Australian with a year: {len(hau):,}, accepted thesis within a year "
          f"{int(sum(abs(ok.get(c, -99) - y) <= 1 for c, y in zip(hau.cluster_id, hau.hand_phd_year))):,}."]
    tb = q[q.accepted_by == "tie-break"]
    L += ["", f"Accepted by tie-break among 2+ passing theses: {(p.accepted_by == 'tie-break').sum():,} ACIFs; "
          f"with an ORCID PhD year {len(tb):,}, agree {tb.orcid_agree.mean():.0%}.",
          f"Not used: a wider window when the person's own works confirm the university (thesis 9-12 years after the "
          f"first accepted work agrees with ORCID 74%, against 94% inside -2..+8; 2026-10-27)."]
    REPORT.write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
