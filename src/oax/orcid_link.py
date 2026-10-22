"""
Step 1 of the ARC<->OpenAlex linker (2026-10-06): link each ACIF to every OpenAlex author that
carries the ACIF's ORCID. An exact identity link -- no names are used to find it. Every author
found is kept: OpenAlex often splits one person into a large record plus small fragments.

Sides:
  ACIFs    acifs_arc.parquet, kept ACIFs only (excluded=False); `orcids` holds at most one ORCID
           (ARC, Scopus-found, hand or ORCID-bulk; the build's ORCID veto), `orcid_sources` says
           which.
  authors  openalex_authors_prep.parquet (00b): the HEP-context pool, ORCID already bare. An
           ACIF ORCID no pool author carries is looked up in the full OpenAlex authors table, and
           those authors are linked too (2026-10-06, user), flagged in_pool=False; their names
           are parsed here with NameParser (display name and alternatives) and only the global
           works count is known.

Decision per link (2026-10-06, user; from a review of the 211 links whose names share no key):
OpenAlex sometimes puts an ORCID on another person's record -- usually a small record of a
co-author beside the person's real one. name_relation() compares the parser's keys of the two
sides (no name handling of its own) and status follows:
    accept_name_key      the two sides share the MAIN name (first given name + family: an ACIF
                         record's arc_names full_name_key is among the author's keys, or the
                         author's main key is among the ACIF's)
    accept_initials_only they share only an initial or middle-name key and OpenAlex has no full
                         given name (nothing to contradict)
    accept_orcid_names   they share only an initial or middle-name key, and the ORCID record's
                         own names include the OpenAlex name form (Chris Power, Kerr Graham:
                         the person publishes under that name); since 2026-10-22 also for links
                         whose names are otherwise unrelated or differ in the given name (Debra
                         Bernhardt's ORCID lists "Debra Searles", the OpenAlex record's name)
    reject_orcid_names   they share only an initial or middle-name key, the ORCID record's names
                         do not include the OpenAlex form, and the record is minor: another
                         person's record (co-author, relative)
    review_initial_only  the same on a main record (2026-10-06, user)
    accept_name_form     same person, name written differently: surname split differently
                         (compound), separator (space/hyphen/run together), given/family order
                         swapped, surname one letter different (4+ letters), non-Latin display
                         name, OpenAlex has initials only
    accept_nickname      (2026-10-21, user) the same family name, and a full given name of each side is
                         one person's: equal once hyphens and spaces are removed (Wei-Hong / Weihong),
                         related in the `nicknames` package's list (Rick/Richard, Cathie/Catherine),
                         or a clipped form of 4+ letters (Chathu/Chathurika) -- checked before
                         reject_orcid_names, review_initial_only and review_given_name
    review_given_name    same surname, different first given name (nickname or another person)
    review_unrelated     unrelated names on a main record (>= MINOR_SHARE of the ACIF's works)
    reject_unrelated     unrelated names on a minor record (< MINOR_SHARE): another person's record
    accept_hand          a review case accepted by hand
Accepted links are those whose status starts with "accept".

Hand decisions: data_persisted/oax_link_overrides.csv, rows `action, orcid, oax_author (A-number),
notes`. reject_link: that OpenAlex author is not the person holding that ORCID -- the link is left
out and reported. accept_link: a review case is the person. A row that matches no candidate link
stops the run. accept_bridge (2026-10-22): for the Scopus DOI bridge (src/oax/scopus_link.py) -- the
ACIF holding `orcid` takes the record `oax_author` although the record carries another ORCID (one
person with two ORCID accounts, or a co-author's ORCID on the record); checked there. accept_record
(2026-10-22): the ACIF holding `orcid` takes the record `oax_author`, which carries no ORCID or another
one and which no linker route reaches (Dean Patterson, Tom Cochrane); added to stage 1 as a link with
status accept_hand (hand_record_authors()).

Name keys are not used to link; `shares_name_key` / `shares_full_name_key` only report whether
the two sides' parser keys agree (a check on the link, and input to step 2's design).
"""

from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

from config.settings import ACIF_ARC_RECORDS as ACIF_RECORDS, ACIFS_ARC, OAX_AUTHORS, OPENALEX_COMPACT_DIR, PROCESSED_DATA
from src.utils.names import HumanNameParser, given_names_related

OVERRIDES = Path(__file__).resolve().parents[2] / "data_persisted" / "oax_link_overrides.csv"
MINOR_SHARE = 0.2
NICK_MIN_CLIPPED = 4
ACTIONS = ("reject_link", "accept_link", "accept_bridge", "accept_record")

AUTHORS_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
ACIF_COLUMNS = ["cluster_id", "orcids", "orcid_sources", "full_names", "full_name_keys",
                "n_records", "first_year", "last_year"]
AUTHOR_COLUMNS = ["author_idx", "orcid", "full_name", "full_name_keys", "works_count",
                  "works_count_au", "works_count_global"]


def load_acifs(path=ACIFS_ARC) -> pd.DataFrame:
    """Kept ACIFs (excluded=False)."""
    a = pd.read_parquet(path, columns=ACIF_COLUMNS + ["excluded"])
    return a[~a.excluded].drop(columns="excluded").reset_index(drop=True)


def load_authors(orcids, path=AUTHORS_PREP) -> pd.DataFrame:
    """Pool authors whose ORCID is in `orcids` (in_pool=True)."""
    con = duckdb.connect()
    con.register("want", pd.DataFrame({"orcid": sorted(set(orcids))}))
    return con.execute(f"""
        SELECT {', '.join(AUTHOR_COLUMNS)}, TRUE AS in_pool FROM read_parquet('{path}')
        WHERE orcid IN (SELECT orcid FROM want)
    """).fetchdf()


def load_outside_authors(orcids) -> pd.DataFrame:
    """Authors outside the pool, from the full OpenAlex authors table, carrying any of `orcids`;
    same columns as load_authors() (in_pool=False; names parsed here; works_count and
    works_count_au unknown)."""
    raw = outside_pool(orcids, alternatives=True)
    parser = HumanNameParser()
    keys = []
    for r in raw.itertuples():
        names = [r.display_name, *(list(r.alternatives) if r.alternatives is not None else [])]
        keys.append(sorted({k for n in names if isinstance(n, str) and n for k in parser.parse(n).full_name_keys}))
    return pd.DataFrame({"author_idx": raw.author_idx, "orcid": raw.orcid, "full_name": raw.display_name,
                         "full_name_keys": keys, "works_count": pd.NA, "works_count_au": pd.NA,
                         "works_count_global": raw.works_count, "in_pool": False})


def load_overrides(path=OVERRIDES) -> pd.DataFrame:
    """Hand rows: (action, orcid, author_idx, notes)."""
    with open(path, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f)]
    bad = [r for r in rows if r["action"] not in ACTIONS]
    if bad:
        raise SystemExit(f"{path.name}: unknown action(s) {sorted({r['action'] for r in bad})}")
    return pd.DataFrame([{"action": r["action"], "orcid": r["orcid"].strip(),
                          "author_idx": int(r["oax_author"].strip().lstrip("Aa")), "notes": r["notes"]}
                         for r in rows], columns=["action", "orcid", "author_idx", "notes"])


def hand_record_authors(overrides: pd.DataFrame, path=AUTHORS_PREP) -> pd.DataFrame:
    """The accept_record rows' records as author rows carrying the row's (the ACIF's) ORCID, so
    orcid_links() pairs them with the ACIF; from the HEP-context pool, else the full authors table
    (names parsed here, as in load_outside_authors()). A row whose record exists nowhere stops the run."""
    rows = overrides[overrides.action == "accept_record"] if "action" in overrides else overrides.iloc[0:0]
    if not len(rows):
        return pd.DataFrame(columns=AUTHOR_COLUMNS + ["in_pool"])
    con = duckdb.connect()
    con.register("want", rows[["orcid", "author_idx"]])
    pool = con.execute(f"""SELECT {', '.join(c for c in AUTHOR_COLUMNS if c != 'orcid')}, w.orcid, TRUE AS in_pool
        FROM read_parquet('{path}') p JOIN want w USING (author_idx)""").fetchdf()
    rest = rows[~rows.author_idx.isin(set(pool.author_idx))]
    out = [pool]
    if len(rest):
        con.register("rest", rest[["orcid", "author_idx"]])
        raw = con.execute(f"""SELECT a.author_idx, r.orcid, a.display_name, a.display_name_alternatives AS alternatives
            FROM read_parquet('{OAX_AUTHORS}/*.parquet') a JOIN rest r USING (author_idx)""").fetchdf()
        if len(raw) < len(rest):
            raise SystemExit(f"oax_link_overrides.csv accept_record rows name records not in OpenAlex: "
                             f"{sorted(set(rest.author_idx) - set(raw.author_idx))}")
        parser = HumanNameParser()
        keys = [sorted({k for n in [r.display_name, *(list(r.alternatives) if r.alternatives is not None else [])]
                        if isinstance(n, str) and n for k in parser.parse(n).full_name_keys}) for r in raw.itertuples()]
        con.register("ids", raw[["author_idx"]])
        wc = dict(con.execute(f"""SELECT author_idx, count(DISTINCT work_idx) FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
                                   WHERE author_idx IN (SELECT author_idx FROM ids) GROUP BY 1""").fetchall())
        out.append(pd.DataFrame({"author_idx": raw.author_idx, "orcid": raw.orcid, "full_name": raw.display_name,
                                 "full_name_keys": keys, "works_count": pd.NA, "works_count_au": pd.NA,
                                 "works_count_global": [wc.get(i, 0) for i in raw.author_idx], "in_pool": False}))
    return pd.concat(out, ignore_index=True)


def apply_overrides(authors: pd.DataFrame, overrides: pd.DataFrame):
    """(authors without the reject_link (orcid, author) pairs, the rejected rows). Any hand row
    that matches no author carrying that ORCID stops the run."""
    key = set(zip(authors.orcid, authors.author_idx.astype("int64")))
    ov = overrides[~overrides.action.isin(["accept_bridge", "accept_record"])] if "action" in overrides else overrides
    missing = [(o, a) for o, a in zip(ov.orcid, ov.author_idx) if (o, a) not in key]
    if missing:
        raise SystemExit(f"oax_link_overrides.csv rows match no candidate link: {missing}")
    rejects = overrides[overrides.action == "reject_link"] if "action" in overrides else overrides
    rej = set(zip(rejects.orcid, rejects.author_idx))
    hit = [(o, int(a)) in rej for o, a in zip(authors.orcid, authors.author_idx)]
    return authors[[not h for h in hit]].reset_index(drop=True), authors[hit].reset_index(drop=True)


def _full_keys(keys) -> set[str]:
    """Keys with a full (multi-letter) given name."""
    return {k for k in keys if len(k.split("_", 1)[0]) > 1}


def orcid_links(acifs: pd.DataFrame, authors: pd.DataFrame) -> pd.DataFrame:
    """One row per (ACIF, author) sharing an ORCID: the ACIF's side, the author's side, the
    number of authors found for that ACIF, the author's share of their works, and whether the
    two sides share a name key (any / one with a full given name)."""
    a = acifs[acifs.orcids.map(len) > 0].copy()
    a["orcid"] = a.orcids.map(lambda x: x[0])
    au = authors.rename(columns={"full_name": "author_name", "full_name_keys": "author_keys"})
    m = a.drop(columns="orcids").merge(au, on="orcid", how="inner")
    if m.empty:
        return m.assign(n_authors=pd.Series(dtype=int), works_share=pd.Series(dtype=float),
                        shares_name_key=pd.Series(dtype=bool), shares_full_name_key=pd.Series(dtype=bool))
    ak = [set(k) for k in m.full_name_keys]
    ok = [set(k) for k in m.author_keys]
    m["shares_name_key"] = [bool(x & y) for x, y in zip(ak, ok)]
    m["shares_full_name_key"] = [bool(_full_keys(x) & y) for x, y in zip(ak, ok)]
    m["n_authors"] = m.groupby("cluster_id").author_idx.transform("size")
    total = m.groupby("cluster_id").works_count_global.transform("sum")
    m["works_share"] = (m.works_count_global / total.where(total > 0)).fillna(0.0)
    return m.sort_values(["cluster_id", "works_count_global"], ascending=[True, False],
                         ignore_index=True)


def _split(keys):
    given, family = set(), set()
    for k in keys:
        g, _, f = k.partition("_")
        given.add(g)
        family.add(f)
    return given, family


def _compact(s: str) -> str:
    return re.sub(r"[\s\-'\u2019]", "", s)


def _one_letter(a: str, b: str) -> bool:
    if min(len(a), len(b)) < 4 or abs(len(a) - len(b)) > 1:
        return False
    if len(a) == len(b):
        return sum(x != y for x, y in zip(a, b)) == 1
    if len(a) > len(b):
        a, b = b, a
    return any(a == b[:i] + b[i + 1:] for i in range(len(b)))


def _non_latin(s) -> bool:
    return any(ord(ch) > 0x24F and unicodedata.category(ch).startswith("L") for ch in (s or ""))


def name_relation(arc_keys, oax_keys, oax_name) -> str:
    """How the two sides' parser keys relate (module docstring); compares keys only."""
    arc_keys, oax_keys = set(arc_keys), set(oax_keys if oax_keys is not None else [])
    if arc_keys & oax_keys:
        return "shared_key"
    if not oax_keys:
        return "non_latin" if _non_latin(oax_name) else "no_oax_keys"
    ag, af = _split(arc_keys)
    og, of = _split(oax_keys)
    if arc_keys & {f"{f}_{g}" for g, f in (k.partition("_")[::2] for k in oax_keys)}:
        return "order_swapped"
    if af & of:
        return "same_family" if {g for g in og if len(g) > 1} else "initials_only"
    if {_compact(x) for x in af} & {_compact(x) for x in of}:
        return "separator"
    arc_tok = {t for x in af for t in re.split(r"[\s\-]", x) if t}
    oax_tok = {t for x in of for t in re.split(r"[\s\-]", x) if t}
    if arc_tok & (oax_tok | og) or oax_tok & arc_tok:
        return "compound_family"
    if _non_latin(oax_name):
        return "non_latin"
    if any(_one_letter(a, b) for a in af for b in of):
        return "family_one_letter"
    return "unrelated"


def nickname_same_family(arc_keys, oax_keys) -> bool:
    """accept_nickname's test (module docstring), on parser keys only."""
    ag, af = _split(arc_keys)
    og, of = _split(oax_keys if oax_keys is not None else [])
    if not af & of:
        return False
    ag, og = {g for g in ag if len(g) > 1}, {g for g in og if len(g) > 1}
    return any(_compact(a) == _compact(b) or given_names_related(a, b, NICK_MIN_CLIPPED) for a in ag for b in og)


NAME_FORMS = {"compound_family", "separator", "order_swapped", "family_one_letter", "non_latin",
              "initials_only"}


def decide(links: pd.DataFrame, accepts=frozenset(), arc_main=None, oax_main=None,
           orcid_names=None) -> pd.DataFrame:
    """Add name_relation and status (module docstring). `accepts`: (orcid, author_idx) pairs
    accepted by hand; `arc_main`: cluster_id -> its records' main keys; `oax_main`: author_idx ->
    its main key; `orcid_names`: orcid -> the ORCID record's own name keys (00d's orcid_facts():
    ORCID cache, else bulk file)."""
    out = links.copy()
    out["name_relation"] = [name_relation(a, o, n) for a, o, n in
                            zip(out.full_name_keys, out.author_keys, out.author_name)]
    arc_main = arc_main or {}
    oax_main = oax_main or {}
    orcid_names = orcid_names or {}

    def key_kind(r):
        """For a shared-key link: 'main' (the main name is shared) or 'minor' (initial or
        middle-name keys only)."""
        okeys, akeys = set(r.author_keys), set(r.full_name_keys)
        if arc_main.get(r.cluster_id, set()) & okeys or oax_main.get(int(r.author_idx)) in akeys:
            return "main"
        return "minor"

    def status(r):
        if (r.orcid, int(r.author_idx)) in accepts:
            return "accept_hand"
        if r.name_relation == "shared_key":
            if key_kind(r) == "main":
                return "accept_name_key"
            ofull = _full_keys(r.author_keys)
            if not ofull:
                return "accept_initials_only"
            if ofull & _full_keys(orcid_names.get(r.orcid, ())):
                return "accept_orcid_names"
            if nickname_same_family(r.full_name_keys, r.author_keys):
                return "accept_nickname"
            return "reject_orcid_names" if r.works_share < MINOR_SHARE else "review_initial_only"
        if r.name_relation in NAME_FORMS:
            return "accept_name_form"
        if r.name_relation == "same_family" and nickname_same_family(r.full_name_keys, r.author_keys):
            return "accept_nickname"
        if _full_keys(r.author_keys) & _full_keys(orcid_names.get(r.orcid, ())):
            return "accept_orcid_names"     # the ORCID record itself lists the OpenAlex form (Debra Bernhardt / Searles)
        if r.name_relation == "same_family":
            return "review_given_name"
        return "review_unrelated" if r.works_share >= MINOR_SHARE else "reject_unrelated"
    out["status"] = [status(r) for r in out.itertuples()]
    return out


def load_name_evidence(links: pd.DataFrame):
    """(arc_main, oax_main, orcid_names) for decide(), for the ACIFs, authors and ORCIDs in
    `links`."""
    import importlib
    import diskcache
    from config.settings import DISKCACHE_DIR
    from src.acif.name_merge import main_keys
    mk = main_keys()
    rec = pd.read_parquet(ACIF_RECORDS, columns=["unique_id", "cluster_id"])
    arc_main: dict[str, set[str]] = {}
    for u, c in zip(rec.unique_id, rec.cluster_id):
        if u in mk:
            arc_main.setdefault(c, set()).add(mk[u])
    con = duckdb.connect()
    con.register("ids", pd.DataFrame({"a": sorted(set(links.author_idx.astype("int64")))}))
    oax_main = dict(con.execute(f"SELECT author_idx, full_name_key FROM read_parquet('{AUTHORS_PREP}') "
                                "WHERE author_idx IN (SELECT a FROM ids)").fetchall())
    x00d = importlib.import_module("src.00d_extract_scopus")
    orcids = set(links.orcid)
    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    facts = x00d.orcid_facts(orcids, cache, x00d.bulk_rows(con, orcids))
    return arc_main, oax_main, {r.orcid: set(r.name_keys) for r in facts.itertuples()}


def unmatched(acifs: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    """Kept ACIFs with an ORCID that no pool author carries."""
    a = acifs[acifs.orcids.map(len) > 0].copy()
    a["orcid"] = a.orcids.map(lambda x: x[0])
    return a[~a.cluster_id.isin(set(links.cluster_id))].drop(columns="orcids").reset_index(drop=True)


def outside_pool(orcids, alternatives: bool = False) -> pd.DataFrame:
    """Authors in the full OpenAlex authors table carrying any of `orcids` (bare form). works_count
    is the number of works on the author's rows of compact/authorships (2026-10-21: the authors
    table's own works_count is stale for records OpenAlex merged away)."""
    con = duckdb.connect()
    con.register("want", pd.DataFrame({"orcid": sorted(set(orcids))}))
    alt = ", display_name_alternatives AS alternatives" if alternatives else ""
    con.execute(f"""
        CREATE TEMP TABLE op AS
        SELECT author_idx, replace(orcid, 'https://orcid.org/', '') AS orcid, display_name{alt}
        FROM read_parquet('{OAX_AUTHORS}/*.parquet')
        WHERE replace(orcid, 'https://orcid.org/', '') IN (SELECT orcid FROM want)""")
    return con.execute(f"""
        SELECT op.*, coalesce(w.n, 0) AS works_count FROM op LEFT JOIN (
            SELECT author_idx, count(DISTINCT work_idx) AS n FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
            WHERE author_idx IN (SELECT author_idx FROM op) GROUP BY 1) w USING (author_idx)""").fetchdf()


def shared_orcids(acifs: pd.DataFrame) -> pd.DataFrame:
    """ORCIDs held by 2+ kept ACIFs (the build refused to merge them on names)."""
    a = acifs[acifs.orcids.map(len) > 0].copy()
    a["orcid"] = a.orcids.map(lambda x: x[0])
    g = a.groupby("orcid").filter(lambda d: len(d) > 1)
    return g[["orcid", "cluster_id", "full_names", "orcid_sources"]].sort_values(["orcid", "cluster_id"],
                                                                                ignore_index=True)
