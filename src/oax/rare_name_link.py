"""
Linker stage 6 (2026-10-23, user): rare family names, for ACIFs the ORCID hints left unlinked.

User: "is there something to do about rare names ... when we have an unlinked acif could we do it as part
of the cycling process?" -- cases such as Reinout Quispel (5-work La Trobe fragment outside every grant
window), Gay Ochiltree (her only record outside the pool), Eva Abal, Samer Akkach, Larry Sitsky: the right
record is reachable from the name alone, but the earlier stages want institution-in-time, co-author,
Scopus or ORCID evidence the record cannot give.

Rule:
  - the ACIF's family name (the family part of its records' main keys, 00a) is rare: at most RARE_MAX
    authors of the HEP-context pool have it as their parsed main family name (family_name_main);
  - candidates: every OpenAlex author, in the pool or not, whose parsed name (display name and alternatives,
    NameParser) carries that family name and a compatible given name -- a full given name shared or
    nickname/separator-related (orcid_hint_link.names_compatible), or a record with initials only (no full
    given name under any family: "Shaoyuan Bai" whose alternatives give "S. Hosseini Bai" is not initials-only)
    whose initials include the ACIF's first initial, or (pool records) a bare first initial matching
    the ACIF's followed by another given name (scopus_link.initial_first: "R. John Watling");
    records with no authorships (merged away by OpenAlex) are not candidates;
  - accept when exactly ONE candidate is compatible (user, 2026-10-23: option (a)), it is not linked to
    another ACIF, it carries no ORCID other than the ACIF's (or one refused by hand for the ACIF's records),
    and no other kept ACIF shares one of the ACIF's main name keys;
  - no field check (user, 2026-10-23: by the time these cases are reached the FOR layout misclassifies
    fields -- Gay Ochiltree's family studies against health/psychology grants, Mick Dodson).

RARE_MAX is set by calibrate() on ORCID-linked ACIFs treated as unlinked (taken and ORCID ignored).
Calibration 2026-10-23 (16,150 ORCID-linked ACIFs, 7,166 families with <= 50 pool authors): accepted 1,708;
the picked record is one of the ACIF's ORCID-linked records for family counts 1: 713/719, 2: 325/327,
3: 165/166, 4-5: 173/174, 6-10: 140/143, 11-20: 60/60, 21-50: 24/27 -> RARE_MAX = 20 (1,576/1,589, 99.2%).
Most of the 13 misses up to 20 are another fragment of the same person (P van Heijster, A. Kotooussov,
K. Swaminatha-Iyer, F David Bulbeck); the clear errors are namesakes (Shaoyuan Bai for Shahla Hosseini Bai,
R. S. Sankarasubramanian for Subashchandran Sankarasubramanian, Ángel José Guerrero González).
Decision per ACIF: accept_rare_name, not_rare, no_candidate, several_candidates, several_acifs,
record_linked_elsewhere, record_other_orcid.
"""

from __future__ import annotations

import re

import pandas as pd

from config.settings import OAX_AUTHORS, OPENALEX_COMPACT_DIR, PROCESSED_DATA
from src.oax.orcid_hint_link import names_compatible
from src.oax.scopus_link import initial_first

AUTHORS_PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
AUTHORSHIPS = OPENALEX_COMPACT_DIR / "authorships"
CACHE = PROCESSED_DATA / "oax_link" / "rare_name_candidates.parquet"
RARE_MAX = 20
CALIBRATION_MAX = 50


def family_counts(con, prep=AUTHORS_PREP) -> dict:
    """family_name_main -> number of pool authors carrying it (the user's 2026-10-23 frequency table)."""
    return dict(con.execute(f"""SELECT family_name_main, count(*) FROM read_parquet('{prep}')
                                WHERE family_name_main IS NOT NULL GROUP BY 1""").fetchall())


def acif_families(main: pd.DataFrame) -> pd.DataFrame:
    """(cluster_id, family) from acif_main_keys() rows (cluster_id, k)."""
    m = main.assign(family=main.k.str.split("_", n=1).str[1])
    return m[["cluster_id", "family"]].dropna().drop_duplicates()


def candidates(con, families: set, prep=AUTHORS_PREP, authors=OAX_AUTHORS, authorships=AUTHORSHIPS) -> pd.DataFrame:
    """Every OpenAlex author whose parsed name carries one of `families`: (author_idx, family, author_name,
    author_keys, orcid, in_pool, first_name, first_initial, works). The full authors table is searched by a
    word match on the display name and alternatives, then every name is parsed (NameParser) and only
    records whose parsed keys carry the family are kept; records with no authorships are dropped."""
    from src.utils.names import HumanNameParser
    if not families:
        return pd.DataFrame(columns=["author_idx", "family", "author_name", "author_keys", "orcid", "has_full",
                                     "in_pool", "first_name", "first_initial", "works"])
    words = sorted({w for f in families for w in re.split(r"[\s\-']+", f) if len(w) >= 3})
    pat = r"\b(" + "|".join(re.escape(w) for w in words) + r")\b"
    raw = con.execute(f"""SELECT author_idx, display_name, display_name_alternatives AS alternatives,
                                 replace(orcid, 'https://orcid.org/', '') AS orcid
                          FROM read_parquet('{authors}/*.parquet')
                          WHERE regexp_matches(lower(coalesce(display_name, '') || ' ' ||
                                               coalesce(array_to_string(display_name_alternatives, ' '), '')), ?)""",
                      [pat]).fetchdf()
    parser = HumanNameParser()
    rows = []
    for r in raw.itertuples():
        names = [r.display_name, *(list(r.alternatives) if r.alternatives is not None else [])]
        keys = sorted({k for n in names if isinstance(n, str) and n for k in parser.parse(n).full_name_keys})
        for f in {k.split("_", 1)[1] for k in keys if "_" in k} & families:
            rows.append((int(r.author_idx), f, r.display_name, [k for k in keys if k.split("_", 1)[1] == f],
                         r.orcid if isinstance(r.orcid, str) and r.orcid else None,
                         any(len(k.split("_", 1)[0]) > 1 for k in keys if "_" in k)))
    c = pd.DataFrame(rows, columns=["author_idx", "family", "author_name", "author_keys", "orcid", "has_full"])
    if not len(c):
        return c.assign(in_pool=[], first_name=[], first_initial=[], works=[])
    con.register("rn_ids", c[["author_idx"]].drop_duplicates())
    works = dict(con.execute(f"""SELECT author_idx, count(DISTINCT work_idx) FROM read_parquet('{authorships}/*.parquet')
                                 WHERE author_idx IN (SELECT author_idx FROM rn_ids) GROUP BY 1""").fetchall())
    pool = con.execute(f"""SELECT author_idx, first_name, first_initial FROM read_parquet('{prep}')
                           WHERE author_idx IN (SELECT author_idx FROM rn_ids)""").fetchdf().set_index("author_idx")
    c["in_pool"] = c.author_idx.isin(pool.index)
    c["first_name"] = c.author_idx.map(pool.first_name)
    c["first_initial"] = c.author_idx.map(pool.first_initial)
    c["works"] = c.author_idx.map(works).fillna(0).astype(int)
    return c[c.works > 0].reset_index(drop=True)


def cached_candidates(con, families: set, cache=CACHE, authors=OAX_AUTHORS) -> pd.DataFrame:
    """candidates() for `families`, searching only families not already in the cache (one full-table scan
    costs minutes; the cycle reruns 02 several times). The cache is dropped when the OpenAlex authors table
    is newer than it."""
    import os
    have, fams_done = None, set()
    if cache.exists() and cache.stat().st_mtime > max(os.path.getmtime(p) for p in authors.glob("*.parquet")):
        have = pd.read_parquet(cache)
        fams_done = set(pd.read_parquet(cache.with_suffix(".families.parquet")).family)
    new = families - fams_done
    got = candidates(con, new) if new else None
    parts = [d for d in (have, got) if d is not None and len(d)]
    out = pd.concat(parts, ignore_index=True) if parts else candidates(con, set())
    if new:
        out.to_parquet(cache, index=False)
        pd.DataFrame({"family": sorted(fams_done | new)}).to_parquet(cache.with_suffix(".families.parquet"), index=False)
    return out[out.family.isin(families)].reset_index(drop=True)


def compatible(acif_keys, acif_main, rec) -> bool:
    """The record's name fits the ACIF's (see the module docstring)."""
    keys = list(rec.author_keys)
    if names_compatible(list(acif_keys), keys):
        return True
    inits = {k[0] for k in keys}
    first = {k[0] for k in acif_main if "_" in k and k.split("_", 1)[1] == rec.family}
    if not rec.has_full and inits & first:
        return True
    return bool(rec.in_pool) and initial_first(acif_main, rec.first_name, rec.first_initial, rec.family)


def decide(acif_ids, acif_keys: dict, main: pd.DataFrame, counts: dict, cands: pd.DataFrame, taken: dict,
           acif_orcid: dict, refused: dict, rare_max: int = RARE_MAX) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(decisions, links). acif_keys: cluster_id -> full_name_keys; main: acif_main_keys() rows of every kept
    ACIF (for the several_acifs guard and the families); counts: family_counts(); taken: author_idx -> ACIF
    linked by an earlier stage; acif_orcid: cluster_id -> ORCID; refused: cluster_id -> ORCIDs refused by hand."""
    fam = acif_families(main)
    fam_of = fam.groupby("cluster_id").family.apply(set).to_dict()
    main_of = main.groupby("cluster_id").k.apply(list).to_dict()
    holders = main.groupby("k").cluster_id.apply(set).to_dict()
    by_fam = {f: d for f, d in cands.groupby("family")}
    dec, links = [], []
    for cid in acif_ids:
        fs = {f for f in fam_of.get(cid, set()) if counts.get(f, 0) <= rare_max}
        if not fs:
            dec.append((cid, "not_rare", None, 0, min((counts.get(f, 0) for f in fam_of.get(cid, set())), default=None)))
            continue
        n_fam = min(counts.get(f, 0) for f in fs)
        if any(len(holders.get(k, set()) - {cid}) for k in main_of.get(cid, [])):
            dec.append((cid, "several_acifs", None, 0, n_fam))
            continue
        ok = [r for f in fs if f in by_fam for r in by_fam[f].itertuples()
              if compatible(acif_keys.get(cid, []), main_of.get(cid, []), r)]
        ids = {r.author_idx for r in ok}
        if not ids:
            dec.append((cid, "no_candidate", None, 0, n_fam))
            continue
        if len(ids) > 1:
            dec.append((cid, "several_candidates", None, len(ids), n_fam))
            continue
        r = ok[0]
        aid = int(r.author_idx)
        own = acif_orcid.get(cid)
        if aid in taken and taken[aid] != cid:
            status = "record_linked_elsewhere"
        elif r.orcid and ((own and r.orcid != own) or r.orcid in refused.get(cid, set())):
            status = "record_other_orcid"
        else:
            status = "accept_rare_name"
            links.append((cid, aid, r.author_name, r.orcid, bool(r.in_pool), int(r.works), status))
        dec.append((cid, status, aid, 1, n_fam))
    return (pd.DataFrame(dec, columns=["cluster_id", "status", "author_idx", "n_candidates", "family_count"]),
            pd.DataFrame(links, columns=["cluster_id", "author_idx", "author_name", "orcid", "in_pool", "works", "status"]))


def calibrate(kdec: pd.DataFrame, correct: pd.DataFrame) -> pd.DataFrame:
    """Per family-count band, on ORCID-linked ACIFs run as if unlinked (decide() with rare_max=CALIBRATION_MAX,
    no taken records, no ORCIDs): ACIFs accepted, and how many on one of the ACIF's ORCID-linked records."""
    ok = set(zip(correct.cluster_id, correct.author_idx.astype("int64")))
    d = kdec[kdec.status == "accept_rare_name"].copy()
    d["right"] = [(c, int(a)) in ok for c, a in zip(d.cluster_id, d.author_idx)]
    bands = [(1, 1), (2, 2), (3, 3), (4, 5), (6, 10), (11, 20), (21, CALIBRATION_MAX)]
    rows = []
    for lo, hi in bands:
        s = d[(d.family_count >= lo) & (d.family_count <= hi)]
        rows.append((f"{lo}" if lo == hi else f"{lo}-{hi}", len(s), int(s.right.sum())))
    return pd.DataFrame(rows, columns=["family_count", "accepted", "right"])
