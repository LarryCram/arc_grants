"""
src/00e_extract_orcid_bulk.py -- ORCID records that could belong to an ARC investigator, from the
ORCID bulk file (ORCID_BULK_PARQUET, Oct-2023 dump, 17M records), with their employers resolved to
ARC's HEP codes (2026-10-06). The build's ORCID-bulk pass (src/acif/orcid_bulk.py) reads only
these outputs.

1. Candidates: bulk records sharing a full-given-name key (jan_smith, not j_smith) with any
   in-scope ARC record (arc_names.parquet full_name_keys). The bulk file's own keys find them;
   their names are then re-parsed with the current NameParser (00d's orcid_facts(): the ORCID
   record cache first -- given+family as a pair, credit and other names -- else the bulk file's
   name and aliases), and those keys are what the build compares.
2. Employers: every employment on the candidate's record (bulk file; plus the cached /record when
   there is one), resolved to an ARC HEP code (admin_orgs.csv) by
     - ROR id: the ROR of the HEP's OpenAlex institution or of any OpenAlex institution whose
       lineage includes it (faculties, institutes); or
     - name: after normalising (norm_org(): a trailing "(QUT)" and a " - Caulfield Campus" suffix
       removed, lowercase, accents and punctuation removed, a leading "the" dropped), equal to one of the HEP's names -- admin_orgs.csv aliases, HEP_concordances.xlsx
       Organisation/OA_name/Variants, the OpenAlex display names and alternatives of the same
       institutions. No partial or "contains" matching (it produced "New York University, Sydney
       campus" -> Sydney before).
   Only candidates with at least one HEP employer are kept.

Outputs (ORCID_BULK_EXTRACT_DIR = processed/orcid_bulk_extract/):
    key_orcids.parquet     (full_name_key, orcid) -- how a candidate was found
    orcid_facts.parquet    orcid, name_source, name_keys (current parser), main_keys (first given
                           name + family of each name form; no middle-name keys), hep_codes
    employers.parquet      orcid, hep_code, org_name, ror, start_year, end_year, source, matched_by
    hep_names.parquet      the crosswalk used: (key, kind name|ror, hep_code)
    orcid_bulk_extract.md  counts

Usage: .venv/bin/python src/00e_extract_orcid_bulk.py
"""

import importlib
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import diskcache
import duckdb
import pandas as pd

from config.scope import admin_orgs_canonical
from config.settings import (DISKCACHE_DIR, OPENALEX_DIR, ORCID_BULK_EXTRACT_DIR, ORCID_BULK_PARQUET,
                             PROCESSED_DATA)

DATA_PERSISTED = Path(__file__).resolve().parents[1] / "data_persisted"


def norm_org(s) -> str:
    """Organisation name for exact comparison: a trailing bracketed part and a ' - ... Campus'
    suffix removed, lowercase, accents and punctuation removed, '&' as 'and', whitespace collapsed,
    a leading 'the' dropped."""
    if not isinstance(s, str):
        return ""
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)                  # trailing "(QUT)"
    s = re.sub(r"\s+[-\u2013,]\s+[^-\u2013,]*\bcampus\b.*$", "", s, flags=re.I)  # " - Caulfield Campus"
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[4:] if s.startswith("the ") else s


def _ror(s) -> str:
    return s.rstrip("/").rsplit("/", 1)[-1].lower() if isinstance(s, str) and s else ""


def hep_crosswalk(con) -> pd.DataFrame:
    """(key, kind, hep_code): normalised names and ROR ids of each ARC HEP."""
    _heps, alias_to_hep, alias_to_inst = admin_orgs_canonical()
    rows = [(norm_org(a), "name", h) for a, h in alias_to_hep.items()]
    hep_inst = {}
    for a, h in alias_to_hep.items():
        if alias_to_inst.get(a):
            hep_inst.setdefault(h, set()).add(alias_to_inst[a])
    inst_hep = {i: h for h, ids in hep_inst.items() for i in ids}

    keys = pd.read_excel(DATA_PERSISTED / "HEP_concordances.xlsx", "Keys")
    var = pd.read_excel(DATA_PERSISTED / "HEP_concordances.xlsx", "Variants")
    org_hep = {}
    for r in keys.itertuples():
        h = inst_hep.get(f"https://openalex.org/{r.institution_idx}")
        if h:
            org_hep[r.Organisation] = h
            rows += [(norm_org(r.Organisation), "name", h), (norm_org(r.OA_name), "name", h)]
    rows += [(norm_org(r.Variants), "name", org_hep[r.Organisation]) for r in var.itertuples()
             if r.Organisation in org_hep]

    con.register("hep_inst", pd.DataFrame([(i, h) for i, h in inst_hep.items()], columns=["id", "hep_code"]))
    inst = con.execute(f"""
        SELECT i.ror, i.display_name, i.display_name_alternatives, h.hep_code
        FROM read_parquet('{OPENALEX_DIR}/institutions.parquet') i
        JOIN hep_inst h ON h.id = i.id OR list_contains(i.lineage, h.id)""").fetchdf()
    for r in inst.itertuples():
        rows.append((_ror(r.ror), "ror", r.hep_code))
        for n in [r.display_name, *(list(r.display_name_alternatives) if r.display_name_alternatives is not None else [])]:
            rows.append((norm_org(n), "name", r.hep_code))
    cw = pd.DataFrame(rows, columns=["key", "kind", "hep_code"]).drop_duplicates()
    cw = cw[cw.key != ""]
    clash = cw.groupby(["key", "kind"]).hep_code.nunique()
    bad = clash[clash > 1].index
    if len(bad):   # a name or ROR claimed by two HEPs is no evidence for either
        print(f"  crosswalk: dropped {len(bad)} keys mapping to 2+ HEPs: {list(bad)[:5]}")
        cw = cw.set_index(["key", "kind"]).drop(index=bad).reset_index()
    return cw.sort_values(["kind", "key"], ignore_index=True)


def arc_full_keys() -> set[str]:
    a = pd.read_parquet(PROCESSED_DATA / "arc_names.parquet", columns=["full_name_keys"])
    return {k for ks in a.full_name_keys for k in ks if len(k.split("_", 1)[0]) > 1}


def find_candidates(con, keys: set[str]) -> pd.DataFrame:
    con.register("arc_keys", pd.DataFrame({"k": sorted(keys)}))
    return con.execute(f"""
        SELECT DISTINCT k AS full_name_key, orcid FROM (
            SELECT orcid, unnest(all_full_name_keys) AS k FROM read_parquet('{ORCID_BULK_PARQUET}'))
        WHERE k IN (SELECT k FROM arc_keys)""").fetchdf()


def bulk_employments(con, orcids: set[str]) -> pd.DataFrame:
    con.register("cand", pd.DataFrame({"orcid": sorted(orcids)}))
    return con.execute(f"""
        SELECT orcid, e.name AS org_name, e.ror AS ror, e.start_year, e.end_year, 'bulk' AS source
        FROM (SELECT orcid, unnest(employments) AS e FROM read_parquet('{ORCID_BULK_PARQUET}')
              WHERE orcid IN (SELECT orcid FROM cand))""").fetchdf()


def _year(d):
    try:
        return int(((d or {}).get("year") or {}).get("value"))
    except (TypeError, ValueError):
        return None


def cache_employments(cache, orcids: set[str], usable) -> pd.DataFrame:
    rows = []
    for o in sorted(orcids):
        rec = cache.get(o)
        if not usable(rec, o):
            continue
        groups = (((rec.get("activities-summary") or {}).get("employments") or {}).get("affiliation-group") or [])
        for g in groups:
            for s in g.get("summaries") or []:
                e = s.get("employment-summary") or {}
                org = e.get("organization") or {}
                dis = org.get("disambiguated-organization") or {}
                ror = dis.get("disambiguated-organization-identifier") if dis.get("disambiguation-source") == "ROR" else None
                rows.append({"orcid": o, "org_name": org.get("name"), "ror": ror,
                             "start_year": _year(e.get("start-date")), "end_year": _year(e.get("end-date")),
                             "source": "cache"})
    return pd.DataFrame(rows, columns=["orcid", "org_name", "ror", "start_year", "end_year", "source"])


def resolve_employers(emp: pd.DataFrame, cw: pd.DataFrame) -> pd.DataFrame:
    by_ror = dict(cw.loc[cw.kind == "ror", ["key", "hep_code"]].values)
    by_name = dict(cw.loc[cw.kind == "name", ["key", "hep_code"]].values)
    rows = []
    for r in emp.itertuples():
        h, how = by_ror.get(_ror(r.ror)), "ror"
        if not h:
            h, how = by_name.get(norm_org(r.org_name)), "name"
        if h:
            rows.append({"orcid": r.orcid, "hep_code": h, "org_name": r.org_name, "ror": _ror(r.ror) or None,
                         "start_year": r.start_year, "end_year": r.end_year, "source": r.source,
                         "matched_by": how})
    cols = ["orcid", "hep_code", "org_name", "ror", "start_year", "end_year", "source", "matched_by"]
    return pd.DataFrame(rows, columns=cols).drop_duplicates(ignore_index=True)


def _main_key(parser, name) -> str | None:
    k = parser.parse(name).full_name_key
    return k if k and len(k.split("_", 1)[0]) > 1 else None


def main_keys(orcids, cache, bulk: pd.DataFrame, usable, parser) -> dict[str, list[str]]:
    """orcid -> the MAIN key (first given name + family; NameParser full_name_key) of each of its
    name forms: the ORCID record cache's given+family pair, credit name and other names when the
    record is usable, else the bulk file's name and aliases. Middle names give no main key, so
    'Peter Robert Marks' is peter_marks, never robert_marks."""
    b = {r.orcid: r for r in bulk.itertuples()}
    out = {}
    for o in sorted(orcids):
        rec, forms = cache.get(o), []
        if usable(rec, o):
            person = rec.get("person") or {}
            name = person.get("name") or {}
            given = (name.get("given-names") or {}).get("value") or ""
            family = (name.get("family-name") or {}).get("value") or ""
            if given or family:
                forms.append((given, family))
            forms.append((name.get("credit-name") or {}).get("value"))
            forms += [x.get("content") for x in (person.get("other-names") or {}).get("other-name") or []]
        elif o in b:
            br = b[o]
            forms = [br.name, *(list(br.aliases) if br.aliases is not None else [])]
        out[o] = sorted({k for f in forms if f for k in [_main_key(parser, f)] if k})
    return out


def main():
    ORCID_BULK_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    x00d = importlib.import_module("src.00d_extract_scopus")
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")

    cw = hep_crosswalk(con)
    keys = arc_full_keys()
    found = find_candidates(con, keys)
    cand = set(found.orcid)
    print(f"ARC full-given-name keys: {len(keys):,}; bulk candidates: {len(cand):,} ORCIDs")

    cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
    emp_all = pd.concat([bulk_employments(con, cand), cache_employments(cache, cand, x00d._usable)],
                        ignore_index=True)
    emp = resolve_employers(emp_all, cw)
    keep = set(emp.orcid)
    print(f"employments read: {len(emp_all):,}; at an ARC HEP: {len(emp):,} rows, {len(keep):,} ORCIDs")

    bulk = x00d.bulk_rows(con, keep)
    facts = x00d.orcid_facts(keep, cache, bulk)
    mk = main_keys(keep, cache, bulk, x00d._usable, x00d._P)
    facts["main_keys"] = facts.orcid.map(mk)
    facts = facts[["orcid", "name_source", "name_keys", "main_keys"]].merge(
        emp.groupby("orcid").hep_code.agg(lambda s: sorted(set(s))).rename("hep_codes").reset_index(), on="orcid")

    found = found[found.orcid.isin(keep)].sort_values(["full_name_key", "orcid"], ignore_index=True)
    found.to_parquet(ORCID_BULK_EXTRACT_DIR / "key_orcids.parquet", index=False)
    facts.to_parquet(ORCID_BULK_EXTRACT_DIR / "orcid_facts.parquet", index=False)
    emp.to_parquet(ORCID_BULK_EXTRACT_DIR / "employers.parquet", index=False)
    cw.to_parquet(ORCID_BULK_EXTRACT_DIR / "hep_names.parquet", index=False)

    unmatched = Counter(emp_all.loc[~emp_all.set_index(["orcid", "org_name"]).index.isin(
        emp.set_index(["orcid", "org_name"]).index), "org_name"])
    au_like = [(n, c) for n, c in unmatched.most_common(400)
               if isinstance(n, str) and re.search(r"australia|sydney|melbourne|queensland|adelaide|perth|"
                                                    r"brisbane|canberra|tasmania|monash|unsw|anu\b|curtin|deakin", n, re.I)]
    L = ["# ORCID bulk extract", "",
         f"- crosswalk: {int((cw.kind == 'name').sum()):,} names, {int((cw.kind == 'ror').sum()):,} ROR ids for "
         f"{cw.hep_code.nunique()} HEPs",
         f"- ARC full-given-name keys: {len(keys):,}; bulk ORCIDs sharing one: {len(cand):,}",
         f"- employments read: {len(emp_all):,}; resolved to an ARC HEP: {len(emp):,} "
         f"(by ROR {int((emp.matched_by == 'ror').sum()):,}, by name {int((emp.matched_by == 'name').sum()):,})",
         f"- ORCIDs kept (an ARC HEP employer): {len(keep):,}; names from: "
         + ", ".join(f"{k} {v:,}" for k, v in facts.name_source.value_counts().items()),
         "", "Most frequent unresolved employer names that look Australian (check the crosswalk):", ""]
    L += [f"- {n} ({c})" for n, c in au_like[:40]]
    (ORCID_BULK_EXTRACT_DIR / "orcid_bulk_extract.md").write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))


if __name__ == "__main__":
    main()
