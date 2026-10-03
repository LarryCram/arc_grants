"""
Scopus (pybliometrics) helpers for src/00d_extract_scopus.py: configuration, the ARC university ->
Scopus affiliation map, and the Author Search query for one ACIF. Copied 2026-10-01 from
analysis/utils/scopus.py (which stays as a resting tool; src/ must not import analysis/).

Credentials and cache: this project's .env -- sco_apikey, sco_insttoken (the same key and
institutional token as SmallProjects) and SCOPUS_CACHE_DIR (SmallProjects' pybliometrics cache,
shared). pybliometrics needs a config file; init_scopus() writes one from .env to
DATA_ROOT/scopus_config.ini (outside the repo, since it holds the credentials).

Findings this module is built on (2026-09-30):
  - Author Search matches a profile's own name variants: "Tom Davis" finds Thomas P. Davis, and
    Kotagiri Ramamohanarao is found with either name order. So an ACIF's full_name_keys are
    searched as they are -- no nicknames or swapped orders are generated here.
  - Initial-only keys (s_ng, t_davis) return mostly other people; they are searched only when an
    ACIF has no key with a full given name.
  - The affiliation filter must be AFFIL("name"): it matches the whole affiliation history and
    department-level entries. AF-ID(id) matches only the current affiliation, and only that exact
    id (missed Karen Marsh, current "ANU Research School of Biology", and Annette Braunack-Mayer,
    now at Wollongong).
"""

from __future__ import annotations

import configparser
import csv
from pathlib import Path

import pandas as pd
from dotenv import dotenv_values

from config.scope import admin_orgs_canonical
from config.settings import ADMIN_ORGS_CSV, DATA_ROOT

REPO = Path(__file__).resolve().parents[2]
HEP_CONCORDANCE_XLSX = REPO / "data_persisted" / "HEP_concordances.xlsx"  # copied from SpectralRankingGlobal/data


def init_scopus() -> None:
    """Configure pybliometrics from this project's .env (see module docstring)."""
    import pybliometrics.scopus as scopus
    from pybliometrics.utils.constants import CACHE_PATH, DEFAULT_PATHS

    env = dotenv_values(REPO / ".env")
    for k in ("sco_apikey", "SCOPUS_CACHE_DIR"):
        if not env.get(k):
            raise SystemExit(f"{k} missing from {REPO / '.env'}")
    cache = Path(env["SCOPUS_CACHE_DIR"])
    cfg = configparser.ConfigParser()
    cfg.optionxform = str
    cfg.add_section("Directories")
    for api, path in DEFAULT_PATHS.items():
        cfg.set("Directories", api, str(cache / Path(path).relative_to(CACHE_PATH)))
    cfg.add_section("Authentication")
    cfg.set("Authentication", "APIKey", env["sco_apikey"])
    if env.get("sco_insttoken"):
        cfg.set("Authentication", "InstToken", env["sco_insttoken"])
    cfg.add_section("Requests")
    cfg.set("Requests", "Timeout", "20")
    cfg.set("Requests", "Retries", "5")
    path = DATA_ROOT / "scopus_config.ini"
    with open(path, "w") as f:
        cfg.write(f)
    scopus.init(config_path=path)


# ── ARC universities -> Scopus ───────────────────────────────────────────────

def _norm(s) -> str:
    return " ".join(str(s).lower().split())


def load_university_map(xlsx: Path = HEP_CONCORDANCE_XLSX) -> pd.DataFrame:
    """One row per ARC university: hep_code, scopus_affiliation_id, name (the concordance's
    organisation name). The concordance ('Keys': Scopus EID per university; 'Variants': name
    variants) uses its own university codes, so it is linked to ARC's hep_code through the
    university names in admin_orgs.csv, widened by the 'Variants' sheet. Raises if any ARC
    university is left without a Scopus id."""
    rows = list(csv.DictReader(open(ADMIN_ORGS_CSV, newline="", encoding="utf-8")))
    hep = {}
    for r in rows:
        if r["hep_code"] and r["organisationName"] not in hep:
            hep[r["organisationName"]] = r["hep_code"]
    name_code = {_norm(n): hep[r["organisationName"]] for r in rows if r["organisationName"] in hep
                 for n in (r["organisationName_alias"], r["organisationName"], r["institution_name"]) if n}
    for r in pd.read_excel(xlsx, sheet_name="Variants").itertuples():
        code = name_code.get(_norm(r.Organisation)) or name_code.get(_norm(r.Variants))
        if code:
            name_code.setdefault(_norm(r.Organisation), code)
            name_code.setdefault(_norm(r.Variants), code)
    out = []
    for r in pd.read_excel(xlsx, sheet_name="Keys").itertuples():
        code = name_code.get(_norm(r.Organisation))
        if code:
            out.append({"hep_code": code, "scopus_affiliation_id": str(r.EID), "name": r.Organisation})
    df = pd.DataFrame(out).drop_duplicates("hep_code")
    missing = sorted(set(hep.values()) - set(df.hep_code))
    if missing:
        raise ValueError(f"ARC universities with no Scopus id in {xlsx.name}: {missing}")
    return df.reset_index(drop=True)


def affiliation_names(university_map: pd.DataFrame) -> dict[str, list[str]]:
    """hep_code -> the names used in AFFIL(...): the concordance's name plus Scopus's own
    preferred name for that affiliation id (one Affiliation Retrieval call each, cached)."""
    from pybliometrics.scopus import AffiliationRetrieval
    out = {}
    for r in university_map.itertuples():
        names = [r.name]
        try:
            scopus_name = AffiliationRetrieval(r.scopus_affiliation_id).affiliation_name
            if scopus_name and _norm(scopus_name) != _norm(r.name):
                names.append(scopus_name)
        except Exception:
            pass
        out[r.hep_code] = names
    return out


def grant_universities() -> dict[str, set[str]]:
    """grant_code -> ARC hep_codes of every organisation on the grant: admin_org,
    announcement_admin_org and eligible_orgs (other eligible and collaborating organisations)."""
    from config.settings import PROCESSED_DATA
    _, alias_to_hep, _ = admin_orgs_canonical()
    g = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet",
                        columns=["grant_code", "admin_org", "announcement_admin_org", "eligible_orgs"])
    out = {}
    for r in g.itertuples(index=False):
        orgs = {r.admin_org, r.announcement_admin_org}
        if r.eligible_orgs is not None:
            orgs |= set(r.eligible_orgs)
        out[r.grant_code] = {alias_to_hep[o] for o in orgs if o in alias_to_hep}
    return out


# ── Queries ─────────────────────────────────────────────────────────────────

def search_keys(full_name_keys) -> list[str]:
    """The keys to search: those with a full given name; only if there are none, the
    initial-only ones."""
    keys = sorted(set(full_name_keys))
    full = [k for k in keys if len(k.split("_", 1)[0]) > 1]
    return full or keys


def _quote(s: str) -> str:
    return '"' + s.replace('"', "") + '"'


def name_clause(key: str) -> str:
    """'given_family' -> (AUTHFIRST("given") AND AUTHLASTNAME("family")). The underscore is only
    the key's separator; the family part keeps its spaces and hyphens."""
    given, family = key.split("_", 1)
    return f"(AUTHFIRST({_quote(given)}) AND AUTHLASTNAME({_quote(family)}))"


def acif_query(full_name_keys, hep_codes, affil_names: dict[str, list[str]]) -> str:
    """OR over the ACIF's search keys, AND an OR over AFFIL(name) for its universities.

    Keys and affiliation names are sorted, so the same ACIF always gives the identical query
    string whatever order its keys, grants or universities arrive in. pybliometrics caches a
    search under a hash of the query string, so this is what lets a rerun hit the cache instead of
    spending quota."""
    names = " OR ".join(name_clause(k) for k in search_keys(full_name_keys))
    affils = sorted({n for h in hep_codes for n in affil_names.get(h, [])})
    if not affils:
        return f"({names})"
    return f"({names}) AND (" + " OR ".join(f"AFFIL({_quote(a)})" for a in affils) + ")"
