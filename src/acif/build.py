"""
Cyclic ACIF construction engine.

Per /home/lc/.claude/plans/plan-that-in-tiny-immutable-heron.md. Built incrementally, one
reviewable piece at a time -- src/utils/awards_cif.py is being archived, so nothing here imports
from it; logic worth keeping is ported and condensed, not wrapped.

Built so far: load_items() + seed() -- raw ARC data in, one singleton AwardsCIF per item out
(the plan's "Explicit seeding (stage 1)"). Nothing else yet: no name parsing, no FOR2020/HEP/
institution derivation, no merge tests, no nested loop. Each of those is its own step, reviewed
before the next lands.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from config.settings import PROCESSED_DATA, GRANT_SUMMARIES_CSV, ADMIN_ORGS_CSV, ARC_GRANTS_CSV
from src.utils.for_resolve import upgrade_for_name, upgrade_for_code, resolve_arc_for_entry, for2020_group_name
from src.utils.names import HumanNameParser
from src.acif.models import AwardCIFItem, AwardsCIF


def _admin_orgs_canonical() -> tuple[set[str], dict[str, str], dict[str, str]]:
    """admin_orgs.csv, read once, resolved via the canonical organisationName GROUP rather than
    trusting each alias row individually (an alias row can be correctly flagged HEP='y' but have
    a blank hep_code/institution_id cell while a sibling alias for the same real institution
    carries the real data -- src/utils/awards_cif.py::_load_admin_orgs_rows()'s own finding,
    ported once here rather than as three separate near-duplicate readers).

    Returns (hep_admin_org_aliases, alias_to_hep_code, alias_to_institution_id)."""
    rows = list(csv.DictReader(open(ADMIN_ORGS_CSV, newline="", encoding="utf-8")))
    canonical_hep_code: dict[str, str] = {}
    canonical_institution_id: dict[str, str] = {}
    for row in rows:
        name = row.get("organisationName", "").strip()
        hep_code = row.get("hep_code", "").strip()
        inst_id = row.get("institution_id", "").strip()
        if name and hep_code and name not in canonical_hep_code:
            canonical_hep_code[name] = hep_code
        if name and inst_id and name not in canonical_institution_id:
            canonical_institution_id[name] = inst_id

    hep_admin_org_aliases: set[str] = set()
    alias_to_hep_code: dict[str, str] = {}
    alias_to_institution_id: dict[str, str] = {}
    for row in rows:
        alias = row.get("organisationName_alias", "").strip()
        name = row.get("organisationName", "").strip()
        if not alias:
            continue
        if name in canonical_hep_code:
            hep_admin_org_aliases.add(alias)
            alias_to_hep_code[alias] = canonical_hep_code[name]
        if name in canonical_institution_id:
            alias_to_institution_id[alias] = canonical_institution_id[name]
    return hep_admin_org_aliases, alias_to_hep_code, alias_to_institution_id


def load_items(con: duckdb.DuckDBPyConnection | None = None) -> list[AwardCIFItem]:
    """Raw ARC facts only -- investigators_raw.parquet joined to grants_flat.parquet and
    grant_summaries.csv, filtered to KEEP_ROLES / KEEP_SCHEMES / genuine-HEP admin_org (the same
    three scope rules src/utils/awards_cif.py::load_award_cif_items() applies, ported and
    condensed rather than imported since that module is being archived).

    Populates every AwardCIFItem field that has no default -- unique_id, grant_code, first_name,
    family_name, role_code, orcid, admin_org, institution_oax_id, funding_commence_year, for_name,
    for_code, full_name -- plus is_fellowship, plus admin_orgs (see below). Deliberately leaves
    every other DERIVED field at its dataclass default (first_names, family_names,
    family_name_main, first_initial, first_name_canonical, full_name_key, for_name_tokens,
    parsed, for2020_codes, hep_codes, inst_ids, for_name_rarity, single_institution_grant,
    institution_rarity) -- name parsing, FOR2020 resolution, HEP/institution-set derivation,
    manual name/ORCID correction application, and 00c_extract_propensities.py attachment are
    each their own later step, not folded in here.

    admin_orgs (2026-09-28 finding, confirmed by the grant's own former CI): admin_org alone
    silently drops the at-award institution whenever it differs from the current one (a fellow
    moved institutions mid-grant -- DE120101452 is admin_org=Sydney now, was ANU at
    announcement). admin_org itself stays "the current one," but admin_orgs retains both.

    Confirmed announcement/current renames (award_rename_map.parquet,
    00c_extract_propensities.py::build_award_rename_map()) are collapsed into ONE item here,
    before anything downstream ever sees them as two people -- the announcement-form row is
    dropped outright; full_name/unique_id/cluster_id all follow the surviving current-form row
    unchanged, and full_name_keys becomes the union of BOTH forms' own parsed matching keys
    (since OpenAlex may have indexed either form -- "we don't know what OAX will be using").
    Resolved here and never revisited, per direct design decision (2026-09-28/29). NOTE for
    whoever wires general name-parsing into this function next: that step must UNION into
    full_name_keys for these items, never overwrite it -- the announcement form's keys have no
    other source once this collapse has happened.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        roles_sql = ", ".join(f"'{r}'" for r in KEEP_ROLES)
        schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)
        rows = con.execute(f"""
            SELECT
                i.unique_id, i.grant_code, i.first_name, i.family_name, i.role_code,
                i.orcid, i.is_fellowship, g.admin_org, g.announcement_admin_org,
                o.institution_id AS institution_oax_id,
                g.funding_commence_year, g.primary_for_name,
                regexp_extract(s.primary_field_of_research, '^\\d{{4}}') AS for2008_code
            FROM read_parquet('{PROCESSED_DATA}/investigators_raw.parquet') i
            LEFT JOIN read_parquet('{PROCESSED_DATA}/grants_flat.parquet') g
                ON i.grant_code = g.grant_code
            LEFT JOIN read_csv_auto('{GRANT_SUMMARIES_CSV}') s
                ON i.grant_code = s.grant_id
            LEFT JOIN read_csv_auto('{ADMIN_ORGS_CSV}') o
                ON g.admin_org = o.organisationName_alias
            WHERE i.role_code IN ({roles_sql})
              AND substring(i.grant_code, 1, 2) IN ({schemes_sql})
            ORDER BY i.unique_id
        """).fetchall()
        col_names = [d[0] for d in con.description]
    finally:
        if own_con:
            con.close()

    hep_admin_orgs, _alias_to_hep_code, _alias_to_institution_id = _admin_orgs_canonical()

    rename_map_path = PROCESSED_DATA / "award_rename_map.parquet"
    announcement_ids: set[str] = set()
    current_to_announcement_name: dict[str, str] = {}
    if rename_map_path.exists():
        rename_df = pd.read_parquet(rename_map_path)
        announcement_ids = set(rename_df["announcement_unique_id"])
        current_to_announcement_name = dict(
            zip(rename_df["current_unique_id"], rename_df["announcement_name"])
        )
    name_parser = HumanNameParser()

    items: list[AwardCIFItem] = []
    for row in rows:
        r = dict(zip(col_names, row))
        if r["admin_org"] not in hep_admin_orgs:
            continue
        if r["unique_id"] in announcement_ids:
            continue  # collapsed into its current-form counterpart below, not its own item
        full_name = f"{r['first_name']} {r['family_name']}"
        full_name_keys: list[str] = []
        announcement_name = current_to_announcement_name.get(r["unique_id"])
        if announcement_name:
            keys = set(name_parser.parse(full_name).full_name_keys)
            keys |= set(name_parser.parse(announcement_name).full_name_keys)
            full_name_keys = sorted(keys)
        items.append(AwardCIFItem(
            unique_id=r["unique_id"],
            grant_code=r["grant_code"],
            first_name=r["first_name"],
            family_name=r["family_name"],
            role_code=r["role_code"],
            orcid=r["orcid"],
            admin_org=r["admin_org"],
            admin_orgs=sorted({n for n in (r["admin_org"], r["announcement_admin_org"]) if n}),
            institution_oax_id=r["institution_oax_id"],
            funding_commence_year=r["funding_commence_year"],
            for_name=upgrade_for_name(r["for2008_code"], r["primary_for_name"]),
            for_code=upgrade_for_code(r["for2008_code"]) or r["for2008_code"],
            full_name=full_name,
            full_name_keys=full_name_keys,
            is_fellowship=bool(r["is_fellowship"]),
        ))
    return items


def load_grant_for2020_codes() -> dict[str, list[dict]]:
    """grant_code -> every ARC field-of-research entry for that grant, resolved to FOR2020 group
    (4-digit) precision -- {code, name, is_primary, confidence}, ordered primary-first then
    alphabetically. Ported from src/utils/awards_cif.py's function of the same name (that logic
    was already clean, single-purpose, and validated -- not part of what needed condensing).

    Scoped to KEEP_SCHEMES before resolving anything: raw_json.csv covers every ARC scheme ever
    run, most out of scope for this project (e.g. "LE" Linkage-Equipment grants, which fund
    shared lab equipment for a whole department and carry FOR-code spreads unrelated to any one
    CI's own research identity)."""
    df = pd.read_csv(ARC_GRANTS_CSV)
    out: dict[str, dict[str, dict]] = defaultdict(dict)  # grant_code -> {code4: entry}
    for _, row in df.iterrows():
        try:
            rec = json.loads(row["single_grant"])
        except (TypeError, ValueError):
            continue
        grant_code = rec.get("data", {}).get("id")
        if not grant_code or grant_code[:2] not in KEEP_SCHEMES:
            continue
        for f in rec.get("data", {}).get("attributes", {}).get("field-of-research") or []:
            resolved = resolve_arc_for_entry(f.get("code"), f.get("type"))
            if resolved is None:
                continue
            code20, name, confidence = resolved
            code4 = code20[:4]
            if code20 != code4:
                name = for2020_group_name(code4) or name
            is_primary = bool(f.get("isPrimary"))
            existing = out[grant_code].get(code4)
            if existing is None:
                out[grant_code][code4] = {
                    "code": code4, "name": name, "is_primary": is_primary, "confidence": confidence,
                }
            else:
                existing["is_primary"] = existing["is_primary"] or is_primary
                existing["confidence"] = max(existing["confidence"], confidence)
    return {
        grant_code: sorted(entries.values(), key=lambda e: (not e["is_primary"], e["name"].lower()))
        for grant_code, entries in out.items()
    }


def load_grant_org_facts() -> dict[str, dict]:
    """grant_code -> {hep_codes, inst_ids} -- both genuinely GRANT-level facts (every investigator
    on the same grant shares the same eligible_orgs/admin_org/announcement_admin_org), so computed
    once per grant here rather than re-derived per item.

    hep_codes: every HEP-eligible organisation formally on the grant (eligible_orgs UNION
    admin_org) -- the broader set, used for subfield/HEP corroboration downstream.
    inst_ids: this grant's own ARC-org set, narrower and deliberately NOT including eligible_orgs
    -- exactly {admin_org, announcement_admin_org} (partner/collaborating orgs aren't reliably
    THIS investigator's own institution any more than admin_org is)."""
    df = pd.read_parquet(
        PROCESSED_DATA / "grants_flat.parquet",
        columns=["grant_code", "admin_org", "announcement_admin_org", "eligible_orgs"],
    )
    _hep_admin_orgs, alias_to_hep_code, alias_to_institution_id = _admin_orgs_canonical()

    out: dict[str, dict] = {}
    for row in df.itertuples(index=False):
        # eligible_orgs is a numpy array (parquet list column via pyarrow) -- `x or []` is
        # invalid, ambiguous truth value for an array with 2+ elements (same pitfall already
        # documented in 00c_extract_propensities.py's build_institution_rarity()).
        eligible = set(row.eligible_orgs) if row.eligible_orgs is not None else set()
        if row.admin_org:
            eligible.add(row.admin_org)
        hep_codes = sorted({alias_to_hep_code[n] for n in eligible if n in alias_to_hep_code})

        arc_orgs = {n for n in (row.admin_org, row.announcement_admin_org) if n}
        inst_ids = sorted({alias_to_institution_id[n] for n in arc_orgs if n in alias_to_institution_id})

        out[row.grant_code] = {"hep_codes": hep_codes, "inst_ids": inst_ids}
    return out


def enrich_items(
    items: list[AwardCIFItem],
    for2020: dict[str, list[dict]],
    org_facts: dict[str, dict],
) -> list[AwardCIFItem]:
    """Attach the three grant-level derived facts (for2020_codes, hep_codes, inst_ids) computed
    once per grant_code, not per item -- each of an item's grant-mates gets the identical value.
    Items whose grant_code has no facts (should not happen post-scope-filter, checked rather than
    assumed) keep the dataclass default (empty list), not a KeyError.

    Takes the two lookups as parameters rather than loading them itself (2026-09-28) -- pure
    transformation, no I/O of its own, so it's testable against small hand-built dicts instead of
    real parquet/CSV data. Production callers pass load_grant_for2020_codes()/
    load_grant_org_facts()'s real output; see build_stage_zero() below for the actual wiring."""
    return [
        replace(
            item,
            for2020_codes=for2020.get(item.grant_code, []),
            hep_codes=org_facts.get(item.grant_code, {}).get("hep_codes", []),
            inst_ids=org_facts.get(item.grant_code, {}).get("inst_ids", []),
        )
        for item in items
    ]


def build_stage_zero() -> list[AwardsCIF]:
    """Production entry point: load real ARC data, enrich it with the real grant-level lookups,
    seed one singleton AwardsCIF per item. The only call site that wires enrich_items() to real
    I/O -- kept separate so enrich_items() itself stays a pure, directly-testable function."""
    items = load_items()
    items = enrich_items(items, load_grant_for2020_codes(), load_grant_org_facts())
    return seed(items)


def seed(items: list[AwardCIFItem]) -> list[AwardsCIF]:
    """Explicit stage-1 seeding (the plan's own term): one singleton AwardsCIF per item, never an
    empty list. cluster_id = item.unique_id needs no tie-break rule yet -- a singleton's id is
    trivially just its own one member; the year/scheme/remainder tie-break only matters once a
    merge has to pick a survivor between two or more."""
    return [
        AwardsCIF(cluster_id=item.unique_id, items=[item], cycle_stages=[1])
        for item in items
    ]
