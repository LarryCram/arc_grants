"""
src/awards_cif.py

AwardsCIF() -- the ARC-side identity object: a provisional grouping of Award-CI/F items
(one row per grant x ARC Chief Investigator/Fellow, already role/scheme-filtered to
KEEP_ROLES ∩ KEEP_SCHEMES) under evidence that they are potentially the same real person.

This module holds the data model only (AwardCIFItem, AwardsCIF). Construction -- the
functions that load items, cluster them, and refine those clusters into resolved
AwardsCIF() instances -- lives alongside this module and is planned separately; see
/home/lc/.claude/plans/review-this-code-base-groovy-key.md for the full design and the
reasoning behind treating this as the primary object with today's 01_prepare_arc.py logic
rebuilt onto it, rather than a wrapper constructed around that logic.

Field provenance: AwardsCIF's fields mirror arc_persons.parquet's real output columns
(01_prepare_arc.py's Phase 2), not a guessed shape -- including gap_candidates and
orcid_for_codes, which an earlier planning draft of this object missed. Aggregated fields
(inst_arr, for_names, for_codes) are sorted deduplicated lists, matching how
_aggregate_clusters() actually produces them, not sets -- kept this way so a rebuilt
AwardsCIF can be diffed field-for-field against today's arc_persons.parquet.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from pathlib import Path

import diskcache
import duckdb
import pandas as pd
from nameparser import HumanName
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

from config.settings import PROCESSED_DATA, ADMIN_ORGS_CSV, GRANT_SUMMARIES_CSV, ARC_GRANTS_CSV, DISKCACHE_DIR, OAX_AUTHORS, TOP_CUT, DUCKDB_TMP_DIR
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import canonicalize_name_punctuation, make_expanded_for_tokens, name_part_tokens, strip_diacriticals, strip_postnominals, for_name_tokens
from src.utils.for_resolve import upgrade_for_code, upgrade_for_name, resolve_arc_for_entry
from src.utils.cluster_checks import (
    first_names_compatible,
    RARE_NAME_TF as _CLUSTER_CHECKS_RARE_NAME_TF,
    for2020_primary_fields as _for2020_primary_fields,
    division_mismatch_for2020 as _self_division_mismatch,
    division_mismatch_for2020_pairwise as _pairwise_division_mismatch,
    is_suspicious_for2020,
    aggregate_for2020_codes,
    INDIGENOUS_DIVISION_PREFIX,
)

_DATA_PERSISTED = Path(__file__).resolve().parents[2] / "data_persisted"
_FOR_CONCORDANCE_CSV = _DATA_PERSISTED / "for_concordance.csv"
_MANUAL_NAME_CORRECTIONS_CSV = _DATA_PERSISTED / "manual_name_corrections.csv"
_MANUAL_ORCID_CORRECTIONS_CSV = _DATA_PERSISTED / "manual_orcid_corrections.csv"
_MANUAL_SPLITS_CSV = _DATA_PERSISTED / "manual_splits.csv"
_MANUAL_ORCIDS_CSV = _DATA_PERSISTED / "manual_orcids.csv"
_MANUAL_MERGES_CSV = _DATA_PERSISTED / "manual_merges.csv"
_ENRICHMENT_BLOCKLIST_CSV = _DATA_PERSISTED / "enrichment_blocklist.csv"
_MANUAL_RESOLUTIONS_CSV = _DATA_PERSISTED / "manual_resolutions.csv"

CLUSTER_THRESHOLD = 0.9  # same value as 01_prepare_arc.py -- high precision, prefer splitting over merging
RARE_NAME_TF = 2e-6  # OAX full_name_key TF below this -> rare name (tier 2 vs 3). Distinct from
                      # cluster_checks.RARE_NAME_TF (5e-5), which governs is_suspicious()'s own
                      # rare-name carve-out -- same constant name, same value as 01_prepare_arc.py,
                      # different meaning/module than cluster_checks's.


@dataclass(frozen=True)
class AwardCIFItem:
    """One row per (grant x ARC CI/Fellow) -- the atomic unit AwardsCIF() clusters.

    Raw ARC facts plus the normalized/derived name forms Splink's dedupe_only blocking and
    comparisons need (family_name_main, first_initial, first_name_canonical, full_name_key,
    for_name_tokens) -- computed once at load time by arc_name_arrays()/_prep()'s current
    logic, not left to be recomputed lazily on every access.
    """

    # raw ARC facts (investigators_raw.parquet joined to grants_flat.parquet)
    unique_id: str
    grant_code: str
    first_name: str
    family_name: str
    role_code: str
    orcid: str | None
    admin_org: str | None
    institution_oax_id: str | None
    funding_commence_year: int | None
    for_name: str | None  # ANZSRC name for this grant, upgraded to 2020 series
    for_code: str | None  # ANZSRC code for this grant, upgraded to 2020 series -- primary only,
                          # sourced from grant_summaries.csv, kept unchanged for backward
                          # compatibility with existing for_names/for_codes aggregation. See
                          # for2020_codes below for the fuller replacement.

    # derived, normalized forms (mirrors 01_prepare_arc.py's arc_name_arrays()/_prep())
    full_name: str
    first_names: list[str] = field(default_factory=list)
    family_names: list[str] = field(default_factory=list)
    family_name_main: str | None = None
    first_initial: str | None = None
    first_name_canonical: str | None = None
    full_name_key: str | None = None
    for_name_tokens: list[str] = field(default_factory=list)

    # Full per-grant FOR2020 code list (2026-08-12) -- every field-of-research entry ARC
    # recorded for this grant (raw_json.csv, not just grant_summaries.csv's single primary),
    # each resolved to a FOR2020 4-digit group via for_resolve.resolve_arc_for_entry() +
    # truncation (see load_grant_for2020_codes()'s docstring). One dict per entry:
    # {"code": "3705", "name": "Geology", "is_primary": True, "confidence": 1.0}, ordered
    # primary-first then alphabetically. Empty list if this grant has no field-of-research
    # block in raw_json.csv (~76/33,650 grants) or nothing resolved.
    for2020_codes: list[dict] = field(default_factory=list)

    # HEP codes for every HEP-eligible organisation formally on this grant (2026-08-16) --
    # NOT just admin_org's own HEP. Resolved from grants_flat.parquet's eligible_orgs column
    # (Administering + Other Eligible + Collaborating Organisation roles -- see
    # 00_extract_arc.py::extract_grant_flat()'s docstring for why those 3 and not the other 4
    # role names ARC records) via _load_hep_crosswalk(). Deduplicated, sorted; empty if none of
    # this grant's eligible orgs resolve to an Australian HEP (rare but real -- ~0.55% of
    # in-scope grants have a non-HEP admin_org, e.g. medical research institutes).
    hep_codes: list[str] = field(default_factory=list)


@dataclass
class CandidateWork:
    """One OpenAlex work reached by ANY of an AwardsCIF's oax_candidates -- the atomic unit
    src/utils/oeuvre_build.py operates on (roadmap step 2). `included`/`exclusion_reason`/
    `signals` are set by oeuvre_build.py's pipeline stages, never by anything in this module --
    this dataclass only defines the shape. `signals` holds actual recorded values (e.g.
    signals["subfield_match"] = True), never a bare verdict alone -- matching the project's
    "provenance with actual values, not just a category label" principle."""

    work_idx: int
    source_author_idxs: list[int] = field(default_factory=list)  # which of this AwardsCIF's own
                                                                    # oax_candidates claim this work

    publication_year: int | None = None
    cited_by_count: int = 0
    type: str | None = None
    title: str | None = None
    doi: str | None = None
    source_id: int | None = None  # works.source_id -- venue/host record

    subfield_idx: int | None = None  # best topic per work_idx (ROW_NUMBER() over score DESC,
    subfield_name: str | None = None  # same pattern 01_fetch_oeuvres.py already uses)
    field_name: str | None = None
    domain_name: str | None = None

    own_institution_idxs: list[int] = field(default_factory=list)  # this AwardsCIF's own
                                                                     # candidate author_idx's
                                                                     # institution(s) on this work
    coauthor_author_idxs: list[int] = field(default_factory=list)
    coauthor_names: list[str] = field(default_factory=list)
    coauthor_institution_idxs: list[int] = field(default_factory=list)

    included: bool = True
    exclusion_reason: str | None = None  # set when included=False
    signals: dict = field(default_factory=dict)


@dataclass
class AwardsCIF:
    """A provisional grouping of AwardCIFItems under evidence they are potentially the same
    real ARC CI/Fellow. Fields mirror arc_persons.parquet's real output columns exactly --
    see module docstring -- so a rebuilt AwardsCIF can be diffed against it field-for-field.

    Deliberately not yet present: any works/oeuvre data, or a definitive-gate status/score.
    Those are added by later roadmap steps (see the plan file), not this object as built here.
    """

    cluster_id: str  # = arc_id, unchanged key
    items: list[AwardCIFItem] = field(default_factory=list)

    # aggregated across items -- sorted deduplicated lists, matching _aggregate_clusters()
    full_names: list[str] = field(default_factory=list)
    first_names: list[str] = field(default_factory=list)
    family_names: list[str] = field(default_factory=list)
    orcids: list[str] = field(default_factory=list)
    inst_arr: list[str] = field(default_factory=list)
    for_names: list[str] = field(default_factory=list)
    for_codes: list[str] = field(default_factory=list)
    full_name_key: str | None = None  # modal full_name_key across items

    # HEP codes across every one of this person's grants' full eligible-org sets (2026-08-16) --
    # union of AwardCIFItem.hep_codes, not just each grant's single admin_org's HEP. See
    # AwardCIFItem.hep_codes' docstring for scope/provenance.
    hep_codes: list[str] = field(default_factory=list)

    # Full FOR2020 code list (2026-08-12), unioned across items' for2020_codes -- see
    # AwardCIFItem.for2020_codes and load_grant_for2020_codes() for provenance. Deduped by
    # code across the person's whole grant history: is_primary True if primary on >=1 grant,
    # confidence = max seen. Ordered by weight = number of this person's grants where the code
    # was primary (descending), then number of grants where it appeared at all (descending),
    # then alphabetically by name -- a natural person-level extension of the per-grant
    # primary-first-then-alpha ordering, not something ARC's data states directly.
    for2020_codes: list[dict] = field(default_factory=list)

    grant_ids: list[str] = field(default_factory=list)  # = [item.unique_id for item in items]
    n_grants: int = 0

    orcid_status: str = "NO_ORCID"  # HAS_ORCID | NO_ORCID | MULTI_ORCID
    orcid_for_codes: list[dict] = field(default_factory=list)  # ERA FOR codes via for_cache
    gap_candidates: list[str] = field(default_factory=list)  # other cluster_ids not ruled out
    reliability_tier: str | None = None  # 1a | 1b | 1c | 2 | 3 | 4 | 4u
    resolution_status: str = "UNRESOLVED"  # RESOLVED | UNRESOLVED

    # candidate OpenAlex author_idx set -- {oax_id} ∪ secondary_oax_ids, populated later
    oax_candidates: list[str] = field(default_factory=list)

    # Set aside by set_aside_indigenous_research() (2026-08-12) -- Indigenous-focused research
    # (FOR2020 division 45) is culturally important and not well portrayed by the bibliometric
    # methods this project uses, so it's deliberately excluded from the working population
    # rather than run through them. excluded_reason holds a short machine string ("indigenous"),
    # matching CandidateWork.exclusion_reason's convention -- a category label, with the actual
    # supporting facts (which grants/codes triggered it) in provenance, not just a bare flag.
    excluded: bool = False
    excluded_reason: str | None = None

    # union of works reached by any oax_candidate (roadmap step 2, src/utils/oeuvre_build.py) --
    # one list, not a separate included/excluded pair, so an included work's own signals stay
    # just as inspectable as an excluded one's (mirrors items: self-contained atomic detail).
    oeuvre: list[CandidateWork] = field(default_factory=list)

    # renamed from cluster_history; same JSON-event-log shape and existing event types
    # (splink_cluster, orcid_merge, manual_split, ...) -- ARC-side identity events only
    # in this pass; oeuvre-level exclusion provenance is appended by later work-set
    # operations, not a different owner.
    provenance: list[dict] = field(default_factory=list)

    def record_event(self, event: str, **details) -> None:
        """Append a structured provenance event. Every construction/refinement operation
        should call this rather than appending to `provenance` directly, so event shape
        stays consistent."""
        self.provenance.append({"event": event, **details})

    @property
    def works(self) -> list[CandidateWork]:
        """Only the still-included works -- oeuvre keeps every candidate work, included or not,
        for audit; most callers want this filtered view."""
        return [w for w in self.oeuvre if w.included]


# ── construction: load_award_cif_items ──────────────────────────────────────────

def _name_forms(first_name: str, family_name: str) -> tuple[list[str], list[str]]:
    """Mirrors 01_prepare_arc.py's arc_name_arrays(): given-name tokens (+ their initials)
    and a single normalized family-name form, from ARC's raw first_name/family_name fields."""
    full = f"{first_name or ''} {family_name or ''}".strip()
    hn = HumanName(strip_postnominals(canonicalize_name_punctuation(full)))
    if not hn.last and hn.first:
        hn.last = hn.first

    f_toks = name_part_tokens(hn.first) + name_part_tokens(hn.middle)
    fam_norm = strip_diacriticals(hn.last).lower().strip() if hn.last else ""

    first_names = list(set(f_toks + [t[0] for t in f_toks if t]))
    family_names = [fam_norm] if fam_norm else []

    if not f_toks and fam_norm:
        first_names.append(fam_norm[0])

    return list(set(first_names)), family_names


def _first_initial(first_names: list[str]) -> str | None:
    if not first_names:
        return None
    full = [t for t in first_names if len(t) > 1]
    return full[0][0] if full else first_names[0][0]


def _first_name_canonical(first_names: list[str]) -> str | None:
    full_toks = [t for t in first_names if len(t) > 1]
    initials = [t for t in first_names if len(t) == 1]
    if full_toks:
        return max(full_toks, key=len)
    if initials:
        return initials[0]
    return None


def _load_manual_name_corrections() -> dict[str, dict]:
    """data_persisted/manual_name_corrections.csv -- hand-confirmed fixes for ARC-source typos
    (e.g. LP0211975_MarieMalherbe: ARC's own investigators-at-announcement record has "Marie"
    where investigators-current and 4 other grants sharing the same ORCID all say "François" --
    confirmed against raw_json.csv directly, not a guess). Keyed on unique_id, not cluster_id,
    since the correction applies to one raw item before any clustering happens."""
    if not _MANUAL_NAME_CORRECTIONS_CSV.exists():
        return {}
    import csv as _csv
    corrections: dict[str, dict] = {}
    with open(_MANUAL_NAME_CORRECTIONS_CSV, newline="") as f:
        for row in _csv.DictReader(f):
            uid = row["unique_id"].strip()
            if uid:
                corrections[uid] = row
    return corrections


def _load_manual_orcid_corrections() -> dict[str, dict]:
    """data_persisted/manual_orcid_corrections.csv -- hand-confirmed fixes for ARC-source ORCID
    data-entry errors, same convention as _load_manual_name_corrections() (keyed on unique_id,
    applied before clustering). Unlike the name-correction file, `correct_orcid` is often blank
    on purpose -- the confirmed cases so far (2026-08-20: Wenhui Duan/Chien Ming Wang across 5
    grants, Alexandra Lasczik/Tracey Bunda, Georgia Curran/Enid Gallagher) are all "two real
    people sharing one ORCID", where the wrong row's own correct ORCID isn't known (or, for
    Gallagher, confirmed not to exist at all) -- so the fix is to null the field, not substitute
    a different value. A blank correct_orcid must still override i.orcid to None, not be treated
    as "no correction" -- see the CASE/NULLIF handling at the call site."""
    if not _MANUAL_ORCID_CORRECTIONS_CSV.exists():
        return {}
    import csv as _csv
    corrections: dict[str, dict] = {}
    with open(_MANUAL_ORCID_CORRECTIONS_CSV, newline="") as f:
        for row in _csv.DictReader(f):
            uid = row["unique_id"].strip()
            if uid:
                corrections[uid] = row
    return corrections


def load_grant_for2020_codes() -> dict[str, list[dict]]:
    """grant_code -> ordered list of {code, name, is_primary, confidence} -- every ARC
    field-of-research entry for that grant (raw_json.csv's
    data.attributes.field-of-research list: {code, name, isPrimary, type}), each resolved to
    FOR2020 via for_resolve.resolve_arc_for_entry() and truncated to 4-digit GROUP precision.

    Truncation is safe, not approximate: ANZSRC codes are hierarchically prefixed, so a 6-digit
    field code's first 4 digits ARE its parent group code by construction -- verified directly
    against the package's own for2020_group_openalex_subfield.csv (every truncated code checked
    is a real listed group), not assumed. ARC's own raw data always carries exactly one 4-digit
    code per grant plus 0-15 additional 6-digit codes (confirmed by scanning the full 33,650-row
    corpus), so this collapses everything to one consistent precision rather than leaving mixed
    4-/6-digit codes for callers to handle.

    Codes stay strings throughout -- never cast to int -- since ~44k of the raw entries (mostly
    FOR2008/RFCD98-vintage, division 01-09) have a leading zero int() would silently drop
    (confirmed present in the real data, not a hypothetical risk).

    ARC's raw `type` field is one of RFCD98 (pre-ANZSRC, ARC's own name for what
    research_classification calls FOR1998 -- confirmed by resolving real RFCD98 codes through
    it and getting sensible FOR2020 matches, not assumed from the label alone), FOR08, or FOR20
    -- resolve_arc_for_entry() handles the ARC-label -> package-scheme translation.

    Deduped by resolved 4-digit code within a grant (is_primary = True if ANY matching raw entry
    was primary; confidence = max seen). Ordered is_primary first, then alphabetically by name --
    ARC's raw data carries no numeric weight field for multi-FOR grants (checked directly), so
    isPrimary is the only ordering signal available.

    Scoped to KEEP_SCHEMES before any resolution work (2026-08-13) -- raw_json.csv covers every
    ARC scheme ever run (33,650 grants), most of which this project never uses at all (e.g. "LE"
    Linkage-Equipment grants, which fund shared lab equipment serving a whole department and so
    carry unusually broad FOR-code spreads that have nothing to do with any one CI's own
    research identity -- confirmed directly on the single largest example found, 16 codes on one
    LE grant). Resolving and inspecting out-of-scope grants wastes work and, worse, risks
    polluting exactly this kind of diagnostic with irrelevant outliers -- scope first, then look.
    """
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
        fors = rec.get("data", {}).get("attributes", {}).get("field-of-research") or []

        for f in fors:
            resolved = resolve_arc_for_entry(f.get("code"), f.get("type"))
            if resolved is None:
                continue
            code20, name, confidence = resolved
            code4 = code20[:4]
            is_primary = bool(f.get("isPrimary"))

            existing = out[grant_code].get(code4)
            if existing is None:
                out[grant_code][code4] = {
                    "code": code4, "name": name,
                    "is_primary": is_primary, "confidence": confidence,
                }
            else:
                existing["is_primary"] = existing["is_primary"] or is_primary
                existing["confidence"] = max(existing["confidence"], confidence)

    return {
        grant_code: sorted(entries.values(), key=lambda e: (not e["is_primary"], e["name"].lower()))
        for grant_code, entries in out.items()
    }


def _load_admin_orgs_rows() -> tuple[list[dict], dict[str, str]]:
    """admin_orgs.csv rows, plus canonical organisationName -> hep_code (any non-null hep_code
    found under any alias row sharing that canonical name -- see _load_hep_crosswalk()'s
    docstring for why resolution goes through the canonical name, not each alias row
    individually). Shared by _load_hep_crosswalk() (alias name -> hep_code) and
    _load_institution_hep_crosswalk() (OpenAlex institution_idx -> hep_code) so both read the
    CSV once and apply the same defensive resolution.
    """
    import csv as _csv
    canonical_hep: dict[str, str] = {}
    rows = []
    with open(ADMIN_ORGS_CSV, newline="", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            rows.append(row)
            name = row.get("organisationName", "").strip()
            hep = row.get("hep_code", "").strip()
            if name and hep and name not in canonical_hep:
                canonical_hep[name] = hep
    return rows, canonical_hep


def _load_hep_crosswalk() -> dict[str, str]:
    """admin_orgs.csv organisationName_alias -> hep_code, resolved via the canonical
    organisationName group rather than trusting each alias row individually.

    Why (2026-08-16, user finding): admin_orgs.csv is designed as alias -> canonical-name ->
    crosswalk-data, but that discipline can be forgotten when a new alias row is added -- found
    3 real instances where an alias row was correctly flagged HEP='y' but its own
    institution_id/hep_code cells were blank while the canonical-name row for the same real
    institution had the correct data (University of Western Sydney / The University of Western
    Sydney -> Western Sydney University; Northern Territory University -> Charles Darwin
    University; The Flinders University of South Australia -> Flinders University). Those three
    have since been fixed directly in admin_orgs.csv, but this resolves defensively regardless --
    any hep_code found under *any* alias of a canonical organisationName is applied to every
    alias sharing that canonical name, so a future forgotten alias doesn't silently resolve to
    "no HEP" the way it did here.
    """
    rows, canonical_hep = _load_admin_orgs_rows()
    crosswalk: dict[str, str] = {}
    for row in rows:
        alias = row.get("organisationName_alias", "").strip()
        name = row.get("organisationName", "").strip()
        hep = canonical_hep.get(name)
        if alias and hep:
            crosswalk[alias] = hep
    return crosswalk


def _load_institution_hep_crosswalk() -> dict[int, str]:
    """admin_orgs.csv OpenAlex institution_idx (int) -> hep_code, same canonical-group
    resolution as _load_hep_crosswalk(). For resolving a candidate's own OpenAlex-side
    institution (from an authorship row's institution_idx, not an ARC organisation name string)
    to a HEP code -- used by oeuvre_build.py's subfield+HEP keep/drop signal (2026-08-16). Only
    ~42/114 admin_orgs.csv rows are real Australian HEPs with a resolvable institution_id at all;
    everything else (foreign institutions, non-HEP research institutes) correctly has no entry.
    """
    rows, canonical_hep = _load_admin_orgs_rows()
    crosswalk: dict[int, str] = {}
    for row in rows:
        name = row.get("organisationName", "").strip()
        inst_id = row.get("institution_id", "").strip()
        hep = canonical_hep.get(name)
        if not (inst_id and hep):
            continue
        try:
            idx = int(inst_id.rsplit("I", 1)[-1])
        except (ValueError, IndexError):
            continue
        crosswalk[idx] = hep
    return crosswalk


def load_award_cif_items(
    con: duckdb.DuckDBPyConnection | None = None,
) -> tuple[list[AwardCIFItem], dict[str, dict], dict[str, dict]]:
    """Load Award-CI/F items: investigators_raw.parquet joined to grants_flat.parquet and
    (for the FOR-code upgrade) grant_summaries.csv, filtered to KEEP_ROLES ∩ KEEP_SCHEMES,
    with normalized name forms and the upgraded ANZSRC FOR name/code attached per item.

    Mirrors 01_prepare_arc.py's Phase 1 (the arc_raw CTE + arc_names()/for_tokens() UDFs +
    final SELECT) -- same source tables and filters, typed objects instead of a parquet file.

    Returns (items, corrections_applied, orcid_corrections_applied) -- corrections_applied maps
    unique_id to the manual_name_corrections.csv row that was applied, if any;
    orcid_corrections_applied is the same shape for manual_orcid_corrections.csv. AwardCIFItem
    itself carries no provenance (it's a raw atomic unit, not a resolved identity) --
    cluster_items() is responsible for recording a provenance event on any AwardsCIF whose items
    were corrected, once that cluster actually exists.
    """
    corrections = _load_manual_name_corrections()
    orcid_corrections = _load_manual_orcid_corrections()
    own_con = con is None
    con = con or duckdb.connect()
    try:
        roles_sql = ", ".join(f"'{r}'" for r in KEEP_ROLES)
        schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)

        rows = con.execute(f"""
            SELECT
                i.unique_id,
                i.grant_code,
                i.first_name,
                i.family_name,
                i.role_code,
                i.orcid,
                g.admin_org,
                o.institution_id AS institution_oax_id,
                g.funding_commence_year,
                g.primary_for_name,
                g.eligible_orgs,
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
        """).fetchall()
        col_names = [d[0] for d in con.description]
    finally:
        if own_con:
            con.close()

    expanded_for_tokens = make_expanded_for_tokens(str(_FOR_CONCORDANCE_CSV))
    grant_for2020_codes = load_grant_for2020_codes()
    hep_crosswalk = _load_hep_crosswalk()

    items: list[AwardCIFItem] = []
    n_dropped_non_hep_admin = 0
    for row in rows:
        r = dict(zip(col_names, row))

        # Scope decision (2026-08-16, user-directed): drop any investigator record whose
        # grant's admin_org does not resolve to a recognised Australian HEP (e.g. Botanic
        # Gardens & Parks Authority, medical research institutes) -- this used to be allowed
        # through (documented on AwardsCIF.hep_codes as "~0.55% of in-scope grants have a
        # non-HEP admin_org, rare but real"), but the oeuvre_build.py subfield/HEP keep/drop
        # rule structurally can never evaluate hep_match for such a cluster (no HEP code exists
        # to match against), so a person whose only/primary grant has a non-HEP admin_org gets
        # judged on subfield_match alone -- a real, avoidable blind spot. Filtered at the
        # earliest point items are constructed, same as the KEEP_ROLES/KEEP_SCHEMES scope
        # filters already applied in the SQL above, so a non-HEP-admin_org record never enters
        # clustering at all -- mirrored in 01_prepare_arc.py's Phase 1 for consistency.
        if r["admin_org"] not in hep_crosswalk:
            n_dropped_non_hep_admin += 1
            continue

        first_name = r["first_name"]
        correction = corrections.get(r["unique_id"])
        if correction is not None:
            first_name = correction["correct_first_name"]

        orcid = r["orcid"]
        orcid_correction = orcid_corrections.get(r["unique_id"])
        if orcid_correction is not None:
            # A blank correct_orcid means "null the field", not "no correction" -- must not
            # fall back to r["orcid"] here.
            orcid = orcid_correction["correct_orcid"] or None

        for_name = upgrade_for_name(r["for2008_code"], r["primary_for_name"])
        for_code = upgrade_for_code(r["for2008_code"]) or r["for2008_code"]

        first_names, family_names = _name_forms(first_name, r["family_name"])
        family_name_main = max(family_names, key=len) if family_names else None
        first_initial = _first_initial(first_names)
        first_name_canonical = _first_name_canonical(first_names)
        full_name_key = (
            f"{first_name_canonical}_{family_name_main}"
            if first_name_canonical and family_name_main
            else None
        )

        # Union eligible_orgs (announcement-time organisations-at-announcement list) with
        # admin_org itself (2026-08-16 fix): these can genuinely disagree -- 12.13% of all
        # grants have admin_org absent from eligible_orgs, found via Patricia Valery's
        # FT100100511 (admin_org='Charles Darwin University', eligible_orgs=['Queensland
        # Institute of Medical Research']) -- confirmed by the user as a real institution
        # change (QIMR -> CDU) upon receiving the fellowship, not a data error in either
        # field. admin_org (extract_grant_flat() prefers the *current*
        # administering-organisation attrs field over the announcement-time one) can
        # therefore be more current than eligible_orgs, which is built entirely from the
        # announcement-time organisations-at-announcement list. Without this union, the scope
        # filter above (which already validates admin_org resolves to a HEP) and hep_codes
        # itself could disagree for the same item.
        eligible_orgs = set(r["eligible_orgs"] or [])
        if r["admin_org"]:
            eligible_orgs.add(r["admin_org"])
        hep_codes = sorted({hep_crosswalk[name] for name in eligible_orgs if name in hep_crosswalk})

        items.append(AwardCIFItem(
            unique_id=r["unique_id"],
            grant_code=r["grant_code"],
            first_name=first_name,
            family_name=r["family_name"],
            role_code=r["role_code"],
            orcid=orcid,
            admin_org=r["admin_org"],
            institution_oax_id=r["institution_oax_id"],
            funding_commence_year=r["funding_commence_year"],
            for_name=for_name,
            for_code=for_code,
            for2020_codes=grant_for2020_codes.get(r["grant_code"], []),
            hep_codes=hep_codes,
            full_name=f"{first_name} {r['family_name']}",
            first_names=first_names,
            family_names=family_names,
            family_name_main=family_name_main,
            first_initial=first_initial,
            first_name_canonical=first_name_canonical,
            full_name_key=full_name_key,
            for_name_tokens=expanded_for_tokens(for_name),
        ))

    print(f"  Dropped {n_dropped_non_hep_admin:,} investigator record(s) with a non-HEP admin_org")
    return items, corrections, orcid_corrections


# ── construction: cluster_items ─────────────────────────────────────────────────

def _aggregate_for2020_codes(items: list[AwardCIFItem]) -> list[dict]:
    """Union items' for2020_codes across a person's whole grant history -- thin wrapper over
    cluster_checks.aggregate_for2020_codes(), shared with 01_prepare_arc.py's own pandas-side
    aggregation so the two pipelines can't drift apart. See that function's docstring."""
    return aggregate_for2020_codes(it.for2020_codes for it in items)


def _build_awards_cif(cluster_id: str, items: list[AwardCIFItem]) -> AwardsCIF:
    """Aggregate a group of items into one AwardsCIF -- mirrors 01_prepare_arc.py's
    _aggregate_clusters(), sorted deduplicated lists per field, modal full_name_key."""
    orcids = sorted({it.orcid for it in items if it.orcid})
    fnk_counts = Counter(it.full_name_key for it in items if it.full_name_key)

    return AwardsCIF(
        cluster_id=cluster_id,
        items=items,
        full_names=sorted({it.full_name for it in items}),
        first_names=sorted({fn for it in items for fn in it.first_names}),
        family_names=sorted({fn for it in items for fn in it.family_names}),
        orcids=orcids,
        inst_arr=sorted({it.institution_oax_id for it in items if it.institution_oax_id}),
        hep_codes=sorted({hc for it in items for hc in it.hep_codes}),
        for_names=sorted({it.for_name for it in items if it.for_name}),
        for_codes=sorted({it.for_code for it in items if it.for_code}),
        for2020_codes=_aggregate_for2020_codes(items),
        full_name_key=fnk_counts.most_common(1)[0][0] if fnk_counts else None,
        grant_ids=[it.unique_id for it in items],
        n_grants=len(items),
        orcid_status=(
            "HAS_ORCID" if len(orcids) == 1 else
            "MULTI_ORCID" if len(orcids) > 1 else
            "NO_ORCID"
        ),
    )


def cluster_items(
    items: list[AwardCIFItem],
    corrections: dict[str, dict] | None = None,
    orcid_corrections: dict[str, dict] | None = None,
) -> list[AwardsCIF]:
    """Cluster Award-CI/F items into provisional AwardsCIF() groupings via Splink `dedupe_only`
    -- the exact comparison/blocking configuration from 01_prepare_arc.py's Phase 2, reused
    unchanged as a tool (Splink itself isn't being redesigned, only what's built from its output
    -- see the tool/architecture split in the plan). What changes: instead of writing a
    cluster_id column onto a DataFrame, the clustering result directly constructs one
    AwardsCIF() per group, recording a splink_cluster provenance event on each, plus a
    name_typo_correction event on any cluster containing an item load_award_cif_items()
    corrected, and an orcid_correction event on any cluster containing an item whose ORCID
    load_award_cif_items() corrected (manual_orcid_corrections.csv).
    """
    corrections = corrections or {}
    orcid_corrections = orcid_corrections or {}

    df = pd.DataFrame([{
        "unique_id": it.unique_id,
        "first_name_canonical": it.first_name_canonical,
        "family_name_main": it.family_name_main,
        "full_name_key": it.full_name_key,
        "first_initial": it.first_initial,
        "orcid": it.orcid,
        "inst_arr": [it.institution_oax_id] if it.institution_oax_id else [],
        "for_name_tokens": it.for_name_tokens,
    } for it in items])

    settings = SettingsCreator(
        unique_id_column_name="unique_id",
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("family_name_main", "first_initial"),
            "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        ],
        comparisons=[
            cl.CustomComparison(
                output_column_name="first_name_canonical",
                comparison_description="First name: exact / initial-match / full-mismatch",
                comparison_levels=[
                    {
                        "sql_condition": "first_name_canonical_l IS NULL OR first_name_canonical_r IS NULL",
                        "label_for_charts": "null",
                        "is_null_level": True,
                    },
                    {
                        "sql_condition": "first_name_canonical_l = first_name_canonical_r",
                        "label_for_charts": "Exact match",
                        "tf_adjustment_column": "first_name_canonical",
                        "tf_adjustment_weight": 1.0,
                    },
                    {
                        "sql_condition": (
                            "(length(first_name_canonical_l) = 1"
                            " AND length(first_name_canonical_r) > 1"
                            " AND first_name_canonical_l = substr(first_name_canonical_r, 1, 1))"
                            " OR"
                            " (length(first_name_canonical_r) = 1"
                            " AND length(first_name_canonical_l) > 1"
                            " AND first_name_canonical_r = substr(first_name_canonical_l, 1, 1))"
                        ),
                        "label_for_charts": "Initial matches full name",
                    },
                    {
                        "sql_condition": (
                            "length(first_name_canonical_l) > 1"
                            " AND length(first_name_canonical_r) > 1"
                            " AND first_name_canonical_l != first_name_canonical_r"
                        ),
                        "label_for_charts": "Full name mismatch",
                        "m_probability": 0.02,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other",
                    },
                ],
            ),
            cl.CustomComparison(
                output_column_name="family_name_main",
                comparison_levels=[
                    cll.NullLevel("family_name_main"),
                    cll.ExactMatchLevel("family_name_main").configure(
                        tf_adjustment_column="family_name_main",
                        tf_adjustment_weight=1.0,
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.CustomComparison(
                output_column_name="full_name_key",
                comparison_levels=[
                    cll.NullLevel("full_name_key"),
                    cll.ExactMatchLevel("full_name_key").configure(
                        tf_adjustment_column="full_name_key",
                        tf_adjustment_weight=1.0,
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.ExactMatch("orcid").configure(
                m_probabilities=[0.85, 0.15]
            ),
            cl.ArrayIntersectAtSizes("inst_arr", [1]),
            cl.ArrayIntersectAtSizes("for_name_tokens", [2, 1]).configure(
                m_probabilities=[0.35, 0.45, 0.20]
            ),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker(df, settings, db_api=db_api)

    for fname, col in [
        ("oax_tf_family_name.parquet", "family_name_main"),
        ("oax_tf_first_name.parquet", "first_name_canonical"),
        ("oax_tf_full_name.parquet", "full_name_key"),
    ]:
        tf = pd.read_parquet(PROCESSED_DATA / fname)
        linker.table_management.register_term_frequency_lookup(tf, col)

    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("family_name_main")],
        recall=0.8,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        fix_u_probabilities=True,
    )

    df_pred = linker.inference.predict(threshold_match_probability=0.5)
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_pred, threshold_match_probability=CLUSTER_THRESHOLD
    )
    df_cluster_ids = df_clusters.as_pandas_dataframe()

    item_by_uid = {it.unique_id: it for it in items}
    groups: dict[str, list[AwardCIFItem]] = defaultdict(list)
    for row in df_cluster_ids.itertuples():
        groups[row.cluster_id].append(item_by_uid[row.unique_id])

    clusters: list[AwardsCIF] = []
    for cluster_id, group_items in groups.items():
        cif = _build_awards_cif(cluster_id, group_items)
        cif.record_event("splink_cluster")
        for it in group_items:
            correction = corrections.get(it.unique_id)
            if correction is not None:
                cif.record_event(
                    "name_typo_correction",
                    unique_id=it.unique_id,
                    wrong_first_name=correction["wrong_first_name"],
                    correct_first_name=correction["correct_first_name"],
                    notes=correction.get("notes"),
                )
            orcid_correction = orcid_corrections.get(it.unique_id)
            if orcid_correction is not None:
                cif.record_event(
                    "orcid_correction",
                    unique_id=it.unique_id,
                    wrong_orcid=orcid_correction["wrong_orcid"],
                    correct_orcid=orcid_correction["correct_orcid"] or None,
                    notes=orcid_correction.get("notes"),
                )
        clusters.append(cif)

    return clusters


# ── construction: refine_clusters ───────────────────────────────────────────────
#
# Nine operations ported from 01_prepare_arc.py's Phase 2, same order as its main(). Each
# takes and returns list[AwardsCIF] -- merges/splits re-aggregate via _build_awards_cif and
# carry provenance forward; ORCID-injection steps mutate in place (AwardsCIF isn't frozen).
# Validated logic, reused unchanged; only the representation (typed objects, not DataFrame
# columns) is new -- see the plan file's tool/architecture split.

def _norm_full(name: str) -> str:
    return re.sub(r"[^a-z ]", "", name.lower()).strip()


def _union_oeuvre_by_work_idx(everyone: list[AwardsCIF]) -> list["CandidateWork"]:
    """Union oeuvre across merging clusters, keyed on work_idx -- two clusters both reaching the
    same OpenAlex work get one CandidateWork with source_author_idxs unioned, not a duplicate
    entry. In every real call site today `oeuvre` is empty (populated later, by oeuvre_build.py,
    long after build_awards_cif_population() runs) -- this exists for merge()'s post-hoc case
    (Phase 3), where a caller may merge two ACIFs that already have real oeuvre attached."""
    by_work: dict[int, "CandidateWork"] = {}
    for c in everyone:
        for w in c.oeuvre:
            existing = by_work.get(w.work_idx)
            if existing is None:
                by_work[w.work_idx] = w
            else:
                merged_ids = sorted(set(existing.source_author_idxs) | set(w.source_author_idxs))
                by_work[w.work_idx] = replace(existing, source_author_idxs=merged_ids)
    return [by_work[k] for k in sorted(by_work)]


def _merge_aggregate_fields(canonical: AwardsCIF, absorbed: list[AwardsCIF]) -> dict:
    """Every AwardsCIF field _build_awards_cif() cannot derive from items -- an explicit rule
    per field, no silent dataclass defaults. Fixes a real, confirmed data-loss bug: several
    refine_clusters() steps set fields like `orcids` in place (apply_enriched_orcids,
    apply_manual_orcids) without touching `items`, and _merge_awards_cifs() used to rebuild a
    merged cluster purely from _build_awards_cif(items) -- silently discarding every in-place
    change on any cluster absorbed into a later merge. Confirmed live on the persisted
    population: 44 AwardsCIF carried an enriched_orcid/manual_orcid provenance event but had
    orcids == [] -- the merge that happened after the ORCID was set destroyed it."""
    everyone = [canonical] + absorbed
    merged_ids = {c.cluster_id for c in absorbed}
    orcid_for: dict[str, dict] = {}
    for c in everyone:
        for entry in c.orcid_for_codes:
            code = entry["code"]
            if code not in orcid_for:
                orcid_for[code] = {"code": code, "name": entry["name"], "count": 0}
            orcid_for[code]["count"] += entry["count"]
    return {
        "orcids": sorted({o for c in everyone for o in c.orcids}),
        "orcid_for_codes": sorted(orcid_for.values(), key=lambda x: -x["count"]),
        "oax_candidates": sorted({x for c in everyone for x in c.oax_candidates}),
        "oeuvre": _union_oeuvre_by_work_idx(everyone),
        "excluded": any(c.excluded for c in everyone),
        "excluded_reason": next((c.excluded_reason for c in everyone if c.excluded_reason), None),
        "gap_candidates": sorted({g for c in everyone for g in c.gap_candidates}
                                  - merged_ids - {canonical.cluster_id}),
        # Deliberately INVALIDATED, never guessed -- a merge changes the inputs a tier/status
        # was computed from enough that carrying a stale value forward would be worse than
        # admitting it's now unknown. Re-run compute_reliability() after any merge.
        "reliability_tier": None,
        "resolution_status": "UNRESOLVED",
    }


def _merge_awards_cifs(canonical: AwardsCIF, absorbed: list[AwardsCIF], event: str, **details) -> AwardsCIF:
    """Merge `absorbed` clusters into `canonical`: re-aggregate over the union of items,
    concatenate provenance, then append a new event describing the merge. Every field NOT
    derivable from items (orcids, oax_candidates, oeuvre, excluded/excluded_reason,
    gap_candidates -- see _merge_aggregate_fields()) is explicitly carried forward, not silently
    dropped to a dataclass default."""
    all_items = canonical.items + [it for c in absorbed for it in c.items]
    if not all_items:
        # Every current caller (merge_by_orcid, merge_persons_by_orcid, apply_manual_merges,
        # merge_same_grant_coinvestigators, all inside refine_clusters()) operates on freshly
        # built AwardsCIF with items intact -- this path is unreachable today. It becomes
        # reachable once Phase 3's merge() operator can be called on two load_awards_cif()-
        # reconstructed objects (items == [] after a round-trip) -- build the field-wise
        # _merge_without_items() path there, not here, so it's built against a real caller.
        raise NotImplementedError(
            "_merge_awards_cifs() called with no items on either side -- field-wise merge for "
            "loaded/reconstructed AwardsCIF is not yet implemented (see merge(), Phase 3)."
        )
    merged = _build_awards_cif(canonical.cluster_id, all_items)
    for k, v in _merge_aggregate_fields(canonical, absorbed).items():
        setattr(merged, k, v)
    # _build_awards_cif() derived orcid_status from items' own orcid field, which
    # apply_enriched_orcids()/apply_manual_orcids() never touch (they set orcids at the cluster
    # level only) -- recompute from the just-corrected `orcids`, or it stays stale/inconsistent.
    merged.orcid_status = (
        "HAS_ORCID" if len(merged.orcids) == 1 else
        "MULTI_ORCID" if len(merged.orcids) > 1 else
        "NO_ORCID"
    )
    if not merged.excluded:
        # Re-apply set_aside_indigenous_research()'s in-place strip of non-primary division-45
        # codes -- _build_awards_cif() recomputes for2020_codes fresh from items, which still
        # carry the original, unstripped codes, so a naive rebuild would silently reinstate them.
        merged.for2020_codes = [
            e for e in merged.for2020_codes
            if not e["code"].startswith(INDIGENOUS_DIVISION_PREFIX)
        ]
    merged.provenance = canonical.provenance + [ev for c in absorbed for ev in c.provenance]
    merged.record_event(event, merged_from=[c.cluster_id for c in absorbed], **details)
    return merged


def merge_by_orcid(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Deterministically merge clusters sharing an ORCID -- mirrors _merge_by_orcid(). Splink's
    own EM-trained clustering, even with ORCID-exact blocking, doesn't always fully unite
    same-ORCID records (confirmed empirically on Malherbe/Nakata in cluster_items()) -- this
    deterministic pass is what actually guarantees it."""
    orcid_to_ids: dict[str, list[str]] = defaultdict(list)
    for c in clusters:
        for orcid in c.orcids:
            orcid_to_ids[orcid].append(c.cluster_id)

    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            keep, drop = (ra, rb) if ra < rb else (rb, ra)
            parent[drop] = keep

    by_id = {c.cluster_id: c for c in clusters}
    for c in clusters:
        find(c.cluster_id)
    for ids in orcid_to_ids.values():
        for cid in ids[1:]:
            union(ids[0], cid)

    groups: dict[str, list[str]] = defaultdict(list)
    for c in clusters:
        groups[find(c.cluster_id)].append(c.cluster_id)

    out = []
    for root, ids in groups.items():
        if len(ids) == 1:
            out.append(by_id[root])
            continue
        canonical_id = min(ids)
        canonical = by_id[canonical_id]
        absorbed = [by_id[cid] for cid in ids if cid != canonical_id]
        shared_orcids = sorted({o for cid in ids for o in by_id[cid].orcids})
        out.append(_merge_awards_cifs(canonical, absorbed, "orcid_merge", orcid=shared_orcids))
    return out


def split_orcid_conflicts(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Split any cluster containing 2+ distinct ORCIDs -- mirrors _split_orcid_conflicts().
    No-ORCID items become singletons; each distinct-ORCID group becomes its own cluster."""
    out = []
    for c in clusters:
        distinct_orcids = sorted({it.orcid for it in c.items if it.orcid})
        if len(distinct_orcids) <= 1:
            out.append(c)
            continue

        groups: dict[str, list[AwardCIFItem]] = defaultdict(list)
        group_orcid: dict[str, str | None] = {}
        for it in c.items:
            key = it.orcid if it.orcid else f"__singleton__{it.unique_id}"
            groups[key].append(it)
            group_orcid[key] = it.orcid

        for key, its in groups.items():
            new_id = min(i.unique_id for i in its)
            new_cif = _build_awards_cif(new_id, its)
            new_cif.provenance = list(c.provenance)
            new_cif.record_event("orcid_conflict_split", split_from=c.cluster_id, orcid=group_orcid[key])
            out.append(new_cif)
    return out


def _load_coinvestigator_names() -> dict[str, list[str]]:
    """grant_code -> ['First Last', ...] for every KEEP_ROLES/KEEP_SCHEMES investigator on that
    grant -- used by split_multi_name_clusters() to find co-investigator overlap."""
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    roles_sql = ", ".join(f"'{r}'" for r in KEEP_ROLES)
    schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)
    rows = con.execute(f"""
        SELECT grant_code, first_name || ' ' || family_name AS coinv_name
        FROM read_parquet('{PROCESSED_DATA}/investigators_raw.parquet')
        WHERE role_code IN ({roles_sql}) AND substring(grant_code, 1, 2) IN ({schemes_sql})
    """).fetchall()
    con.close()
    out: dict[str, list[str]] = defaultdict(list)
    for grant_code, name in rows:
        out[grant_code].append(name.strip())
    return out


def _load_enriched_orcid_by_name() -> dict[str, str]:
    """norm_full_name -> orcid, from orcid_enrichment.parquet (if present)."""
    path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    out: dict[str, str] = {}
    for _, r in df.iterrows():
        if pd.notna(r.get("orcid")):
            out[_norm_full(f"{r['first_name']} {r['family_name']}")] = r["orcid"]
    return out


def split_multi_name_clusters(
    clusters: list[AwardsCIF],
    coinv_by_grant: dict[str, list[str]] | None = None,
    enriched_orcid: dict[str, str] | None = None,
) -> list[AwardsCIF]:
    """For clusters containing 2+ genuinely distinct full name forms, use co-investigator
    overlap, FOR-name overlap, and enriched-ORCID disagreement to detect mis-merged different
    people -- mirrors _split_multi_name_clusters(). Abbreviated name forms (first token <=2
    chars, e.g. "Chun Li") don't drive splits; after full-name forms are split, each
    abbreviated item is assigned to the sub-cluster with the best FOR overlap, or left
    singleton if none overlaps."""
    if coinv_by_grant is None:
        coinv_by_grant = _load_coinvestigator_names()
    if enriched_orcid is None:
        enriched_orcid = _load_enriched_orcid_by_name()

    def _first_tok(norm: str) -> str:
        parts = norm.split()
        return parts[0] if parts else ""

    out = []
    for c in clusters:
        distinct_orcids = {it.orcid for it in c.items if it.orcid}
        if len(distinct_orcids) == 1:
            out.append(c)
            continue

        norm_names = {it.unique_id: _norm_full(it.full_name) for it in c.items}
        full_forms = {n for n in norm_names.values() if len(_first_tok(n)) > 2}
        if len(full_forms) <= 1:
            out.append(c)
            continue

        coinv: dict[str, set] = defaultdict(set)
        for_sets: dict[str, set] = defaultdict(set)
        for it in c.items:
            nn = norm_names[it.unique_id]
            if nn not in full_forms:
                continue
            others = [n for n in coinv_by_grant.get(it.grant_code, []) if _norm_full(n) != nn]
            coinv[nn].update(others)
            if it.for_name:
                for_sets[nn].add(it.for_name)

        form_list = sorted(full_forms)
        split_pairs: set[tuple] = set()
        for i, a in enumerate(form_list):
            for b in form_list[i + 1:]:
                oa, ob = enriched_orcid.get(a), enriched_orcid.get(b)
                different_orcid = bool(oa and ob and oa != ob)
                disjoint_coinv = len(coinv[a] & coinv[b]) == 0
                disjoint_for = len(for_sets[a] & for_sets[b]) == 0
                if different_orcid or (disjoint_coinv and disjoint_for):
                    split_pairs.add((a, b))

        if not split_pairs:
            out.append(c)
            continue

        parent = {f: f for f in form_list}

        def find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for i, a in enumerate(form_list):
            for b in form_list[i + 1:]:
                if (a, b) not in split_pairs:
                    union(a, b)

        root_to_forms: dict[str, list] = defaultdict(list)
        for f in form_list:
            root_to_forms[find(f)].append(f)

        if len(root_to_forms) <= 1:
            out.append(c)
            continue

        sub_for = {
            root: set().union(*(for_sets[f] for f in forms))
            for root, forms in root_to_forms.items()
        }
        norm_to_root = {f: find(f) for f in form_list}

        def get_root(it: AwardCIFItem) -> str | None:
            nn = norm_names[it.unique_id]
            if nn in norm_to_root:
                return norm_to_root[nn]
            if it.for_name:
                scores = {root: (1 if it.for_name in sf else 0) for root, sf in sub_for.items()}
                best_root, best_score = max(scores.items(), key=lambda x: x[1])
                if best_score > 0:
                    return best_root
            return None

        assigned: dict[str, list[AwardCIFItem]] = defaultdict(list)
        for it in c.items:
            root = get_root(it)
            key = root if root is not None else f"__singleton__{it.unique_id}"
            assigned[key].append(it)

        for items in assigned.values():
            new_id = min(it.unique_id for it in items)
            new_cif = _build_awards_cif(new_id, items)
            new_cif.provenance = list(c.provenance)
            new_cif.record_event(
                "name_split", split_from=c.cluster_id,
                name_forms=sorted({norm_names[it.unique_id] for it in items}),
            )
            out.append(new_cif)

    return out


def apply_manual_splits(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Split clusters confirmed as containing 2+ different people in
    data_persisted/manual_splits.csv, dividing by institution_oax_id -- mirrors
    _apply_manual_splits()."""
    if not _MANUAL_SPLITS_CSV.exists():
        return clusters
    split_ids: set[str] = set()
    with open(_MANUAL_SPLITS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("confirmed_different_people", "").strip().lower() == "true":
                split_ids.add(row["cluster_id"])
    if not split_ids:
        return clusters

    out = []
    for c in clusters:
        if c.cluster_id not in split_ids:
            out.append(c)
            continue
        groups: dict[str, list[AwardCIFItem]] = defaultdict(list)
        for it in c.items:
            key = it.institution_oax_id or f"__singleton__{it.unique_id}"
            groups[key].append(it)
        for its in groups.values():
            new_id = min(it.unique_id for it in its)
            new_cif = _build_awards_cif(new_id, its)
            new_cif.provenance = list(c.provenance)
            new_cif.record_event(
                "manual_split", split_from=c.cluster_id, institution=its[0].institution_oax_id,
            )
            out.append(new_cif)
    return out


def apply_enriched_orcids(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Promote high/au_match-confidence ORCIDs from orcid_enrichment.parquet -- mirrors
    _apply_enriched_orcids(). Only promotes when exactly 1 distinct ORCID is found across all
    enriched name forms in a cluster, and the cluster currently has no ORCID."""
    enrichment_path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if not enrichment_path.exists():
        return clusters
    enrichment = pd.read_parquet(enrichment_path)
    enrichment = enrichment[
        enrichment["confidence"].isin(["high", "au_match"]) & enrichment["orcid"].notna()
    ]
    if len(enrichment) == 0:
        return clusters

    blocklist: set[tuple[str, str]] = set()
    if _ENRICHMENT_BLOCKLIST_CSV.exists():
        with open(_ENRICHMENT_BLOCKLIST_CSV, newline="") as f:
            for row in csv.DictReader(f):
                cid, orcid = row["cluster_id"].strip(), row["orcid"].strip()
                if cid and orcid:
                    blocklist.add((cid, orcid))

    enrich_by_name: dict[tuple[str, str], set[str]] = defaultdict(set)
    conf_by_name: dict[tuple[str, str], str] = {}
    for _, r in enrichment.iterrows():
        key = (r["first_name"], r["family_name"])
        enrich_by_name[key].add(r["orcid"])
        conf_by_name[key] = r["confidence"]

    for c in clusters:
        if c.orcids:
            continue
        found: set[str] = set()
        conf = None
        for it in c.items:
            key = (it.first_name, it.family_name)
            if key in enrich_by_name:
                found |= enrich_by_name[key]
                conf = conf_by_name[key]
        if len(found) != 1:
            continue
        orcid = next(iter(found))
        if (c.cluster_id, orcid) in blocklist:
            continue
        c.orcids = [orcid]
        c.orcid_status = "HAS_ORCID"
        c.record_event("enriched_orcid", orcid=orcid, confidence=conf)
    return clusters


def promote_low_by_for(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Promote low-confidence enrichment candidates when FOR-token overlap uniquely picks one
    AU candidate -- mirrors _promote_low_by_for(). Compares each AU candidate's ORCID-derived
    ERA FOR (from for_cache, populated by 00b_enrich_orcid.py) against the cluster's ARC
    for_names."""
    enrichment_path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if not enrichment_path.exists():
        return clusters
    enrichment = pd.read_parquet(enrichment_path)
    enrichment = enrichment[
        (enrichment["confidence"] == "low")
        & enrichment["au_candidates"].notna()
        & (enrichment["au_candidates"] != "[]")
    ]
    if len(enrichment) == 0:
        return clusters

    candidates_by_name: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for _, r in enrichment.iterrows():
        key = (r["first_name"], r["family_name"])
        raw = r["au_candidates"]
        for cand in (json.loads(raw) if isinstance(raw, str) else raw):
            if cand.get("orcid"):
                candidates_by_name[key].append(cand)

    with diskcache.Cache(str(DISKCACHE_DIR / "orcid_for")) as for_cache:
        for c in clusters:
            if c.orcids:
                continue
            seen: set[str] = set()
            candidates: list[dict] = []
            for it in c.items:
                for cand in candidates_by_name.get((it.first_name, it.family_name), []):
                    if cand["orcid"] not in seen:
                        seen.add(cand["orcid"])
                        candidates.append(cand)
            if len(candidates) < 2:
                continue
            arc_toks = {tok for name in c.for_names for tok in for_name_tokens(name)}
            if not arc_toks:
                continue
            scores = []
            for cand in candidates:
                oid = cand["orcid"]
                orcid_for = for_cache.get(oid, [])
                orcid_toks = {tok for e in orcid_for for tok in for_name_tokens(e["name"])}
                scores.append((oid, len(arc_toks & orcid_toks)))
            max_score = max(s for _, s in scores)
            if max_score == 0:
                continue
            winners = [oid for oid, s in scores if s == max_score]
            if len(winners) != 1:
                continue
            winner = winners[0]
            c.orcids = [winner]
            c.orcid_status = "HAS_ORCID"
            c.record_event(
                "enriched_orcid", orcid=winner, confidence="low_for_disambiguated", for_score=max_score,
            )
    return clusters


def apply_manual_orcids(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Inject verified ORCIDs for clusters ARC data has none for --
    data_persisted/manual_orcids.csv. Mirrors _apply_manual_orcids()."""
    if not _MANUAL_ORCIDS_CSV.exists():
        return clusters
    overrides: dict[str, str] = {}
    with open(_MANUAL_ORCIDS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            cid, orcid = row["cluster_id"].strip(), row["orcid"].strip()
            if cid and orcid:
                overrides[cid] = orcid
    if not overrides:
        return clusters
    by_id = {c.cluster_id: c for c in clusters}
    for cid, orcid in overrides.items():
        c = by_id.get(cid)
        if c is None:
            continue
        c.orcids = [orcid]
        c.orcid_status = "HAS_ORCID"
        c.record_event("manual_orcid", orcid=orcid)
    return clusters


def merge_persons_by_orcid(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Merge clusters sharing an ORCID after enrichment/manual additions -- mirrors
    _merge_persons_by_orcid(). Skips merges where first-name initials are incompatible
    (signals a wrong enrichment hit)."""
    orcid_to_ids: dict[str, list[str]] = defaultdict(list)
    for c in clusters:
        for orcid in c.orcids:
            orcid_to_ids[orcid].append(c.cluster_id)
    conflicts = {o: sorted(ids) for o, ids in orcid_to_ids.items() if len(ids) > 1}
    if not conflicts:
        return clusters

    by_id = {c.cluster_id: c for c in clusters}
    remapping: dict[str, str] = {}
    for orcid, ids in conflicts.items():
        canonical = min(ids)
        absorbed = [cid for cid in ids if cid != canonical]
        can_initials = {x for x in by_id[canonical].first_names if len(x) == 1}
        skip = False
        for cid in absorbed:
            abs_initials = {x for x in by_id[cid].first_names if len(x) == 1}
            if can_initials and abs_initials and not (can_initials & abs_initials):
                skip = True
                break
        if not skip:
            for cid in absorbed:
                remapping[cid] = canonical

    if not remapping:
        return clusters

    def resolve(cid: str) -> str:
        seen = set()
        while cid in remapping and cid not in seen:
            seen.add(cid)
            cid = remapping[cid]
        return cid

    groups: dict[str, list[str]] = defaultdict(list)
    for cid in by_id:
        groups[resolve(cid)].append(cid)

    out = []
    for root, ids in groups.items():
        if len(ids) == 1:
            out.append(by_id[root])
            continue
        canonical = by_id[root]
        absorbed = [by_id[cid] for cid in ids if cid != root]
        out.append(_merge_awards_cifs(canonical, absorbed, "post_enrichment_merge"))
    return out


def apply_manual_merges(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Merge cluster pairs listed in data_persisted/manual_merges.csv -- mirrors
    _apply_manual_merges(). Applied unconditionally; cluster_keep survives, cluster_drop is
    absorbed into it."""
    if not _MANUAL_MERGES_CSV.exists():
        return clusters
    by_id = {c.cluster_id: c for c in clusters}
    remapping: dict[str, str] = {}
    with open(_MANUAL_MERGES_CSV, newline="") as f:
        for row in csv.DictReader(f):
            keep, drop = row["cluster_keep"].strip(), row["cluster_drop"].strip()
            if keep and drop and drop in by_id:
                remapping[drop] = keep
    if not remapping:
        return clusters

    def resolve(cid: str) -> str:
        seen = set()
        while cid in remapping and cid not in seen:
            seen.add(cid)
            cid = remapping[cid]
        return cid

    groups: dict[str, list[str]] = defaultdict(list)
    for cid in by_id:
        groups[resolve(cid)].append(cid)

    out = []
    for root, ids in groups.items():
        if root not in by_id:
            continue
        if len(ids) == 1:
            out.append(by_id[root])
            continue
        canonical = by_id[root]
        absorbed = [by_id[cid] for cid in ids if cid != root]
        out.append(_merge_awards_cifs(canonical, absorbed, "manual_merge"))
    return out


def merge_same_grant_coinvestigators(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Auto-merge same-blocking-key clusters on the same single-org grant -- mirrors
    _merge_same_grant_coinvestigators(). If a grant's only eligible organisation is one
    university, two clusters sharing (family_name_main, first_initial) on that grant are the
    same person. Skips pairs whose clusters already carry distinct non-empty ORCIDs."""
    grants = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")
    if "n_eligible_orgs" not in grants.columns:
        return clusters
    single_org = set(grants.loc[grants["n_eligible_orgs"] == 1, "grant_code"])
    if not single_org:
        return clusters

    key_groups: dict[tuple, set[str]] = defaultdict(set)
    for c in clusters:
        fam = max(c.family_names, key=len) if c.family_names else None
        ini = _first_initial(c.first_names)
        if fam is None or ini is None:
            continue
        for gid in c.grant_ids:
            grant_code = gid.rsplit("_", 1)[0]
            if grant_code in single_org:
                key_groups[(grant_code, fam, ini)].add(c.cluster_id)

    groups = [ids for ids in key_groups.values() if len(ids) >= 2]
    if not groups:
        return clusters

    by_id = {c.cluster_id: c for c in clusters}
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        keep, drop = (ra, rb) if ra < rb else (rb, ra)
        parent[drop] = keep

    for ids in groups:
        ids = sorted(ids)
        non_empty = [set(by_id[cid].orcids) for cid in ids if by_id[cid].orcids]
        if len(non_empty) >= 2:
            union_all = set.union(*non_empty)
            shared = set.intersection(*non_empty)
            if len(union_all) > len(shared):
                continue  # ORCID conflict -- skip this group
        for cid in ids[1:]:
            union(ids[0], cid)

    merge_groups: dict[str, list[str]] = defaultdict(list)
    for c in clusters:
        merge_groups[find(c.cluster_id)].append(c.cluster_id)

    out = []
    for root, ids in merge_groups.items():
        if len(ids) == 1:
            out.append(by_id[root])
            continue
        canonical = by_id[root]
        absorbed = [by_id[cid] for cid in ids if cid != root]
        out.append(_merge_awards_cifs(canonical, absorbed, "same_grant_merge"))
    return out


def refine_clusters(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Apply the full Phase-2 refinement sequence to Splink's provisional clusters -- same
    step order as 01_prepare_arc.py's main(). See the plan file for why this order and
    decomposition is preserved (validated logic, rebuilt architecture, not a redesign)."""
    clusters = merge_by_orcid(clusters)
    clusters = split_orcid_conflicts(clusters)
    clusters = split_multi_name_clusters(clusters)
    clusters = apply_manual_splits(clusters)
    clusters = apply_enriched_orcids(clusters)
    clusters = promote_low_by_for(clusters)
    clusters = apply_manual_orcids(clusters)
    clusters = merge_persons_by_orcid(clusters)
    clusters = apply_manual_merges(clusters)
    clusters = merge_same_grant_coinvestigators(clusters)
    return clusters


def set_aside_indigenous_research(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Set aside AwardsCIF whose research is Indigenous-focused (2026-08-12 decision): a
    person is set aside if ANY of their grants had a FOR2020 division-45 (Indigenous Studies)
    code as its PRIMARY declared field -- not merely present as a secondary code on an
    otherwise-differently-focused grant, since AwardsCIF.for2020_codes is already ordered
    primary-first, checking entries where is_primary=True is a direct, explicit test, not an
    inferred one.

    Marks AwardsCIF.excluded=True, excluded_reason="indigenous", and records a provenance event
    with the actual triggering grant/code facts (not just the bare flag) -- but does NOT remove
    them from the returned list; callers filter on `.excluded` themselves, so an excluded
    AwardsCIF stays inspectable rather than silently vanishing. Indigenous-focused research is
    culturally important and not well portrayed by the bibliometric methods this project uses,
    so it's deliberately kept out of downstream oeuvre-building/scoring rather than run through
    them -- this is a scope decision about method fit, not a judgement about the research itself.

    Second, separate step (2026-08-17): for AwardsCIF that are NOT excluded (primary division is
    something other than 45), strip any division-45 entries out of their own for2020_codes union.
    A person whose primary work is elsewhere but who also carries a non-primary division-45 code
    stays in the working population (their real research is analyzable by this project's methods),
    but the 45 code itself should never contribute to any downstream classification that now uses
    the full for2020_codes list (Stage 3's field filter, division_mismatch_for2020(), pile-to-ACIF
    channeling) -- confirmed empirically to matter: 1,124 of 1,228 AwardsCIF carrying a division-45
    code have it as non-primary only (see the whitelist re-derivation investigation), and leaving
    those 45 entries in generated spurious (45, X) division pairs with very high z-scores purely
    from volume, not genuine legitimate co-occurrence -- a data-leakage artifact of the FOR2008/
    RFCD98-to-FOR2020 upgrade preserving the OLD scheme's primary/non-primary labelling (which
    routinely put a substantive discipline primary and Indigenous-relatedness secondary) even
    though FOR2020's own convention would have made Indigenous-focus primary for that research.
    """
    n_excluded = 0
    n_stripped = 0
    for c in clusters:
        triggers = [
            {"grant_code": it.grant_code, "code": e["code"], "name": e["name"]}
            for it in c.items
            for e in it.for2020_codes
            if e["is_primary"] and e["code"].startswith(INDIGENOUS_DIVISION_PREFIX)
        ]
        if triggers:
            c.excluded = True
            c.excluded_reason = "indigenous"
            c.record_event("excluded_indigenous_research", triggers=triggers)
            n_excluded += 1
        else:
            before = len(c.for2020_codes)
            c.for2020_codes = [
                e for e in c.for2020_codes if not e["code"].startswith(INDIGENOUS_DIVISION_PREFIX)
            ]
            if len(c.for2020_codes) < before:
                n_stripped += 1

    print(f"  Set aside {n_excluded} Indigenous-focused AwardsCIF (FOR2020 division 45 as a primary grant code)")
    print(f"  Stripped non-primary division-45 codes from {n_stripped} AwardsCIF (kept in population, 45 excluded from classification)")
    return clusters


# ── construction: populate_oax_candidates ───────────────────────────────────────

OAX_CANDIDATE_THRESHOLD = 0.5  # = 03_link_arc_oax.py's PREDICT_THRESHOLD -- the floor already
                                # stored in arc_oax_links.parquet, not a new Splink run.


def populate_oax_candidates(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[AwardsCIF]:
    """Populate oax_candidates as one undifferentiated set -- deliberately reading
    arc_oax_links.parquet (03_link_arc_oax.py's own output, which already retains every
    candidate pair >= OAX_CANDIDATE_THRESHOLD) directly, NOT arc_oax_resolved.parquet's
    oax_id/secondary_oax_ids (04_resolve_links.py's output). This is the resolution of the
    under-inclusion investigation in the plan file: 04's unique_hc path hardcodes
    secondary_oax_ids to [] regardless of what else scored 0.5-0.89 for that person (confirmed
    at 27% of the unique_hc bucket, 5,901 hidden pairs) -- reading 03's own output instead
    closes that gap by construction, without touching 03 or 04 themselves. Every candidate
    >= threshold is treated as equally possible; no primary/secondary distinction, per the
    Aim's "set, not a pointer" design. 03/04 themselves are NOT rerun or modified -- this only
    changes what populate_oax_candidates() reads, consistent with "Splink reused unchanged as
    a tool." Expected (not yet verified, since the later work-scoring step that would filter
    candidates down doesn't exist yet): a small increase in total candidates, with most of the
    previously-correctly-dropped ones still needing to be dropped once that step is built --
    this is a guess pending that tooling, not a confirmed outcome.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        con.execute("SET enable_progress_bar = false")
        links = con.execute(f"""
            SELECT arc_id, oax_id
            FROM read_parquet('{PROCESSED_DATA}/arc_oax_links.parquet')
            WHERE match_probability >= {OAX_CANDIDATE_THRESHOLD}
        """).fetchall()
    finally:
        if own_con:
            con.close()

    candidates_by_arc_id: dict[str, list[str]] = defaultdict(list)
    for arc_id, oax_id in links:
        candidates_by_arc_id[arc_id].append(oax_id)

    n_matched = 0
    for c in clusters:
        found = candidates_by_arc_id.get(c.cluster_id)
        if found:
            n_matched += 1
            c.oax_candidates = sorted(set(found))
            c.record_event(
                "oax_candidates_populated",
                n_candidates=len(c.oax_candidates),
                threshold=OAX_CANDIDATE_THRESHOLD,
            )

    return clusters


def _load_manual_unlinks() -> dict[str, set[str]]:
    """arc_id -> set of oax_ids a human has confirmed are NOT this person, from
    data_persisted/manual_resolutions.csv's "unlink" rows. Pure noise removal -- can only
    remove a confirmed-wrong candidate, never risks discarding a genuine one, unlike a
    "resolve" row (see the plan file for why "resolve" is deliberately NOT applied here)."""
    if not _MANUAL_RESOLUTIONS_CSV.exists():
        return {}
    df = pd.read_csv(_MANUAL_RESOLUTIONS_CSV).dropna(subset=["arc_id"])
    out: dict[str, set[str]] = defaultdict(set)
    for _, row in df[df["action"] == "unlink"].iterrows():
        oax_id = row.get("oax_id")
        if pd.notna(oax_id) and oax_id:
            out[row["arc_id"]].add(oax_id)
    return dict(out)


def _oax_names_compat(oax_ids: list[str], oax_firstname: dict, oax_familyname: dict) -> bool:
    """True when every OAX candidate in a group could plausibly be the same person --
    mirrors 04_resolve_links.py's _oax_names_compat(). Requires identical family_name_main
    and mutually compatible first names (same 3-char prefix among full names; initials pass
    through). Guards the split-record dedup below against collapsing genuinely different
    people who happen to share a topic."""
    fams = {oax_familyname.get(oid, "") for oid in oax_ids}
    if len(fams) != 1:
        return False
    full_firsts = [oax_firstname.get(oid, "") for oid in oax_ids if len(oax_firstname.get(oid, "")) >= 4]
    if len(full_firsts) < 2:
        return True
    prefix = full_firsts[0][:3]
    return all(n[:3] == prefix for n in full_firsts[1:])


def dedup_oax_candidates(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[AwardsCIF]:
    """Stage-1 cleanup of oax_candidates: pure noise removal, never a choice between distinct
    real people (see the plan file's "waterfall" design). Two operations, both ported from
    04_resolve_links.py:

    1. Manual unlink -- remove any candidate a human has directly confirmed is not this
       person (data_persisted/manual_resolutions.csv). Deliberately NOT applying "resolve"
       rows here -- see _load_manual_unlinks()'s docstring.
    2. OAX-side split-record dedup (04's Steps 0/0b) -- when 2+ candidates for one AwardsCIF
       share an ORCID, or share a specific topic AND are name-compatible, they are almost
       certainly split records of ONE real OpenAlex author, not competing different people.
       Collapse to the one holding the dominant share of combined works_count (> TOP_CUT);
       leave the group untouched if no single record dominates.

    Everything past this point in the existing 04 cascade (name-character filter, ORCID-
    match-based selection, institution/field-score narrowing, works-count dominance, unique-
    highest-probability) picks between candidates believed to be different real people --
    deliberately NOT ported here, deferred to the not-yet-built work-level scoring step.
    """
    unlinks = _load_manual_unlinks()
    for c in clusters:
        blocked = unlinks.get(c.cluster_id)
        if blocked and c.oax_candidates:
            before = set(c.oax_candidates)
            after = sorted(before - blocked)
            if after != c.oax_candidates:
                c.record_event("manual_unlink", removed=sorted(before - set(after)))
                c.oax_candidates = after

    all_oax_ids = sorted({oid for c in clusters for oid in c.oax_candidates})
    if not all_oax_ids:
        return clusters

    own_con = con is None
    con = con or duckdb.connect()
    try:
        con.execute("SET enable_progress_bar = false")
        con.execute("CREATE OR REPLACE TEMP TABLE _cand_ids AS SELECT UNNEST(?) AS oax_id", [all_oax_ids])
        rows = con.execute(f"""
            SELECT o.unique_id, o.orcid, o.topic_names, o.first_name, o.family_name_main
            FROM read_parquet('{PROCESSED_DATA}/openalex_authors_prep.parquet') o
            JOIN _cand_ids c ON c.oax_id = o.unique_id
        """).fetchall()
        oax_orcid, oax_topics, oax_firstname, oax_familyname = {}, {}, {}, {}
        for uid, orcid, topics, first_name, family_name_main in rows:
            oax_orcid[uid] = orcid
            oax_topics[uid] = list(topics) if topics is not None else []
            oax_firstname[uid] = (first_name or "").lower().strip()
            oax_familyname[uid] = (family_name_main or "").lower().strip()

        idx_sql = ", ".join(i.replace("https://openalex.org/A", "") for i in all_oax_ids)
        wc_rows = con.execute(f"""
            SELECT author_idx, works_count FROM read_parquet('{OAX_AUTHORS}/*.parquet')
            WHERE author_idx IN ({idx_sql})
        """).fetchall()
        oax_works = {f"https://openalex.org/A{idx}": wc for idx, wc in wc_rows}
    finally:
        if own_con:
            con.close()

    for c in clusters:
        if len(c.oax_candidates) < 2:
            continue
        group = set(c.oax_candidates)
        removed: set[str] = set()

        orcid_to_ids: dict[str, list[str]] = defaultdict(list)
        for oid in group:
            orcid = oax_orcid.get(oid)
            if orcid:
                orcid_to_ids[orcid].append(oid)
        for ids in orcid_to_ids.values():
            if len(ids) > 1:
                wcs = {oid: oax_works.get(oid, 0) for oid in ids}
                total = sum(wcs.values())
                best = max(wcs, key=wcs.get)
                if total > 0 and wcs[best] / total > TOP_CUT:
                    removed.update(oid for oid in ids if oid != best)

        remaining = group - removed
        topic_to_ids: dict[str, list[str]] = defaultdict(list)
        for oid in remaining:
            for t in oax_topics.get(oid, []):
                topic_to_ids[t].append(oid)
        orcid_protected = {oid for oid in remaining if oax_orcid.get(oid)}
        for ids in topic_to_ids.values():
            if len(ids) > 1 and _oax_names_compat(ids, oax_firstname, oax_familyname):
                wcs = {oid: oax_works.get(oid, 0) for oid in ids}
                best = max(wcs, key=wcs.get)
                removed.update(
                    oid for oid in ids
                    if oid != best and oid not in orcid_protected
                )

        if removed:
            c.oax_candidates = sorted(group - removed)
            c.record_event("oax_split_record_dedup", removed=sorted(removed))

    return clusters


# ── reliability: compute_reliability ────────────────────────────────────────────
# Division-mismatch/is_suspicious logic itself now lives in src/utils/cluster_checks.py
# (for2020_primary_fields, division_mismatch_for2020[_pairwise], is_suspicious_for2020),
# shared with 01_prepare_arc.py/01a_diagnose.py's production pipeline -- see that module's
# docstring for the full rationale. Imported above under the names used here.

def compute_orcid_for(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Populate orcid_for_codes -- ERA FOR codes derived from each cluster's ORCID works, read
    from for_cache (populated by 00b_enrich_orcid.py). Merges codes across all ORCIDs on a
    cluster, summing counts. Mirrors 01_prepare_arc.py's _enrich_orcid_for() exactly; not a
    provenance event -- a data enrichment, not an identity-changing operation, matching the
    original (which also doesn't touch cluster_history for this step)."""
    with diskcache.Cache(str(DISKCACHE_DIR / "orcid_for")) as for_cache:
        for c in clusters:
            merged: dict[str, dict] = {}
            for oid in c.orcids:
                if not oid:
                    continue
                for entry in for_cache.get(oid, []):
                    code = entry["code"]
                    if code not in merged:
                        merged[code] = {"code": code, "name": entry["name"], "count": 0}
                    merged[code]["count"] += entry["count"]
            c.orcid_for_codes = sorted(merged.values(), key=lambda x: -x["count"])

    n_with_for = sum(1 for c in clusters if c.orcid_for_codes)
    print(f"  orcid_for_codes: {n_with_for} clusters with >=1 ERA FOR code")
    return clusters


def compute_gap_candidates(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Populate gap_candidates -- other cluster_ids sharing the same (longest) family name that
    cannot be ruled out as the same person. Mirrors 01_prepare_arc.py's _compute_gap_candidates():
    pairwise within each family-name group, incompatible on any of name / FOR-division / ORCID ->
    kept separate; otherwise both sides record each other as a gap candidate.

    Feeds compute_reliability()'s tier 4 vs 4u distinction -- must run first.

    Division check (2026-08-12): uses for2020_codes' numeric divisions via
    _pairwise_division_mismatch(), not the old for_names + for_divisions.csv route -- see that
    function's docstring for why (no adjacency tolerance, stricter than before)."""

    def _fnm(family_names):
        return max(family_names, key=len) if family_names else None

    by_fnm: dict[str, list[AwardsCIF]] = defaultdict(list)
    for c in clusters:
        fnm = _fnm(c.family_names)
        if fnm:
            by_fnm[fnm].append(c)

    gap: dict[str, list[str]] = {c.cluster_id: [] for c in clusters}
    n_compat = n_incompat = 0
    for fnm, grp in by_fnm.items():
        if len(grp) < 2:
            continue
        for i, c1 in enumerate(grp):
            for c2 in grp[i + 1:]:
                name_incompat = (
                    not first_names_compatible(c1.first_names, c2.first_names) or
                    not first_names_compatible(c2.first_names, c1.first_names)
                )
                div_incompat = _pairwise_division_mismatch(c1.for2020_codes, c2.for2020_codes)
                orcid_incompat = (
                    len(c1.orcids) > 0 and len(c2.orcids) > 0
                    and not set(c1.orcids) & set(c2.orcids)
                )
                if name_incompat or div_incompat or orcid_incompat:
                    n_incompat += 1
                else:
                    gap[c1.cluster_id].append(c2.cluster_id)
                    gap[c2.cluster_id].append(c1.cluster_id)
                    n_compat += 1

    for c in clusters:
        c.gap_candidates = gap[c.cluster_id]

    print(f"  Gap 1: {n_compat} compatible pairs, {n_incompat} incompatible pairs")
    return clusters


def compute_reliability(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Populate resolution_status and reliability_tier -- the final refinement step. Mirrors
    01_prepare_arc.py's resolution_status assignment (is_suspicious) + _compute_reliability_tier().
    Requires gap_candidates already populated (tier 4 vs 4u depends on it) -- run
    compute_gap_candidates() first.

    resolution_status: RESOLVED unless this cluster is common-name/cross-OAX-field (flagged for
    manual review, see cluster_checks.is_suspicious_for2020() -- no longer ORCID-gated as of
    2026-08-16, since an ORCID on a minority of a cluster's own grant records was found to say
    nothing about the records that don't carry it), or has MULTI_ORCID (an unresolved ORCID
    conflict, forced UNRESOLVED separately below regardless of is_suspicious_for2020's verdict).

    Division check (2026-08-13): uses cluster_checks.is_suspicious_for2020() -- the same shared
    function 01_prepare_arc.py's production pipeline now uses too, both built on for2020_codes'
    OAX fields rather than the old for_names + for_divisions.csv route. See cluster_checks.py's
    module docstring for the full rationale (a real, verified casing bug plus a structural
    letter/numeric-division mismatch, not just a preference).

    reliability_tier:
      1a  HAS_ORCID, source = ARC data
      1b  HAS_ORCID, source = ORCID enrichment (00b)
      1c  HAS_ORCID, source = manual_orcids.csv
      2   NO_ORCID, multi-grant, rare name  (tf < RARE_NAME_TF)
      3   NO_ORCID, multi-grant, common name
      4   NO_ORCID, singleton, no gap_candidates (isolated)
      4u  NO_ORCID, singleton, has gap_candidates (unresolved collision)
    """
    tf_df = pd.read_parquet(PROCESSED_DATA / "oax_tf_full_name.parquet")
    tf_lookup = dict(zip(tf_df["full_name_key"], tf_df["tf_full_name_key"]))

    for c in clusters:
        suspicious = is_suspicious_for2020(c.full_name_key, c.for2020_codes, tf_lookup)
        c.resolution_status = "UNRESOLVED" if suspicious else "RESOLVED"
        if c.orcid_status == "MULTI_ORCID":
            c.resolution_status = "UNRESOLVED"

        events = {e.get("event") for e in c.provenance}
        if c.orcid_status in ("HAS_ORCID", "MULTI_ORCID"):
            if "manual_orcid" in events:
                c.reliability_tier = "1c"
            elif "enriched_orcid" in events:
                c.reliability_tier = "1b"
            else:
                c.reliability_tier = "1a"
        elif c.n_grants >= 2:
            tf = tf_lookup.get(c.full_name_key, 1.0) if c.full_name_key else 1.0
            c.reliability_tier = "2" if tf < RARE_NAME_TF else "3"
        else:
            c.reliability_tier = "4u" if c.gap_candidates else "4"

    counts = Counter(c.reliability_tier for c in clusters)
    print(f"  Reliability tiers: {dict(sorted(counts.items()))}")
    n_unresolved = sum(1 for c in clusters if c.resolution_status == "UNRESOLVED")
    print(f"  resolution_status: {len(clusters) - n_unresolved} RESOLVED, {n_unresolved} UNRESOLVED")
    return clusters

    return clusters


# ── population-level construction: build_awards_cif_population, persistence ─────────

AWARDS_CIF_PARQUET = PROCESSED_DATA / "awards_cif.parquet"


def build_awards_cif_population(con: duckdb.DuckDBPyConnection | None = None) -> list[AwardsCIF]:
    """Composes the full step-1 chain in order -- the AwardsCIF-side equivalent of
    01_prepare_arc.py end to end, not previously composed anywhere as a single function
    (each stage has always been called manually by whoever needed it, e.g. tests or ad hoc
    validation runs). Mirrors refine_clusters()'s and oeuvre_build.py's build_oeuvre()'s own
    "compose the small single-responsibility functions in order" style.

    load_award_cif_items -> cluster_items -> refine_clusters -> set_aside_indigenous_research
    -> populate_oax_candidates -> dedup_oax_candidates -> compute_orcid_for
    -> compute_gap_candidates -> compute_reliability
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        if own_con:
            con.execute("SET enable_progress_bar = false")
            con.execute("SET memory_limit = '24GB'")
            con.execute(f"SET temp_directory = '{DUCKDB_TMP_DIR}'")
        items, corrections, orcid_corrections = load_award_cif_items(con)
        clusters = cluster_items(items, corrections, orcid_corrections)
        clusters = refine_clusters(clusters)
        clusters = set_aside_indigenous_research(clusters)
        clusters = populate_oax_candidates(clusters, con)
        clusters = dedup_oax_candidates(clusters, con)
        clusters = compute_orcid_for(clusters)
        clusters = compute_gap_candidates(clusters)
        clusters = compute_reliability(clusters)
    finally:
        if own_con:
            con.close()
    return clusters


def persist_awards_cif(clusters: list[AwardsCIF], path: Path = AWARDS_CIF_PARQUET) -> None:
    """Persists the step-1 AwardsCIF population to a real parquet file -- this never existed
    before (confirmed by grep: awards_cif.py only ever read from PROCESSED_DATA, never wrote to
    it; CLAUDE.md's "roadmap step 1 is DONE" note describes the functional chain validated by an
    in-memory diff against arc_persons.parquet, not a persisted output). Same column shape as
    arc_persons.parquet by design (diffable field-for-field) plus oax_candidates, excluded,
    excluded_reason -- fields arc_persons.parquet doesn't have. `items` (raw per-grant detail,
    reconstructable from load_award_cif_items()) and `oeuvre` (populated by later oeuvre_build.py
    stages, empty at this point in the chain) are deliberately not persisted here.
    """
    rows = [{
        "cluster_id": c.cluster_id,
        "full_names": c.full_names,
        "first_names": c.first_names,
        "family_names": c.family_names,
        "orcids": c.orcids,
        "inst_arr": c.inst_arr,
        "hep_codes": c.hep_codes,
        "for_names": c.for_names,
        "for_codes": c.for_codes,
        "for2020_codes": c.for2020_codes,
        "grant_ids": c.grant_ids,
        "n_grants": c.n_grants,
        "full_name_key": c.full_name_key,
        "orcid_status": c.orcid_status,
        "orcid_for_codes": c.orcid_for_codes,
        "gap_candidates": c.gap_candidates,
        "reliability_tier": c.reliability_tier,
        "resolution_status": c.resolution_status,
        "oax_candidates": c.oax_candidates,
        "excluded": c.excluded,
        "excluded_reason": c.excluded_reason,
        # JSON string, not a native list-of-struct column -- provenance events carry different
        # kwarg keys per event type (record_event()'s whole point is a free-form **details), so
        # pyarrow can't infer one uniform STRUCT schema across heterogeneous event dicts
        # (confirmed: "cannot mix list and non-list, non-null values" on a real run). Matches
        # arc_persons.parquet's own cluster_history column, which is VARCHAR for the same reason.
        "provenance": json.dumps(c.provenance),
    } for c in clusters]
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"  Persisted {len(df):,} AwardsCIF to {path}")


GRANT_CLUSTER_MAP_PARQUET = PROCESSED_DATA / "arc_grant_cluster_map.parquet"


def persist_grant_cluster_map(
    clusters: list[AwardsCIF], path: Path = GRANT_CLUSTER_MAP_PARQUET,
) -> None:
    """grant unique_id -> cluster_id, one row per grant appearance -- mirrors
    01_prepare_arc.py's main() (exploded cluster_id/grant_ids, same two-column shape:
    unique_id, cluster_id), consumed by 01a_diagnose.py and
    analysis/07_analyse_ecr_fellowships.py's GRANT_MAP. Nothing in awards_cif.py persisted this
    before Phase 0 of the consolidation plan -- a real gap, since it's the only artifact of
    01_prepare_arc.py's output that awards_cif.parquet alone can't reconstruct (grant_ids is
    already on AwardsCIF, this is purely the explode)."""
    rows = [
        {"unique_id": gid, "cluster_id": c.cluster_id}
        for c in clusters
        for gid in c.grant_ids
    ]
    df = pd.DataFrame(rows, columns=["unique_id", "cluster_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"  Saved grant→cluster map ({len(df):,} rows) → {path}")


def load_awards_cif(path: Path = AWARDS_CIF_PARQUET) -> list[AwardsCIF]:
    """Reconstructs AwardsCIF instances from persist_awards_cif()'s output. `items`/`oeuvre`
    come back empty (never persisted) -- callers needing those must run the relevant chain
    (load_award_cif_items()/cluster_items(), or oeuvre_build.py's fetch stages) themselves."""
    df = pd.read_parquet(path)
    clusters = []
    for row in df.to_dict(orient="records"):
        clusters.append(AwardsCIF(
            cluster_id=row["cluster_id"],
            full_names=list(row["full_names"]),
            first_names=list(row["first_names"]),
            family_names=list(row["family_names"]),
            orcids=list(row["orcids"]),
            inst_arr=list(row["inst_arr"]),
            hep_codes=list(row["hep_codes"]),
            for_names=list(row["for_names"]),
            for_codes=list(row["for_codes"]),
            for2020_codes=[dict(x) for x in row["for2020_codes"]],
            grant_ids=list(row["grant_ids"]),
            n_grants=row["n_grants"],
            full_name_key=row["full_name_key"],
            orcid_status=row["orcid_status"],
            orcid_for_codes=[dict(x) for x in row["orcid_for_codes"]],
            gap_candidates=list(row["gap_candidates"]),
            reliability_tier=row["reliability_tier"],
            resolution_status=row["resolution_status"],
            oax_candidates=list(row["oax_candidates"]),
            excluded=row["excluded"],
            excluded_reason=row["excluded_reason"],
            provenance=json.loads(row["provenance"]),
        ))
    return clusters
