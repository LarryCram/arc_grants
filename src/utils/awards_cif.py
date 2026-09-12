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
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path

import diskcache
import duckdb
import pandas as pd
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

from config.settings import PROCESSED_DATA, ADMIN_ORGS_CSV, GRANT_SUMMARIES_CSV, ARC_GRANTS_CSV, DISKCACHE_DIR, OAX_AUTHORS, TOP_CUT, DUCKDB_TMP_DIR, ORCID_BULK_PARQUET
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import make_expanded_for_tokens, for_name_tokens, HumanNameParser, ParsedName
from src.utils.for_resolve import (
    upgrade_for_code, upgrade_for_name, resolve_arc_for_entry, for2020_group_name,
)
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
_MANUAL_SPLITS_BY_GRANT_CSV = _DATA_PERSISTED / "manual_splits_by_grant.csv"
_MANUAL_ORCIDS_CSV = _DATA_PERSISTED / "manual_orcids.csv"
_MANUAL_MERGES_CSV = _DATA_PERSISTED / "manual_merges.csv"
_ENRICHMENT_BLOCKLIST_CSV = _DATA_PERSISTED / "enrichment_blocklist.csv"
_MANUAL_RESOLUTIONS_CSV = _DATA_PERSISTED / "manual_resolutions.csv"
_MANUAL_CONFIRMED_NOT_SUSPICIOUS_CSV = _DATA_PERSISTED / "manual_confirmed_not_suspicious.csv"
_MANUAL_CONFIRMED_DISTINCT_CSV = _DATA_PERSISTED / "manual_confirmed_distinct.csv"

class StaleClusterIdError(Exception):
    """Raised when a cluster_id/arc_id captured in a data_persisted/manual_*.csv override
    file can no longer be resolved against the current AwardsCIF population.

    cluster_id is derived (min() over a cluster's own unique_ids -- see refine_clusters()'s
    merge steps) and can change value on any membership-altering rerun: a new grant added
    under an earlier-sorting scheme letter, a merge triggered by newly-discovered ORCID
    evidence, a split, etc. -- none of which need mean anything about the underlying person's
    real identity. Every manual_*.csv loader keyed on cluster_id used to handle this with its
    own ad hoc, silent behaviour (skip / fall back to a worse method / delete a real person
    outright -- see CLAUDE.md's 2026-08-26 stale-reference risk audit for the full case-by-case
    list). resolve_cluster_id() replaces all of those with one shared, loud failure instead."""


def resolve_cluster_id(old_id: str, clusters: "list[AwardsCIF] | pd.DataFrame") -> str:
    """Resolve a possibly-stale cluster_id/arc_id to whichever AwardsCIF it currently lives in.

    `clusters` may be either a list[AwardsCIF] (the in-memory representation used throughout
    awards_cif.py/04_resolve_links.py) or a DataFrame with cluster_id/grant_ids columns (the
    persisted-parquet shape read directly by 01a_diagnose.py) -- both are genuinely used
    side by side across this project for the same population, so this function accepts
    either rather than forcing every caller to reconstruct one from the other.

    1. old_id is already a current cluster_id -> returned unchanged (the common case, and the
       only case before this function existed).
    2. old_id is stale, but its literal string still appears in exactly one current cluster's
       grant_ids -- the person moved (a rename/merge/split changed which of their own
       unique_ids sorts lowest), not disappeared -- that cluster's current cluster_id is
       returned.
    3. old_id can't be found anywhere, or is ambiguous (2+ current clusters both contain it,
       which should not be possible since a unique_id belongs to exactly one cluster, but is
       checked rather than assumed) -> raises StaleClusterIdError. Never silently skipped,
       never guessed at.
    """
    if isinstance(clusters, pd.DataFrame):
        pairs = list(zip(clusters["cluster_id"], clusters["grant_ids"]))
    else:
        pairs = [(c.cluster_id, c.grant_ids) for c in clusters]
    if old_id in {cid for cid, _ in pairs}:
        return old_id
    matches = [cid for cid, grant_ids in pairs if old_id in grant_ids]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise StaleClusterIdError(
            f"{old_id!r} is not a current cluster_id and appears in {len(matches)} different "
            f"current clusters ({matches!r}) -- ambiguous, cannot resolve automatically."
        )
    raise StaleClusterIdError(
        f"{old_id!r} is not a current cluster_id and does not appear in any current cluster's "
        "grant_ids. This manual override reference is stale and needs human review -- do not "
        "guess at a replacement; find out what actually happened to this person's records."
    )


CLUSTER_THRESHOLD = 0.9  # same value as 01_prepare_arc.py -- high precision, prefer splitting over merging
RARE_NAME_TF = 2e-6  # OAX full_name_key TF below this -> rare name (tier 2 vs 3). Distinct from
                      # cluster_checks.RARE_NAME_TF (1e-5 as of 2026-08-21, was 5e-5), which
                      # governs is_suspicious()'s own rare-name carve-out -- same constant name,
                      # same value as 01_prepare_arc.py,
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

    # The standard parser's own output for this item's raw name, carried whole (2026-09-04) --
    # first_names/family_names/family_name_main/first_name_canonical/full_name_key above are
    # kept as-is for every existing reader (Splink comparisons, 01a_diagnose.py) but are now
    # populated FROM this object, ASCII-reduced-with-raw-fallback per ParsedName's own
    # documented convention, instead of being re-derived a second time with no fallback (the
    # bug this fixes -- see _prep_arc-adjacent construction site below).
    parsed: ParsedName | None = None

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

    # This grant's own set of OAX institution ids (2026-08-25) -- current admin_org UNION
    # announcement_admin_org UNION eligible_orgs, each resolved via
    # _load_institution_oax_crosswalk(). Deliberately a set, not the single scalar
    # institution_oax_id above (kept unchanged as "the current admin org" field): a grant whose
    # administering institution changed during its life (confirmed real at scale -- 13.03% of
    # grants the pipeline treats as "single institution" via n_eligible_orgs==1 actually differ
    # between snapshots, e.g. DP110100989) is genuine additional evidence for Splink's
    # comparison, not noise to discard by only keeping whichever value happens to be current.
    # Same "loose evidence, precise scoring" reasoning already applied to family_names/
    # first_names_multichar -- more evidence is safe here because this feeds AwardsCIF.inst_arr
    # (a Splink comparison), not a hard-exclusion gate on its own.
    inst_ids: list[str] = field(default_factory=list)

    # ARC's own raw isFellowship flag for THIS person on THIS grant (investigators_raw.parquet's
    # is_fellowship column, extracted directly in 00_extract_arc.py) -- added 2026-08-24. Was
    # previously absent from AwardCIFItem entirely, even though a downstream Dossier-reporting
    # consumer (analysis/utils/dossier.py's AwardContext) already declared a same-named field
    # that nothing ever populated.
    is_fellowship: bool = False


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
    # Modal family_name_main across items (2026-09-07) -- the cluster-level analogue of
    # full_name_key's own Counter.most_common(1) design, added specifically so
    # 03_link_arc_oax.py can read an already-correct, frequency-based scalar instead of
    # re-deriving one from the deduped family_names SET via max_by_len() ("longest variant
    # wins" -- confirmed structurally wrong: it discards every count, and a contaminant only
    # has to be longer once to win, not more frequent). A cluster-level max_by_len() over
    # family_names has no per-item counts to work from (family_names is already a deduped
    # set by the time it reaches AwardsCIF); this field is computed from the items directly,
    # before that information is lost.
    family_name_main: str | None = None

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

    # Every OTHER investigator appearing on any grant this ACIF holds (2026-09-04) -- not
    # co-authorship, co-awardee-ship: known with certainty from the grant record itself, unlike
    # anything from OpenAlex. One entry per distinct co-awardee ParsedName (full parser output,
    # not a shortcut key), plus `count` = how many of this ACIF's own grants they co-appear on.
    # Deliberately NOT filtered against this ACIF's own name(s) -- a co-awardee entry that
    # collides with the ACIF's own name is itself a useful signal (a candidate same-person
    # under-merge across two ACIFs sharing a grant), not an error to hide. See
    # compute_coawardees() for construction and docs/pipeline_todo.md for the standing check
    # this motivates.
    coawardees: list[dict] = field(default_factory=list)

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

_name_parser = HumanNameParser()


def _name_forms(first_name: str, family_name: str) -> tuple[list[str], list[str]]:
    """Mirrors 01_prepare_arc.py's arc_name_arrays(): given-name tokens (+ their initials)
    and normalized family-name form(s), from ARC's raw first_name/family_name fields.

    2026-09-02: pure delegation to HumanNameParser (names.py) -- the full chain (canonicalize,
    postnominal-strip, HumanName parse, single-token fallback, diacritic-widen, tokenize,
    order-preserving dedup, the last-name-only fallback) now lives in exactly one place; this
    function just adapts ParsedName's shape to this call site's existing (list, list) contract
    so every caller here keeps working unchanged. See HumanNameParser/ParsedName's own
    docstrings (names.py) for the full history this used to carry inline: family_names/given-name
    tokens are widened to their bare/digraph pair whenever the raw ARC string itself carries a
    literal diacritic character -- ARC data is NOT "always ASCII" (confirmed 189 real investigator
    records carry a genuine diacritic character); the corpus-wide equivalence table this used to
    also consult was removed 2026-08-26 as a real bug (see name_diacritic_variants.py)."""
    full = f"{first_name or ''} {family_name or ''}".strip()
    parsed = _name_parser.parse(full)
    return list(parsed.given_tokens), list(parsed.family_names)


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
            # 2026-08-21 fix: `name` may be a finer-precision (6-digit field) label if the
            # original ARC entry resolved at field precision -- truncating the CODE to group
            # precision without also re-deriving the NAME left a mismatched label (e.g. code
            # "5004" carrying field 500405's name "Religion, society and culture" instead of
            # group 5004's own "Religious studies"). Only re-resolve when truncation actually
            # happened; skip the extra lookup when code20 was already 4-digit.
            if code20 != code4:
                name = for2020_group_name(code4) or name
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


def _load_admin_orgs_rows() -> tuple[list[dict], dict[str, str], dict[str, str]]:
    """admin_orgs.csv rows, plus canonical organisationName -> hep_code AND organisationName ->
    institution_id (OAX URL, e.g. "https://openalex.org/I204824540") -- any non-null value found
    under any alias row sharing that canonical name (see _load_hep_crosswalk()'s docstring for
    why resolution goes through the canonical name, not each alias row individually). Shared by
    _load_hep_crosswalk() (alias name -> hep_code), _load_institution_hep_crosswalk() (OpenAlex
    institution_idx -> hep_code), and _load_institution_oax_crosswalk() (alias name ->
    institution_id) so all three read the CSV once and apply the same defensive resolution.
    """
    import csv as _csv
    canonical_hep: dict[str, str] = {}
    canonical_institution_id: dict[str, str] = {}
    rows = []
    with open(ADMIN_ORGS_CSV, newline="", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            rows.append(row)
            name = row.get("organisationName", "").strip()
            hep = row.get("hep_code", "").strip()
            inst_id = row.get("institution_id", "").strip()
            if name and hep and name not in canonical_hep:
                canonical_hep[name] = hep
            if name and inst_id and name not in canonical_institution_id:
                canonical_institution_id[name] = inst_id
    return rows, canonical_hep, canonical_institution_id


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
    rows, canonical_hep, _canonical_inst_id = _load_admin_orgs_rows()
    crosswalk: dict[str, str] = {}
    for row in rows:
        alias = row.get("organisationName_alias", "").strip()
        name = row.get("organisationName", "").strip()
        hep = canonical_hep.get(name)
        if alias and hep:
            crosswalk[alias] = hep
    return crosswalk


def _load_institution_oax_crosswalk() -> dict[str, str]:
    """admin_orgs.csv organisationName_alias -> institution_id (OAX URL), same canonical-group
    defensive resolution as _load_hep_crosswalk() -- for building AwardCIFItem.inst_ids (the
    per-grant set of OAX institution ids, see load_award_cif_items()), which must use the SAME
    id space as OAX's own inst_ids field (institution_id, e.g. "https://openalex.org/I204824540")
    for 03_link_arc_oax.py's ArrayIntersectAtSizes("inst_arr", ...) comparison to work at all --
    confirmed directly (2026-08-25) that this is a DIFFERENT code space from hep_code (e.g. "UOW"),
    so hep_codes/_load_hep_crosswalk() cannot be reused for this purpose despite the superficial
    similarity.
    """
    rows, _canonical_hep, canonical_institution_id = _load_admin_orgs_rows()
    crosswalk: dict[str, str] = {}
    for row in rows:
        alias = row.get("organisationName_alias", "").strip()
        name = row.get("organisationName", "").strip()
        inst_id = canonical_institution_id.get(name)
        if alias and inst_id:
            crosswalk[alias] = inst_id
    return crosswalk


def _load_institution_hep_crosswalk() -> dict[int, str]:
    """admin_orgs.csv OpenAlex institution_idx (int) -> hep_code, same canonical-group
    resolution as _load_hep_crosswalk(). For resolving a candidate's own OpenAlex-side
    institution (from an authorship row's institution_idx, not an ARC organisation name string)
    to a HEP code -- used by oeuvre_build.py's subfield+HEP keep/drop signal (2026-08-16). Only
    ~42/114 admin_orgs.csv rows are real Australian HEPs with a resolvable institution_id at all;
    everything else (foreign institutions, non-HEP research institutes) correctly has no entry.
    """
    rows, canonical_hep, _canonical_inst_id = _load_admin_orgs_rows()
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
                i.is_fellowship,
                g.admin_org,
                g.announcement_admin_org,
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
            ORDER BY i.unique_id
        """).fetchall()
        col_names = [d[0] for d in con.description]
    finally:
        if own_con:
            con.close()

    expanded_for_tokens = make_expanded_for_tokens(str(_FOR_CONCORDANCE_CSV))
    grant_for2020_codes = load_grant_for2020_codes()
    hep_crosswalk = _load_hep_crosswalk()
    institution_oax_crosswalk = _load_institution_oax_crosswalk()

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

        # Direct call, not _name_forms()'s narrowed (list, list) adapter -- that adapter exists
        # to keep OTHER call sites' old contract stable, but was also (wrongly) feeding this
        # site, discarding full_name_key/full_name_key_raw/family_name_main/first_name_canonical
        # entirely; this site then re-derived them a second time with no raw-script fallback at
        # all -- a non-Latin-script name got full_name_key=None here even though ParsedName had
        # already computed a usable one. Fixed 2026-09-04: every field below prefers the
        # ASCII-reduced form and falls back to the raw one, per ParsedName's own documented
        # calling convention, and the full parsed object is kept (item.parsed) rather than
        # thrown away.
        #
        # 2026-09-05 correction: first_names/family_names are LISTS -- for a list, the raw
        # NFC/casefold form is not a fallback to use INSTEAD of the ASCII-reduced one (that's
        # only correct for the scalars below, which can hold exactly one value); it's a
        # genuinely separate, additional matchable form that must be UNIONED in. The original
        # `or` here was a short-circuit: since the ASCII-reduced list is non-empty for nearly
        # every real name, the raw form -- built specifically to catch non-Latin-script/
        # uncatalogued-diacritic names the ASCII path drops -- was silently discarded in
        # virtually every case, defeating the reason it exists.
        parsed = _name_parser.parse(f"{first_name or ''} {r['family_name'] or ''}".strip())
        first_names = list(dict.fromkeys(parsed.given_tokens + parsed.given_tokens_raw))
        family_names = list(dict.fromkeys(
            parsed.family_names + ((parsed.family_name_raw,) if parsed.family_name_raw else ())
        ))
        family_name_main = parsed.family_name_main or parsed.family_name_raw
        first_initial = _first_initial(first_names)
        first_name_canonical = parsed.first_name_canonical or (
            parsed.given_tokens_raw[0] if parsed.given_tokens_raw else None
        )
        full_name_key = parsed.full_name_key or parsed.full_name_key_raw

        # Union eligible_orgs (announcement-time organisations-at-announcement list) with
        # admin_org itself (2026-08-16 fix): these can genuinely disagree -- 12.13% of all
        # grants have admin_org absent from eligible_orgs, found via Patricia Valery's
        # FT100100511 (admin_org='Charles Darwin University', eligible_orgs=['Queensland
        # Institute of Medical Research']) -- confirmed by the user as a real institution
        # change (QIMR -> CDU) upon receiving the fellowship, not a data error in either
        # field. Unchanged, untouched by the 2026-08-25 inst_ids addition below -- already
        # resolved, not part of today's fix, kept exactly as it was.
        eligible_orgs = set(r["eligible_orgs"] or [])
        if r["admin_org"]:
            eligible_orgs.add(r["admin_org"])
        hep_codes = sorted({hep_crosswalk[name] for name in eligible_orgs if name in hep_crosswalk})

        # inst_ids (2026-08-25): this grant's own ARC-org set -- exactly admin_org (current) and
        # announcement_admin_org, nothing from eligible_orgs (a deliberately different, narrower
        # set than hep_codes' own input above -- "Other Eligible"/"Collaborating Organisation"
        # aren't this specific investigator's own institution any more reliably than admin_org
        # is, and this set feeds a Splink comparison plus 04_resolve_links.py's institution
        # gate, where that distinction matters). Mapped cleanly to OAX institution ids via
        # institution_oax_crosswalk; rebuilt fresh every load_award_cif_items() call, never
        # persisted separately. Confirmed real at scale: 13.03% of grants the pipeline trusts as
        # "single institution" via n_eligible_orgs==1 actually differ between snapshots (e.g.
        # DP110100989: Wollongong at announcement, Australian Catholic University current, same
        # investigators throughout).
        arc_org_names = {n for n in (r["admin_org"], r["announcement_admin_org"]) if n}
        inst_ids = sorted({
            institution_oax_crosswalk[name] for name in arc_org_names
            if name in institution_oax_crosswalk
        })

        items.append(AwardCIFItem(
            unique_id=r["unique_id"],
            grant_code=r["grant_code"],
            first_name=first_name,
            family_name=r["family_name"],
            role_code=r["role_code"],
            orcid=orcid,
            is_fellowship=bool(r["is_fellowship"]),
            admin_org=r["admin_org"],
            institution_oax_id=r["institution_oax_id"],
            inst_ids=inst_ids,
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
            parsed=parsed,
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
    # Counter.most_common(1) breaks ties by insertion order (a stable property of Python's
    # dict/heapq machinery) -- but insertion order here is `items`' own order, which is NOT
    # guaranteed stable run-to-run (upstream Splink clustering has documented, accepted
    # non-determinism -- see CLAUDE.md's EM-training/seed notes). Found live 2026-08-23: a
    # genuine 50/50 first-name tie ("Xiao Dong Chen" tokenizing to "xiao"/"dong" with equal
    # frequency across his own grant records) made full_name_key flip between "xiao_chen" and
    # "dong_chen" across two back-to-back reruns with zero other changes, which flipped
    # is_suspicious_for2020()'s verdict and therefore resolution_status. Sorting by unique_id
    # first fixes the *tie-break*, not Splink's own upstream clustering drift (out of scope,
    # already a parked, accepted residual) -- this only guarantees that whichever items DO end
    # up in a cluster together always produce the same full_name_key, regardless of what order
    # they arrived in.
    # Canonical order for the whole object, not just the full_name_key computation below --
    # grant_ids=[it.unique_id for it in items] (and anything else iterating .items without its
    # own explicit sort) inherits this determinism for free rather than needing its own fix.
    items = sorted(items, key=lambda it: it.unique_id)
    fnk_counts = Counter(it.full_name_key for it in items if it.full_name_key)
    # Same modal design as full_name_key above, same tie-break (sorted-by-unique_id item
    # order, via Counter.most_common(1) breaking ties by first-seen) -- see
    # AwardsCIF.family_name_main's own docstring for why this can't be recovered later from
    # the already-deduped family_names set alone.
    fnm_counts = Counter(it.family_name_main for it in items if it.family_name_main)

    return AwardsCIF(
        cluster_id=cluster_id,
        items=items,
        full_names=sorted({it.full_name for it in items}),
        first_names=sorted({fn for it in items for fn in it.first_names}),
        family_names=sorted({fn for it in items for fn in it.family_names}),
        orcids=orcids,
        # 2026-08-25: widened from the single-scalar institution_oax_id per item to the full
        # inst_ids union (current admin_org + announcement_admin_org per grant, across every
        # grant this ACIF has) -- see AwardCIFItem.inst_ids' own docstring. Feeds
        # 03_link_arc_oax.py's ArrayIntersectAtSizes("inst_arr", ...) Splink comparison as loose
        # evidence (more is safe here, same reasoning as family_names/first_names_multichar) and
        # 04_resolve_links.py's institution-overlap gate, where len(inst_arr)==1 replaces the
        # old per-grant arc_all_single_org check (see that file's own docstring for why those
        # two are NOT equivalent -- the new check is stricter and correct, not a simplification).
        inst_arr=sorted({oid for it in items for oid in it.inst_ids}),
        hep_codes=sorted({hc for it in items for hc in it.hep_codes}),
        for_names=sorted({it.for_name for it in items if it.for_name}),
        for_codes=sorted({it.for_code for it in items if it.for_code}),
        for2020_codes=_aggregate_for2020_codes(items),
        full_name_key=fnk_counts.most_common(1)[0][0] if fnk_counts else None,
        family_name_main=fnm_counts.most_common(1)[0][0] if fnm_counts else None,
        grant_ids=[it.unique_id for it in items],
        # 2026-08-21 fix: was len(items) -- counted raw (grant x investigator) records, not
        # distinct grants, so an announcement/current same-grant name-snapshot pair (e.g.
        # LP0211723_DTNguyen: "D.T. Nguyen" + "Duc-Tho Nguyen", one real grant) was miscounted
        # as 2 grants. Found while gating is_suspicious_for2020() on n_grants<=1 -- that gate's
        # whole premise (a single-grant cluster was never the product of a multi-grant merge)
        # needs the real distinct-grant count to hold, not an item count that can overcount it.
        n_grants=len({it.grant_code for it in items}),
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
        "family_names": list(it.family_names),
        "full_name_key": it.full_name_key,
        "first_initial": it.first_initial,
        "orcid": it.orcid,
        "inst_arr": [it.institution_oax_id] if it.institution_oax_id else [],
        "for_name_tokens": it.for_name_tokens,
        # given_multichar (2026-09-08): given_tokens + nickname_tokens, single-character
        # initials filtered out. Feeds the given-name set-overlap blocking rule below --
        # bare initials must be excluded, or the rule fires on any coincidentally-shared
        # letter (confirmed directly: unfiltered, "Ying Zhu"/"Huai-Yong Zhu" blocked together
        # purely because "Huai-Yong" tokenizes to include a bare "y", nothing to do with
        # either person's actual given name). Anchored on family_name_main (not first_initial)
        # so it can still catch a genuine given-name mismatch -- first_initial itself is
        # exactly what a nickname breaks (Yingzi vs Jenny).
        "given_multichar": (
            [t for t in (it.parsed.given_tokens + it.parsed.nickname_tokens) if len(t) > 1]
            if it.parsed else []
        ),
    } for it in items])

    settings = SettingsCreator(
        unique_id_column_name="unique_id",
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("family_name_main", "first_initial"),
            "l.orcid = r.orcid AND l.orcid IS NOT NULL",
            # Set-overlap blocking (2026-09-07) -- mirrors 03_link_arc_oax.py's own 2026-08-25
            # fix (see that file's module docstring for the full incident/measurement). Same
            # root cause here: family_name_main is one scalar picked from a genuinely
            # multi-valued family_names set (confirmed real, not theoretical -- 77 real
            # AwardCIFItem records carry 2+ family_names, e.g. Schröder ->
            # ['schroder','schroeder','schröder']), so two of the SAME real person's own grant
            # records can carry disagreeing family_name_main scalars (a diacritic-convention
            # difference between two data-entry points, or a genuine mid-career spelling
            # correction) with nothing here to rescue the pair -- this dedupe run had neither
            # this blocking rule nor the matching comparison level below before this fix, unlike
            # its already-fixed 03_link_arc_oax.py sibling.
            "list_has_any(l.family_names, r.family_names) AND l.first_initial = r.first_initial",
            # Given-name set-overlap (2026-09-08) -- the given-name-side sibling of the rule
            # above, needed for the case first_initial itself can't survive: a real nickname
            # (Yingzi/Jenny) changes the initial, not just the spelling. Anchored on exact
            # family_name_main (not first_initial, which is exactly what's unreliable here) --
            # measured directly against the real ~64,830-item population: unanchored
            # (list_has_any(full_name_keys, full_name_keys) alone) cost 241s and produced
            # noise from bare-initial collisions; this anchored, multichar-filtered version
            # cost 0.5s and the false positive it was compared against was confirmed excluded.
            "l.family_name_main = r.family_name_main AND list_has_any(l.given_multichar, r.given_multichar)",
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
                    # Set overlap (2026-09-08) -- pairs reaching Splink only via the
                    # given_multichar blocking rule need to score as real evidence here, same
                    # reasoning as family_name_main's own set-overlap level below -- otherwise
                    # the blocking rule generates the pair but this comparison scores it as a
                    # mismatch, defeating the fix. Placed after the exact/initial levels,
                    # before the mismatch level below, mirroring 03_link_arc_oax.py's own
                    # 2026-08-25 given-name set-overlap level exactly (same m/u probabilities --
                    # hand-set, not EM-trained, per this project's one-EM-session rule).
                    {
                        "sql_condition": "list_has_any(given_multichar_l, given_multichar_r)",
                        "label_for_charts": "Set overlap (shared given-name/nickname form)",
                        "m_probability": 0.5, "u_probability": 0.02,
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
                    # Set overlap (2026-09-08 fix -- was missing despite the matching blocking
                    # rule above already existing since 2026-09-07, confirmed by direct
                    # comparison against 03_link_arc_oax.py's own equivalent level; a pair
                    # reaching Splink only via the family_names blocking rule was falling
                    # through to ElseLevel here, defeating that fix). Mirrors
                    # 03_link_arc_oax.py's "Set overlap (shared spelling variant)" level exactly.
                    cll.CustomLevel(
                        "list_has_any(family_names_l, family_names_r)",
                        label_for_charts="Set overlap (shared spelling variant)",
                    ).configure(m_probability=0.5, u_probability=0.02),
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

    # seed=42: pins u-probability random sampling (matches this project's existing seed
    # convention, 00_samples.py's reservoir sampling). 2026-08-21: confirmed this alone does
    # NOT make the pipeline fully deterministic -- a direct double-run test (with this seed AND
    # a stable ORDER BY on the base items query) still showed ~40/22,927 clusters differing
    # between runs. Root cause is deeper in Splink's own EM training/clustering internals
    # (suspected parallel floating-point summation order), not input-row ordering or this
    # sampling step alone. Accepted as a known, small (~0.1-0.2%) residual per user decision --
    # matches this file's own prior documented precedent ("~99.15% field-for-field agreement,
    # attributed to Splink's own run-to-run clustering stochasticity, not a logic gap"). This
    # seed is kept anyway since it removes one real, understood source, even though incomplete.
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000, seed=42)
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
    long after the ARC-only/OAX-enriched population is built) -- this exists for merge()'s
    post-hoc case (Phase 3), where a caller may merge two ACIFs that already have real oeuvre
    attached."""
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


def _load_manual_splits_by_grant(clusters: list[AwardsCIF]) -> dict[str, dict[str, str]]:
    """data_persisted/manual_splits_by_grant.csv -- cluster_id -> {unique_id: split_label}, an
    explicit finer-than-institution split assignment. 2026-08-21: added after a confirmed real
    case (DE250100317_LIANGWANG) where institution-based splitting is structurally a no-op --
    both real people (an early-career chemical engineer, DE250100317, and an unrelated
    religious-studies scholar sharing only the name, DP200100524) are administered by the same
    institution (Monash), so grouping by institution_oax_id would put them straight back
    together. Rows not covering every item in a cluster are fine -- any unlisted unique_id falls
    back to its own singleton group, same semantics as the institution-based path's own
    no-institution fallback.

    Each row's cluster_id is resolved via resolve_cluster_id() -- raises StaleClusterIdError
    rather than silently falling back to the institution-based method this file exists to
    supersede (a stale id here previously meant apply_manual_splits() would silently regress
    to a known-insufficient split, not merely skip one)."""
    if not _MANUAL_SPLITS_BY_GRANT_CSV.exists():
        return {}
    out: dict[str, dict[str, str]] = defaultdict(dict)
    with open(_MANUAL_SPLITS_BY_GRANT_CSV, newline="") as f:
        for row in csv.DictReader(f):
            cid, uid, label = row["cluster_id"].strip(), row["unique_id"].strip(), row["split_label"].strip()
            if cid and uid and label:
                out[resolve_cluster_id(cid, clusters)][uid] = label
    return dict(out)


def apply_manual_splits(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Split clusters confirmed as containing 2+ different people in
    data_persisted/manual_splits.csv. Dividing key: an explicit per-unique_id assignment from
    data_persisted/manual_splits_by_grant.csv when the cluster has one (any item not covered by
    it falls back to its own singleton group), else institution_oax_id (original behaviour) --
    mirrors _apply_manual_splits().

    Each row's cluster_id is resolved via resolve_cluster_id() -- raises StaleClusterIdError
    rather than silently letting a confirmed wrongful-merge split stop being applied when the
    target cluster's id has drifted (see that function's docstring)."""
    if not _MANUAL_SPLITS_CSV.exists():
        return clusters
    split_ids: set[str] = set()
    with open(_MANUAL_SPLITS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("confirmed_different_people", "").strip().lower() == "true":
                split_ids.add(resolve_cluster_id(row["cluster_id"], clusters))
    if not split_ids:
        return clusters
    by_grant = _load_manual_splits_by_grant(clusters)

    out = []
    for c in clusters:
        if c.cluster_id not in split_ids:
            out.append(c)
            continue
        grant_assignments = by_grant.get(c.cluster_id)
        groups: dict[str, list[AwardCIFItem]] = defaultdict(list)
        for it in c.items:
            if grant_assignments is not None:
                key = grant_assignments.get(it.unique_id, f"__singleton__{it.unique_id}")
            else:
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
    enriched name forms in a cluster, and the cluster currently has no ORCID.

    enrichment_blocklist.csv rows are keyed on cluster_id too, resolved via
    resolve_cluster_id() -- raises StaleClusterIdError rather than silently letting a
    previously-confirmed wrong ORCID match get re-promoted once the blocked cluster's own id
    has drifted (see that function's docstring)."""
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
                    blocklist.add((resolve_cluster_id(cid, clusters), orcid))

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
    data_persisted/manual_orcids.csv. Mirrors _apply_manual_orcids().

    Each row's cluster_id is resolved via resolve_cluster_id() -- raises StaleClusterIdError
    rather than silently dropping a hard-won manual ORCID confirmation when the target
    cluster's id has drifted (see that function's docstring)."""
    if not _MANUAL_ORCIDS_CSV.exists():
        return clusters
    overrides: dict[str, str] = {}
    with open(_MANUAL_ORCIDS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            cid, orcid = row["cluster_id"].strip(), row["orcid"].strip()
            if cid and orcid:
                overrides[resolve_cluster_id(cid, clusters)] = orcid
    if not overrides:
        return clusters
    by_id = {c.cluster_id: c for c in clusters}
    for cid, orcid in overrides.items():
        c = by_id[cid]
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
    absorbed into it.

    Both cluster_keep and cluster_drop are resolved via resolve_cluster_id(). This replaces
    the previous silent behaviour, which only ever checked that cluster_drop currently
    existed: if cluster_keep alone had gone stale, cluster_drop -- a real, currently-existing
    person -- was silently dropped from the entire population outright (not merged, not left
    standalone, just deleted, with nothing printed anywhere; see CLAUDE.md's 2026-08-26
    stale-reference risk audit, the most severe of the risks found there). If keep and drop
    resolve to the same current cluster, the merge has already happened via some other route
    (e.g. an automatic ORCID merge reached the same conclusion first) -- skipped as a
    harmless no-op, not an error, since nothing here is actually stale."""
    if not _MANUAL_MERGES_CSV.exists():
        return clusters
    by_id = {c.cluster_id: c for c in clusters}
    remapping: dict[str, str] = {}
    with open(_MANUAL_MERGES_CSV, newline="") as f:
        for row in csv.DictReader(f):
            keep, drop = row["cluster_keep"].strip(), row["cluster_drop"].strip()
            if not (keep and drop):
                continue
            keep = resolve_cluster_id(keep, clusters)
            drop = resolve_cluster_id(drop, clusters)
            if keep != drop:
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
    university, two clusters sharing ANY family-name spelling variant (not just one collapsed
    via max_by_len -- see the 2026-08-25 fix note, same class of bug confirmed on
    DP0345157_HansMuhlhaus in the ARC<->OAX linking blocking rule, applicable here too since a
    cluster's aggregated family_names can genuinely hold 2+ distinct ARC-side spellings) +
    first_initial on that grant are the same person. Skips pairs whose clusters already carry
    distinct non-empty ORCIDs."""
    grants = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")
    if "n_eligible_orgs" not in grants.columns:
        return clusters
    single_org = set(grants.loc[grants["n_eligible_orgs"] == 1, "grant_code"])
    if not single_org:
        return clusters

    key_groups: dict[tuple, set[str]] = defaultdict(set)
    for c in clusters:
        ini = _first_initial(c.first_names)
        if not c.family_names or ini is None:
            continue
        for gid in c.grant_ids:
            grant_code = gid.rsplit("_", 1)[0]
            if grant_code in single_org:
                for fam in c.family_names:
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


def _load_manual_unlinks(clusters: list[AwardsCIF]) -> dict[str, set[str]]:
    """arc_id -> set of oax_ids a human has confirmed are NOT this person, from
    data_persisted/manual_resolutions.csv's "unlink" rows. Pure noise removal -- can only
    remove a confirmed-wrong candidate, never risks discarding a genuine one, unlike a
    "resolve" row (see the plan file for why "resolve" is deliberately NOT applied here).

    Each row's arc_id is resolved via resolve_cluster_id() before use -- raises
    StaleClusterIdError rather than silently no-op'ing when a row's arc_id has drifted or
    can no longer be found (see that function's docstring)."""
    if not _MANUAL_RESOLUTIONS_CSV.exists():
        return {}
    df = pd.read_csv(_MANUAL_RESOLUTIONS_CSV).dropna(subset=["arc_id"])
    out: dict[str, set[str]] = defaultdict(set)
    for _, row in df[df["action"] == "unlink"].iterrows():
        oax_id = row.get("oax_id")
        if pd.notna(oax_id) and oax_id:
            cid = resolve_cluster_id(row["arc_id"], clusters)
            out[cid].add(oax_id)
    return dict(out)


def _oax_names_compat(oax_ids: list[str], oax_full_name_keys: dict) -> bool:
    """True when every OAX candidate in a group could plausibly be the same person --
    guards the split-record dedup below against collapsing genuinely different people who
    happen to share a topic.

    2026-09-09 fix: checked via genuine exact-string overlap on each candidate's own
    full_name_keys (every given/nickname x family combination that candidate's own
    display_name + alternatives produce -- see 02_prepare_oax.py::oax_name_arrays()), not the
    old single-scalar first_name/family_name_main + 3-char-prefix heuristic. That heuristic
    only ever compared first names >=4 characters long and returned True by default whenever
    fewer than 2 candidates had one -- confirmed on a real case (DE120100315_BenjaminIsakhan,
    "ben" vs "benjamin"): "ben" is 3 characters, so it was silently dropped from the
    comparison entirely and the function returned True without ever really testing anything --
    it happened to reach the right answer, but not because it verified any real relationship
    between "ben" and "benjamin" (there isn't a shared exact full_name_key between them either;
    see the module's own open-question note on nickname/short-form matching).

    full_name_keys itself stays complete/unfiltered (NameProcessor's own output contract --
    see oax_name_arrays()'s docstring) -- the bare-initial exclusion below is local to THIS
    comparison's own needs, not baked into the shared field. A full_name_key whose given-side
    is a single character (e.g. "b_isakhan") is excluded here because it carries no identifying
    information on its own: every given-name token self-adds its own first letter (needed for
    Splink's family+first_initial blocking key), so "Ben"/"Barbara"/"Bruce" all reduce to "b" --
    a shared bare-initial key is exactly as consistent with two different people as with one.

    Requires EVERY pair of candidates to share >=1 (informative) full_name_keys string, not just
    that all candidates share one common string across the whole group -- avoids the same
    false-chain-bridging risk found elsewhere this project (piling's DBSCAN mega-pools): A/B and
    B/C sharing a key each doesn't mean A and C are the same person if A and C themselves share
    nothing. Returns False (does not merge) if any candidate has no informative full_name_keys
    at all -- insufficient evidence should leave both candidates for the later disambiguation
    cascade to sort out, not default to merging them."""
    def _informative(keys):
        out = set()
        for k in keys:
            given, _, family = k.partition("_")
            if len(given) > 1 and family:
                out.add(k)
        return out

    keysets = [_informative(oax_full_name_keys.get(oid) or []) for oid in oax_ids]
    if any(not ks for ks in keysets):
        return False
    return all(
        keysets[i] & keysets[j]
        for i in range(len(keysets)) for j in range(i + 1, len(keysets))
    )


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
    unlinks = _load_manual_unlinks(clusters)
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
            SELECT o.unique_id, o.orcid, o.topic_names, o.full_name_keys
            FROM read_parquet('{PROCESSED_DATA}/openalex_authors_prep.parquet') o
            JOIN _cand_ids c ON c.oax_id = o.unique_id
        """).fetchall()
        oax_orcid, oax_topics, oax_full_name_keys = {}, {}, {}
        for uid, orcid, topics, full_name_keys in rows:
            oax_orcid[uid] = orcid
            oax_topics[uid] = list(topics) if topics is not None else []
            oax_full_name_keys[uid] = list(full_name_keys) if full_name_keys is not None else []

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
        # sorted, not set() -- set() iteration order is randomized per-process
        # (PYTHONHASHSEED), and this feeds max(wcs, key=wcs.get) below, whose tie-break (two
        # candidates with equal works_count) would otherwise silently differ across runs. Same
        # class of bug found and fixed 2026-08-23 in _name_forms() (see that function's
        # docstring); dedup itself doesn't need any particular order, just a stable one.
        group = sorted(set(c.oax_candidates))
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

        remaining = [oid for oid in group if oid not in removed]  # list, not set - to keep this same order-preserving
        topic_to_ids: dict[str, list[str]] = defaultdict(list)
        for oid in remaining:
            for t in oax_topics.get(oid, []):
                topic_to_ids[t].append(oid)
        orcid_protected = {oid for oid in remaining if oax_orcid.get(oid)}
        for ids in topic_to_ids.values():
            if len(ids) > 1 and _oax_names_compat(ids, oax_full_name_keys):
                wcs = {oid: oax_works.get(oid, 0) for oid in ids}
                best = max(wcs, key=wcs.get)
                removed.update(
                    oid for oid in ids
                    if oid != best and oid not in orcid_protected
                )

        if removed:
            c.oax_candidates = sorted(oid for oid in group if oid not in removed)
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


def compute_coawardees(clusters: list[AwardsCIF], items: list[AwardCIFItem]) -> list[AwardsCIF]:
    """Populate coawardees -- every OTHER investigator appearing on any grant this ACIF holds,
    keyed by their own full parsed name (full_name_key, falling back to full_name_key_raw, same
    convention as everywhere else), with a count of how many of this ACIF's own grants they
    co-appear on. Reuses `items` already loaded by load_award_cif_items() -- no second read of
    investigators_raw.parquet.

    Deliberately NOT filtered against this cluster's own name(s) -- per direct user direction,
    a co-awardee key colliding with the ACIF's own is a useful signal (found via exactly this
    check: DP0665337_JocelynCraig / DP0665337_JocelynLynCraig, a real candidate same-person
    split later confirmed via each side's own ORCID record -- see docs/pipeline_todo.md), not
    an error to hide by construction.
    """
    grant_investigators: dict[str, list[tuple[str, ParsedName]]] = defaultdict(list)
    for it in items:
        if it.parsed is not None:
            grant_investigators[it.grant_code].append((it.unique_id, it.parsed))

    for c in clusters:
        own_ids = {it.unique_id for it in c.items}
        grant_codes = {it.grant_code for it in c.items}
        tally: dict[str, dict] = {}
        for gc in grant_codes:
            for uid, parsed in grant_investigators.get(gc, []):
                if uid in own_ids:
                    continue
                key = parsed.full_name_key or parsed.full_name_key_raw
                if not key:
                    continue
                if key not in tally:
                    d = asdict(parsed)
                    d["given_tokens"] = list(d["given_tokens"])
                    d["middle_tokens"] = list(d["middle_tokens"])
                    d["nickname_tokens"] = list(d["nickname_tokens"])
                    d["family_names"] = list(d["family_names"])
                    d["full_name_keys"] = list(d["full_name_keys"])
                    d["given_tokens_raw"] = list(d["given_tokens_raw"])
                    tally[key] = {**d, "count": 0}
                tally[key]["count"] += 1
        c.coawardees = sorted(
            tally.values(), key=lambda x: (-x["count"], x.get("full_name_key") or "")
        )

    n_with_coaw = sum(1 for c in clusters if c.coawardees)
    print(f"  coawardees: {n_with_coaw} clusters with >=1 co-awardee")
    return clusters


def find_coawardee_self_collisions(clusters: list[AwardsCIF]) -> list[dict]:
    """The standing population-wide check docs/pipeline_todo.md #20 asks for: does any ACIF's
    own `coawardees` entry share a full_name_key/full_name_key_raw with that SAME ACIF's own
    name? Found by hand on 4 clusters (Craig, Restubog) before this existed -- both confirmed
    real same-person splits, one externally confirmed via a live OpenAlex lookup showing the
    "loser" side's own ORCID belongs to a different, unrelated person entirely.

    Checks every one of the ACIF's own ITEMS' full_name_key/full_name_key_raw (not just the
    cluster-level modal `full_name_key` scalar) against the same on the coawardee side -- a
    cluster with 2+ genuinely different recorded name-forms (the whole point of the Craig case)
    would otherwise only ever get checked against whichever form happened to be modal, silently
    missing a collision on its other own name-form. Surfaced as a reviewable list, same
    discipline as gap_candidates -- never auto-resolved."""
    results = []
    for c in clusters:
        if c.excluded:
            continue
        own_keys: set[str] = set()
        for it in c.items:
            if it.full_name_key:
                own_keys.add(it.full_name_key)
            if it.parsed is not None and it.parsed.full_name_key_raw:
                own_keys.add(it.parsed.full_name_key_raw)
        if not own_keys:
            continue
        for co in c.coawardees:
            co_key = co.get("full_name_key") or co.get("full_name_key_raw")
            if co_key and co_key in own_keys:
                results.append({
                    "cluster_id": c.cluster_id,
                    "full_names": list(c.full_names),
                    "orcids": list(c.orcids),
                    "grant_ids": list(c.grant_ids),
                    "coawardee_key": co_key,
                    "coawardee_count": co["count"],
                })
    return results


def widen_names_with_orcid_bulk_db(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Additive name-form widening from the local ORCID bulk snapshot
    (src/utils/orcid_processor.py's orcid_bulk.parquet -- 17.15M ORCID records, the full Zenodo
    population, no rate limit, ~0.6s for this project's whole HAS_ORCID population; 2026-09-02:
    supersedes the older orcid_bulk_lookup.py/orcid_persons.parquet ~4.8M HQ-only snapshot,
    retired the same day -- see docs/pipeline_todo.md #19). For every cluster with >=1 resolved
    ORCID, fetches that ORCID's own self-reported `name` + `aliases` and unions their parsed
    tokens into full_names/first_names/family_names -- never removes or overrides anything
    already there, matching this project's established "more evidence, never fewer" pattern
    (diacritic variants, family_names set-overlap blocking).

    Deliberately NOT a data-quality check (2026-08-25 direct user redirect, mid-build): an ad
    hoc pass comparing ARC's existing family name against the bulk record's own name for every
    HAS_ORCID cluster found 31/14,852 apparent mismatches; manual inspection showed most were
    formatting artifacts (Mc Credden vs mccredden, van der Heijden vs vanderheijden, O'Brien vs
    obrien) this project's own tokenization doesn't fully normalise, not genuine errors --
    separating a real typo from a genuine married-name/variant needs real case-by-case
    judgement, exactly the manual burden this project has been trying to reduce. Folding the
    extra name forms in additively sidesteps that judgement call entirely: worst case it's
    redundant, best case it recovers a genuine variant nothing else would have caught.

    Runs after compute_orcid_for() (needs c.orcids finalised -- ORCID promotion/enrichment/
    manual overrides all happen earlier, inside refine_clusters()). This only widens
    awards_cif_arc_only.parquet's own name fields; the actual benefit shows up downstream, in
    03_link_arc_oax.py's ARC<->OAX blocking (which reads family_names/first_names from this
    file), since 01_prepare_arc.py's own ARC-internal Splink dedupe_only run has already
    completed by the time this step runs -- widening here can't retroactively change
    ARC-internal cluster membership, by design (confirmed acceptable, not a gap: 2026-08-25).
    """
    from src.utils.orcid_processor import OrcidProcessor

    all_orcids = sorted({oid for c in clusters for oid in c.orcids if oid})
    if not all_orcids:
        return clusters
    with OrcidProcessor(bulk_parquet=ORCID_BULK_PARQUET) as proc:
        bulk = proc.lookup_by_orcid(all_orcids)
    bulk_names_by_orcid: dict[str, list[str]] = {}
    for orcid, rec in bulk.items():
        names = ([rec["name"]] if rec["name"] else []) + list(rec["aliases"])
        bulk_names_by_orcid[orcid] = [n for n in names if n]

    n_widened = 0
    for c in clusters:
        new_full: set[str] = set()
        new_first: set[str] = set()
        new_family: set[str] = set()
        for oid in c.orcids:
            for raw_name in bulk_names_by_orcid.get(oid, []):
                new_full.add(raw_name)
                fn, fam = _name_forms(raw_name, "")
                new_first.update(fn)
                new_family.update(fam)
        added = (
            (new_full - set(c.full_names))
            | (new_first - set(c.first_names))
            | (new_family - set(c.family_names))
        )
        if added:
            c.full_names = sorted(set(c.full_names) | new_full)
            c.first_names = sorted(set(c.first_names) | new_first)
            c.family_names = sorted(set(c.family_names) | new_family)
            n_widened += 1

    print(f"  ORCID bulk-DB name widening: {n_widened} clusters gained >=1 new name form "
          f"({len(bulk_names_by_orcid)}/{len(all_orcids)} ORCIDs found in local snapshot)")
    return clusters


def cluster_detail_data(
    cluster_id: str, gmap: pd.DataFrame, items: list[AwardCIFItem], grants: pd.DataFrame,
    gap_candidate_ids: list[str] | None = None,
) -> dict:
    """Structured per-grant breakdown for one cluster: scheme/year/admin_org/FOR per grant,
    plus every co-investigator recorded on that grant (across ALL clusters, not just this
    one) with their own ORCID, cluster_id, and is_fellowship status. Built from
    load_award_cif_items()'s own items list, arc_grant_cluster_map.parquet, and
    grants_flat.parquet -- the same sources 01a_diagnose.py's A/B/C checks already treat as
    canonical -- rather than a fresh ad hoc join, so it can't drift from what those checks are
    actually testing. Returns a dict (not text) so console output, JSON export, and any
    downstream review report (e.g. a Dossier's ARC-story header) all render from the same one
    assembly. Relocated here from src/01a_diagnose.py (2026-08-24) so it's importable by
    non-diagnostic consumers without a second implementation.

    gap_candidate_ids, if given, nests each named cluster's OWN cluster_detail_data() (called
    with gap_candidate_ids=None) under d["gap_candidates"] -- exactly one level, never
    recursive, since gap_candidates is a symmetric relation (A's list contains B, B's list
    contains A) and recursing further would revisit clusters indefinitely."""
    my_uids = set(gmap[gmap["cluster_id"] == cluster_id]["unique_id"])
    my_items = [it for it in items if it.unique_id in my_uids]
    if not my_items:
        return {"cluster_id": cluster_id, "found": False}

    uid_to_cluster = dict(zip(gmap["unique_id"], gmap["cluster_id"]))

    grant_codes = sorted({it.grant_code for it in my_items})
    by_grant: dict[str, list[AwardCIFItem]] = {}
    for it in items:
        by_grant.setdefault(it.grant_code, []).append(it)

    grant_rows = []
    for gc in grant_codes:
        grow = grants[grants["grant_code"] == gc]
        scheme = grow.iloc[0]["scheme_name"] if len(grow) else None
        year_val = grow.iloc[0]["funding_commence_year"] if len(grow) else None
        year = int(year_val) if pd.notna(year_val) else None
        admin_org = grow.iloc[0]["admin_org"] if len(grow) else None
        mine_here = [it for it in my_items if it.grant_code == gc]
        for_codes = mine_here[0].for2020_codes if mine_here else []
        for_list = [{"code": e["code"], "name": e["name"], "is_primary": e.get("is_primary", False)} for e in for_codes]
        if not for_list and mine_here and mine_here[0].for_name:
            for_list = [{"code": mine_here[0].for_code, "name": mine_here[0].for_name, "is_primary": True}]
        investigators = [
            {
                "first_name": it.first_name, "family_name": it.family_name,
                "role_code": it.role_code, "orcid": it.orcid,
                "is_this_cluster": it.unique_id in my_uids,
                "cluster_id": uid_to_cluster.get(it.unique_id),
                "is_fellowship": it.is_fellowship,
            }
            for it in sorted(
                by_grant.get(gc, []),
                key=lambda x: (x.unique_id not in my_uids, x.family_name.lower(), x.first_name.lower()),
            )
        ]
        grant_rows.append({
            "grant_code": gc, "scheme": scheme, "year": year, "admin_org": admin_org,
            "for_codes": for_list, "investigators": investigators,
            "hep_codes": mine_here[0].hep_codes if mine_here else [],
        })

    return {
        "cluster_id": cluster_id, "found": True,
        "n_grants": len(grant_codes),
        "orcids_on_file": sorted({it.orcid for it in my_items if it.orcid}),
        "grants": grant_rows,
        "gap_candidates": [
            cluster_detail_data(gc_id, gmap, items, grants) for gc_id in (gap_candidate_ids or [])
        ],
    }


def cluster_detail_text(d: dict, heading_level: int = 3) -> str:
    """Text/markdown rendering of a cluster_detail_data() dict (already fetched, not re-fetched
    here -- the two are deliberately decoupled so a caller can fetch once and render more than
    once, or render a dict built some other way). Recurses one level into d["gap_candidates"]
    at heading_level+1, matching cluster_detail_data()'s own one-level cap."""
    if not d["found"]:
        return f"{'#' * heading_level} {d['cluster_id']}\n(no items found under this cluster_id)"
    lines = [f"{'#' * heading_level} {d['cluster_id']}  ({d['n_grants']} grants)"]
    lines.append(f"ARC ORCID(s) on file: {d['orcids_on_file'] or 'none'}")
    for g in d["grants"]:
        for_str = ", ".join(f"{e['code']}:{e['name']}" for e in g["for_codes"]) or "?"
        lines.append(f"- **{g['grant_code']}** ({g['scheme']}, {g['year']}, {g['admin_org']}) — FOR: {for_str}")
        for it in g["investigators"]:
            marker = "  <== this cluster" if it["is_this_cluster"] else ""
            o = it["orcid"] or "-"
            fell = ", fellowship" if it.get("is_fellowship") else ""
            lines.append(f"    {it['first_name']} {it['family_name']} ({it['role_code']}, ORCID {o}{fell}) [{it.get('cluster_id')}]{marker}")
    for gc in d.get("gap_candidates", []):
        lines.append("")
        lines.append(cluster_detail_text(gc, heading_level=heading_level + 1))
    return "\n".join(lines)


def sample_4u_clusters(pool: pd.DataFrame, n: int = 30, seed: int = 42) -> list[str]:
    """Deterministic random sample of cluster_ids from `pool` (caller filters to
    reliability_tier=='4u' -- or whatever other subset -- first; kept a pure, testable sampling
    primitive over whatever frame it's given, not coupled to that column name). Sorted for
    stable, reproducible output order regardless of pool's own row order."""
    if len(pool) <= n:
        return sorted(pool["cluster_id"].tolist())
    return sorted(pool.sample(n=n, random_state=seed)["cluster_id"].tolist())


# DECRA (DE) is a narrowly-defined early-career award -- ARC's own eligibility rule requires
# the PhD to have been conferred within a bounded number of years before application (career-
# interruption extensions can add years, but not decades). This bounds how far apart, in time,
# a DECRA can plausibly sit from any other ARC-funded grant the same real person holds. Set
# deliberately conservative (generous, not tight) so this only flags genuinely implausible
# pairings, not edge cases -- found live 2026-08-24 via a 4u sample review: real gap_candidate
# pairs included two independently-recorded DECRAs 12 years apart (DE120100016_KhoaNguyen 2012
# vs DE240100408_TuanKhoaNguyen 2024), two DECRAs in essentially the same year
# (DE140100735_SangWonLee 2014 vs DE130100614_SangHongLee 2013 -- ARC does not fund the same
# person's DECRA proposal twice under two different grant codes), and DP/LP grants predating a
# DECRA by 12-22 years.
DE_ELIGIBILITY_YEARS = 12


def _scheme_years(items: list[AwardCIFItem]) -> list[tuple[str, int | None, str]]:
    """(scheme_prefix, funding_commence_year, grant_code) per item -- the raw material
    _scheme_incompat() needs; kept as a separate, testable step."""
    out = []
    for it in items:
        year = int(it.funding_commence_year) if it.funding_commence_year else None
        out.append((it.grant_code[:2], year, it.grant_code))
    return out


def _scheme_incompat(sy_a: list[tuple[str, int | None, str]], sy_b: list[tuple[str, int | None, str]]) -> bool:
    """True when the two sides' own scheme/year histories make them structurally impossible to
    be the same real person, independent of name/FOR-division/ORCID evidence -- ARC
    scheme-eligibility windows are a hard career-stage fact, not a heuristic. Three checks, all
    confirmed against real cases this project has found (see DE_ELIGIBILITY_YEARS's docstring
    and the already-resolved Tao Liu FT/DE case, 2026-08-21):
      1. Both sides hold a DE grant under a DIFFERENT grant_code -- DECRA is a one-shot award,
         so two independently-recorded DECRAs can never be one career.
      2. One side holds an FT (Future Fellowship, mid-career) and the other a DE (DECRA,
         early-career) -- the two eligibility windows cannot overlap for one career.
      3. Either side's DE grant is more than DE_ELIGIBILITY_YEARS after the OTHER side's
         earliest grant of any scheme -- already having ARC funding under one's own name that
         long before a DECRA is inconsistent with DECRA's early-career eligibility window."""
    de_a = [t for t in sy_a if t[0] == "DE"]
    de_b = [t for t in sy_b if t[0] == "DE"]

    if de_a and de_b and {gc for _, _, gc in de_a} != {gc for _, _, gc in de_b}:
        return True

    ft_a = any(t[0] == "FT" for t in sy_a)
    ft_b = any(t[0] == "FT" for t in sy_b)
    if (ft_a and de_b) or (ft_b and de_a):
        return True

    years_a = [y for _, y, _ in sy_a if y is not None]
    years_b = [y for _, y, _ in sy_b if y is not None]
    de_years_a = [y for _, y, _ in de_a if y is not None]
    de_years_b = [y for _, y, _ in de_b if y is not None]
    if de_years_a and years_b and min(de_years_a) - min(years_b) > DE_ELIGIBILITY_YEARS:
        return True
    if de_years_b and years_a and min(de_years_b) - min(years_a) > DE_ELIGIBILITY_YEARS:
        return True

    return False


def _load_confirmed_distinct(clusters: list[AwardsCIF]) -> set[tuple[str, str]]:
    """data_persisted/manual_confirmed_distinct.csv -- cluster_id pairs a human has reviewed and
    decided should stop being flagged as a gap_candidate of each other, despite
    compute_gap_candidates() being unable to rule them out automatically. Two distinct kinds of
    row, both stored here rather than in two separate files (2026-08-26 direction: the practical
    effect on gap_candidates is identical either way, and a third mechanism for a rarer case
    wasn't worth the extra schema) -- the `notes` column says which applies to a given row:
      1. Confirmed genuinely two different people (co-investigator overlap, institution/
         employment history, co-authorship checks, external ORCID lookups).
      2. Reviewed and found permanently INDETERMINATE, not confirmed distinct from anyone in
         particular -- e.g. DP0210133_JWilson: ARC's raw grant record shows "Dr J Wilson" only
         in the announcement snapshot, dropped entirely from the current snapshot, with no
         fuller given name ever recorded anywhere -- there is no remaining ARC-side evidence
         that could ever resolve which (if any) of its 6 gap_candidates is the same person, so
         further review is pointless, even though "confirmed distinct" would overstate what's
         actually known.

    Exists for the same reason manual_confirmed_not_suspicious.csv does (see that function's own
    docstring): compute_gap_candidates() is a pure, stateless function of current cluster data,
    recomputed identically on every 01_prepare_arc.py run with no memory of prior human review --
    without this file, a case a human has already checked (David Evans, David Walker, Mark Baker,
    J Wilson -- all repeatedly re-flagged across multiple past sessions) would keep resurfacing
    in reliability_tier=='4u' forever, exactly the "reviewed forever" loop already fixed once for
    is_suspicious_for2020(). Deliberately pair-level, not cluster-level -- a cluster can have
    several gap_candidates and only some of them may be reviewed (e.g. Mark Baker's LP0776387 is
    confirmed distinct from DP110100984, but deliberately left open against DP0557854 -- a live,
    unconfirmed lead the user does not want settled either way).

    Each row's two cluster_ids are resolved via resolve_cluster_id() before use -- built this way
    from the start (2026-08-26), applying the same lesson learned earlier this session for every
    other cluster_id-keyed manual_*.csv file, rather than waiting to discover the same staleness
    bug in a new file later."""
    if not _MANUAL_CONFIRMED_DISTINCT_CSV.exists():
        return set()
    pairs: set[tuple[str, str]] = set()
    with open(_MANUAL_CONFIRMED_DISTINCT_CSV, newline="") as f:
        for row in csv.DictReader(f):
            a, b = row["cluster_id_1"].strip(), row["cluster_id_2"].strip()
            if a and b:
                a = resolve_cluster_id(a, clusters)
                b = resolve_cluster_id(b, clusters)
                pairs.add(tuple(sorted((a, b))))
    return pairs


def compute_gap_candidates(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Populate gap_candidates -- other cluster_ids sharing the same (longest) family name that
    cannot be ruled out as the same person. Mirrors 01_prepare_arc.py's _compute_gap_candidates():
    pairwise within each family-name group, incompatible on any of name / FOR-division / ORCID /
    scheme-eligibility -> kept separate; otherwise both sides record each other as a gap
    candidate.

    Feeds compute_reliability()'s tier 4 vs 4u distinction -- must run first.

    Division check (2026-08-12): uses for2020_codes' numeric divisions via
    _pairwise_division_mismatch(), not the old for_names + for_divisions.csv route -- see that
    function's docstring for why (no adjacency tolerance, stricter than before).

    Scheme-eligibility check (2026-08-24): see _scheme_incompat()'s own docstring. Reads each
    cluster's own .items (still populated at this point in the pipeline -- compute_gap_candidates
    runs before persistence, and nothing clears .items along the way) rather than taking a
    separate items parameter, so this function's signature and every existing call site stay
    unchanged.

    Grouping (2026-08-25): candidate pairs are generated from ANY shared family_names token, not
    one collapsed via max_by_len() -- same class of bug as the ARC<->OAX linking blocking rule
    fixed the same day (see 03_link_arc_oax.py's blocking_rules_to_generate_predictions): a
    cluster's own family_names can hold 2+ genuinely distinct ARC-side spellings, and picking
    only the longest one for grouping can silently miss a real shared-surname pair whose other
    cluster happened to collapse to a different variant. Each qualifying pair is deduplicated
    (via a set of sorted cluster_id tuples) before the compatibility checks run, so a pair
    sharing multiple tokens is still only evaluated once -- the original single-token grouping
    could never double-count a pair, and this preserves that."""

    # Sorted by cluster_id, not clusters' own incoming order -- that order isn't guaranteed
    # stable run-to-run, and it directly determined gap_candidates' list order below (append
    # order within each family-name group). Content was already correct (same set every run);
    # this just makes the order deterministic too, matching the same fix applied to
    # full_name_key/grant_ids in _build_awards_cif().
    by_token: dict[str, list[AwardsCIF]] = defaultdict(list)
    for c in sorted(clusters, key=lambda c: c.cluster_id):
        for fam in c.family_names:
            by_token[fam].append(c)

    by_id = {c.cluster_id: c for c in clusters}
    candidate_pairs: set[tuple[str, str]] = set()
    for fam, grp in by_token.items():
        if len(grp) < 2:
            continue
        for i, c1 in enumerate(grp):
            for c2 in grp[i + 1:]:
                candidate_pairs.add(tuple(sorted((c1.cluster_id, c2.cluster_id))))

    confirmed_distinct = _load_confirmed_distinct(clusters)
    gap: dict[str, list[str]] = {c.cluster_id: [] for c in clusters}
    n_compat = n_incompat = n_manual_distinct = 0
    for cid1, cid2 in sorted(candidate_pairs):
        c1, c2 = by_id[cid1], by_id[cid2]
        if (cid1, cid2) in confirmed_distinct:
            n_manual_distinct += 1
            n_incompat += 1
            continue
        name_incompat = (
            not first_names_compatible(c1.first_names, c2.first_names) or
            not first_names_compatible(c2.first_names, c1.first_names)
        )
        div_incompat = _pairwise_division_mismatch(c1.for2020_codes, c2.for2020_codes)
        orcid_incompat = (
            len(c1.orcids) > 0 and len(c2.orcids) > 0
            and not set(c1.orcids) & set(c2.orcids)
        )
        scheme_incompat = _scheme_incompat(_scheme_years(c1.items), _scheme_years(c2.items))
        if name_incompat or div_incompat or orcid_incompat or scheme_incompat:
            n_incompat += 1
        else:
            gap[c1.cluster_id].append(c2.cluster_id)
            gap[c2.cluster_id].append(c1.cluster_id)
            n_compat += 1

    for c in clusters:
        c.gap_candidates = sorted(gap[c.cluster_id])

    print(
        f"  Gap 1: {n_compat} compatible pairs, {n_incompat} incompatible pairs "
        f"({n_manual_distinct} via manual_confirmed_distinct.csv)"
    )
    return clusters


def _load_confirmed_not_suspicious(clusters: list[AwardsCIF]) -> set[str]:
    """data_persisted/manual_confirmed_not_suspicious.csv -- cluster_ids a human has reviewed
    (typically via external evidence: co-authorship, employment history, news/press coverage)
    and confirmed are genuinely one person, despite is_suspicious_for2020() flagging them.

    Exists because is_suspicious_for2020() is a pure, stateless function of a cluster's own
    current full_name_key/for2020_codes/n_grants -- it is recomputed identically on every
    01_prepare_arc.py run and has no memory of prior human review. Without this file, a
    confirmed-correct cluster that happens to span FOR2020 divisions outside
    ACCEPTABLE_DIVISION_PAIRS would be re-flagged UNRESOLVED forever, including immediately
    after a manual_splits.csv split correctly separates it from a wrongly-merged companion --
    the split fixes the membership, not the whitelist gap that flagged the surviving piece.
    Found and fixed 2026-08-23 after exactly that happened to several splits in one session
    (Michael Anderson's arts-education pair, Jian Liu's nanotechnology-career pieces) --
    user: "that is silly - a recipe for going around in circles."

    Deliberately NOT a substitute for fixing division_mismatch_for2020()'s own whitelist gaps
    (see cluster_checks.py's module docstring) -- this is the same two-tier pattern as every
    other data_persisted/manual_*.csv: a human-confirmed override recorded permanently,
    reviewed once, not re-litigated on every run.

    2026-08-26: this docstring used to end "stable as long as the cluster's own grant_ids
    don't change again" -- that assumption turned out unsafe (cluster_id can drift for
    reasons having nothing to do with the reviewed suspicious-division facts, e.g. a new
    grant added under an earlier-sorting scheme letter), and a drifted id here silently
    reopened the exact "reviewed forever" loop this file exists to close. Each row's
    cluster_id is now resolved via resolve_cluster_id() -- raises StaleClusterIdError
    instead."""
    if not _MANUAL_CONFIRMED_NOT_SUSPICIOUS_CSV.exists():
        return set()
    with open(_MANUAL_CONFIRMED_NOT_SUSPICIOUS_CSV, newline="") as f:
        return {
            resolve_cluster_id(row["cluster_id"].strip(), clusters)
            for row in csv.DictReader(f) if row["cluster_id"].strip()
        }


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
    A cluster_id listed in data_persisted/manual_confirmed_not_suspicious.csv is always RESOLVED
    regardless of is_suspicious_for2020's verdict -- see _load_confirmed_not_suspicious() -- but
    MULTI_ORCID still overrides even that, since a live ORCID conflict is a hard data fact, not
    a heuristic false positive a human can pre-clear.

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
    confirmed_not_suspicious = _load_confirmed_not_suspicious(clusters)

    for c in clusters:
        suspicious = is_suspicious_for2020(c.full_name_key, c.for2020_codes, tf_lookup, c.n_grants)
        if suspicious and c.cluster_id in confirmed_not_suspicious:
            suspicious = False
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


# ── population-level construction: build_arc_only_population, enrich_with_oax_candidates,
#    persistence ─────────────────────────────────────────────────────────────────────────
#
# 2026-08-21: split from a single build_awards_cif_population() after a real architectural
# bug was found -- that one function bundled two genuinely different concerns (ARC/ORCID-only
# identity resolution, and OAX-candidate population that reads arc_oax_links.parquet, i.e.
# 03_link_arc_oax.py's OWN output) into one call, so "01_prepare_arc.py" (meant to produce
# checkable, OAX-independent output) was silently depending on 03 having already run. The two
# halves below are genuinely sequential (build_arc_only_population's output is 03's input;
# enrich_with_oax_candidates needs 03's output), never bundled again. See CLAUDE.md's
# "Bind them together..." session notes for the full incident.

AWARDS_CIF_PARQUET = PROCESSED_DATA / "awards_cif.parquet"
ARC_ONLY_PARQUET = PROCESSED_DATA / "awards_cif_arc_only.parquet"


def build_arc_only_population(
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[AwardsCIF]:
    """The genuinely ARC/ORCID-only half of identity resolution -- no OpenAlex data read of its
    own. This is what 01_prepare_arc.py calls and persists to awards_cif_arc_only.parquet:
    a checkable, inspectable checkpoint that exists and is complete BEFORE any connection to
    OAX is made (03_link_arc_oax.py reads this file, never the OAX-enriched one, closing off
    the circularity that motivated this split).

    load_award_cif_items -> cluster_items -> refine_clusters -> set_aside_indigenous_research
    -> compute_orcid_for -> compute_coawardees -> widen_names_with_orcid_bulk_db
    -> compute_gap_candidates -> compute_reliability

    compute_orcid_for() reads only the local ORCID diskcache (00b_enrich_orcid.py's own output,
    not OAX); widen_names_with_orcid_bulk_db() reads a separate local ORCID bulk snapshot (see
    its own docstring) -- neither is OpenAlex/OAX data, so "no OpenAlex data read of its own"
    still holds. compute_gap_candidates()/compute_reliability() operate purely on fields already
    populated by the steps above them (family_names, for2020_codes, orcids) -- confirmed by
    direct code read, not assumed, before this split was made. load_award_cif_items()'s own name
    widening (diacritic_variants()) is per-name and local too (2026-08-26) -- no longer reads any
    OAX-derived corpus table, so "no OpenAlex data read of its own" is unconditionally true now,
    not just true when the caller happens to pass nothing in.
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
        clusters = compute_orcid_for(clusters)
        clusters = compute_coawardees(clusters, items)
        clusters = widen_names_with_orcid_bulk_db(clusters)
        clusters = compute_gap_candidates(clusters)
        clusters = compute_reliability(clusters)
    finally:
        if own_con:
            con.close()
    return clusters


def enrich_with_oax_candidates(
    clusters: list[AwardsCIF],
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[AwardsCIF]:
    """The genuinely OAX-dependent half: populates oax_candidates by reading
    arc_oax_links.parquet (03_link_arc_oax.py's own output) and cleans it up. Must run AFTER
    03_link_arc_oax.py, on the ARC-only population build_arc_only_population() produced (either
    freshly built, or reloaded via load_awards_cif(ARC_ONLY_PARQUET) -- both populate_oax_candidates
    and dedup_oax_candidates only ever touch cluster_id/family_names/oax_candidates/provenance,
    all of which round-trip correctly through persist/load, so a loaded, items-less population is
    a valid input here).

    populate_oax_candidates -> dedup_oax_candidates
    """
    clusters = populate_oax_candidates(clusters, con)
    clusters = dedup_oax_candidates(clusters, con)
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
        "coawardees": c.coawardees,
        "full_name_key": c.full_name_key,
        "family_name_main": c.family_name_main,
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
            coawardees=[dict(x) for x in row["coawardees"]],
            full_name_key=row["full_name_key"],
            family_name_main=row["family_name_main"],
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
