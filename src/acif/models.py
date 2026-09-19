"""
Core data model for the ARC-side identity build (AwardCIFItem, AwardsCIF).

Moved out of src/utils/awards_cif.py per the implementation-organization section of
/home/lc/.claude/plans/plan-that-in-tiny-immutable-heron.md: this is the project's central
domain model, not a "utility" -- src/utils/ stays for genuine stateless helpers (name parsing,
FOR-code resolution, ORCID lookups), src/acif/ holds the model and the (not-yet-built) cyclic
construction engine that operates on it.

Field-for-field copy of the current src/utils/awards_cif.py dataclasses, plus the new fields the
plan requires (marked below), minus all methods -- deliberately no behaviour yet
(record_event()/the `works` property will be added in a later step, once the cyclic build's own
provenance/query needs are settled, not carried over unchanged just because they existed before).

src/utils/awards_cif.py is NOT yet modified or retired -- this module exists alongside it until
the new cyclic engine (src/acif/build.py) is built and validated against the 10-different-
starting-sorts equivalence check the plan calls for. Retirement of the superseded functions
there (cluster_items(), refine_clusters(), compute_gap_candidates(),
merge_by_coawardee_corroboration()) happens after that, not now.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from src.utils.names import ParsedName


@dataclass(frozen=True)
class AwardCIFItem:
    """One row per (grant x ARC CI/Fellow) -- the atomic unit AwardsCIF clusters.

    Raw ARC facts plus the normalized/derived name forms the identity build needs
    (family_name_main, first_initial, first_name_canonical, full_name_key, for_name_tokens),
    computed once at load time -- not left to be recomputed lazily on every access.
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
    for_code: str | None  # ANZSRC code for this grant, upgraded to 2020 series -- primary only

    # derived, normalized forms
    full_name: str
    first_names: list[str] = field(default_factory=list)
    family_names: list[str] = field(default_factory=list)
    family_name_main: str | None = None
    first_initial: str | None = None
    first_name_canonical: str | None = None
    full_name_key: str | None = None
    for_name_tokens: list[str] = field(default_factory=list)

    # the name parser's own full output for this item's raw name, carried whole
    parsed: ParsedName | None = None

    # every field-of-research entry ARC recorded for this grant, each resolved to a FOR2020
    # 4-digit group. One dict per entry: {"code": "3705", "name": "Geology",
    # "is_primary": True, "confidence": 1.0}, ordered primary-first then alphabetically.
    for2020_codes: list[dict] = field(default_factory=list)

    # HEP codes for every HEP-eligible organisation formally on this grant, not just
    # admin_org's own HEP.
    hep_codes: list[str] = field(default_factory=list)

    # this grant's own set of OAX institution ids -- current admin_org UNION
    # announcement_admin_org UNION eligible_orgs.
    inst_ids: list[str] = field(default_factory=list)

    # ARC's own raw isFellowship flag for THIS person on THIS grant.
    is_fellowship: bool = False

    # --- new fields required by the cyclic-build plan (2026-09-19), not yet populated by
    # anything -- see the plan's "Full AwardCIFItem property list" section for each one's
    # rationale. ---

    # Population frequency of this item's own grant's for_name signature, computed once per
    # GRANT (not per item -- counting per item would inflate a for_name's frequency by however
    # many investigators share that grant, a real bug caught 2026-09-19). ARC-population-computed
    # (not OpenAlex-population -- that table serves is_suspicious_for2020()'s unrelated rare-name
    # gate). None until 00c_extract_propensities.py's table is built and wired in by features.py.
    for_name_rarity: float | None = None

    # Whether this item's grant has n_eligible_orgs == 1 -- an ATTRIBUTION flag only ("can we say
    # THIS investigator is unambiguously at THIS institution"), unrelated to institution_rarity
    # below (corrected 2026-09-19 -- an earlier draft wrongly gated the rarity table on this flag).
    single_institution_grant: bool = False

    # Population frequency of institution_oax_id, computed ungated across every eligible_orgs
    # entry on every ARC award (once per grant) -- NOT restricted to single_institution_grant
    # items. Gating on n_eligible_orgs==1 would systematically undercount (or, for an institution
    # that never appears as sole admin org, entirely exclude) institutions that mostly appear as
    # a co-eligible partner org rather than sole admin -- a real bias, not a simplification.
    institution_rarity: float | None = None


@dataclass
class CandidateWork:
    """One OpenAlex work reached by any of an AwardsCIF's oax_candidates. Unchanged copy of
    src/utils/awards_cif.py's own definition -- oeuvre-building is a separate, later concern
    from ACIF identity-building, kept here only because AwardsCIF.oeuvre references the type."""

    work_idx: int
    source_author_idxs: list[int] = field(default_factory=list)

    publication_year: int | None = None
    cited_by_count: int = 0
    type: str | None = None
    title: str | None = None
    doi: str | None = None
    source_id: int | None = None

    subfield_idx: int | None = None
    subfield_name: str | None = None
    field_name: str | None = None
    domain_name: str | None = None

    own_institution_idxs: list[int] = field(default_factory=list)
    coauthor_author_idxs: list[int] = field(default_factory=list)
    coauthor_names: list[str] = field(default_factory=list)
    coauthor_institution_idxs: list[int] = field(default_factory=list)

    included: bool = True
    exclusion_reason: str | None = None
    signals: dict = field(default_factory=dict)


@dataclass
class AwardsCIF:
    """A provisional grouping of AwardCIFItems under evidence they are the same real ARC
    CI/Fellow. Fields mirror arc_persons.parquet's real output columns.

    No methods yet, deliberately -- record_event()/the `works` property from the current
    src/utils/awards_cif.py version are not carried over unchanged; they'll be added once the
    cyclic build's own needs are settled, in a later step.
    """

    cluster_id: str  # = arc_id. Computed by the year/scheme/remainder tie-break rule (see
                      # plan), NOT plain min(unique_id) -- that sorts by scheme-prefix letter
                      # first and would let a newer DE... grant outrank an older DP... grant.
    items: list[AwardCIFItem] = field(default_factory=list)

    # aggregated across items -- sorted deduplicated lists. NOT @property (2026-09-19 --
    # checked against the real src/utils/awards_cif.py usage, not assumed): both full_names
    # (raw name strings) and full_name_keys (given/nickname x family key set) are set once at
    # construction as a union over items (_build_awards_cif(): full_names=sorted({it.full_name
    # for it in items}), full_name_keys=sorted({k for it in items if it.parsed for k in
    # it.parsed.full_name_keys})) -- but THEN, separately, widen_names_with_orcid_bulk_db()
    # unions in MORE names/keys straight from an ORCID lookup result that has nothing to do
    # with `items` at all (c.full_names = sorted(set(c.full_names) | new_full), same for
    # full_name_keys). A @property computed purely from self.items cannot reproduce that --
    # ORCID (and later OAX) names are fetched externally and stored, not derived. So these stay
    # genuinely mutable stored fields, seeded from items, widened in place afterward -- there is
    # no separate "full_name_raws" name needed at this level, full_names already IS that list.
    full_names: list[str] = field(default_factory=list)
    first_names: list[str] = field(default_factory=list)
    family_names: list[str] = field(default_factory=list)
    orcids: list[str] = field(default_factory=list)
    inst_arr: list[str] = field(default_factory=list)
    for_names: list[str] = field(default_factory=list)
    for_codes: list[str] = field(default_factory=list)
    full_name_key: str | None = None  # modal full_name_key across items
    full_name_keys: list[str] = field(default_factory=list)
    family_name_main: str | None = None  # modal family_name_main across items

    hep_codes: list[str] = field(default_factory=list)
    for2020_codes: list[dict] = field(default_factory=list)

    grant_ids: list[str] = field(default_factory=list)  # = [item.unique_id for item in items]
    n_grants: int = 0

    coawardees: list[dict] = field(default_factory=list)

    orcid_status: str = "NO_ORCID"  # HAS_ORCID | NO_ORCID | MULTI_ORCID
    orcid_for_codes: list[dict] = field(default_factory=list)
    gap_candidates: list[str] = field(default_factory=list)  # other cluster_ids not ruled out
    reliability_tier: str | None = None  # 1a | 1b | 1c | 2 | 3 | 4 | 4u
    resolution_status: str = "UNRESOLVED"  # RESOLVED | UNRESOLVED

    oax_candidates: list[str] = field(default_factory=list)

    excluded: bool = False
    excluded_reason: str | None = None

    oeuvre: list[CandidateWork] = field(default_factory=list)

    provenance: list[dict] = field(default_factory=list)

    # --- new field required by the cyclic-build plan (2026-09-19) ---

    # Every cycle_stage this ACIF was live at, merged or not -- appended each round to the
    # SAME persisting object (merge-into-pre-existing, not re-minted), so an untouched
    # singleton legitimately carries e.g. [1, 2] at the end of stage 2. Answers "was this
    # reconsidered and found nothing, or never reached" -- the explicit point of adding a
    # stage index at all.
    cycle_stages: list[int] = field(default_factory=list)
