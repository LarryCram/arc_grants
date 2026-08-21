"""
Cluster quality checks shared between 01_prepare_arc.py, 01a_diagnose.py, and
src/utils/awards_cif.py.

Division-mismatch checking was rebuilt 2026-08-13 around FOR2020 codes resolved via
research_classification's Resolver() (see src/utils/for_resolve.py), replacing the old
for_names + data_persisted/for_divisions.csv/for_adjacent_divisions.csv route. Two problems
drove the replacement, both found empirically, not suspected in advance:
  1. for_divisions.csv's for_name-keyed lookup silently failed on casing alone for ~79% of
     clusters (ARC's own raw for_name field is sentence-case, e.g. "Health services and
     systems"; the CSV was title-case, "Health Services and Systems" -- built in an earlier,
     separately-curated era predating the Resolver() migration). A failed lookup just drops
     that for_name from consideration rather than erroring, so the old is_suspicious() check
     had been silently finding ZERO cross-division mismatches across the entire ARC population
     (verified directly: old=0 flagged, new=179, on the same 22,940-cluster population) --
     not because researchers rarely span multiple divisions (they clearly do), but because the
     check itself never actually looked at real division data for most clusters.
  2. Even where the CSV's own division letters (A-W, 23 total) DID resolve, they don't
     correspond 1:1 to FOR2020's own numeric divisions (~22 total) at all -- checked directly:
     10/23 letters each spanned >=2 different FOR2020 divisions, and 8 FOR2020 divisions each
     spanned >=2 different letters. The letter scheme looks like a leftover from an earlier,
     separately-curated classification era (predating the Resolver()-backed package this
     project now uses throughout for FOR-code work), not an alias for FOR2020's own structure.

The replacement uses OAX_FIELD (OpenAlex's own ~25-category content-derived field taxonomy),
not ANZSRC's own ~22-category administrative division -- because ANZSRC's division boundaries
split disciplines that plainly belong together for this check's purpose: "Legal systems"
(division 48) and "Political science" (division 44) are ANZSRC-administratively distinct but
both resolve to OAX field "Social Sciences" (same for "Ecology"/"Forestry sciences", both
"Agricultural and Biological Sciences" despite different ANZSRC divisions).

ACCEPTABLE_DIVISION_PAIRS (added 2026-08-16) is a second, narrower tolerance on top of OAX_FIELD's
own coarsening, for pairs that resolve to genuinely DIFFERENT OAX fields but are still common,
legitimate combinations for one real person -- discovered investigating a confirmed false-merge
(DE230100180_WeiWang: a 2023 DECRA fire-safety-materials engineer at UNSW/RMIT, PhD 2019, merged
by Splink's ARC-internal dedupe_only run with an unrelated 6-grant 2008-2021 UNSW information-
systems CI and two further unrelated people at Curtin/UQ -- all sharing the literal, byte-identical
name "Wei Wang", so Splink's name-comparison features had zero discriminating power and the merge
decision rested entirely on institution/FOR-token overlap). The earlier "no adjacency tolerance...
inventing new judgment calls, not reusing validated ones" stance was reconsidered once the user
flagged a concrete, real mechanism for legitimate cross-division ARC applications this check had
no way to distinguish from a false merge: applicants sometimes deliberately spread proposals
across different FOR divisions (e.g. PHYS/ENG, CHEM/ENG, MATH/IT) specifically to be assessed by
different panels -- not hypothetical, a named tactic. Rather than guess which pairs matter, the
list is empirically derived, not hand-curated: computed from every ORCID-confirmed (single ORCID,
present on 100% of the cluster's own grant records -- the only subset where every record's own
identity is independently verified, not merely a Splink co-clustering decision) multi-grant,
non-excluded cluster's own primary-FOR2020-division pairs, tested against a null model of "two
people, each independently drawn from the population's own single-division base rate, happened
to be merged" (z = (observed - expected) / sqrt(expected), expected = 2 * p(A) * p(B) *
n_confirmed_clusters -- the factor of 2 because either division could be "person 1" or "person 2").
Threshold z >= 3.4 approximates a Bonferroni correction for the ~171 possible division pairs
tested at a 0.05 family-wise rate. All three of the user's named example pairs came out clearly
above threshold (CHEM/ENG z=9.25, ENG/PHYS z=6.51, IT/MATH z=5.98), independently validating the
method against real domain knowledge; the pair implicated in the Wei Wang case itself (ENG/IT,
divisions 40/46) did NOT clear the threshold -- i.e. this tolerance list does not undermine the
very case that motivated building it. First attempt at this computation accidentally included the
107 Indigenous-focused AwardsCIF that set_aside_indigenous_research() marks excluded=True but
deliberately leaves in the returned list (callers must filter on .excluded themselves, per that
function's own docstring) -- corrected by restricting both the confirmed-cluster population and
the base-rate population to non-excluded grants; division 45 (Indigenous Studies) is absent from
every pair as a direct result of that correction, as it should be -- Indigenous-primary AwardsCIF
are out of the working population entirely.

This makes the check somewhat stricter than full ANZSRC-division-adjacency would be on a few
genuinely-coherent multi-field careers still not covered by either OAX_FIELD's coarsening or
ACCEPTABLE_DIVISION_PAIRS (e.g. psychology + linguistics, still 2 different OAX fields, no
division pair for it clears the empirical threshold) -- an accepted, explicit tradeoff, not an
oversight.

Re-derivation attempted 2026-08-17, ultimately NOT adopted -- reverted back to the original 41
pairs below after the attempt surfaced a real regression risk and failed to converge, documented
here in full since the investigation itself surfaced genuine, useful findings even though the
whitelist itself wasn't changed. Two real, separate problems were found and are already fixed
independently of the whitelist:
  1. for2020_all_fields()/for2020_all_subfields() (this module, added 2026-08-17) became the
     production functions everywhere a FOR2020 code set drives a topic/field decision -- every
     code ARC recorded is real evidence of a CI's declared discipline mix, and is_primary marks
     emphasis, not exclusivity, so restricting to primary-only throughout the codebase was
     discarding real signal. division_mismatch_for2020() switched from for2020_primary_fields()
     to for2020_all_fields() as part of that same change -- this part IS in production. But the
     41-pair whitelist below was calibrated against PRIMARY-only division data, so it's now being
     tested against a broader all-codes division set it was never calibrated for (measured: 73.8%
     of ACIFs show exactly one division under primary-only vs. 34.2% under all-codes -- most
     people's grants carry several FOR codes, and once every code counts, a person doesn't need
     many grants before their union spans multiple divisions almost automatically). This mismatch
     is real and unresolved -- see "not yet fixed" below.
  2. Division 45 (Indigenous Studies) leakage: 1,124 of 1,228 AwardsCIF carrying a division-45 code
     have it as non-primary only -- a real, understood artifact of the FOR2008/RFCD98-to-FOR2020
     upgrade preserving the OLD scheme's primary/non-primary labelling (which routinely put a
     substantive discipline primary and Indigenous-relatedness secondary) even though FOR2020's
     own convention would have made Indigenous-focus primary for that research. This IS fixed, at
     the source: set_aside_indigenous_research() (src/utils/awards_cif.py) now strips non-primary
     division-45 codes from the AwardsCIF it keeps in the working population, so 45 no longer
     leaks into any downstream all-codes division computation, whitelist re-derivation included.

The re-derivation attempt itself (same core methodology as the original: z-score against a null
model of "two people, each independently drawn from the population's own single-division base
rate, happened to be merged", now against all-codes divisions, with an added lift requirement
(observed/expected >= 2.0) since large sample sizes make even a trivial ~20% relative elevation
look statistically significant by z-score alone) went through three iterations, each surfacing a
new problem rather than converging:
  - First pass (all-codes, division-45 already fixed, no lift): 84 pairs (primary-only) / 120
    pairs (all-codes) -- already 2-3x the original 41, unexplained.
  - Added lift >= 2.0: barely moved the count (84 -> 83 primary-only) -- ruled out lift/propensity
    as the main explanation for the gap.
  - Testing the resulting 99-pair (all-codes + lift) list against the full test suite caught a
    real regression: it whitelisted (40, 46) -- the exact Engineering/Information-Computing-
    Sciences division pair from the confirmed DE230100180_WeiWang false-merge case that
    ACCEPTABLE_DIVISION_PAIRS was originally built to NOT cover (see below). Root cause: the
    "confirmed cluster" population used orcid_status=='HAS_ORCID', which only requires no
    CONFLICTING orcid among a cluster's records that happen to have one -- not orcid present on
    100% of records, as the original derivation's stated methodology requires. DE230100180_WeiWang
    itself (9 merged grant records, only 1 carries an ORCID -- confirmed directly, has never
    actually been split, see CLAUDE.md) satisfies the loose definition and was sitting inside the
    "ground truth" population, directly inflating the observed count for the exact pair it falsely
    merges. Rebuilding the confirmed population with a strict 100%-orcid-coverage check (via
    investigators_raw.parquet, not the aggregated orcid_status field) correctly excludes Wei Wang
    and correctly drops (40,46) back below the lift threshold (lift 1.96, just under 2.0) -- but
    the overall pair count under this corrected population is STILL 86-92 pairs, not 41. Fixing
    the population-contamination bug did not converge toward the original number, it just landed
    on a different wrong number -- strong evidence some other, still-unidentified methodological
    difference separates this re-derivation from the original.

Decision: NOT adopted. The original derivation script was never persisted, only its resulting
41-pair list was committed, so there's no way to diff this re-derivation against the original
methodology directly, and three separate attempts each surfaced a new problem rather than
converging -- continuing to iterate risked shipping a worse whitelist than the original under time
pressure, for a component whose only failure mode (see below) is under-flagging genuine problems
for manual review. The original 41 pairs remain in force. Revisiting this needs a slower, more
deliberate pass -- ideally starting from real, named case validation (the way the original 3
example pairs, and Wei Wang as a negative control, were validated) rather than population
statistics alone, which is what let the Wei Wang regression through undetected until the test
suite caught it.

Scope of impact if this ever does get revisited: ACCEPTABLE_DIVISION_PAIRS only feeds
division_mismatch_for2020() -> is_suspicious_for2020() -> AwardsCIF.resolution_status, not Stage
1/3, piling, or the working oeuvre data itself -- getting it wrong under-flags clusters for manual
review, it doesn't silently corrupt anything downstream. Current population effect, unchanged
(41-pair list, but now applied against all-codes divisions after fix #1 above): resolution_status
UNRESOLVED = 731 / 22,814 non-excluded AwardsCIF, up from the historical baseline documented
elsewhere in this file's history precisely because of the primary-vs-all-codes mismatch in point 1
above -- a known, accepted over-flagging (more manual review burden, not missed problems) until a
properly-validated re-derivation replaces it.

Separately, found while tracing DP230101204_MohammadIslam,
externally ORCID-confirmed as correctly resolved by this pipeline's institution/HEP evidence): a
real gap in division_mismatch_for2020()'s own logic, independent of whitelist contents entirely.
Islam's two FOR codes ("Materials engineering", primary; "Numerical modelling and mechanical
characterisation") both sit in ANZSRC division 40 (Engineering) -- only one division, ever. But
they resolve to two different OAX fields ("Materials Science" and "Engineering"), so the function's
first gate (len(fields) <= 1: return False) doesn't fire, and it falls through to the division-pair
check -- which, with only one division present, can never form a pair to test against any
whitelist, so it unconditionally returns True (mismatch). Confirmed directly: substituting an
all-inclusive whitelist (every possible division pair) into division_mismatch_for2020() for Islam's
real codes still returns True -- the whitelist's contents are provably irrelevant to this failure
mode. A legitimate single-division researcher can be wrongly flagged whenever OAX_FIELD splits
their one ANZSRC division into 2+ fields, and no whitelist size fixes it. Left unfixed (2026-08-17)
-- the correct fix is in the function's own gating logic (recognize "only one division present" as
"nothing to check" and return False before ever reaching the pairwise-whitelist branch), not here.
"""
from collections import Counter
from typing import Iterable

from src.utils.for_resolve import oax_field_name, oax_subfield_name

RARE_NAME_TF = 1e-5  # 2026-08-21 recalibration: was 5e-5 (~p99.995 of the AU-context full_name_key
# distribution, only 110/2,045,346 names qualified as "common") -- empirically far stricter than
# where the real collision-prone tail starts. Measured against the actual distribution: median tf
# is 3.6e-7 (over half of all distinct names occur exactly once in the 2.78M-author AU/HEP-context
# population), p99=2.5e-6, p99.9=1.1e-5. The old threshold missed documented real collision cases
# in this project's own data (Andrew Martin tf=1.5e-5, Xiaolin Wang tf=1.7e-5, Jun Li tf=3.4e-5,
# Paul Thomas tf=6.1e-6, Mark Baker tf=2.9e-6 -- all below 5e-5 despite being real, multi-cluster
# same-name collisions). New value (~p99.9) reclassifies 2,414 names as "common" (up from 110);
# measured population effect on ARC clusters: 11 -> 169 flagged suspicious, dominated by
# genuinely common Chinese/Vietnamese/Korean given+family combinations (Wei Wei, Yan Yan, Wei
# Zhang x2, Wei Liu x2, Yang Liu x2 ...) -- a reviewable, well-targeted set, not a false-positive
# flood. Also see is_suspicious_for2020()'s docstring for the separate "missing from tf_lookup"
# default fix (also 2026-08-21) -- that fix and this recalibration are independent, complementary
# corrections to the same rare/common signal.

INDIGENOUS_DIVISION_PREFIX = "45"  # FOR2020 division 45 "Indigenous Studies" -- confirmed
                                    # directly against real ARC grant records (e.g. group 4501
                                    # "Aboriginal and Torres Strait Islander Culture, Language
                                    # and History", 4513 "Pacific Peoples Culture, Language and
                                    # History"), not assumed from the package's own doc comments.

# Empirically-derived (2026-08-16, see module docstring for full derivation) 2-digit FOR2020
# division pairs that co-occur far more often than chance in ORCID-confirmed single-person
# clusters -- legitimate multi-division careers/assessor-panel-targeting, not false merges.
# z >= 3.4 (~ Bonferroni-corrected for ~171 possible pairs at 0.05 family-wise). Frozenset of
# frozensets so lookup is order-independent: frozenset({a, b}) in ACCEPTABLE_DIVISION_PAIRS.
# A 2026-08-17 re-derivation attempt (against all-codes divisions, with a lift correction) was
# NOT adopted -- see module docstring for why (it surfaced a real regression, whitelisting the
# (40,46) pair from the confirmed Wei Wang false-merge case, and never converged on the original
# count even after fixing that). This 41-pair list, calibrated on primary-only division data, is
# still what's in force -- now applied against all-codes divisions since division_mismatch_for2020()
# itself moved to for2020_all_fields() (2026-08-17), a known, accepted mismatch (over-flags for
# manual review, documented in the module docstring) until a properly-validated replacement exists.
ACCEPTABLE_DIVISION_PAIRS: frozenset[frozenset[str]] = frozenset(
    frozenset(pair) for pair in [
        ("30", "31"), ("30", "32"), ("30", "34"), ("30", "41"),
        ("31", "32"), ("31", "34"), ("31", "41"),
        ("32", "34"), ("32", "42"), ("32", "52"),
        ("33", "36"), ("33", "42"), ("33", "43"), ("33", "44"),
        ("34", "40"), ("34", "51"),
        ("35", "38"), ("35", "42"), ("35", "44"), ("35", "52"),
        ("36", "43"), ("36", "44"), ("36", "47"),
        ("37", "41"), ("37", "43"),
        ("39", "42"), ("39", "44"), ("39", "47"), ("39", "52"),
        ("40", "51"),
        ("42", "44"), ("42", "47"), ("42", "52"),
        ("43", "44"), ("43", "47"), ("43", "50"),
        ("44", "47"), ("44", "48"), ("44", "50"),
        ("46", "49"),
        ("48", "50"),
    ]
)


def aggregate_for2020_codes(per_record_codes: Iterable[list[dict]]) -> list[dict]:
    """Union a person's for2020_codes across every one of their (grant) records into one
    deduped, ordered list -- shared by src/utils/awards_cif.py (an iterable of
    AwardCIFItem.for2020_codes) and 01_prepare_arc.py (a pandas Series of per-row lists), so
    the aggregation logic can't drift between the two pipelines that both need it.

    Deduped by code: is_primary = True if primary on >=1 record, confidence = max seen.
    Ordered by weight = (records where primary, descending), then (records where it appeared
    at all, descending), then alphabetically -- see AwardsCIF.for2020_codes' docstring for why
    this is a person-level extension of the per-grant primary-first-then-alpha ordering, not
    something ARC's data states directly."""
    agg: dict[str, dict] = {}
    n_primary: Counter = Counter()
    n_seen: Counter = Counter()

    for codes in per_record_codes:
        if not codes:
            continue
        for entry in codes:
            code = entry["code"]
            n_seen[code] += 1
            if entry["is_primary"]:
                n_primary[code] += 1
            existing = agg.get(code)
            if existing is None:
                agg[code] = dict(entry)
            else:
                existing["is_primary"] = existing["is_primary"] or entry["is_primary"]
                existing["confidence"] = max(existing["confidence"], entry["confidence"])

    return sorted(
        agg.values(),
        key=lambda e: (-n_primary[e["code"]], -n_seen[e["code"]], e["name"].lower()),
    )


def is_case_a(full_names: list) -> bool:
    """Single distinct first-name token across all name forms in the cluster."""
    firsts = set()
    for name in full_names:
        parts = name.strip().split()
        if parts:
            firsts.add(parts[0].lower())
    return len(firsts) <= 1


def first_names_compatible(names_s: list, names_t: list) -> bool:
    """True if at least one token in T is compatible with some token in S.

    Cascade (per token in T):
      - full name: exact match in S's full names, OR initial matches S's initials
      - initial:   matches S's initials (derived from S's full names + S's bare initials)
    Applied symmetrically by caller: incompatible if either direction returns False.
    """
    s_full  = {n for n in names_s if len(n) > 1}
    s_inits = {n for n in names_s if len(n) == 1} | {n[0] for n in s_full}
    for t in names_t:
        if len(t) > 1:
            if t in s_full or t[0] in s_inits:
                return True
        else:
            if t in s_inits:
                return True
    return False


def for2020_primary_fields(codes: list[dict]) -> set[str]:
    """OAX FIELD names (via for_resolve.oax_field_name()) implied by the PRIMARY entries in a
    for2020_codes list (AwardCIFItem/AwardsCIF.for2020_codes, or the equivalent column built by
    01_prepare_arc.py's Phase 1) -- primary-only to stay semantically close to the old
    for_names/for_code (ARC's single declared primary per grant), even though for2020_codes
    itself also carries secondary codes for other purposes (e.g.
    src/utils/oeuvre_build.py::apply_subfield_filter(), which does use the full list).

    Superseded in production by for2020_all_fields() as of 2026-08-17 (see its docstring) --
    kept here, unchanged, only for whichever caller genuinely wants strictly-primary semantics
    and for the existing tests asserting that behavior. No production call site uses this
    function anymore; check before assuming otherwise."""
    fields = {oax_field_name(e["code"]) for e in codes if e.get("is_primary") and e.get("code")}
    return fields - {None}


def for2020_primary_subfields(codes: list[dict]) -> set[str]:
    """OAX SUBFIELD names (via for_resolve.oax_subfield_name()) implied by the PRIMARY entries in
    a for2020_codes list -- same shape as for2020_primary_fields(), one level finer-grained
    (~252 OAX subfields vs. ~25 OAX fields). Added 2026-08-16 for the per-candidate keep/drop rule
    (src/utils/oeuvre_build.py): the field-level check alone (Stage 3) can't distinguish "same
    broad field" from "same specific research area" -- two different real people both working in
    "Social Sciences" pass the field check easily, but rarely share a subfield. Not a new
    resolver: oax_subfield_name() already exists and is already used in production
    (04_resolve_links.py's _field_score).

    Superseded in production by for2020_all_subfields() as of 2026-08-17 -- see
    for2020_primary_fields()'s docstring for why this is kept but no longer called in production."""
    subfields = {oax_subfield_name(e["code"]) for e in codes if e.get("is_primary") and e.get("code")}
    return subfields - {None}


def for2020_all_fields(codes: list[dict]) -> set[str]:
    """OAX FIELD names implied by EVERY entry in a for2020_codes list, not just the primary one --
    the production function as of 2026-08-17: every FOR2020 code ARC recorded for a grant is real
    evidence of the CI's own declared discipline mix, and `is_primary` marks emphasis, not
    exclusivity, so restricting to primary-only (the original for2020_primary_fields() behavior)
    discarded real signal throughout the pipeline, not just in the pile-to-ACIF channeling use
    case this was first built for. for2020_primary_fields() is kept, unchanged, for whichever
    callers still want strictly-primary semantics (currently none in production -- see its own
    docstring/tests), but this is what Stage 3's field filter, is_suspicious_for2020()/
    division_mismatch_for2020(), and 01_prepare_arc.py's report labelling all call now."""
    fields = {oax_field_name(e["code"]) for e in codes if e.get("code")}
    return fields - {None}


def for2020_all_subfields(codes: list[dict]) -> set[str]:
    """Same as for2020_all_fields(), one level finer-grained -- see its docstring for why this
    (not for2020_primary_subfields()) is the production function as of 2026-08-17."""
    subfields = {oax_subfield_name(e["code"]) for e in codes if e.get("code")}
    return subfields - {None}


def division_mismatch_for2020(codes: list[dict]) -> bool:
    """True if a single cluster's own primary FOR2020 codes span more than one OAX field --
    self-consistency check, replaces the old division_mismatch(for_names, div_map, adj) call
    inside is_suspicious(). See module docstring for why OAX_FIELD, and for
    ACCEPTABLE_DIVISION_PAIRS, the empirically-derived exemption applied below.

    A cluster spanning 2+ OAX fields is NOT flagged if EVERY pairwise combination of the
    underlying 2-digit FOR2020 divisions behind those fields is a whitelisted pair -- e.g. a
    prolific researcher spanning divisions {30, 31, 41} (Agricultural/Biological/Environmental
    Sciences) passes since (30,31), (30,41), and (31,41) are each individually whitelisted.
    Widened 2026-08-16 from an exactly-2-division-only version, which was flagging real, single,
    highly prolific researchers (e.g. a 27-grant plant ecophysiologist spanning exactly this kind
    of 3-4-division agricultural/biological/environmental spread) purely for having a broad but
    entirely legitimate research career, not for any actual identity concern.

    Known residual risk of the all-pairwise-whitelisted approach (flagged by the user, not
    hypothetical): this can't distinguish one real person whose own career spans several
    whitelisted-pairwise divisions from two DIFFERENT people wrongly merged whose individual
    division-sets happen to combine into a union that is still all-pairwise-whitelisted (e.g.
    person A in {30,31}, person B in {31,41} -- if merged, the union {30,31,41} passes this check
    even though it's two people). This function's exemption only protects against one specific
    false-positive pattern (a genuine multi-field researcher); a high grant count is not itself
    evidence of one person either (a common name can accumulate many grants from several real
    people just as easily as one prolific one) -- this check does not replace the rare-name gate,
    ORCID-based checks, or manual review as ways of actually catching a wrongful merge.

    Widened again 2026-08-17: `fields` and `divisions` now come from EVERY FOR2020 code on a
    grant, not just the primary one -- every code is real, ARC-recorded evidence of the CI's own
    declared research area, and primary only marks emphasis, not exclusivity, so restricting to
    primary-only was discarding real signal throughout the codebase, not just here. Known
    consequence, not yet acted on: `ACCEPTABLE_DIVISION_PAIRS` was empirically derived (z-score
    method, see module docstring) against PRIMARY-only division data -- broadening the division
    set this function now tests means more clusters will show 3+ divisions than the whitelist was
    calibrated against, so some genuine multi-field researchers may now get flagged again until
    the whitelist itself is re-derived against the all-codes division definition."""
    fields = for2020_all_fields(codes)
    if len(fields) <= 1:
        return False
    divisions = sorted({e["code"][:2] for e in codes if e.get("code")})
    # 2026-08-21 fix: a single ANZSRC division is administratively self-consistent even when
    # OAX's finer, content-based crosswalk splits it into 2+ OAX fields (confirmed real,
    # e.g. Mohammad Islam's "Materials engineering"/"Numerical modelling" both in division 40
    # but resolving to different OAX fields; Timothy Connallon's single division 31 similarly
    # flagged before this fix). The old `return True` here treated "can't form a division pair
    # to test" as "therefore suspicious" -- backwards from the intent documented above; no
    # pair to test means nothing here contradicts a single coherent division, not evidence of
    # one. There is no whitelist to consult when only one division is present.
    if len(divisions) < 2:
        return False
    all_pairs_ok = all(
        frozenset((divisions[i], divisions[j])) in ACCEPTABLE_DIVISION_PAIRS
        for i in range(len(divisions)) for j in range(i + 1, len(divisions))
    )
    return not all_pairs_ok


def division_mismatch_for2020_pairwise(codes1: list[dict], codes2: list[dict]) -> bool:
    """True if two clusters' own primary FOR2020 codes' OAX fields share nothing in common --
    pairwise check, replaces the old division_mismatch(c1_names + c2_names, div_map, adj) call
    in gap-candidate detection. No mismatch is ever flagged if either side has no primary field
    to compare, matching the old "no mapped reference, don't penalize" behaviour."""
    d1 = for2020_all_fields(codes1)
    d2 = for2020_all_fields(codes2)
    if not d1 or not d2:
        return False
    return not (d1 & d2)


def is_suspicious_for2020(full_name_key: str | None, for2020_codes: list[dict], tf_lookup: dict) -> bool:
    """True if cluster warrants manual review: common name, cross-OAX-field FOR (not exempted by
    ACCEPTABLE_DIVISION_PAIRS).

    The "any ORCID present -> never suspicious" bypass this function used to have (unconditional,
    regardless of how many of the cluster's own grant records that ORCID actually appears on) was
    removed 2026-08-16 -- found wrong via a real case, DE230100180_WeiWang: a single ORCID on 1 of
    9 merged grant records was enough to fully exempt the cluster from review, even though the
    other 8 records (no ORCID at all) belonged to 3 different, unrelated people (confirmed via an
    independent external biography and a PhD-year timeline impossibility -- the ORCID holder's PhD
    postdates several of the other grants by years). An ORCID on a minority of a cluster's records
    says nothing about the records that don't carry it; those were clustered together by Splink's
    ARC-internal dedupe alone. The bypass was never meant to protect that case -- it was built
    assuming "has an ORCID" approximates "verified as one person," which this case disproves.

    Removing it outright (rather than requiring some coverage threshold) is safe now that
    division_mismatch_for2020() is itself ACCEPTABLE_DIVISION_PAIRS-aware: a genuinely single
    person with a real, ORCID-confirmed, multi-division career won't be flagged if their division
    pair is a recognized legitimate combination (e.g. Chemical Sciences/Engineering), and if it
    isn't a recognized combination, that's exactly the kind of case worth a human glancing at
    regardless of ORCID -- "suspicious" only ever means "warrants manual review," not "will be
    split": actually splitting a cluster still requires an explicit, separate
    manual_splits.csv/confirmed_different_people entry.

    The remaining "common name" gate is untouched and still doing real work independently:
    a genuinely rare full_name_key spanning multiple divisions is extremely unlikely to be two
    different people (a name collision needs two people sharing that exact rare name), regardless
    of ORCID or division-pair whitelist status.

    2026-08-21 fix: a full_name_key ABSENT from tf_lookup now counts as rare, not common.
    Previously defaulted to 1.0 (common) on the reasoning "don't silently exempt a name we have
    no data on" -- sound in the abstract, but empirically backwards: tf_lookup is built from
    2.78M OAX authors' own full_name_key values, so a genuinely common name (e.g. "wei_wang")
    essentially always appears in a population that large. Absence is itself strong evidence of
    rarity, not an absence of evidence. Measured real-world impact of the old default: 16.8% of
    ALL ARC full_name_keys are missing from tf_lookup, but 99.2% of currently-UNRESOLVED
    clusters' keys are missing -- i.e. the "common name" gate was accidentally providing almost
    no protection for exactly the population it exists to protect. Confirmed via a spot-check
    (Ann Williamson): her real linked OAX candidate's own full_name_key is corrupted to
    "williamson_williamson" (the already-documented family_name_main/max_by_len contamination
    bug, CLAUDE.md's "Next Priority"), not "ann_williamson" -- so her correct key never had a
    chance of matching, through no fault of her name's actual rarity."""
    if not full_name_key or full_name_key not in tf_lookup:
        return False
    if tf_lookup[full_name_key] < RARE_NAME_TF:
        return False
    return division_mismatch_for2020(for2020_codes)
