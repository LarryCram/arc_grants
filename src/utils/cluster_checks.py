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
"""
from collections import Counter
from typing import Iterable

from src.utils.for_resolve import oax_field_name, oax_subfield_name

RARE_NAME_TF = 5e-5

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
    src/utils/oeuvre_build.py::apply_subfield_filter(), which does use the full list)."""
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
    (04_resolve_links.py's _field_score)."""
    subfields = {oax_subfield_name(e["code"]) for e in codes if e.get("is_primary") and e.get("code")}
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
    ORCID-based checks, or manual review as ways of actually catching a wrongful merge."""
    fields = for2020_primary_fields(codes)
    if len(fields) <= 1:
        return False
    divisions = sorted({e["code"][:2] for e in codes if e.get("is_primary") and e.get("code")})
    if len(divisions) < 2:
        return True
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
    d1 = for2020_primary_fields(codes1)
    d2 = for2020_primary_fields(codes2)
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
    of ORCID or division-pair whitelist status."""
    if full_name_key and tf_lookup.get(full_name_key, 1.0) < RARE_NAME_TF:
        return False
    return division_mismatch_for2020(for2020_codes)
