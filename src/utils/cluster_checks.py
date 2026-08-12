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
"Agricultural and Biological Sciences" despite different ANZSRC divisions). Deliberately no
adjacency-table tolerance beyond what OAX_FIELD's own coarsening provides -- porting the old
letter-keyed adjacency pairs onto FOR2020's numeric divisions isn't well-defined (see problem 2
above), so it would mean inventing new judgment calls, not reusing validated ones. This makes
the check somewhat stricter than ANZSRC-division-adjacency would be on a few genuinely-coherent
multi-field careers (e.g. psychology + linguistics, still 2 different OAX fields) -- an
accepted, explicit tradeoff, not an oversight.
"""
from collections import Counter
from typing import Iterable

from src.utils.for_resolve import oax_field_name

RARE_NAME_TF = 5e-5

INDIGENOUS_DIVISION_PREFIX = "45"  # FOR2020 division 45 "Indigenous Studies" -- confirmed
                                    # directly against real ARC grant records (e.g. group 4501
                                    # "Aboriginal and Torres Strait Islander Culture, Language
                                    # and History", 4513 "Pacific Peoples Culture, Language and
                                    # History"), not assumed from the package's own doc comments.


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


def division_mismatch_for2020(codes: list[dict]) -> bool:
    """True if a single cluster's own primary FOR2020 codes span more than one OAX field --
    self-consistency check, replaces the old division_mismatch(for_names, div_map, adj) call
    inside is_suspicious(). See module docstring for why OAX_FIELD, not ANZSRC division, and
    why there's no adjacency tolerance."""
    return len(for2020_primary_fields(codes)) > 1


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


def is_suspicious_for2020(orcids: list, full_name_key: str | None, for2020_codes: list[dict], tf_lookup: dict) -> bool:
    """True if cluster warrants manual review: common name, no ORCID, cross-OAX-field FOR.
    Same three conditions and order as the old is_suspicious(for_names + for_divisions.csv),
    only the division check itself changed -- see module docstring."""
    if len(orcids) > 0:
        return False
    if full_name_key and tf_lookup.get(full_name_key, 1.0) < RARE_NAME_TF:
        return False
    return division_mismatch_for2020(for2020_codes)
