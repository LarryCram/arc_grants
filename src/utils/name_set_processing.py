"""
src/utils/name_set_processing.py

A single, canonical way to hold a person's (or one record's) name-forms tagged by where they
came from -- ARC's own grant records, an ORCID record's primary/alias names, OpenAlex's
display_name vs display_name_alternatives -- and a single, canonical way to compare two such
objects for Splink blocking/scoring/TF-adjustment. Meant to replace the several independent,
ad hoc "pick one representative string from a set" implementations found scattered across this
codebase (family_name_main/full_name_key/first_name_canonical in names.py, awards_cif.py,
02_prepare_oax.py, orcid_processor.py -- see docs/pipeline_todo.md item #14 for the full
incident history this is responding to).

Design, settled across a long back-and-forth (2026-09-07/08), not re-derived here:
- Provenance is a flat dict {provenance_label: {value: count}} -- e.g. {"arc_name": {...},
  "orcid_alias": {...}}, {"oax_display": {...}, "oax_alternatives": {...}}. Not capped at two
  keys. Counts matter, not just membership -- see for_report()'s own docstring for why a bare
  deduplicated set is not enough for reporting purposes.
- NameSetProcessing never exposes a bare scalar or bare set for MATCHING purposes. for_label() (a
  single, low-stakes display string) and for_report() (a frequency table per provenance) are
  its only public single-object outputs.
- NameComparison (two NameSetProcessing objects) is the only way to get blocking/scoring/
  TF-adjustment evidence -- it operates on the full value pool across ALL provenances on both
  sides, regardless of trust tier. Trust does not gate what's ELIGIBLE for matching, only what
  becomes a committed scalar (for_label, the internal "exact key" used by for_scoring) -- see
  the 2026-09-08 discussion: OAX's own recorded form is unknown in advance, so excluding a
  candidate (e.g. an orcid alias) from blocking/scoring risks a real missed match for no
  matching safety gain. NameComparison does NOT raise when the two sides simply don't relate --
  that's the ordinary, expected outcome for most blocked candidate pairs, not an anomaly.
- Validation raises loudly (NameSetAnomaly), not silently, at NameSetProcessing construction
  time, while this scheme is being developed and calibrated -- every anomaly needs a human look
  right now, not a quiet log line. Two kinds of anomaly: within one provenance's own set (do its
  members look like genuine variants of one name), and across provenances (does ANY member of
  one relate to ANY member of another at all).
- The "related" check is a small, transparent heuristic (substring or short edit distance),
  documented as exactly that -- not a claim of linguistic precision. Single-character tokens
  (bare initials) are exempt from every coherence check -- they carry no information either way,
  matching this project's own established first_names_multichar convention elsewhere
  (03_link_arc_oax.py).

Not yet wired into any production pipeline code -- see scratch/test_name_set_processing_cases.py
for behaviour against real cases found this session, and docs/pipeline_todo.md item #14 for the
migration plan (change the actual field types on AwardCIFItem/AwardsCIF/NameForms/ParsedName to
this class, then let the type-checker/test-suite/a full pipeline rerun surface every consumer
that needs updating -- not a hand-searched migration).
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations


class NameSetAnomaly(ValueError):
    """Raised by NameSetProcessing's own construction-time validation. Carries enough detail
    (which provenance(s), which specific values) that a developer looking at the traceback can
    immediately see what to inspect -- this is meant to be looked at and individually judged,
    not caught and swallowed."""


def levenshtein(a: str, b: str) -> int:
    """Standard edit distance -- no external dependency for something this small."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            curr[j] = min(
                prev[j] + 1,          # deletion
                curr[j - 1] + 1,      # insertion
                prev[j - 1] + (ca != cb),  # substitution
            )
        prev = curr
    return prev[-1]


def related(a: str, b: str) -> bool:
    """A deliberately simple, transparent heuristic -- not linguistic ground truth. True when
    one string contains the other (handles compound/hyphenated-surname partial overlap, e.g.
    'alvarez' in 'mejias-alvarez'), or when they're a short edit distance apart relative to
    their own length (handles digraph/diacritic spelling variants, e.g. 'muller'/'mueller').
    Callers should filter out single-character tokens before using this -- it does not
    special-case them, and a bare initial is "related" to almost nothing by this measure
    despite carrying no real evidence of anything."""
    if a == b:
        return True
    if a in b or b in a:
        return True
    return levenshtein(a, b) <= max(1, min(len(a), len(b)) // 4)


def _multichar(values) -> set[str]:
    """Bare single-character tokens (initials) carry no coherence information either way --
    exempt from every relatedness check, matching this project's own first_names_multichar
    convention (03_link_arc_oax.py)."""
    return {v for v in values if len(v) > 1}


@dataclass(frozen=True)
class NameSetProcessing:
    """One record's (or one person's) name-forms, tagged by provenance. Each provenance is a
    LIST OF ORIGIN GROUPS, not one flat pool -- an origin group is every sibling form produced
    by widening ONE recorded source string (e.g. one HumanNameParser.parse() call on one
    display_name or one ARC grant record's own family_name field). This is free information a
    single parse already produces (ParsedName.family_names IS one origin group) -- it was
    previously discarded the moment multiple source strings got unioned into one set (e.g.
    02_prepare_oax.py parsing display_name plus every display_name_alternative separately, then
    dumping all their family_names tokens into one shared dict with no memory of which alternative
    produced which token). Tracking it here, since we already compute it for free, sharpens
    validation: members WITHIN one origin group never need a relatedness check (they're
    guaranteed siblings of one input by construction -- diacritic_variants() only ever produces
    genuine variants of its own argument); only ACROSS origin groups, within or between
    provenances, is relatedness actually informative to check.

    Never mutated after construction: validation runs once, here, and the result must stay
    trustworthy for the object's whole lifetime. `sets` itself is a plain dict/list structure
    (Python's frozen dataclass only blocks field *reassignment*, not deep mutation of what a
    field points to) -- callers should treat it as read-only by convention, the same way this
    project already treats several other "frozen in spirit" dataclasses."""

    # provenance label -> list of origin groups, each origin group its own {value: count}.
    # A provenance with only one recorded source string has exactly one group in its list.
    sets: dict[str, list[dict[str, int]]]

    def _groups(self, label: str) -> list[dict[str, int]]:
        return self.sets.get(label, [])

    def _flat(self, label: str) -> dict[str, int]:
        """One provenance's own groups, flattened -- for callers (all_counts, for_report) that
        don't care about origin boundaries, only about "what values exist under this label."""
        out: dict[str, int] = {}
        for group in self._groups(label):
            for v, n in group.items():
                out[v] = out.get(v, 0) + n
        return out

    def __post_init__(self) -> None:
        if not self.sets:
            raise NameSetAnomaly("NameSetProcessing built with no provenance sets at all.")

        # Cross-group coherence WITHIN one provenance: only checked when a provenance has 2+
        # origin groups (a single group needs no check at all -- see class docstring). Each
        # pair of groups must relate via at least one multi-char member on each side, or one of
        # them looks like a genuinely different name recorded under the same provenance label.
        for label, groups in self.sets.items():
            multi_groups = [g for g in (_multichar(g) for g in groups) if g]
            for i, j in combinations(range(len(multi_groups)), 2):
                a, b = multi_groups[i], multi_groups[j]
                if not any(related(x, y) for x in a for y in b):
                    raise NameSetAnomaly(
                        f"provenance {label!r} has unrelated origin groups {sorted(a)} and "
                        f"{sorted(b)} -- looks like two different names under one provenance."
                    )

        # Cross-provenance plausibility: when 2+ provenances exist, at least one multi-char
        # value in each pair of provenances (across all of that provenance's own groups) must
        # relate to at least one value in the other -- otherwise one of them looks like it
        # belongs to a different identity entirely (the David Allen / Camryn D. Allen shape).
        labels = list(self.sets)
        for i, j in combinations(range(len(labels)), 2):
            a_multi = _multichar(self._flat(labels[i]))
            b_multi = _multichar(self._flat(labels[j]))
            if not a_multi or not b_multi:
                continue
            if not any(related(a, b) for a in a_multi for b in b_multi):
                raise NameSetAnomaly(
                    f"provenance {labels[i]!r} {sorted(a_multi)} does not plausibly relate to "
                    f"provenance {labels[j]!r} {sorted(b_multi)} -- no related pair across them."
                )

    def all_counts(self) -> dict[str, int]:
        """Every value across every provenance and every origin group, counts summed -- the
        pool for_blocking()/for_scoring()/for_tf_idf() draw on. Deliberately provenance- and
        group-blind: see the module docstring for why trust must not gate matching eligibility."""
        out: dict[str, int] = {}
        for label in self.sets:
            for v, n in self._flat(label).items():
                out[v] = out.get(v, 0) + n
        return out

    def for_label(self, preferred: tuple[str, ...] | None = None) -> str | None:
        """A single, low-stakes display string. Plausibility is the only bar -- this is the one
        place a bare scalar is fine, because it's chosen only once a person is already resolved,
        never used for matching (see the module docstring). Prefers the given provenance order
        (first non-empty wins), then the longest (most complete) form within it; falls back to
        the longest form across everything if no preference is given or every preferred
        provenance is empty."""
        order = preferred or tuple(self.sets)
        for label in order:
            values = _multichar(self._flat(label))
            if values:
                return max(values, key=len)
        values = _multichar(self.all_counts())
        return max(values, key=len) if values else None

    def for_report(self, by_origin_group: bool = False) -> dict[str, dict[str, int] | list[dict[str, int]]]:
        """One real frequency table per provenance -- not a merged pool, not a single scalar,
        and not a bare deduplicated set either. A set only says a form exists; this says how
        many independent records support it, which is exactly the signal that makes an isolated
        low-count outlier next to a consistent high-count cluster visible at a glance (the
        David Allen / Camryn D. Allen shape) -- see docs/pipeline_todo.md item #14's design
        discussion. Callers building a NameSetProcessing from raw records are responsible for
        reflecting real per-item counts in the groups they pass in; this class only stores and
        reports whatever counts it was given. Flattens each provenance's own origin groups by
        default (the normal reporting view); pass by_origin_group=True to instead get each
        provenance's groups kept separate (e.g. to show that an "arc_name" provenance's own
        widened-variant group and its plain-record group are still one recorded name, not two)."""
        if by_origin_group:
            return {label: [dict(g) for g in groups] for label, groups in self.sets.items()}
        return {label: self._flat(label) for label in self.sets}


@dataclass(frozen=True)
class NameComparison:
    """The only way to get blocking/scoring/TF-adjustment evidence between two NameSetProcessing
    objects. Does not raise on construction merely because the two sides don't relate -- that's
    the ordinary outcome for most Splink-blocked candidate pairs, not an anomaly; each side's
    own internal/cross-provenance coherence was already checked when it was built."""

    left: NameSetProcessing
    right: NameSetProcessing

    def for_blocking(self) -> bool:
        """Loose, provenance-blind overlap -- do the two sides share any multi-char value at
        all, across every provenance on both sides. Mirrors 03_link_arc_oax.py's already-shipped
        list_has_any() blocking rule; "loose blocking, precise scoring" means a spurious overlap
        still has to survive for_scoring()."""
        l = set(_multichar(self.left.all_counts()))
        r = set(_multichar(self.right.all_counts()))
        return not l.isdisjoint(r)

    def _matching_key(self, prov: NameSetProcessing) -> str | None:
        """The scalar used for the "exact match" scoring tier -- the single most-recorded form
        across ALL of one side's provenances (mode, ties broken by first-seen insertion order),
        NOT the longest. This is deliberately a different criterion from for_label()'s
        completeness-based choice -- see the module docstring on why matching and display
        selection are different questions."""
        counts = _multichar(prov.all_counts())
        if not counts:
            return None
        full = {v: n for v, n in prov.all_counts().items() if len(v) > 1}
        return max(full, key=lambda v: full[v]) if full else None

    def for_scoring(self) -> str:
        """One of "exact" / "overlap" / "none" -- a graded evidence level, not a bool. "exact"
        only when each side's own dominant (modal) form agrees; "overlap" when the sides share
        some other value without the dominant forms matching (e.g. a genuine spelling-convention
        difference, the Hans Muhlhaus shape); "none" otherwise. Mirrors the comparison-level
        cascade already shipped in 03_link_arc_oax.py's family_name_main comparison."""
        lk, rk = self._matching_key(self.left), self._matching_key(self.right)
        if lk is not None and rk is not None and lk == rk:
            return "exact"
        if self.for_blocking():
            return "overlap"
        return "none"

    def for_tf_idf(self, reference: dict[str, float]) -> float | None:
        """The rarity of the actual shared evidence -- looks up every value the two sides
        genuinely have in common in `reference` (a value -> population-frequency table, e.g.
        oax_tf_family_name_variant.parquet) and returns the MINIMUM (rarest) frequency among
        them, since the rarest shared form is the most informative evidence available. Returns
        None when the sides share nothing, or share nothing the reference table has ever seen."""
        l = set(_multichar(self.left.all_counts()))
        r = set(_multichar(self.right.all_counts()))
        shared = l & r
        freqs = [reference[v] for v in shared if v in reference]
        return min(freqs) if freqs else None
