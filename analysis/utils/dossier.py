"""
analysis/utils/dossier.py

Data model for the per-researcher bibliometric coaching dossier (see the ECR analysis
extension plan). `Dossier` is flat and person-specific -- earlier drafts grouped fields into
sub-objects ("identity", "oeuvre", "cohort context") for no reason beyond implementation
convenience; a person doesn't have an "identity section", they just have a name, works,
awards. The one place real multiplicity exists is awards -- a handful of people hold two ARC
fellowships -- so award-anchored facts (coauthor track record, career age at that award) live
in a list of `AwardContext` records, one per award, rather than at the person level.

`works` is the single source of truth for a person's publication record. Per-work facts
(field, type, outlet, institution, topic entropy, novelty score) live only on `Work`;
aggregate views (by_field, by_type, mean_topic_entropy, etc.) are *methods*, not stored
fields, so they never drift out of sync with `works` -- and each takes a `through_year`
cutoff, because "whole career" summaries are only meaningful retrospective to a specified
year (an award year, or any other reference point a future study needs), never as an
implicit "current state". Comparing e.g. "current h-index" across people with different
career lengths and different calendar-time citation windows is exactly the kind of comparison
that misleads. `annual_series` (cumulative h-index per year) is the one stored exception to
"derive, don't store" -- it needs the same cumulative-ranking logic already implemented in
03_annual_metrics.py, and recomputing that here at render time would duplicate the algorithm
in a second place; it's naturally year-indexed already, so callers filter it with
`annual_series_through()` rather than needing a parameterized method.

For the same reason, cohort-relative percentiles are never a single ambient "current"
snapshot -- they're keyed by the reference year they were computed against
(`cohort_percentiles_by_year`), sparse, populated only for whichever years were actually
computed (typically each award_year). `AwardContext.percentiles` doesn't need its own year
key since `award_year` already is its temporal anchor.

No author-position field exists anywhere in this module -- confirmed unrecoverable from this
project's local OpenAlex extract (see docs/author_position_investigation.md); omitted rather
than filled with a proxy, so its absence is a documented decision, not an oversight.

Lives under analysis/utils/, mirroring src/utils/ -- src/ is the ARC<->OpenAlex extraction/
linkage pipeline (00-05) with its own utils/ for shared helpers; analysis/ is everything
downstream of that, with the same split between numbered scripts and shared code. Dossier has
no relationship to extraction/linkage, so it belongs on the analysis/ side, not src/utils/.
"""

from collections import Counter
from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class Work:
    work_idx: int
    publication_year: int
    cited_by_count: int
    type: str
    title: str
    field_name: str | None = None
    subfield_name: str | None = None
    domain_name: str | None = None
    outlet_name: str | None = None
    outlet_type: str | None = None
    outlet_h_index: int | None = None
    institution_names: list[str] = field(default_factory=list)  # can be >1 -- multi-affiliation on one paper is real
    topic_entropy: float | None = None  # own-work topic diversity, once work_topic_diversity.parquet exists
    novelty_score: float | None = None  # per-work Uzzi score, once integrated
    conventionality_score: float | None = None


@dataclass(frozen=True)
class YearRecord:
    year: int
    n_pubs: int
    n_citations_snapshot: float
    h_index: int | None  # NULL in annual_metrics.parquet for years with no citation-ranked works yet
    n_works_cumul: int
    total_citations_cumul: float
    top_field: str | None
    n_highly_cited: int


@dataclass(frozen=True)
class CoauthorRecord:
    coauthor_author_idx: int
    coauthor_name: str
    coauthor_country_code: str | None
    n_shared_works_pre_award: int
    cumulative_works_at_award: int | None
    cumulative_citations_at_award: float | None


@dataclass(frozen=True)
class Percentile:
    value: float
    percentile: float
    basis: Literal["panel", "full_cohort"]  # which comparison group this was computed against
    n: int  # size of that comparison group -- carried alongside the number, not just implied


@dataclass(frozen=True)
class AwardContext:
    """Everything anchored to one specific award -- what a review panel would have
    seen about this person at the time of that particular proposal, not their
    present-day standing. One person can have several of these.
    """

    grant_id: str
    scheme: str  # e.g. "DECRA"
    award_year: int
    career_age_at_award: int | None  # award_year - Dossier.first_pub_year
    coauthor_track_record: list[CoauthorRecord] = field(default_factory=list)
    percentiles: dict[str, Percentile] = field(default_factory=dict)  # vs peers of the same scheme/award_year

    @property
    def coauthor_countries(self) -> Counter[str]:
        return Counter(
            c.coauthor_country_code for c in self.coauthor_track_record if c.coauthor_country_code
        )


@dataclass(frozen=True)
class Dossier:
    arc_id: str
    preferred_name: str  # current/most-recent name, for display
    name_variants: list[str] = field(default_factory=list)  # other names in the same person-cluster (maiden names, aliases) -- from arc_persons.full_names, excluding preferred_name
    for_codes: list[str] = field(default_factory=list)
    for_division: str | None = None
    panel: str | None = None  # BSB / EIC / HCA / MPCE / SBE, from data_persisted/for_panels_2020.csv
    oax_id: str | None = None  # human-facing string form ("https://openalex.org/A..."), not author_idx

    works: list[Work] = field(default_factory=list)
    annual_series: list[YearRecord] = field(default_factory=list)
    first_pub_year: int | None = None
    excluded_work_counts: dict[str, int] = field(default_factory=dict)  # reason (analysis/utils/exclusions.py) -> count; works already filtered out of `works`, kept here for transparency

    @property
    def n_excluded_works(self) -> int:
        return sum(self.excluded_work_counts.values())

    award_contexts: list[AwardContext] = field(default_factory=list)  # one per award this person has held

    cohort_percentiles_by_year: dict[int, dict[str, Percentile]] = field(default_factory=dict)
    archetype_label: str | None = None
    archetype_description: str | None = None
    archetype_second_nearest: tuple[str, float] | None = None  # (label, distance)

    def works_through(self, year: int | None = None) -> list[Work]:
        if year is None:
            return self.works
        return [w for w in self.works if w.publication_year <= year]

    def annual_series_through(self, year: int | None = None) -> list[YearRecord]:
        if year is None:
            return self.annual_series
        return [r for r in self.annual_series if r.year <= year]

    def by_field(self, through_year: int | None = None) -> Counter[str]:
        return Counter(w.field_name for w in self.works_through(through_year) if w.field_name)

    def by_type(self, through_year: int | None = None) -> Counter[str]:
        return Counter(w.type for w in self.works_through(through_year) if w.type)

    def by_outlet(self, through_year: int | None = None) -> Counter[str]:
        return Counter(w.outlet_name for w in self.works_through(through_year) if w.outlet_name)

    def institution_counts(self, through_year: int | None = None) -> Counter[str]:
        # a count, not a deduplicated set -- repeat affiliations should accumulate
        # ("two Harvard papers is better than one Harvard paper")
        return Counter(inst for w in self.works_through(through_year) for inst in w.institution_names)

    def mean_topic_entropy(self, through_year: int | None = None) -> float | None:
        vals = [w.topic_entropy for w in self.works_through(through_year) if w.topic_entropy is not None]
        return sum(vals) / len(vals) if vals else None

    def mean_novelty_score(self, through_year: int | None = None) -> float | None:
        vals = [w.novelty_score for w in self.works_through(through_year) if w.novelty_score is not None]
        return sum(vals) / len(vals) if vals else None

    def mean_conventionality_score(self, through_year: int | None = None) -> float | None:
        vals = [
            w.conventionality_score
            for w in self.works_through(through_year)
            if w.conventionality_score is not None
        ]
        return sum(vals) / len(vals) if vals else None

    def to_markdown(self) -> str:
        """Human-readable render for inspecting a Dossier while designing against real
        data -- sections with nothing populated yet (outlet, institution, coauthor
        track record, novelty) say so explicitly rather than silently disappearing,
        so it's visible at a glance which fields are still pending upstream tables.
        """
        lines = [f"# {self.preferred_name}"]
        if self.name_variants:
            lines.append(f"_also: {', '.join(self.name_variants)}_")
        lines.append("")
        lines.append(f"- arc_id: `{self.arc_id}`")
        lines.append(f"- panel: {self.panel or '(unmapped)'} / division: {self.for_division or '(unknown)'}")
        lines.append(f"- FOR codes: {', '.join(self.for_codes) or '(none)'}")
        lines.append(f"- OpenAlex: {self.oax_id or '(unlinked)'}")
        lines.append(f"- first publication year: {self.first_pub_year or '(unknown)'}")
        lines.append("")

        lines.append("## Awards")
        if not self.award_contexts:
            lines.append("(none found)")
        for ac in self.award_contexts:
            lines.append(f"- **{ac.scheme} {ac.award_year}** (`{ac.grant_id}`) -- career age at award: "
                         f"{ac.career_age_at_award if ac.career_age_at_award is not None else '(unknown)'}")
            if ac.coauthor_track_record:
                lines.append(f"  - {len(ac.coauthor_track_record)} co-authors on record, "
                             f"countries: {dict(ac.coauthor_countries)}")
            else:
                lines.append("  - coauthor track record: not yet available (pending coauthor_track_record_at_award.parquet)")
            if ac.percentiles:
                for k, p in ac.percentiles.items():
                    lines.append(f"  - {k}: {p.value:.2f} (P{p.percentile:.0f}, {p.basis}, n={p.n})")
        lines.append("")

        lines.append(f"## Oeuvre ({len(self.works)} works)")
        if self.excluded_work_counts:
            lines.append(f"- excluded: {self.n_excluded_works} ({dict(self.excluded_work_counts)})")
        lines.append(f"- by field: {dict(self.by_field())}")
        lines.append(f"- by type: {dict(self.by_type())}")
        by_outlet = self.by_outlet()
        lines.append(f"- by outlet: {dict(by_outlet) if by_outlet else '(not yet available, pending outlet columns)'}")
        inst = self.institution_counts()
        lines.append(f"- institutions: {dict(inst) if inst else '(not yet available, pending own_institution_metrics.parquet)'}")
        mte = self.mean_topic_entropy()
        lines.append(f"- mean topic entropy: {mte if mte is not None else '(not yet available, pending work_topic_diversity.parquet)'}")
        mns = self.mean_novelty_score()
        lines.append(f"- mean novelty score: {mns if mns is not None else '(not yet available, pending Uzzi integration)'}")
        lines.append("")

        lines.append("## Annual trajectory")
        if not self.annual_series:
            lines.append("(none)")
        else:
            lines.append("| year | n_pubs | h_index | n_works_cumul | total_citations_cumul |")
            lines.append("|---|---|---|---|---|")
            for yr in self.annual_series:
                lines.append(f"| {yr.year} | {yr.n_pubs} | {yr.h_index} | {yr.n_works_cumul} | {yr.total_citations_cumul:.0f} |")

        return "\n".join(lines)
