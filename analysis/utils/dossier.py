"""
analysis/utils/dossier.py -- the per-person dossier (rebuilt 2026-10-09 on the rebuild's outputs;
the earlier model read the archived pipeline's awards_cif / arc_oax_resolved / piling tables).

A Dossier is one ACIF (one person): the overall profile, the ARC awards, how the person was linked
to OpenAlex (and why not, if unlinked), the works with their step-5 decisions, and a yearly
time-line of works and citations received with the award years marked. Built from persisted
outputs only by analysis/utils/dossier_build.py; rendered by to_markdown() and plot_timeline().

Aggregates (counts by type / field / venue, h-index, the time-line) are methods over `works` and
`citations`, not stored, so they cannot drift from them. Accepted works are the person's oeuvre;
unsure works are shown alongside, never mixed in; rejected works are counted only.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

FIRST_YEAR = 1950
SNAPSHOT_NOTE = "OpenAlex snapshot of July 2026: 2026 is a part year"


@dataclass(frozen=True)
class Award:
    grant_code: str
    scheme: str                       # scheme code (grant code prefix: DP, LP, DE, FT, FL, ...)
    scheme_name: str | None
    role_code: str | None             # the person's role on this grant
    is_fellowship: bool
    year: int | None                  # funding commencement year
    years_funded: int | None
    end_year: int | None              # project end, when known (anticipated or actual)
    funding_announced: float | None
    admin_org: str | None
    n_eligible_orgs: int | None
    primary_for: str | None
    declined: bool = False
    ended_early: bool = False
    coinvestigators: list[str] = field(default_factory=list)   # "Name (role)" of the other in-scope investigators


@dataclass(frozen=True)
class LinkedRecord:
    author_idx: int
    name: str | None                  # OpenAlex display name
    orcid: str | None                 # the record's own ORCID
    stage: str                        # orcid / name / works / scopus
    status: str                       # the linker's accepting status
    works_count: int | None           # OpenAlex works_count of the record
    evidence: str                     # stage-specific evidence, one line


@dataclass(frozen=True)
class Work:
    work_idx: int
    year: int | None                  # earliest version's publication year
    type: str | None
    title: str | None
    venue: str | None
    doi: str | None
    cited_by_count: int
    authors_count: int | None
    field: str | None                 # top OpenAlex field by topic weight
    decision: str                     # accept / reject / unsure (step 5)
    rule: str | None
    decided_by: str | None
    reason: str | None
    in_core: bool = False             # in the work graph's anchored core (tied to the grants)
    on_scopus_profile: bool | None = None
    n_versions: int = 1


@dataclass(frozen=True)
class Dossier:
    cluster_id: str
    name: str
    name_variants: list[str] = field(default_factory=list)
    orcids: list[str] = field(default_factory=list)
    orcid_sources: list[str] = field(default_factory=list)
    for_codes: list[str] = field(default_factory=list)       # "code name" of every FOR2020 group, primary first
    main_division: str | None = None
    universities: list[str] = field(default_factory=list)
    excluded: bool = False
    excluded_reason: str | None = None
    awards: list[Award] = field(default_factory=list)
    links: list[LinkedRecord] = field(default_factory=list)
    link_route: list[str] = field(default_factory=list)      # each linker stage's verdict, when not linked by ORCID
    works: list[Work] = field(default_factory=list)          # accepted and unsure works; rejects counted below
    rejected: dict[str, int] = field(default_factory=dict)   # rule -> rejected works
    citations: dict[int, dict[int, int]] = field(default_factory=dict)  # work_idx -> {year: citations received}

    # ---- profile ----------------------------------------------------------------------------------
    @property
    def first_grant_year(self) -> int | None:
        ys = [a.year for a in self.awards if a.year]
        return min(ys) if ys else None

    @property
    def last_grant_year(self) -> int | None:
        ys = [a.year for a in self.awards if a.year]
        return max(ys) if ys else None

    @property
    def fellowships(self) -> list[Award]:
        return [a for a in self.awards if a.is_fellowship]

    @staticmethod
    def label(a: Award) -> str:
        """An award's short label: the fellowship role for a fellowship (APD, DECRA, FT, ...), else the scheme."""
        return (a.role_code or a.scheme) if a.is_fellowship else a.scheme

    # ---- works ------------------------------------------------------------------------------------
    def accepted(self, through_year: int | None = None) -> list[Work]:
        return [w for w in self.works if w.decision == "accept"
                and (through_year is None or (w.year is not None and w.year <= through_year))]

    def unsure(self) -> list[Work]:
        return [w for w in self.works if w.decision == "unsure"]

    def first_pub_year(self) -> int | None:
        ys = [w.year for w in self.accepted() if w.year and w.year >= FIRST_YEAR]
        return min(ys) if ys else None

    def by_type(self, through_year: int | None = None) -> Counter:
        return Counter(w.type for w in self.accepted(through_year) if w.type)

    def by_field(self, through_year: int | None = None) -> Counter:
        return Counter(w.field for w in self.accepted(through_year) if w.field)

    def by_venue(self, through_year: int | None = None) -> Counter:
        return Counter(w.venue for w in self.accepted(through_year) if w.venue)

    def by_rule(self) -> Counter:
        return Counter((w.decision, w.rule, w.decided_by) for w in self.works)

    def citations_received(self, work_idx: int, through_year: int | None = None) -> int:
        c = self.citations.get(work_idx, {})
        return sum(n for y, n in c.items() if through_year is None or y <= through_year)

    def h_index(self, through_year: int | None = None) -> int:
        """h-index of the accepted works published by `through_year`, counting only citations
        received by then (from the reference lists, so it can differ from cited_by_count)."""
        cs = sorted((self.citations_received(w.work_idx, through_year) for w in self.accepted(through_year)),
                    reverse=True)
        return sum(1 for i, c in enumerate(cs, 1) if c >= i)

    # ---- time-line --------------------------------------------------------------------------------
    def timeline(self, start: int | None = None, end: int | None = None) -> list[dict]:
        """One row per year: accepted and unsure works published, citations received that year by
        the accepted works, cumulative h-index, and the awards commencing that year."""
        acc, uns = self.accepted(), self.unsure()
        cites = Counter()
        for w in acc:
            for y, n in self.citations.get(w.work_idx, {}).items():
                cites[y] += n
        years = [w.year for w in acc + uns if w.year] + [a.year for a in self.awards if a.year]
        if not years:
            return []
        start = start or max(FIRST_YEAR, min(years))
        end = end or max(max(years), max((y for y in cites if y <= 2026), default=start))
        n_acc, n_uns = Counter(w.year for w in acc), Counter(w.year for w in uns)
        rows = []
        for y in range(start, end + 1):
            ev = [a for a in self.awards if a.year == y]
            rows.append({"year": y, "works": n_acc[y], "unsure": n_uns[y], "citations": cites[y],
                         "h_index": self.h_index(y),
                         "events": [f"{self.label(a)}{'*' if a.is_fellowship else ''} {a.grant_code}"
                                    + (" (declined)" if a.declined else " (ended early)" if a.ended_early else "")
                                    for a in ev]})
        return rows

    # ---- rendering --------------------------------------------------------------------------------
    def to_markdown(self, chart: str | None = None, max_works: int = 40) -> str:
        acc, uns = self.accepted(), self.unsure()
        L = [f"# {self.name}", ""]
        if self.name_variants:
            L.append(f"_also recorded as: {', '.join(self.name_variants)}_")
            L.append("")
        L += ["## Profile", "",
              f"- ACIF: `{self.cluster_id}`" + (f" (set aside: {self.excluded_reason})" if self.excluded else ""),
              f"- ORCID: {', '.join(self.orcids) or 'none'}" + (f" (source: {', '.join(self.orcid_sources)})" if self.orcids else ""),
              f"- ARC awards: {len(self.awards)}, {self.first_grant_year}-{self.last_grant_year}; fellowships: "
              + (", ".join(f"{self.label(a)} {a.year}" for a in self.fellowships) or "none"),
              f"- main field of research: {self.main_division or 'unknown'}",
              f"- FOR codes: {'; '.join(self.for_codes) or 'none'}",
              f"- universities on the grants: {', '.join(self.universities) or 'none'}",
              f"- OpenAlex: " + (", ".join(f"A{r.author_idx} {r.name or ''}".strip() for r in self.links) or "not linked"),
              f"- works: {len(acc):,} accepted, {len(uns):,} unsure, {sum(self.rejected.values()):,} rejected; "
              f"first accepted work {self.first_pub_year() or '-'}; citations received {sum(self.citations_received(w.work_idx) for w in acc):,}; "
              f"h-index {self.h_index()}", ""]
        L += ["## ARC awards", "", "| year | grant | scheme | role | fellowship | years | end | funding | administering org | primary FOR | note |",
              "|---|---|---|---|---|---|---|---|---|---|---|"]
        for a in self.awards:
            note = "declined" if a.declined else "ended early" if a.ended_early else ""
            fund = f"${a.funding_announced:,.0f}" if a.funding_announced else ""
            L.append(f"| {a.year or ''} | {a.grant_code} | {a.scheme} | {a.role_code or ''} | {'yes' if a.is_fellowship else ''} | "
                     f"{a.years_funded or ''} | {a.end_year or ''} | {fund} | {a.admin_org or ''}"
                     f"{f' (+{a.n_eligible_orgs - 1} orgs)' if a.n_eligible_orgs and a.n_eligible_orgs > 1 else ''} | "
                     f"{a.primary_for or ''} | {note} |")
        coinv = "; ".join(f"{a.grant_code}: {', '.join(a.coinvestigators)}" for a in self.awards if a.coinvestigators)
        L += ["", f"Co-investigators: {coinv or 'none'}", ""]
        L += ["## Link to OpenAlex", ""]
        if self.links:
            L += ["| record | name | ORCID on record | linked by | status | works | evidence |", "|---|---|---|---|---|---|---|"]
            for r in self.links:
                L.append(f"| A{r.author_idx} | {r.name or ''} | {r.orcid or ''} | {r.stage} | {r.status} | "
                         f"{r.works_count if r.works_count is not None else ''} | {r.evidence} |")
        else:
            L.append("Not linked.")
        if self.link_route:
            L += ["", "Linker route: " + " -> ".join(self.link_route)]
        L += ["", "## Works", ""]
        if acc or uns:
            L.append("| decision | rule | decided by | works |")
            L.append("|---|---|---|---|")
            for (d, r, b), n in sorted(self.by_rule().items(), key=lambda x: (x[0][0], -x[1])):
                L.append(f"| {d} | {r} | {b or ''} | {n:,} |")
            if self.rejected:
                L.append("")
                L.append("Rejected (not listed): " + ", ".join(f"{r} {n:,}" for r, n in sorted(self.rejected.items(), key=lambda x: -x[1])))
            L += ["", "Accepted works by type: " + ", ".join(f"{k} {v}" for k, v in self.by_type().most_common()),
                  "", "By field: " + ", ".join(f"{k} {v}" for k, v in self.by_field().most_common(8)),
                  "", "Top venues: " + ", ".join(f"{k} ({v})" for k, v in self.by_venue().most_common(8)), "",
                  f"Most cited accepted works (citations received; {max_works} shown):", "",
                  "| year | type | title | venue | citations |", "|---|---|---|---|---|"]
            for w in sorted(acc, key=lambda w: -self.citations_received(w.work_idx))[:max_works]:
                L.append(f"| {w.year or ''} | {w.type or ''} | {(w.title or '')[:90]} | {(w.venue or '')[:40]} | "
                         f"{self.citations_received(w.work_idx):,} |")
            if uns:
                L += ["", f"Unsure works ({len(uns):,}; {min(len(uns), max_works)} shown):", "",
                      "| year | type | title | venue | why unsure |", "|---|---|---|---|---|"]
                for w in sorted(uns, key=lambda w: (w.year or 0))[:max_works]:
                    L.append(f"| {w.year or ''} | {w.type or ''} | {(w.title or '')[:90]} | {(w.venue or '')[:40]} | "
                             f"{w.reason or w.rule or ''} |")
        else:
            L.append("No works.")
        L += ["", "## Time-line", ""]
        if chart:
            L += [f"![time-line]({chart})", ""]
        tl = self.timeline()
        if tl:
            L += [f"Citations received are counted from OpenAlex reference lists ({SNAPSHOT_NOTE}).", "",
                  "| year | works | unsure | citations received | h-index | awards (* fellowship) |", "|---|---|---|---|---|---|"]
            for r in tl:
                if r["works"] or r["unsure"] or r["citations"] or r["events"]:
                    L.append(f"| {r['year']} | {r['works']} | {r['unsure'] or ''} | {r['citations']:,} | {r['h_index']} | "
                             f"{'; '.join(r['events'])} |")
        return "\n".join(L) + "\n"

    def plot_timeline(self, path) -> str | None:
        """Works per year (bars; unsure stacked, lighter) and citations received per year (line, right
        axis), award commencement years marked with a star (circled for a fellowship). Returns the
        path, or None when there is nothing to plot."""
        tl = self.timeline()
        if not tl:
            return None
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        ys = [r["year"] for r in tl]
        fig, ax1 = plt.subplots(figsize=(11, 4.2))
        ax1.bar(ys, [r["works"] for r in tl], color="#3b6ea5", label="works (accepted)")
        ax1.bar(ys, [r["unsure"] for r in tl], bottom=[r["works"] for r in tl], color="#b9cde5", label="works (unsure)")
        ax1.set_ylabel("works per year")
        ax1.set_xlim(min(ys) - 1, max(ys) + 1)
        ax2 = ax1.twinx()           # shares the x-axis: never clear its x ticks (that wipes ax1's)
        ax2.plot(ys, [r["citations"] for r in tl], color="#c0392b", lw=1.8, label="citations received")
        ax2.set_ylabel("citations received per year")
        ax2.set_ylim(bottom=0)
        top = max(max(r["works"] + r["unsure"] for r in tl), 1)
        for a in self.awards:
            if a.year is None:
                continue
            y = top * 1.08
            if a.is_fellowship:
                ax1.scatter([a.year], [y], s=260, facecolors="none", edgecolors="#222", zorder=5)
            ax1.scatter([a.year], [y], marker="*", s=140, color="#e67e22" if not a.declined else "#999", zorder=6)
            ax1.annotate(self.label(a), (a.year, y), textcoords="offset points", xytext=(0, 9), ha="center", fontsize=7)
        ax1.set_ylim(0, top * 1.3)
        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax1.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8, frameon=False)
        ax1.set_title(f"{self.name} -- works and citations per year (* award commences; circled = fellowship; "
                      f"{SNAPSHOT_NOTE})", fontsize=10)
        fig.tight_layout()
        fig.savefig(path, dpi=110)
        plt.close(fig)
        return str(path)
