"""
analysis/42_ecr_boxplots.py -- early-career fellows' output around their award, as box-and-whisker time series
(2026-10-25, user: "draft an analysis py that plots a box-and-whisker time series. Start with ECRs. x-axis is
year relative to award year ... from -15 to +30 ... y-axis is distribution of pub count or cite count. Make it a
facet plot with vertical facets the area5 categories ... side-by-side facets for works count and cite count").

Cohort: kept, linked ACIFs holding a fellowship of the --tier (config.scope.FELLOWSHIP_TIER): ecr 'Early-career'
(DECRA, APD, APDI, CI-DORA, ...; from -15), mcr 'Mid-career' (FT, QEII, ARF, ...; from -20), sr 'Senior' (FF, FL,
APF; from -25) (user, 2026-10-26); the award is the ACIF's first fellowship of that tier. Award year = the grant's
funding-commencement year minus ANNOUNCEMENT_LAG (ARC announces in the year before commencement; the user's
award time = announcement year, 2026-10-23); --commencement uses the commencement year itself.
Area: the AREA5 area of the award grant's primary FOR2020 code (research_classification Resolver,
FOR2020 -> FOR2020_AREA5, a division-level table: MCS, PSE, LES, BHS, SSH; IND, division 45, is set aside upstream).
AREA5 is the Leiden-like coarse grouping (the Leiden main fields with Veterinary moved to LES) and gives groups of
much more similar output (user, 2026-10-26). It replaced, the same day, the ARC panels first used here:
data_persisted/for_panels_2020.csv maps FOR2020 groups/divisions to the current ARC advisory committees (BSB, EIC,
HCA, MPCE, SBE; user, 2026-10-26), which still serves analysis/07_analyse_ecr_fellowships.py. The two schemes
differ: mathematics sits with physics and chemistry in MPCE but with computing in MCS; earth sciences move from
MPCE to LES; HCA and SBE are both SSH. Area order top to bottom is provisional (user: decide after the violins).
Per person and year relative to the award (the tier's first year..REL_MAX), counting only calendar years up to
LAST_FULL_YEAR:
  works      accepted works (step 5 decision 'accept') published that year
  citations  citations received that year by all the person's accepted works (step 6, full counts)
Years with none count as 0. Years after the person's last accepted work are dropped (user, 2026-10-25), so people
who left research are not counted as zeros. Cumulative works count all accepted works up to the end of the year
(including any before the first year); citations are per year received.
Output (user, 2026-10-28: "plot only cumulative works and annual citations ... one page for works and another for
citations ... three panels across for ECR/MCR/SR ... common y and common x"): processed/figures/
fellows_cumulative_works.png and fellows_citations.png -- rows the AREA5 areas (PSE, MCS, LES, BHS, SSH), columns the
three tiers, boxes per relative year (quartiles, whiskers 1.5 IQR, no fliers), x from -25 to +30 and a symlog y shared
by all panels, the award year a dashed line, grey lines at 1, 10 and 100, the median a heavy red bar and the
mean a black diamond (2026-10-28); and a fan-chart version of each (*_fan.png, user, 2026-10-28: median line,
25-75 and 10-90 percentile bands, each panel noting at the award year the share below the mean and the top 10%'s
share of the total -- the mean >> median skew put in terms policy makers read); the same award-year numbers per area and group,
for works and citations, in processed/figures/fellows_award_summary.md; and processed/{ecr,mcr,sr,ci}_person_years.parquet. fellows_ci_cumulative_works.png and fellows_ci_citations.png
(user, 2026-10-28) add a 4th column: CIs who never held a fellowship (taken up) of any tier, the award being their
first in-scope grant (announcement year), the area that grant's. They replace the
per-tier works-and-citations boxplots and the split violins by award era (2026-10-25/26), removed.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from config.scope import FELLOWSHIP_TIER
from research_classification import Resolver

from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, OAX_LINK_DIR, OEUVRE_DIR, PROCESSED_DATA
from src.acif.build import load_grant_for2020_codes

REL_MAX = 30
# --tier: FELLOWSHIP_TIER label, first relative year (user, 2026-10-26: MCR from -20, SR from -25), file prefix, title
TIERS = {"ecr": ("Early-career", -15, "ecr", "Early-career"),
         "mcr": ("Mid-career", -20, "mcr", "Mid-career"),
         "sr": ("Senior", -25, "sr", "Senior"),
         "ci": (None, -25, "ci", "Non-fellow CI")}   # user, 2026-10-28: a 4th column for CIs who never held a fellowship
FELLOW_TIERS = ["ecr", "mcr", "sr"]
LAST_FULL_YEAR = 2025
ANNOUNCEMENT_LAG = 1
AREAS = ["PSE", "MCS", "LES", "BHS", "SSH"]   # top to bottom (user, 2026-10-28: PSE above MCS)
AREA_NAMES = {"MCS": "Maths, computing & information sci.", "PSE": "Physical sci. & engineering",
              "LES": "Life & earth sci.", "BHS": "Biomedical & health sci.", "SSH": "Social sci. & humanities"}
FIG_DIR = PROCESSED_DATA / "figures"


def cohort(commencement: bool, tier: str | None = "Early-career") -> pd.DataFrame:
    """cluster_id, role_code, grant_code, award_year, area for the first fellowship of the given tier of each kept,
    linked ACIF; tier None: ACIFs holding no fellowship (taken up) of any tier, at their first grant."""
    rec = pd.read_parquet(ACIF_ARC_RECORDS)
    taken = ~rec.not_taken_up.fillna(False)
    if tier is None:
        fellows = set(rec.cluster_id[rec.is_fellowship.fillna(False) & rec.role_code.isin(FELLOWSHIP_TIER) & taken])
        e = rec[taken & ~rec.cluster_id.isin(fellows)]
    else:
        roles = {r for r, t in FELLOWSHIP_TIER.items() if t == tier}
        e = rec[rec.role_code.isin(roles) & rec.is_fellowship.fillna(False) & taken]
    kept = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "excluded"])
    linked = set(pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet").cluster_id)
    e = e[e.cluster_id.isin(set(kept.cluster_id[~kept.excluded])) & e.cluster_id.isin(linked)]
    e = e.sort_values(["cluster_id", "funding_commence_year", "grant_code"]).drop_duplicates("cluster_id")
    e["award_year"] = e.funding_commence_year.astype(int) - (0 if commencement else ANNOUNCEMENT_LAG)
    resolver = Resolver()
    g4 = load_grant_for2020_codes()
    def primary(gc):
        codes = g4.get(gc, [])
        prim = [c["code"] for c in codes if c.get("is_primary")] or [c["code"] for c in codes]
        return resolver.resolve(prim[0], "FOR2020", "FOR2020_AREA5").code if prim else None
    e["area"] = e.grant_code.map(primary)
    return e[["cluster_id", "role_code", "grant_code", "award_year", "area"]].reset_index(drop=True)


def person_years(c: pd.DataFrame, rel_min: int = -15) -> pd.DataFrame:
    """One row per (ACIF, relative year) with works and citations (zeros filled)."""
    con = duckdb.connect()
    con.register("coh", c[["cluster_id", "award_year"]])
    works = con.execute(f"""
        SELECT k.cluster_id, s.publication_year AS y, count(*) AS works
        FROM read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') k
        JOIN read_parquet('{OEUVRE_DIR}/acif_works_single.parquet') s USING (cluster_id, work_idx)
        WHERE k.decision = 'accept' AND s.publication_year IS NOT NULL AND k.cluster_id IN (SELECT cluster_id FROM coh)
        GROUP BY 1, 2""").df().astype({"y": "int64"})
    cites = con.execute(f"""
        SELECT k.cluster_id, ci.year AS y, sum(ci.citations) AS citations
        FROM read_parquet('{OEUVRE_DIR}/acif_works_classified.parquet') k
        JOIN read_parquet('{OEUVRE_DIR}/acif_work_citations.parquet') ci USING (work_idx)
        WHERE k.decision = 'accept' AND k.cluster_id IN (SELECT cluster_id FROM coh) GROUP BY 1, 2""").df()
    grid = c[["cluster_id", "award_year", "area", "role_code"]].merge(
        pd.DataFrame({"rel": np.arange(rel_min, REL_MAX + 1)}), how="cross")
    grid["y"] = (grid.award_year + grid.rel).astype("int64")
    grid = grid[grid.y <= LAST_FULL_YEAR]
    # years after the person's last accepted work are dropped (user, 2026-10-25: leavers are not zeros)
    last = works.groupby("cluster_id").y.max().rename("last_work_year")
    grid = grid.merge(last, on="cluster_id")
    grid = grid[grid.y <= grid.last_work_year]
    out = grid.merge(works, on=["cluster_id", "y"], how="left").merge(cites, on=["cluster_id", "y"], how="left")
    out = out.fillna({"works": 0, "citations": 0}).astype({"works": int, "citations": int})
    # cumulative accepted works to the end of each year, counting works of every earlier year (also before rel_min)
    w = works.sort_values(["cluster_id", "y"]).assign(cum_works=lambda d: d.groupby("cluster_id").works.cumsum())
    out = pd.merge_asof(out.sort_values("y"), w[["cluster_id", "y", "cum_works"]].sort_values("y"),
                        on="y", by="cluster_id", direction="backward")
    return out.fillna({"cum_works": 0}).astype({"cum_works": int}).sort_values(["cluster_id", "rel"])


MEASURES = {"cum_works": ("cumulative accepted works", "#9ecae1", "cumulative_works"),
            "citations": ("citations received per year", "#fdae6b", "citations")}


def column_title(t: str, c: pd.DataFrame) -> str:
    if TIERS[t][0] is None:
        sch = c.grant_code.str.extract(r"^([A-Z]+)")[0].value_counts()
        return f"{TIERS[t][3]}s\n(first grant: " + ", ".join(f"{k} {v:,}" for k, v in sch.items()) + ")"
    roles = ", ".join(f"{k} {v:,}" for k, v in c.role_code.value_counts().items())
    return f"{TIERS[t][3]} fellows\n({roles})"


def plot_grid(pys: dict, cohorts: dict, col: str, title: str, out) -> None:
    """One figure for one measure (user, 2026-10-28): rows the AREA5 areas, columns the tiers (ECR, MCR, SR), common
    x (the widest tier's range) and common y (symlog, so early- and senior-career scales both read)."""
    label, colour, _ = MEASURES[col]
    rel_min = min(TIERS[t][1] for t in pys)
    rels = list(range(rel_min, REL_MAX + 1))
    tiers = list(pys)
    fig, axes = plt.subplots(len(AREAS), len(tiers), figsize=(7 * len(tiers), 3.0 * len(AREAS)),
                             sharex=True, sharey=True, squeeze=False)
    for j, t in enumerate(tiers):
        py, c = pys[t], cohorts[t]
        axes[0, j].set_title(column_title(t, c), fontsize=11)
        for i, pn in enumerate(AREAS):
            ax = axes[i, j]
            d = py[py.area == pn]
            data = [d.loc[d.rel == r, col].to_numpy() for r in rels]
            keep = [k for k, v in enumerate(data) if len(v)]
            if keep:
                # median a heavy dark-red bar, mean a black diamond (user, 2026-10-28: "enhance the ... mean/median
                # indicators on the box")
                ax.boxplot([data[k] for k in keep], positions=[rels[k] for k in keep], widths=0.7, showfliers=False,
                           patch_artist=True, showmeans=True,
                           medianprops={"color": "#a50f15", "linewidth": 2.2, "solid_capstyle": "butt"},
                           meanprops={"marker": "D", "markersize": 3.2, "markerfacecolor": "black",
                                      "markeredgecolor": "black"},
                           boxprops={"facecolor": colour, "linewidth": 0.5},
                           whiskerprops={"linewidth": 0.5}, capprops={"linewidth": 0.5})
            for h in (1, 10, 100):   # reference lines (user, 2026-10-28)
                ax.axhline(h, color="grey", linewidth=0.7, alpha=0.8, zorder=0.5)
            ax.axvline(0, color="black", linestyle="--", linewidth=1.0, alpha=0.7, zorder=5)   # award year (user, 2026-10-26)
            ax.text(0.01, 0.97, f"n={d.cluster_id.nunique():,}", transform=ax.transAxes, fontsize=8, va="top")
            ax.grid(axis="y", linewidth=0.3, alpha=0.6)
            if j == 0:
                ax.set_ylabel(f"{pn}\n{AREA_NAMES[pn]}", fontsize=9)
    ax0 = axes[0, 0]
    ax0.set_yscale("symlog", linthresh=10)
    ax0.set_ylim(bottom=0)
    ticks = list(range(rel_min, REL_MAX + 1, 5))
    ax0.set_xticks(ticks)
    ax0.set_xticklabels([str(t) for t in ticks])
    ax0.set_xlim(rel_min - 1, REL_MAX + 1)
    for ax in axes[-1]:
        ax.set_xlabel("year relative to award (0 = award year)")
        ax.tick_params(labelbottom=True)
    fig.suptitle(f"{label}: {title}", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=130)
    plt.close(fig)


def plot_fan(pys: dict, cohorts: dict, col: str, title: str, out) -> None:
    """Fan-chart version of plot_grid (user, 2026-10-28: the mean >> median skew is poorly understood by policy makers):
    per relative year the median (line) with 25-75 and 10-90 percentile bands; in each panel, at the award year, the
    share of people below the mean and the share of the total held by the top 10%."""
    label, colour, _ = MEASURES[col]
    rel_min = min(TIERS[t][1] for t in pys)
    rels = np.arange(rel_min, REL_MAX + 1)
    tiers = list(pys)
    fig, axes = plt.subplots(len(AREAS), len(tiers), figsize=(7 * len(tiers), 3.0 * len(AREAS)),
                             sharex=True, sharey=True, squeeze=False)
    dark = {"cum_works": "#08519c", "citations": "#a63603"}[col]
    for j, t in enumerate(tiers):
        py, c = pys[t], cohorts[t]
        axes[0, j].set_title(column_title(t, c), fontsize=11)
        for i, pn in enumerate(AREAS):
            ax = axes[i, j]
            d = py[py.area == pn]
            q = d.groupby("rel")[col].quantile([0.1, 0.25, 0.5, 0.75, 0.9]).unstack().reindex(rels)
            ax.fill_between(rels, q[0.1], q[0.9], color=colour, alpha=0.35, linewidth=0, label="10th-90th percentile")
            ax.fill_between(rels, q[0.25], q[0.75], color=colour, alpha=0.8, linewidth=0, label="25th-75th percentile")
            ax.plot(rels, q[0.5], color=dark, linewidth=2.0, label="median (typical person)")
            for h in (1, 10, 100):   # reference lines (user, 2026-10-28)
                ax.axhline(h, color="grey", linewidth=0.7, alpha=0.8, zorder=0.5)
            ax.axvline(0, color="black", linestyle="--", linewidth=1.0, alpha=0.7, zorder=5)   # award year
            v = np.sort(d.loc[d.rel == 0, col].to_numpy())[::-1]
            txt = f"n={d.cluster_id.nunique():,}"
            if len(v) and v.sum() > 0:
                top = v[:max(1, int(round(0.1 * len(v))))].sum() / v.sum()
                txt += (f"\nat award: median {np.median(v):,.0f}, mean {v.mean():,.0f}"
                        f"\n{(v < v.mean()).mean():.0%} below the mean; top 10% hold {top:.0%}")
            ax.text(0.01, 0.97, txt, transform=ax.transAxes, fontsize=7.5, va="top",
                    bbox={"facecolor": "white", "alpha": 0.8, "linewidth": 0, "pad": 1.5})
            ax.grid(axis="y", linewidth=0.3, alpha=0.6)
            if j == 0:
                ax.set_ylabel(f"{pn}\n{AREA_NAMES[pn]}", fontsize=9)
    ax0 = axes[0, 0]
    ax0.set_yscale("symlog", linthresh=10)
    ax0.set_ylim(bottom=0)
    ticks = list(range(rel_min, REL_MAX + 1, 5))
    ax0.set_xticks(ticks)
    ax0.set_xticklabels([str(t) for t in ticks])
    ax0.set_xlim(rel_min - 1, REL_MAX + 1)
    for ax in axes[-1]:
        ax.set_xlabel("year relative to award (0 = award year)")
        ax.tick_params(labelbottom=True)
    axes[0, -1].legend(loc="lower right", fontsize=8)
    fig.suptitle(f"{label}: {title}", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out, dpi=130)
    plt.close(fig)


def award_summary(pys: dict, out) -> None:
    """Markdown tables (user, 2026-10-28: "make one for each of the area5"): per area and tier, cumulative accepted works
    and citations received in the award year -- median, mean, share of people below the mean, share of the total held
    by the top 10%."""
    L = ["# Fellows and non-fellow CIs at the award year", "",
         "Award year = announcement year of the first fellowship of the tier (non-fellow CIs: their first grant). "
         "Linked ACIFs; people whose last accepted work is before the award year are left out.", ""]
    for col, (label, _, _) in MEASURES.items():
        L += [f"## {label} in the award year", ""]
        for pn in AREAS:
            L += [f"### {pn} -- {AREA_NAMES[pn]}", "", "| group | n | median | mean | below the mean | top 10% hold |",
                  "|---|---|---|---|---|---|"]
            for t, py in pys.items():
                v = np.sort(py.loc[(py.area == pn) & (py.rel == 0), col].to_numpy())[::-1]
                if not len(v):
                    continue
                top = v[:max(1, int(round(0.1 * len(v))))].sum() / v.sum() if v.sum() else float("nan")
                name = TIERS[t][3] + ("s" if TIERS[t][0] is None else " fellows")
                L.append(f"| {name} | {len(v):,} | {np.median(v):,.0f} | {v.mean():,.0f} | {(v < v.mean()).mean():.0%} "
                         f"| {top:.0%} |")
            L.append("")
    out.write_text("\n".join(L) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commencement", action="store_true", help="award year = funding-commencement year")
    args = ap.parse_args()
    pys, cohorts = {}, {}
    for t, (tier, rel_min, prefix, _) in TIERS.items():
        c = cohort(args.commencement, tier)
        py = person_years(c, rel_min)
        py.to_parquet(PROCESSED_DATA / f"{prefix}_person_years.parquet", index=False)
        pys[t], cohorts[t] = py[py.area.isin(AREAS)], c
        print(f"{t}: cohort {len(c):,}; by area: {c.area.value_counts(dropna=False).to_dict()}")
    yr = "commencement" if args.commencement else "announcement (commencement - 1)"
    note = (f"(linked ACIFs; award year = {yr}; years up to {LAST_FULL_YEAR}; years after the last accepted work "
            "dropped; boxes = quartiles, red bar = median, diamond = mean, whiskers 1.5 IQR, symlog y; lines at 1, 10, 100)")
    fan_note = (f"(linked ACIFs; award year = {yr}; years up to {LAST_FULL_YEAR}; years after the last accepted work "
                "dropped; line = median, bands = 25th-75th and 10th-90th percentiles, symlog y; lines at 1, 10, 100)")
    award_summary(pys, FIG_DIR / "fellows_award_summary.md")
    print(FIG_DIR / "fellows_award_summary.md")
    fel = {t: pys[t] for t in FELLOW_TIERS}
    for col, (_, _, name) in MEASURES.items():
        for sub, stem, head in [(fel, "fellows", "ARC fellows by year relative to their first fellowship of the tier"),
                                (pys, "fellows_ci", "ARC fellows by year relative to their first fellowship of the tier; "
                                                    "non-fellow CIs relative to their first grant")]:
            out = FIG_DIR / f"{stem}_{name}.png"
            plot_grid(sub, cohorts, col, f"{head}\n{note}", out)
            print(out)
            out = FIG_DIR / f"{stem}_{name}_fan.png"
            plot_fan(sub, cohorts, col, f"{head}\n{fan_note}", out)
            print(out)


if __name__ == "__main__":
    main()
