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
Years with none count as 0. Boxes: quartiles; whiskers 1.5 IQR; no fliers; citations on a symlog axis. The award
year is a dashed vertical line drawn over the boxes/violins.
Years after the person's last accepted work are dropped (user, 2026-10-25), so people who left research are not
counted as zeros. --cumulative plots cumulative accepted works (all works up to the end of the year, including
any before the first year) instead of works per year; citations are always per year received (not cumulative).
Writes processed/figures/{ecr,mcr,sr}_works_citations_boxplot[_cumulative].png and processed/{tier}_person_years.parquet.
--violin (user, 2026-10-25: "draw them as violins with LHS up to 2012 and RHS since 2013"): split violins per
relative year, left half awards announced up to ERA_SPLIT_YEAR, right half after; citations as log10(1 + c);
quartile lines inside; each half scaled to its own width. Writes processed/figures/{tier}_works_citations_violin[_cumulative].png.
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
         "sr": ("Senior", -25, "sr", "Senior")}
LAST_FULL_YEAR = 2025
ANNOUNCEMENT_LAG = 1
AREAS = ["MCS", "PSE", "LES", "BHS", "SSH"]   # provisional order, top to bottom (user, 2026-10-26: decide after the violins)
AREA_NAMES = {"MCS": "Maths, computing & information sci.", "PSE": "Physical sci. & engineering",
              "LES": "Life & earth sci.", "BHS": "Biomedical & health sci.", "SSH": "Social sci. & humanities"}
FIG_DIR = PROCESSED_DATA / "figures"
ERA_SPLIT_YEAR = 2012


def cohort(commencement: bool, tier: str = "Early-career") -> pd.DataFrame:
    """cluster_id, role_code, grant_code, award_year, area for the first fellowship of the given tier of each kept,
    linked ACIF."""
    rec = pd.read_parquet(ACIF_ARC_RECORDS)
    roles = {r for r, t in FELLOWSHIP_TIER.items() if t == tier}
    e = rec[rec.role_code.isin(roles) & rec.is_fellowship.fillna(False) & ~rec.not_taken_up.fillna(False)]
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


WORKS_LABEL = {"works": "accepted works per year", "cum_works": "cumulative accepted works"}


def plot(py: pd.DataFrame, title: str, out, wcol: str = "works", rel_min: int = -15) -> None:
    rels = list(range(rel_min, REL_MAX + 1))
    fig, axes = plt.subplots(len(AREAS), 2, figsize=(18, 3.2 * len(AREAS)), sharex=True, squeeze=False)
    for i, pn in enumerate(AREAS):
        d = py[py.area == pn]
        n_people = d.cluster_id.nunique()
        for j, (col, label) in enumerate(((wcol, WORKS_LABEL[wcol]), ("citations", "citations received per year"))):
            ax = axes[i, j]
            data = [d.loc[d.rel == r, col].to_numpy() for r in rels]
            keep = [k for k, v in enumerate(data) if len(v)]
            ax.boxplot([data[k] for k in keep], positions=[rels[k] for k in keep], widths=0.7, showfliers=False,
                       patch_artist=True, medianprops={"color": "black", "linewidth": 1.2},
                       boxprops={"facecolor": "#9ecae1" if col != "citations" else "#fdae6b", "linewidth": 0.6},
                       whiskerprops={"linewidth": 0.6}, capprops={"linewidth": 0.6})
            ax.axvline(0, color="black", linestyle="--", linewidth=1.0, alpha=0.7, zorder=5)   # award year (user, 2026-10-26)
            if col == "citations":
                ax.set_yscale("symlog", linthresh=10)
            ax.set_ylim(bottom=0)
            ax.grid(axis="y", linewidth=0.3, alpha=0.6)
            if i == 0:
                ax.set_title(label, fontsize=11)
            if j == 0:
                ax.set_ylabel(f"{pn}\n{AREA_NAMES[pn]}\n(n={n_people:,})", fontsize=9)
            ticks = list(range(rel_min, REL_MAX + 1, 5))
            ax.set_xticks(ticks)
            ax.set_xticklabels([str(t) for t in ticks])
            ax.set_xlim(rel_min - 1, REL_MAX + 1)
    for ax in axes[-1]:
        ax.set_xlabel("year relative to award (0 = award year)")
    fig.suptitle(title, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=130)


def plot_violin(py: pd.DataFrame, title: str, out, wcol: str = "works", rel_min: int = -15) -> None:
    import seaborn as sns
    rels = list(range(rel_min, REL_MAX + 1))
    d0 = py.assign(era=np.where(py.award_year <= ERA_SPLIT_YEAR, f"award <= {ERA_SPLIT_YEAR}", f"award >= {ERA_SPLIT_YEAR + 1}"),
                   log_c=np.log10(1 + py.citations))
    order = [f"award <= {ERA_SPLIT_YEAR}", f"award >= {ERA_SPLIT_YEAR + 1}"]
    fig, axes = plt.subplots(len(AREAS), 2, figsize=(22, 3.4 * len(AREAS)), sharex=True, squeeze=False)
    for i, pn in enumerate(AREAS):
        d = d0[d0.area == pn]
        n_era = d.groupby("era").cluster_id.nunique()
        for j, (col, label) in enumerate(((wcol, WORKS_LABEL[wcol]),
                                          ("log_c", "citations received per year (log10 scale)"))):
            ax = axes[i, j]
            sns.violinplot(data=d, x="rel", y=col, hue="era", hue_order=order, order=rels, split=True, inner="quart",
                           cut=0, density_norm="width", common_norm=False, linewidth=0.4, ax=ax,
                           palette={order[0]: "#9ecae1", order[1]: "#fdae6b"}, legend=(i == 0 and j == 0))
            ax.axvline(rels.index(0), color="black", linestyle="--", linewidth=1.0, alpha=0.7, zorder=5)   # award year
            ax.grid(axis="y", linewidth=0.3, alpha=0.6)
            if col != "log_c":
                ax.set_ylim(0, max(1, d[col].quantile(0.99)))
            else:
                top = int(np.ceil(d.log_c.max())) if len(d) else 1
                ax.set_yticks(range(0, top + 1))
                ax.set_yticklabels(["0"] + [f"{10 ** k:,}" for k in range(1, top + 1)])
                ax.set_ylim(0, top)
            ax.set_ylabel(f"{pn} ({AREA_NAMES[pn]})\n" + ", ".join(f"{k}: {n_era.get(k, 0):,}" for k in order)
                          if j == 0 else "", fontsize=8)
            if i == 0:
                ax.set_title(label, fontsize=11)
            ax.set_xticks([rels.index(t) for t in range(rel_min, REL_MAX + 1, 5)])
            ax.set_xticklabels([str(t) for t in range(rel_min, REL_MAX + 1, 5)])
            ax.set_xlabel("")
    for ax in axes[-1]:
        ax.set_xlabel("year relative to award (0 = award year)")
    fig.suptitle(title, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out, dpi=120)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", choices=sorted(TIERS), default="ecr", help="fellowship tier (default ecr)")
    ap.add_argument("--commencement", action="store_true", help="award year = funding-commencement year")
    ap.add_argument("--violin", action="store_true", help="also draw split violins by award era")
    ap.add_argument("--cumulative", action="store_true", help="cumulative accepted works instead of works per year")
    args = ap.parse_args()
    tier, rel_min, prefix, tname = TIERS[args.tier]
    c = cohort(args.commencement, tier)
    py = person_years(c, rel_min)
    py.to_parquet(PROCESSED_DATA / f"{prefix}_person_years.parquet", index=False)
    roles = ", ".join(f"{k} {v:,}" for k, v in c.role_code.value_counts().items())
    yr = "commencement" if args.commencement else "announcement (commencement - 1)"
    wcol = "cum_works" if args.cumulative else "works"
    sfx = "_cumulative" if args.cumulative else ""
    out_png = FIG_DIR / f"{prefix}_works_citations_boxplot{sfx}.png"
    out_violin = FIG_DIR / f"{prefix}_works_citations_violin{sfx}.png"
    keep = py[py.area.isin(AREAS)]
    plot(keep, f"{tname} ARC fellows: output by year relative to award\n({len(c):,} linked ACIFs: {roles}; award year = {yr}; "
               f"years up to {LAST_FULL_YEAR}; years after the last accepted work dropped)", out_png, wcol, rel_min)
    print(out_png)
    if args.violin:
        plot_violin(keep, f"{tname} ARC fellows by year relative to award: left half awards announced up to {ERA_SPLIT_YEAR}, "
                          f"right half from {ERA_SPLIT_YEAR + 1}\n({len(c):,} linked ACIFs: {roles}; years up to {LAST_FULL_YEAR}; "
                          "years after the last accepted work dropped; works axis cut at the area's 99th percentile)",
                    out_violin, wcol, rel_min)
        print(out_violin)
    print(f"cohort {len(c):,}; by area: {c.area.value_counts(dropna=False).to_dict()}")


if __name__ == "__main__":
    main()
