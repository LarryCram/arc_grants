"""
Fellowship bibliometric analysis.

Plot 1: Distribution of academic ages (2025 − first_pub_year) by fellowship role_code.
Plot 2: Median annual publications vs Δyear (publication_year − award_year) by role_code,
         among fellows who published in each year (inactive fellows excluded).

Reads oeuvres (sample or full) — no OAX scan needed.
Output: PNG plots in ANALYSIS_OUT.

Usage:
  python analysis/06_analyse_fellowships.py            # sample-1000
  python analysis/06_analyse_fellowships.py --full     # full oeuvres
"""

import argparse
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

from config.settings import PROCESSED_DATA, OUTPUT_ROOT

ANALYSIS_OUT = OUTPUT_ROOT / "analysis"
PLOTS_OUT    = Path(__file__).parent.parent / "plots"
INV_RAW      = str(PROCESSED_DATA / "investigators_raw.parquet")
GRANT_MAP    = str(PROCESSED_DATA / "arc_grant_cluster_map.parquet")
GRANTS_FLAT  = str(PROCESSED_DATA / "grants_flat.parquet")

MIN_PUB_YEAR = 1950
MAX_PUB_YEAR = 2026

# Fellowship role codes to include and their display labels
ROLES = {
    "FF": "Federation Fellow",
    "FL": "Laureate Fellow",
    "FT": "Future Fellow",
    "DECRA": "DECRA",
    "APF": "APF",
    "APD": "ARC Postdoctoral",
    "APDI": "ARC Postdoc Industry",
}

COLORS = plt.cm.tab10.colors


def oeuvres_path(sample_n):
    if sample_n:
        return str(ANALYSIS_OUT / f"oeuvres_sample-{sample_n}.parquet")
    return str(ANALYSIS_OUT / "oeuvres.parquet")


def load_data(con, oeuvres):
    """Build fellows table and publication data."""

    # Fellows: is_fellowship=True, role_code in ROLES, resolved and in oeuvres
    con.execute(f"""
    CREATE TABLE fellows AS
    SELECT DISTINCT
        ir.role_code,
        gf.funding_commence_year::INT AS award_year,
        m.cluster_id AS arc_id
    FROM read_parquet('{INV_RAW}') ir
    JOIN read_parquet('{GRANT_MAP}') m ON m.unique_id = ir.unique_id
    JOIN read_parquet('{GRANTS_FLAT}') gf ON gf.grant_code = ir.grant_code
    WHERE ir.is_fellowship = True
      AND ir.role_code IN ({','.join(f"'{r}'" for r in ROLES)})
      AND m.cluster_id IN (SELECT DISTINCT arc_id FROM read_parquet('{oeuvres}'))
    """)

    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT arc_id), COUNT(DISTINCT role_code) FROM fellows").fetchone()
    print(f"  Fellows in oeuvres: {n[0]} rows, {n[1]} persons, {n[2]} role types")
    con.execute("SELECT role_code, COUNT(DISTINCT arc_id) AS n FROM fellows GROUP BY 1 ORDER BY 1")
    for row in con.fetchall():
        print(f"    {row[0]:8s} {row[1]:4d}")

    # First pub year per person (plausible years only)
    con.execute(f"""
    CREATE TABLE first_pubs AS
    SELECT arc_id, MIN(publication_year) AS first_pub_year
    FROM read_parquet('{oeuvres}')
    WHERE publication_year BETWEEN {MIN_PUB_YEAR} AND {MAX_PUB_YEAR}
    GROUP BY arc_id
    """)

    # Annual pub counts per person
    con.execute(f"""
    CREATE TABLE annual_pubs AS
    SELECT arc_id, publication_year, COUNT(DISTINCT work_idx) AS n_pubs
    FROM read_parquet('{oeuvres}')
    WHERE publication_year BETWEEN {MIN_PUB_YEAR} AND {MAX_PUB_YEAR}
    GROUP BY arc_id, publication_year
    """)


def plot_academic_age(con, out_dir, label):
    """Histogram of award_year − first_pub_year (academic age at time of award) per role_code."""

    rows = con.execute("""
        SELECT f.role_code, f.award_year - fp.first_pub_year AS academic_age
        FROM fellows f
        JOIN first_pubs fp ON fp.arc_id = f.arc_id
        WHERE fp.first_pub_year IS NOT NULL
          AND f.award_year IS NOT NULL
        ORDER BY f.role_code, academic_age
    """).fetchall()

    by_role = {}
    for role, age in rows:
        by_role.setdefault(role, []).append(age)

    roles = sorted(by_role)
    if not roles:
        print("  No academic age data — skipping plot 1")
        return

    fig, axes = plt.subplots(1, len(roles), figsize=(3.5 * len(roles), 4), sharey=False)
    if len(roles) == 1:
        axes = [axes]

    for ax, (role, color) in zip(axes, zip(roles, COLORS)):
        ages = by_role[role]
        max_age = max(ages)
        bins = list(range(-10, max_age + 2))
        ax.hist(ages, bins=bins, color=color, edgecolor="white", linewidth=0.3)
        ax.set_xlim(-10, max_age + 1)
        ax.set_title(f"{ROLES.get(role, role)}\n(n={len(ages)})", fontsize=10)
        ax.set_xlabel("Academic age at award\n(award_year − first_pub_year)")
        median_age = np.median(ages)
        ax.axvline(median_age, color="black", linestyle="--", linewidth=1, alpha=0.7)
        # tick labels: only 0, median, and top end
        ticks = sorted({0, int(round(median_age)), max_age})
        ax.set_xticks(ticks)
        ax.set_xticklabels([str(t) for t in ticks])

    axes[0].set_ylabel("Number of fellows")
    fig.suptitle("Academic age at time of award by fellowship scheme", fontsize=12, y=1.01)
    fig.tight_layout()

    out = out_dir / f"fellowship_academic_age_{label}.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot 1 → {out}")


def plot_pub_trajectory(con, out_dir, label):
    """Median publications per year vs Δ(year − award_year), per role_code.

    Only persons who published in a given year contribute to that year's median.
    Inactive persons (zero pubs that year) are excluded from numerator and denominator.
    """

    rows = con.execute("""
        SELECT f.role_code,
               ap.publication_year - f.award_year AS delta_year,
               MEDIAN(ap.n_pubs)                  AS median_pubs,
               COUNT(DISTINCT f.arc_id)            AS n_fellows
        FROM fellows f
        JOIN first_pubs fp ON fp.arc_id = f.arc_id
        JOIN annual_pubs ap ON ap.arc_id = f.arc_id
        WHERE ap.publication_year - f.award_year BETWEEN -10 AND 15
          AND ap.publication_year >= fp.first_pub_year
          AND f.award_year >= 2015
        GROUP BY f.role_code, delta_year
        HAVING COUNT(DISTINCT f.arc_id) >= 2
        ORDER BY f.role_code, delta_year
    """).fetchall()

    by_role = {}
    for role, delta, median_pubs, n in rows:
        by_role.setdefault(role, {"delta": [], "median": [], "n": []})
        by_role[role]["delta"].append(delta)
        by_role[role]["median"].append(median_pubs)
        by_role[role]["n"].append(n)

    roles = sorted(by_role)
    if not roles:
        print("  No trajectory data — skipping plot 2")
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    for role, color in zip(roles, COLORS):
        d = by_role[role]
        ax.plot(d["delta"], d["median"], label=f"{ROLES.get(role, role)} (n≥{min(d['n'])})",
                color=color, linewidth=2, marker="o", markersize=3)

    ax.axvline(0, color="grey", linestyle="--", linewidth=1, alpha=0.6)
    ax.text(0.3, ax.get_ylim()[1] * 0.98, "award\nyear", fontsize=8,
            color="grey", va="top")
    ax.set_xlabel("Years relative to fellowship award year (Δ)")
    ax.set_ylabel("Median publications per year (active publishers)")
    ax.set_title("Publication productivity around fellowship award year")
    ax.legend(fontsize=9, loc="upper left")
    ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    fig.tight_layout()

    out = out_dir / f"fellowship_pub_trajectory_{label}.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot 2 → {out}")


def main(sample_n=None):
    label   = f"sample-{sample_n}" if sample_n else "full"
    oeuvres = oeuvres_path(sample_n)

    if not Path(oeuvres).exists():
        print(f"ERROR: {oeuvres} not found — run 01_fetch_oeuvres.py first")
        sys.exit(1)

    print(f"=== 06_analyse_fellowships ({label}) ===")
    con = duckdb.connect()
    ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)

    PLOTS_OUT.mkdir(parents=True, exist_ok=True)
    load_data(con, oeuvres)
    plot_academic_age(con, PLOTS_OUT, label)
    plot_pub_trajectory(con, PLOTS_OUT, label)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--sample", type=int, choices=[10, 1000], default=1000)
    args = parser.parse_args()
    main(sample_n=None if args.full else args.sample)
