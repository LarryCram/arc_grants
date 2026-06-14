"""
Exploratory analysis and plots — grows as ideas develop.

Reads only persisted parquets; no OAX scans.

Usage:
  python analysis/05_explore.py [--sample 10|1000]
"""

import argparse
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from config.settings import PROCESSED_DATA, OUTPUT_ROOT

ANALYSIS_OUT = OUTPUT_ROOT / "analysis"
PERSONS      = str(PROCESSED_DATA / "arc_persons.parquet")

def paths(sample_n):
    s = f"_sample-{sample_n}" if sample_n else ""
    return {
        "oeuvres":        str(ANALYSIS_OUT / f"oeuvres{s}.parquet"),
        "annual":         str(ANALYSIS_OUT / f"annual_metrics{s}.parquet"),
        "collab":         str(ANALYSIS_OUT / f"collab_metrics{s}.parquet"),
        "au_annual":      str(ANALYSIS_OUT / "au_annual.parquet"),
        "world_annual":   str(ANALYSIS_OUT / "world_annual.parquet"),
    }


def main(sample_n=None):
    label = f"sample-{sample_n}" if sample_n else "full"
    p = paths(sample_n)

    con = duckdb.connect()

    print(f"=== 05_explore ({label}) ===\n")

    # ── Summary counts ─────────────────────────────────────────────────────
    if Path(p["oeuvres"]).exists():
        row = con.execute(f"""
            SELECT COUNT(DISTINCT arc_id) AS persons,
                   COUNT(DISTINCT work_idx) AS works,
                   MIN(publication_year) AS yr_min,
                   MAX(publication_year) AS yr_max
            FROM read_parquet('{p["oeuvres"]}')
        """).fetchone()
        print(f"Oeuvres: {row[0]:,} persons, {row[1]:,} works, years {row[2]}–{row[3]}")

    # ── Top 20 by total snapshot citations ─────────────────────────────────
    if Path(p["oeuvres"]).exists():
        print("\nTop 20 ARC persons by total snapshot citations:")
        rows = con.execute(f"""
            SELECT o.arc_id, p.full_names[1] AS name,
                   COUNT(DISTINCT o.work_idx) AS n_works,
                   SUM(o.cited_by_count) AS total_cites,
                   MIN(o.publication_year) AS first_pub
            FROM read_parquet('{p["oeuvres"]}') o
            JOIN read_parquet('{PERSONS}') p ON p.cluster_id = o.arc_id
            GROUP BY o.arc_id, p.full_names
            ORDER BY total_cites DESC NULLS LAST
            LIMIT 20
        """).fetchall()
        print(f"  {'Name':40s}  {'Works':>6}  {'Cites':>10}  {'First pub':>10}")
        for r in rows:
            print(f"  {str(r[1]):40s}  {r[2]:>6,}  {r[3]:>10,}  {r[4]:>10}")

    # ── H-index distribution (most recent year) ────────────────────────────
    if Path(p["annual"]).exists():
        print("\nH-index distribution at 2024:")
        hdist = con.execute(f"""
            SELECT
                CASE
                    WHEN h_index < 10  THEN '0-9'
                    WHEN h_index < 20  THEN '10-19'
                    WHEN h_index < 30  THEN '20-29'
                    WHEN h_index < 50  THEN '30-49'
                    WHEN h_index < 100 THEN '50-99'
                    ELSE '100+'
                END AS bucket,
                COUNT(*) AS n_persons
            FROM read_parquet('{p["annual"]}')
            WHERE year = 2024 AND h_index IS NOT NULL
            GROUP BY bucket
            ORDER BY MIN(h_index)
        """).fetchall()
        for r in hdist:
            bar = '█' * (r[1] // max(1, rows[0][2] // 40))
            print(f"  h {r[0]:>6}:  {r[1]:>5,}  {bar}")

    # ── Academic age distribution ──────────────────────────────────────────
    if Path(p["annual"]).exists():
        print("\nAcademic age (2026 − first_pub_year) distribution:")
        age = con.execute(f"""
            SELECT
                2026 - first_pub_year AS acad_age,
                COUNT(*) AS n
            FROM (
                SELECT arc_id, MIN(first_pub_year) AS first_pub_year
                FROM read_parquet('{p["annual"]}')
                GROUP BY arc_id
            )
            WHERE acad_age BETWEEN 0 AND 60
            GROUP BY acad_age ORDER BY acad_age
        """).fetchall()
        if age:
            max_n = max(r[1] for r in age)
            for r in age:
                bar = '█' * max(1, round(30 * r[1] / max_n))
                print(f"  age {r[0]:3d}:  {r[1]:>5,}  {bar}")

    # ── Time-series plot: ARC vs AU vs World publications ─────────────────
    if Path(p["annual"]).exists() and Path(p["au_annual"]).exists() and Path(p["world_annual"]).exists():
        print("\nGenerating time-series plots...")

        arc = con.execute(f"""
            SELECT year, SUM(n_pubs) AS n_pubs, SUM(total_citations_cumul) AS cites
            FROM read_parquet('{p["annual"]}')
            WHERE year BETWEEN 2000 AND 2024
            GROUP BY year ORDER BY year
        """).df()

        au = con.execute(f"""
            SELECT year, n_pubs, total_citations
            FROM read_parquet('{p["au_annual"]}')
            WHERE year BETWEEN 2000 AND 2024
        """).df()

        world = con.execute(f"""
            SELECT year, n_pubs
            FROM read_parquet('{p["world_annual"]}')
            WHERE year BETWEEN 2000 AND 2024
        """).df()

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        fig.suptitle(f"ARC vs AU vs World — publications over time ({label})")

        ax = axes[0]
        ax.plot(arc["year"],   arc["n_pubs"],   label="ARC CIFs")
        ax.plot(au["year"],    au["n_pubs"],     label="All AU")
        ax.set_title("Annual publications")
        ax.set_xlabel("Year")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.legend()

        ax2 = axes[1]
        ax2.plot(arc["year"], arc["cites"], label="ARC CIFs (cumul)")
        ax2.set_title("Cumulative snapshot citations")
        ax2.set_xlabel("Year")
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax2.legend()

        plt.tight_layout()
        out_fig = str(ANALYSIS_OUT / f"timeseries_{label}.png")
        plt.savefig(out_fig, dpi=150)
        print(f"  Saved: {out_fig}")
        plt.show()

    # ── Collaboration: top countries ───────────────────────────────────────
    if Path(p["collab"]).exists():
        print("\nTop 15 collaborating countries (by distinct ARC persons):")
        ctry = con.execute(f"""
            SELECT country_code,
                   COUNT(DISTINCT arc_id) AS n_arc_persons,
                   SUM(n_shared_works) AS n_works
            FROM read_parquet('{p["collab"]}')
            WHERE country_code != 'AU' AND country_code IS NOT NULL
            GROUP BY country_code
            ORDER BY n_arc_persons DESC
            LIMIT 15
        """).fetchall()
        print(f"  {'Country':6s}  {'ARC persons':>12}  {'Shared works':>12}")
        for r in ctry:
            print(f"  {r[0]:6s}  {r[1]:>12,}  {r[2]:>12,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, choices=[10, 1000], default=None)
    args = parser.parse_args()
    main(sample_n=args.sample)
