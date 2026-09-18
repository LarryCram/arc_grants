"""
analysis/utils/acif_report.py

Report rendering over results.db (see analysis/utils/results_db.py for the table
definitions). Markdown is the only output format built so far -- `render_acif_markdown()`
returns a plain string precisely so a future PDF path (e.g. piping through pandoc) can wrap
it without this module needing to know about PDF at all.

Deliberately reads ONLY results.db, never the upstream pipeline parquet/duckdb files directly
-- the whole point of the results.db pivot is that a report reflects whatever was last
persisted there, and grows new sections as new tables (arc/oax link, oeuvres) are added to
that db, not by reaching around it.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb
import numpy as np
import pandas as pd
from tabulate import tabulate


def _as_list(x) -> list:
    """DuckDB LIST(STRUCT) columns come back from fetchdf() as numpy.ndarray when populated,
    but a NULL (e.g. a LEFT JOIN that matched nothing) comes back as a bare NA scalar (None or
    pd.NA) -- neither is a plain Python list, so `isinstance(x, list)` is wrong for BOTH cases
    (found 2026-09-18: an earlier fix that only handled the NA case broke the populated case,
    silently rendering real data as "(none)" everywhere). This normalizes both to a plain list,
    empty for the missing case."""
    if isinstance(x, np.ndarray):
        return list(x)
    if isinstance(x, list):
        return x
    return []


def category_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Counts by reliability_tier x resolution_status x selection_status -- the two axes
    settled on for selecting ACIF categories to work through (2026-09-17)."""
    return con.execute("""
        SELECT reliability_tier, resolution_status, selection_status, count(*) AS n
        FROM title
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """).fetchdf()


def list_cluster_ids(
    con: duckdb.DuckDBPyConnection,
    tier: str | None = None,
    status: str | None = None,
    selection_status: str | None = None,
    limit: int | None = None,
) -> list[str]:
    where, params = [], []
    if tier:
        where.append("reliability_tier = ?")
        params.append(tier)
    if status:
        where.append("resolution_status = ?")
        params.append(status)
    if selection_status:
        where.append("selection_status = ?")
        params.append(selection_status)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    limit_clause = f" LIMIT {int(limit)}" if limit else ""
    q = f"SELECT cluster_id FROM title {clause} ORDER BY cluster_id{limit_clause}"
    return con.execute(q, params).df()["cluster_id"].tolist()


def markdown_table(df: pd.DataFrame, floatfmt: str = "g", missingval: str = "") -> str:
    if df.empty:
        return "(none)"
    return tabulate(
        df, headers="keys", tablefmt="github", showindex=False,
        floatfmt=floatfmt, missingval=missingval,
    )


def render_acif_markdown(con: duckdb.DuckDBPyConnection, cluster_id: str) -> str:
    """Full markdown report for one ACIF, built entirely from results.db's title/arc tables."""
    title_df = con.execute("SELECT * FROM title WHERE cluster_id = ?", [cluster_id]).fetchdf()
    if title_df.empty:
        raise ValueError(f"No such cluster_id in results.db: {cluster_id!r}")
    t = title_df.iloc[0]
    grants = con.execute(
        "SELECT * FROM arc WHERE cluster_id = ? ORDER BY funding_commence_year", [cluster_id]
    ).fetchdf()
    candidates = con.execute(
        "SELECT * FROM oax_candidates WHERE cluster_id = ? ORDER BY works_count DESC NULLS LAST",
        [cluster_id],
    ).fetchdf()
    accepted = con.execute(
        """
        SELECT r.author_idx, r.full_name, r.total_score, r.orcid_mismatch, c.orcid AS candidate_orcid
        FROM oax_resolve r
        JOIN oax_candidates c ON c.cluster_id = r.cluster_id AND c.author_idx = r.author_idx
        WHERE r.cluster_id = ? AND r.status = 'accepted'
        ORDER BY c.works_count_global DESC
        """,
        [cluster_id],
    ).fetchdf()

    name = t["oax_full_name"] or "(OpenAlex identity unresolved)"
    lines = [f"# {name}", ""]
    lines.append(f"- ACIF: `{t['cluster_id']}`")
    lines.append(f"- Report generated: {t['report_generated_at']}")
    orcids = list(t["orcids"]) if t["orcids"] is not None else []
    lines.append(f"- ORCID: {', '.join(orcids) if orcids else '(none recorded)'}")
    lines.append("")

    lines.append("## ARC awards")
    if grants.empty:
        lines.append("(no grants on record)")
        lines.append(f"- Reliability tier: {t['reliability_tier']}  /  resolution status: {t['resolution_status']}")
    else:
        total = grants["funding_current"].sum(skipna=True)
        n_fellowships = int(grants["is_fellowship"].fillna(False).sum())
        lines.append(
            f"- {len(grants)} grant(s), {n_fellowships} fellowship award(s), "
            f"total recorded funding ${total:,.0f}"
        )
        lines.append(f"- Reliability tier: {t['reliability_tier']}  /  resolution status: {t['resolution_status']}")
    top_for = _as_list(t["top_for_codes"])
    if len(top_for) > 0:
        for_str = ", ".join(f"{e['name']} ({e['fraction']*100:.0f}%)" for e in top_for)
    else:
        for_str = "(none)"
    lines.append(f"- Top FOR fields: {for_str}")
    if not grants.empty:
        lines.append("")
        display = pd.DataFrame({
            "Year": grants["funding_commence_year"].apply(
                lambda v: str(int(v)) if pd.notna(v) else "?"
            ),
            "Grant": grants["grant_code"],
            "Role": grants["role_code"],
            "Fellowship": grants["is_fellowship"].apply(lambda v: "yes" if v else ""),
            "Amount": grants["funding_current"].apply(
                lambda v: f"${v:,.0f}" if pd.notna(v) else "?"
            ),
            "HEP": grants["hep_code"],
        })
        lines.append(markdown_table(display))
    lines.append("")

    lines.append("## ARC-OAX link")
    # Candidate count sourced from THIS table's own pool (AcifOaxLinker.block()) -- always
    # agrees with the table below by construction, since both come from the same query.
    lines.append(f"- {len(candidates)} candidate(s) in AcifOaxLinker's block() pool")
    top_oax_sf = _as_list(t["top_oax_subfields"])
    if len(top_oax_sf) > 0:
        sf_str = ", ".join(f"{e['name']} ({e['fraction']*100:.0f}%)" for e in top_oax_sf)
    else:
        sf_str = "(none)"
    lines.append(f"- Top OAX subfields (highest-scoring accepted candidate): {sf_str}")
    if not candidates.empty:
        lines.append("")
        display = pd.DataFrame({
            "author_idx": candidates["author_idx"],
            "Full name": candidates["full_name"],
            "Works (Global)": candidates["works_count_global"],
            "Works (AU)": candidates["works_count_au"],
            "Cited by": candidates["cited_by_count"],
            "H-index": candidates["h_index"],
            "ORCID": candidates["orcid"],
            "Subfield FD": candidates["subfield_fd"],
            "Institution FD": candidates["institution_fd"],
            "Coawardee": candidates["n_corroborating_coauthors"],
            "Given name": candidates["given_name_check"],
            "Provenance": candidates["provenance"],
        })
        # Per-column float format, by name (not trailing position -- fragile once non-FD columns
        # get appended after the FD ones): only the two FD columns need fixed 2 decimals, "g"
        # (tabulate's own default) everywhere else so integer-typed columns aren't affected if a
        # NULL upcasts one of them to float.
        fd_cols = {"Subfield FD", "Institution FD"}
        floatfmt = tuple(".2f" if c in fd_cols else "g" for c in display.columns)
        lines.append(markdown_table(display, floatfmt=floatfmt, missingval="n/a"))
    lines.append("")

    lines.append("## Works")
    lines.append(f"- {len(accepted)} accepted candidate(s) (oax_resolve, score >= 4/6; sorted by works_count DESC)")
    arc_orcids = list(t["orcids"]) if t["orcids"] is not None else []
    arc_orcid_str = ", ".join(arc_orcids) if arc_orcids else "(none recorded)"
    for _, row in accepted[accepted["orcid_mismatch"] == True].iterrows():  # noqa: E712
        candidate_orcid = str(row["candidate_orcid"]).rstrip("*")
        lines.append(
            f"- candidate included with mismatched orcid: {row['full_name']} "
            f"({row['author_idx']}) -- ARC: {arc_orcid_str} vs OAX: {candidate_orcid}"
        )
    lines.append("")
    if not accepted.empty:
        lines.append(markdown_table(accepted[["author_idx", "full_name", "total_score"]].rename(columns={
            "full_name": "Full name", "total_score": "Score",
        })))
        lines.append("")
    lines.append("- (oeuvre works themselves still pending)")
    lines.append("")

    return "\n".join(lines)
