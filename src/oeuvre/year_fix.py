"""
Step 2b of the oeuvre extractor (2026-10-27, user: "yes" to a 1970 guard): OpenAlex dates a work 1970 when its
source metadata carries no date -- 1970-01-01 is the start of Unix time. OpenAlex has ~1,004,000 works dated 1970
against 854,000 in 1969 and 926,000 in 1971; UTS ePress (DOI prefix 10.5130, publishing since the 2000s) alone has
427. Example: Matt Allen's "Power, pork and patronage" (10.5130/cjlg.v0i6.1617) is 2010 (publisher page), dated
1970. Before this step 1,086 accepted ACIF x work rows were dated 1970 (1,020 works, 542 ACIFs).

For each kept (ACIF, work) row dated PLACEHOLDER, in this order:
  orcid    the ACIF's own ORCID record (ORCID record cache) lists the work's DOI with another year -> that year
  scopus   a Scopus document with the DOI (00f's scopus_profile_documents) has another year -> that year
  late     the work's source has no other OpenAlex work before LATE_SOURCE -> year unknown (NULL)
Otherwise the year stays: most remaining 1970 works are genuine (old medical and science journals active then).
Runs on acif_works_kept.parquet in place, before step 3 (versions) -- whose publication_year is the earliest
year of a work's versions, so a 1970 version would otherwise pull the whole work to 1970. Adds two columns to
every row: publication_year_oax (OpenAlex's year) and year_fix (orcid / scopus / late / NULL).
"""

from __future__ import annotations

import os

import pandas as pd

from config.settings import DISKCACHE_DIR, OPENALEX_COMPACT_DIR, SCOPUS_EXTRACT_DIR
from src.oeuvre.orcid_works import _DOI_URL

PLACEHOLDER = 1970
LATE_SOURCE = 1976
DOCS = SCOPUS_EXTRACT_DIR / "scopus_profile_documents.parquet"


def orcid_doi_years(rec: dict) -> dict[str, int]:
    """DOI (lower case, no URL) -> publication year, from an ORCID /record's works summary."""
    out: dict[str, int] = {}
    for g in ((rec.get("activities-summary") or {}).get("works") or {}).get("group", []) or []:
        dois = set()
        for e in (g.get("external-ids") or {}).get("external-id", []) or []:
            if e.get("external-id-type") == "doi":
                v = ((e.get("external-id-normalized") or {}).get("value") or e.get("external-id-value") or "").lower()
                v = _DOI_URL.sub("", v.strip())
                if v:
                    dois.add(v)
        years = []
        for ws in g.get("work-summary", []) or []:
            y = (((ws.get("publication-date") or {}).get("year") or {}).get("value"))
            if y and str(y).isdigit():
                years.append(int(y))
        if dois and years:
            for d in dois:
                out.setdefault(d, min(years))
    return out


def fix_placeholder_years(con, path, acifs: pd.DataFrame, cache=None, docs=DOCS,
                          works=OPENALEX_COMPACT_DIR / "works") -> dict:
    """Correct PLACEHOLDER years in the kept works at `path` (module docstring); `acifs`: cluster_id, orcids.
    Returns counts by fix."""
    rows = con.execute(f"""SELECT DISTINCT cluster_id, work_idx, lower(doi) AS doi, source_id
                           FROM read_parquet('{path}') WHERE publication_year = {PLACEHOLDER}""").df()
    fix = {}
    if len(rows):
        if cache is None:
            import diskcache
            cache = diskcache.Cache(str(DISKCACHE_DIR / "orcid_records_authenticated"))
        orcids = dict(zip(acifs.cluster_id, acifs.orcids))
        by_acif: dict = {}
        for cid in rows.cluster_id.unique():
            years: dict[str, int] = {}
            for o in (list(orcids.get(cid)) if orcids.get(cid) is not None else []):
                rec = cache.get(o)
                if rec and (rec.get("orcid-identifier") or {}).get("path") in (None, o):
                    for d, y in orcid_doi_years(rec).items():
                        years.setdefault(d, y)
            by_acif[cid] = years
        sc = {}
        if os.path.exists(docs):
            s = con.execute(f"""SELECT lower(doi) AS doi, min(TRY_CAST(year AS INTEGER)) AS y
                                FROM read_parquet('{docs}') WHERE doi IS NOT NULL GROUP BY 1""").df()
            sc = dict(zip(s.doi, s.y))
        src = [int(x) for x in rows.source_id.dropna().unique()]
        late = set()
        if src:
            con.register("yf_src", pd.DataFrame({"source_id": src}))
            first = con.execute(f"""SELECT source_id, min(publication_year) FILTER (WHERE publication_year <> {PLACEHOLDER})
                                    FROM read_parquet('{works}/*.parquet')
                                    WHERE source_id IN (SELECT source_id FROM yf_src) GROUP BY 1""").fetchall()
            late = {s for s, y in first if y is None or y >= LATE_SOURCE}
        for r in rows.itertuples():
            y_o = by_acif.get(r.cluster_id, {}).get(r.doi) if isinstance(r.doi, str) else None
            y_s = sc.get(r.doi) if isinstance(r.doi, str) else None
            if y_o and y_o != PLACEHOLDER:
                fix[(r.cluster_id, r.work_idx)] = (int(y_o), "orcid")
            elif y_s and not pd.isna(y_s) and int(y_s) != PLACEHOLDER:
                fix[(r.cluster_id, r.work_idx)] = (int(y_s), "scopus")
            elif not pd.isna(r.source_id) and int(r.source_id) in late:
                fix[(r.cluster_id, r.work_idx)] = (None, "late")
    f = pd.DataFrame([(c, w, y, why) for (c, w), (y, why) in fix.items()],
                     columns=["cluster_id", "work_idx", "new_year", "year_fix"]).astype({"new_year": "Int64"})
    con.register("yf_fix", f)
    tmp = f"{path}.tmp"
    con.execute(f"""COPY (SELECT k.* REPLACE (CASE WHEN f.year_fix IS NULL THEN k.publication_year
                                                    ELSE f.new_year END AS publication_year),
                                 k.publication_year AS publication_year_oax, f.year_fix
                          FROM read_parquet('{path}') k
                          LEFT JOIN yf_fix f USING (cluster_id, work_idx)
                          ORDER BY k.cluster_id, k.work_idx) TO '{tmp}' (FORMAT parquet)""")
    os.replace(tmp, path)
    counts = {"dated_placeholder": len(rows)}
    counts.update(f.year_fix.value_counts().to_dict())
    counts["kept_placeholder"] = len(rows) - len(f)
    return counts
