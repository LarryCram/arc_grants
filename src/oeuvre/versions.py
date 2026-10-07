"""
Step 3 of the oeuvre extractor (2026-10-07, user): reduce each ACIF's kept works to one row per
work, where a work may exist in OpenAlex as several records (preprint and published article, a
repository copy, a reprint as a book chapter, OpenAlex duplicates).

Versions: within one ACIF, two kept rows are versions of one work when they share a DOI (OpenAlex
duplicates) or a normalised title (src/oeuvre/title_norm.py); linked rows chain (A-B by DOI, B-C by
title: one work). A title links only when it is usable: at least MIN_TITLE_LEN characters, not
empty after normalising (non-Latin titles normalise to ""), and held by at most MAX_TITLE_WORKS
distinct works across all kept rows -- a title many works share is generic ("list of
contributors", "powered by nict", "highlights of this issue", per-month data deposits). No limit on
the years between versions: large gaps are mostly reprints and repository copies of one work.

Editions (user, 2026-10-07: updated editions are different works; the narrow definition): within
a version group, published copies (not a preprint, not in a repository, with a DOI) of one type
whose years differ are different editions when they are in the same source (Cochrane-style
updates) or both book chapters (revised handbook and living-reference chapters). Each edition
year starts an edition; every row of the group joins the latest edition year at or before its own
year (an earlier row, e.g. a preprint, joins the first edition). A conference paper and its journal
version, or an article reprinted in a book, stay one work.

Duplicate records (user, 2026-10-07): rows in the same source with the same volume, issue and
first page are one article recorded twice (e.g. a publisher DOI and a JSTOR DOI); among them only
the most cited can be the version of record.

One row per work (an edition counts as a work): the version of record's row (its work_idx, DOI,
source, type, authorships, fields), chosen by, in order: not in a repository (OpenAlex source type
'repository') and not without a source; not a less-cited duplicate record; type (TYPE_RANK:
article > review > book-chapter > book > report > dissertation > preprint); the most recent year;
most cited; lowest work_idx. Then:
  publication_year           the EARLIEST year of any version (when the work was done)
  vor_publication_year       the version of record's own year
  cited_by_count             the version of record's own count
  cited_by_count_versions    the sum over all versions (citations to a preprint are real too)
  n_versions, linked_by ('doi' / 'title' / 'doi+title'), versions (list of {work_idx, type,
  publication_year, doi, source_type, cited_by_count}, version of record first)
  version_group              lowest work_idx of the version group (shared by its editions)
  edition_year, n_editions   the edition's start year (null when the group has one edition)
"""

from __future__ import annotations

import pandas as pd

from config.settings import OPENALEX_DIR
from src.oeuvre import title_norm

SOURCES = OPENALEX_DIR / "sources.parquet"
MIN_TITLE_LEN = 20
MAX_TITLE_WORKS = 6
TYPE_RANK = {"article": 1, "review": 2, "book-chapter": 3, "book": 4, "report": 5, "dissertation": 6,
             "preprint": 7}


class _UnionFind:
    def __init__(self):
        self.parent: dict = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def version_groups(keys: pd.DataFrame) -> pd.DataFrame:
    """keys: (cluster_id, work_idx, key) with key 'd:<doi>' or 't:<title>' -- only keys held by 2+
    rows of one ACIF. Returns (cluster_id, work_idx, group_work_idx, linked_by) for every row in a
    group of 2+; group_work_idx is the group's lowest work_idx."""
    uf = _UnionFind()
    kinds: dict = {}
    for (cid, key), sub in keys.groupby(["cluster_id", "key"]):
        w = sorted(sub.work_idx)
        for x in w[1:]:
            uf.union((cid, w[0]), (cid, x))
        kinds.setdefault(cid, []).append((w, key[0]))
    rows = [(cid, w, uf.find((cid, w))[1]) for (cid, w) in list(uf.parent)]
    out = pd.DataFrame(rows, columns=["cluster_id", "work_idx", "group_work_idx"])
    link = {}
    for cid, items in kinds.items():
        for w, k in items:
            g = uf.find((cid, w[0]))
            link.setdefault(g, set()).add({"d": "doi", "t": "title"}[k])
    out["linked_by"] = [("+".join(sorted(link[(c, g)]))) for c, g in zip(out.cluster_id, out.group_work_idx)]
    return out


def reduce_versions(con, in_path, out_path, sources=SOURCES) -> dict:
    """Write one row per (ACIF, work) from the kept works at `in_path`; returns counts."""
    title_norm.register(con)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE vk AS
        SELECT k.*, s.type AS source_type,
               CASE WHEN length(k.title) >= {MIN_TITLE_LEN} THEN norm_title(k.title) END AS title_key
        FROM read_parquet('{in_path}') k LEFT JOIN read_parquet('{sources}') s ON s.source_idx = k.source_id""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE usable_titles AS
        SELECT title_key FROM vk WHERE title_key <> '' GROUP BY 1 HAVING count(DISTINCT work_idx) <= {MAX_TITLE_WORKS}""")
    keys = con.execute("""
        WITH k AS (SELECT cluster_id, work_idx, 'd:' || lower(doi) AS key FROM vk WHERE doi IS NOT NULL
                   UNION ALL
                   SELECT cluster_id, work_idx, 't:' || title_key FROM vk JOIN usable_titles USING (title_key))
        SELECT * FROM k WHERE (cluster_id, key) IN
            (SELECT (cluster_id, key) FROM k GROUP BY 1 HAVING count(DISTINCT work_idx) > 1)""").fetchdf()
    con.register("groups_df", version_groups(keys) if len(keys) else
                 pd.DataFrame(columns=["cluster_id", "work_idx", "group_work_idx", "linked_by"]))
    rank = " ".join(f"WHEN '{t}' THEN {r}" for t, r in TYPE_RANK.items())
    con.execute("""
        CREATE OR REPLACE TEMP TABLE v0 AS
        SELECT vk.*, coalesce(g.group_work_idx, vk.work_idx) AS grp, g.linked_by,
               CASE WHEN source_id IS NOT NULL AND first_page IS NOT NULL
                    THEN concat_ws('|', source_id, coalesce(volume, ''), coalesce(issue, ''), first_page)
                    ELSE CAST(vk.work_idx AS VARCHAR) END AS place,
               (type <> 'preprint' AND source_type IS DISTINCT FROM 'repository' AND doi IS NOT NULL) AS published
        FROM vk LEFT JOIN groups_df g USING (cluster_id, work_idx)""")
    # editions: published places of one type in different years, in the same source or both book chapters
    con.execute("""
        CREATE OR REPLACE TEMP TABLE places AS
        SELECT cluster_id, grp, place, min(publication_year) AS y, arg_max(type, cited_by_count) AS type,
               any_value(source_id) AS source_id
        FROM v0 WHERE published AND linked_by IS NOT NULL
        GROUP BY 1, 2, 3""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE edition_years AS
        SELECT DISTINCT cluster_id, grp, unnest([a.y, b.y]) AS ey
        FROM places a JOIN places b USING (cluster_id, grp)
        WHERE a.type = b.type AND a.y < b.y AND (a.source_id = b.source_id OR a.type = 'book-chapter')""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE vr AS
        WITH ed AS (
            SELECT v.cluster_id, v.work_idx,
                   coalesce(max(e.ey) FILTER (WHERE e.ey <= v.publication_year), min(e.ey)) AS edition_year
            FROM v0 v JOIN edition_years e USING (cluster_id, grp) GROUP BY 1, 2),
        e2 AS (SELECT v0.*, ed.edition_year FROM v0 LEFT JOIN ed USING (cluster_id, work_idx)),
        e3 AS (SELECT *, row_number() OVER (PARTITION BY cluster_id, grp, edition_year, place
                                            ORDER BY cited_by_count DESC NULLS LAST, work_idx) AS place_rank
               FROM e2)
        SELECT *, row_number() OVER (PARTITION BY cluster_id, grp, edition_year ORDER BY
                   (source_type = 'repository' OR source_type IS NULL), place_rank > 1, CASE type {rank} ELSE 99 END,
                   publication_year DESC NULLS LAST, cited_by_count DESC NULLS LAST, work_idx) AS vrank,
               coalesce(n.n, 1) AS n_editions_raw
        FROM e3 LEFT JOIN (SELECT cluster_id, grp, count(*) AS n FROM edition_years GROUP BY 1, 2) n
             USING (cluster_id, grp)""")
    con.execute(f"""
        COPY (
            WITH agg AS (
                SELECT cluster_id, grp, edition_year, min(publication_year) AS first_year,
                       sum(cited_by_count) AS cites, count(*) AS n_versions,
                       list({{'work_idx': work_idx, 'type': type, 'publication_year': publication_year, 'doi': doi,
                              'source_type': source_type, 'cited_by_count': cited_by_count}} ORDER BY vrank) AS versions
                FROM vr GROUP BY 1, 2, 3)
            SELECT v.* EXCLUDE (grp, vrank, title_key, publication_year, linked_by, place, published, place_rank,
                                n_editions_raw, edition_year),
                   a.first_year AS publication_year, v.publication_year AS vor_publication_year,
                   a.cites AS cited_by_count_versions, a.n_versions, v.linked_by, a.versions,
                   v.grp AS version_group, v.edition_year, greatest(v.n_editions_raw, 1) AS n_editions
            FROM vr v JOIN agg a ON a.cluster_id = v.cluster_id AND a.grp = v.grp
                                 AND a.edition_year IS NOT DISTINCT FROM v.edition_year
            WHERE v.vrank = 1
            ORDER BY v.cluster_id, v.work_idx
        ) TO '{out_path}' (FORMAT parquet)""")
    one = lambda q: con.execute(q).fetchone()
    return dict(zip(("rows_in", "rows_out", "groups", "split_groups", "duplicate_places", "titles_refused"),
                    (*one("SELECT count(*), count(DISTINCT (cluster_id, grp, edition_year)), "
                          "count(DISTINCT (cluster_id, grp)) FILTER (WHERE linked_by IS NOT NULL), "
                          "count(DISTINCT (cluster_id, grp)) FILTER (WHERE edition_year IS NOT NULL), "
                          "count(DISTINCT (cluster_id, place)) FILTER (WHERE place_rank > 1) FROM vr"),
                     one(f"SELECT count(DISTINCT title_key) FROM vk WHERE title_key <> '' AND title_key NOT IN "
                         f"(SELECT title_key FROM usable_titles)")[0])))
