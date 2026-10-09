"""
Step 4 of the oeuvre extractor (2026-10-07, user): the graph of each ACIF's works and its core.
Revives the design of analysis/utils/identity_clustering.py (August 2026, in git history): works
are joined by discrete, auditable shared facts, and groups are the connected components -- not a
weighted similarity graph with tuned clustering. Built set-based in DuckDB instead of a pairwise
Python scan.

Links (a work and a feature; two works sharing a feature are joined):
  coauthor     an author_idx on both works, other than the ACIF's own linked authors; only works
               with fewer than HYPER_AUTHORS authors (a 1,000-author paper links everyone)
  institution  an institution on the ACIF's own authorship of both works
  venue        the same source, except repositories and ebook platforms and sources with more than
               VENUE_MAX_WORKS works (arXiv, SSRN, PLoS ONE, Scientific Reports, Nature, ...)
A feature held by only one work links nothing and is dropped.

Components: label propagation over the work-feature graph, all ACIFs at once (each work and each
feature takes the smallest label among its neighbours, with pointer jumping, until nothing changes);
a component is named by its smallest work_idx.

Anchors (a work tied to the ARC record, not just to other works):
  anchor_coinvestigator  a co-author is an accepted OpenAlex author of one of the ACIF's ARC
                         co-investigators (coawardee_acif_ids)
  anchor_grant_university the ACIF's own authorship is at one of its grant universities (inst_ids)
                         within ANCHOR_YEAR_PAD years of its grant years
Core: the component with the most anchored works (then the most works); core_by says whether
anchors or size decided. Works outside the core are not rejected here: they go to the next step.
"""

from __future__ import annotations

import pandas as pd

from config.settings import ACIFS_ARC, OAX_LINK_DIR, OPENALEX_COMPACT_DIR, OPENALEX_DIR

HYPER_AUTHORS = 50
VENUE_MAX_WORKS = 100_000
VENUE_SKIP_TYPES = ("repository", "ebook platform")
ANCHOR_YEAR_PAD = 5
KINDS = {1: "coauthor", 2: "institution", 3: "venue"}


def acif_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """(acif rows: cluster_id, first_year, last_year, grant institution_idx list) and
    (cluster_id, coinvestigator author_idx) from acifs_arc and the linker's accepted links."""
    a = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "first_year", "last_year", "inst_ids", "coawardee_acif_ids"])
    a["grant_inst"] = a.inst_ids.map(lambda xs: [int(x.rsplit("/I", 1)[1]) for x in xs] if xs is not None else [])
    links = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet", columns=["cluster_id", "author_idx", "status"])
    acc = links[links.status.str.startswith("accept")][["cluster_id", "author_idx"]].astype({"author_idx": "int64"})
    co = a[["cluster_id", "coawardee_acif_ids"]].explode("coawardee_acif_ids").dropna()
    co = co.merge(acc.rename(columns={"cluster_id": "coawardee_acif_ids"}), on="coawardee_acif_ids")
    return a[["cluster_id", "first_year", "last_year", "grant_inst"]], co[["cluster_id", "author_idx"]].drop_duplicates()


def build_work_graph(con, works_path, out_path, acifs: pd.DataFrame, coinv: pd.DataFrame,
                     authorships=OPENALEX_COMPACT_DIR / "authorships", sources=OPENALEX_DIR / "sources.parquet",
                     max_iter: int = 100) -> dict:
    """Write one row per (ACIF, work) with its component, links, anchors and core flag; returns counts."""
    con.register("acif_in", acifs)
    con.register("coinv_in", coinv)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE gw AS
        SELECT dense_rank() OVER (ORDER BY cluster_id) AS cid, cluster_id, work_idx, publication_year AS y,
               authors_count, source_id, authorships
        FROM read_parquet('{works_path}')""")
    con.execute("""CREATE OR REPLACE TEMP TABLE own AS
                   SELECT DISTINCT cid, a.author_idx FROM gw, unnest(authorships) u(a)""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE coauth AS
        SELECT DISTINCT g.cid, g.work_idx, a.author_idx
        FROM read_parquet('{authorships}/*.parquet') a JOIN gw g USING (work_idx)
        WHERE a.author_idx IS NOT NULL AND g.authors_count < {HYPER_AUTHORS}
          AND NOT EXISTS (SELECT 1 FROM own o WHERE o.cid = g.cid AND o.author_idx = a.author_idx)""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE e0 AS
        SELECT cid, work_idx, 1 AS kind, author_idx AS fid FROM coauth
        UNION
        SELECT DISTINCT cid, work_idx, 2, i.institution_idx
        FROM gw, unnest(authorships) u(a), unnest(a.institutions) v(i) WHERE i.institution_idx IS NOT NULL
        UNION
        SELECT cid, work_idx, 3, source_id FROM gw JOIN read_parquet('{sources}') s ON s.source_idx = gw.source_id
        WHERE s.type NOT IN {VENUE_SKIP_TYPES} AND coalesce(s.works_count, 0) <= {VENUE_MAX_WORKS}""")
    con.execute("""CREATE OR REPLACE TEMP TABLE e AS
                   SELECT e0.* FROM e0 JOIN (SELECT cid, kind, fid FROM e0 GROUP BY ALL HAVING count(*) > 1)
                   USING (cid, kind, fid)""")
    # label propagation with pointer jumping
    con.execute("CREATE OR REPLACE TEMP TABLE lab AS SELECT cid, work_idx, work_idx AS l FROM gw")
    it = 0
    for it in range(1, max_iter + 1):
        con.execute("""CREATE OR REPLACE TEMP TABLE fl AS
                       SELECT e.cid, kind, fid, min(lab.l) AS l FROM e JOIN lab USING (cid, work_idx) GROUP BY ALL""")
        con.execute("""CREATE OR REPLACE TEMP TABLE lab2 AS
                       SELECT lab.cid, lab.work_idx, least(lab.l, coalesce(min(fl.l), lab.l)) AS l
                       FROM lab LEFT JOIN e USING (cid, work_idx) LEFT JOIN fl USING (cid, kind, fid)
                       GROUP BY lab.cid, lab.work_idx, lab.l""")
        con.execute("""CREATE OR REPLACE TEMP TABLE lab3 AS
                       SELECT a.cid, a.work_idx, least(a.l, b.l) AS l
                       FROM lab2 a JOIN lab2 b ON b.cid = a.cid AND b.work_idx = a.l""")
        changed = con.execute("""SELECT count(*) FROM lab3 JOIN lab USING (cid, work_idx)
                                 WHERE lab3.l <> lab.l""").fetchone()[0]
        con.execute("CREATE OR REPLACE TEMP TABLE lab AS SELECT * FROM lab3")
        if changed == 0:
            break
    else:
        raise SystemExit(f"label propagation did not converge in {max_iter} iterations")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE anch AS
        SELECT g.cid, g.work_idx,
               EXISTS (SELECT 1 FROM coauth c JOIN coinv_in ci ON ci.author_idx = c.author_idx
                       WHERE c.cid = g.cid AND c.work_idx = g.work_idx AND ci.cluster_id = g.cluster_id) AS anchor_coinvestigator,
               coalesce(len(list_intersect(flatten([[i.institution_idx FOR i IN a.institutions] FOR a IN g.authorships]),
                                           ai.grant_inst)) > 0
                        AND g.y BETWEEN ai.first_year - {ANCHOR_YEAR_PAD} AND ai.last_year + {ANCHOR_YEAR_PAD},
                        false) AS anchor_grant_university
        FROM gw g JOIN acif_in ai USING (cluster_id)""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE comp AS
        SELECT cid, l AS component, count(*) AS component_size,
               count(*) FILTER (WHERE anchor_coinvestigator OR anchor_grant_university) AS component_anchored
        FROM lab JOIN anch USING (cid, work_idx) GROUP BY 1, 2""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE core AS
        SELECT cid, arg_max(component, (component_anchored, component_size, -component)) AS core,
               max(component_anchored) > 0 AS by_anchor FROM comp GROUP BY 1""")
    con.execute(f"""
        COPY (
            SELECT g.cluster_id, g.work_idx, lab.l AS component, comp.component_size, comp.component_anchored,
                   lab.l = core.core AS in_core, CASE WHEN core.by_anchor THEN 'anchors' ELSE 'size' END AS core_by,
                   coalesce(n.coauthor, 0) AS coauthor_links, coalesce(n.institution, 0) AS institution_links,
                   coalesce(n.venue, 0) AS venue_links, anch.anchor_coinvestigator, anch.anchor_grant_university
            FROM gw g JOIN lab USING (cid, work_idx) JOIN anch USING (cid, work_idx)
            JOIN comp ON comp.cid = g.cid AND comp.component = lab.l JOIN core ON core.cid = g.cid
            LEFT JOIN (SELECT cid, work_idx, count(*) FILTER (WHERE kind = 1) AS coauthor,
                              count(*) FILTER (WHERE kind = 2) AS institution, count(*) FILTER (WHERE kind = 3) AS venue
                       FROM e GROUP BY 1, 2) n USING (cid, work_idx)
            ORDER BY g.cluster_id, g.work_idx
        ) TO '{out_path}' (FORMAT parquet)""")
    edges = dict(con.execute("SELECT kind, count(*) FROM e GROUP BY 1").fetchall())
    return {"iterations": it, **{f"links_{KINDS[k]}": v for k, v in edges.items()}}
