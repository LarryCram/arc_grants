"""
Step 5 of the oeuvre extractor (2026-10-07, user): class every (ACIF, work) as accept / reject /
unsure, accepting liberally. Rules settle most works; the doubtful remainder becomes requests for
Gemini (src/oeuvre/gemini.py, run separately with a budget by src/03a_gemini_judge.py), whose saved
verdicts are applied here on the next run. Until a request is answered its works are 'pending'.

Reference-work entries first (2026-10-08, user): OpenAlex sometimes gives every chapter of a
reference work (an encyclopedia) the whole book's contributor list, so a person who wrote one entry
holds hundreds. A book chapter is such an entry when it has REF_AUTHORS+ authors, or REF_AUTHORS_MULTI+
authors and the ACIF holds REF_SAME_BOOK+ such chapters of the same book (the book is the ISBN in the
DOI). Below 20 authors chapters are claimed on people's ORCID lists as often as small ones (2026-10-08).
  accept  'reference work: on ORCID list'  the entry's DOI is on the person's own ORCID record
  accept  'reference work: one per book'   otherwise one entry per (ACIF, book) stands for the
                                            contribution (lowest work_idx; its title is not
                                            necessarily the person's entry)
  reject  'reference work: collapsed'      the other entries of that book

Then, in order (the core and components come from step 4, acif_work_graph.parquet):
  accept  'core'                works in the ACIF's anchored core
  accept  'anchored component'  another component holding an anchored work
  reject  'before career'       published more than PRE_CAREER years before both the first grant
                                and the core's first work
  reject  'namesake component'  an unanchored component of BIG_COMPONENT+ works whose works mostly
                                (>= SAME_YEARS) fall in the core's active years (p10-p90) while its
                                field mix differs (cosine < SIMILAR_FIELD): the same years at other
                                places in another field means another person
  request 'component'           any other unanchored component of BIG_COMPONENT+ works: Gemini is
                                asked whether it is the same person as the core
  accept  'fits core'           smaller components and isolated works whose top field is one of the
                                core's fields (>= FIELD_MIN_SHARE of core works) or unknown, within
                                FIT_PAD years of the core's years
  request 'works'               the rest, sent per ACIF as lists of up to MAX_WORKS_PER_REQUEST works
Gemini verdicts: component 'same' -> accept, works 'in' -> accept; 'different' / 'out' with high
confidence -> reject; anything else -> unsure. Per-work verdicts are kept per (ACIF, work): a work
already judged is not sent again even if the candidate list around it changes.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter

import pandas as pd

from config.settings import OPENALEX_COMPACT_DIR, OPENALEX_DIR

PRE_CAREER = 15
FIT_PAD = 5
FIELD_MIN_SHARE = 0.05
BIG_COMPONENT = 5
SAME_YEARS = 0.5
SIMILAR_FIELD = 0.5
PROMPT_VERSION = "2026-10-07a"
MAX_LIST = 10
REF_AUTHORS = 50
REF_AUTHORS_MULTI = 20
REF_SAME_BOOK = 3
MAX_WORKS_PER_REQUEST = 40

COMPONENT_PROMPT = """You are checking whether two groups of publications belong to the same researcher.

Both groups come from OpenAlex author records that carry one Australian researcher's ORCID. OpenAlex sometimes attaches a namesake's works to the wrong record. Group A (the core) is tied to the researcher's Australian Research Council (ARC) grants: its works are co-authored with the researcher's ARC co-investigators or written at the grant university in the grant years. Group B shares no co-author, institution or specialised journal with group A.

Decide whether group B is the same person as group A (for example the researcher's earlier or later career elsewhere, or a separate line of work) or a different person with the same name.

RESEARCHER (from ARC grants):
{ARC}

GROUP A (core):
{CORE}

GROUP B:
{COMPONENT}

Rules:
- Careers move between places and topics; a different place or field alone is not proof of a different person, but the same years at unconnected places in an unrelated field is strong evidence of one.
- A different full first name or different middle initials in the printed names is evidence of a different person; format differences (initials, order, punctuation) are not.
- Base the decision on the evidence given. You may use general knowledge (what a field or journal covers, that a namesake exists), but say so in the reason.

Answer with JSON only: {{"verdict": "same" | "different" | "unsure", "confidence": "high" | "medium" | "low", "reason": "one or two short sentences"}}"""

WORKS_PROMPT = """You are checking a researcher's publication list for works that belong to a different person.

The researcher is an Australian Research Council (ARC) grant holder. The works below come from OpenAlex author records that carry this researcher's ORCID, so nearly all of them are genuinely theirs. OpenAlex sometimes attaches a namesake's paper to the wrong person, though, and your task is to find those few. Expect most works to be the researcher's.

RESEARCHER (from ARC grants):
{ARC}

THE RESEARCHER'S CORE PUBLICATIONS (tied to the ARC grants):
{CORE}

WORKS TO CHECK:
{WORKS}

For each work, decide:
- "in": consistent with this researcher (field, venue, period, affiliations, co-authors, name), or no real reason to doubt it.
- "out": clear evidence it is another person's work. Examples: a different full first name is printed; the topic belongs to an unrelated discipline AND nothing else (co-authors, affiliation, venue) connects it to the researcher; it predates any plausible career for this researcher; the affiliation is somewhere the researcher has no connection with, in an unrelated field.
- "unsure": genuinely mixed evidence.

Rules:
- Base each decision on the evidence given here. You may use general knowledge (for example, what a field or venue covers, or that a namesake exists), but say so in the reason.
- Researchers' careers are broad: an unusual field alone is not enough for "out". Prefaces, book reviews, introductions, commentary pieces and media articles in the researcher's area are "in".
- Missing affiliation or a single author is not evidence on its own.
- Name format differences (initials, "Family Given" order, punctuation) are not evidence.

Answer with JSON only, no other text: a list with one object per work, in the order given:
[{{"id": 1, "verdict": "in" | "out" | "unsure", "confidence": "high" | "medium" | "low", "reason": "one short sentence"}}]"""


def _rules(con, works_path, graph_path) -> None:
    """Temp table cls: one row per (ACIF, work) with rule decision or request kind."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cw AS
        SELECT g.*, w.publication_year AS y, w.fields[1].name AS top_field, w.title, w.type, w.source_id, w.doi,
               w.authors_count, w.cited_by_count, w.authorships, a.first_year
        FROM read_parquet('{graph_path}') g JOIN read_parquet('{works_path}') w USING (cluster_id, work_idx)
        JOIN acif_in a USING (cluster_id)""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE core_stats AS
        SELECT cluster_id, min(y) AS core_min, max(y) AS core_max, quantile_cont(y, 0.1) AS core_p10,
               quantile_cont(y, 0.9) AS core_p90, count(*) AS core_n
        FROM cw WHERE in_core GROUP BY 1""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE core_fields AS
        SELECT cw.cluster_id, cw.top_field FROM cw JOIN core_stats s USING (cluster_id)
        WHERE cw.in_core AND cw.top_field IS NOT NULL
        GROUP BY cw.cluster_id, cw.top_field, s.core_n HAVING count(*) >= {FIELD_MIN_SHARE} * s.core_n""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE comp_stats AS
        WITH big AS (SELECT * FROM cw WHERE NOT in_core AND component_anchored = 0 AND component_size >= {BIG_COMPONENT}),
             cf AS (SELECT cluster_id, component, top_field, count(*) k FROM big WHERE top_field IS NOT NULL GROUP BY ALL),
             kf AS (SELECT cluster_id, top_field, count(*) k FROM cw WHERE in_core AND top_field IS NOT NULL GROUP BY ALL),
             kn AS (SELECT cluster_id, sqrt(sum(k * k)) nk FROM kf GROUP BY 1),
             cos AS (SELECT cf.cluster_id, cf.component,
                            sum(cf.k * coalesce(kf.k, 0)) / (sqrt(sum(cf.k * cf.k)) * any_value(kn.nk)) AS field_cos
                     FROM cf LEFT JOIN kf USING (cluster_id, top_field) JOIN kn USING (cluster_id) GROUP BY 1, 2)
        SELECT b.cluster_id, b.component, avg((b.y BETWEEN s.core_p10 AND s.core_p90)::int) AS same_years,
               any_value(coalesce(cos.field_cos, 0)) AS field_cos
        FROM big b JOIN core_stats s USING (cluster_id) LEFT JOIN cos USING (cluster_id, component) GROUP BY 1, 2""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ref AS
        WITH ch AS (SELECT cluster_id, work_idx, authors_count,
                           replace(regexp_extract(doi, '(97[89][0-9-]{{10,14}})', 1), '-', '') AS isbn,
                           EXISTS (SELECT 1 FROM orcid_dois o WHERE o.cluster_id = cw.cluster_id
                                   AND o.doi = lower(cw.doi)) AS on_orcid
                    FROM cw WHERE type = 'book-chapter' AND doi IS NOT NULL AND authors_count >= {REF_AUTHORS_MULTI}),
             ch2 AS (SELECT *, count(*) OVER (PARTITION BY cluster_id, isbn) AS same_book FROM ch WHERE isbn <> ''),
             r AS (SELECT * FROM ch2 WHERE authors_count >= {REF_AUTHORS} OR same_book >= {REF_SAME_BOOK})
        SELECT cluster_id, work_idx,
               CASE WHEN on_orcid THEN 'reference work: on ORCID list'
                    WHEN NOT bool_or(on_orcid) OVER (PARTITION BY cluster_id, isbn)
                         AND work_idx = min(work_idx) FILTER (WHERE NOT on_orcid) OVER (PARTITION BY cluster_id, isbn)
                    THEN 'reference work: one per book'
                    ELSE 'reference work: collapsed' END AS ref_rule
        FROM r""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cls0 AS
        SELECT cw.cluster_id, cw.work_idx, cw.component,
            CASE WHEN in_core THEN 'accept'
                 WHEN component_anchored > 0 THEN 'accept'
                 WHEN y < least(first_year, s.core_min) - {PRE_CAREER} THEN 'reject'
                 WHEN c.component IS NOT NULL AND c.same_years >= {SAME_YEARS} AND c.field_cos < {SIMILAR_FIELD} THEN 'reject'
                 WHEN c.component IS NOT NULL THEN 'request'
                 WHEN (cw.top_field IS NULL OR f.top_field IS NOT NULL)
                      AND y BETWEEN s.core_min - {FIT_PAD} AND s.core_max + {FIT_PAD} THEN 'accept'
                 ELSE 'request' END AS rule_decision,
            CASE WHEN in_core THEN 'core'
                 WHEN component_anchored > 0 THEN 'anchored component'
                 WHEN y < least(first_year, s.core_min) - {PRE_CAREER} THEN 'before career'
                 WHEN c.component IS NOT NULL AND c.same_years >= {SAME_YEARS} AND c.field_cos < {SIMILAR_FIELD} THEN 'namesake component'
                 WHEN c.component IS NOT NULL THEN 'component'
                 WHEN (cw.top_field IS NULL OR f.top_field IS NOT NULL)
                      AND y BETWEEN s.core_min - {FIT_PAD} AND s.core_max + {FIT_PAD} THEN 'fits core'
                 ELSE 'works' END AS rule
        FROM cw LEFT JOIN core_stats s USING (cluster_id) LEFT JOIN comp_stats c USING (cluster_id, component)
        LEFT JOIN core_fields f ON f.cluster_id = cw.cluster_id AND f.top_field = cw.top_field""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE cls AS
        SELECT c.cluster_id, c.work_idx, c.component,
               CASE WHEN ref.ref_rule IS NULL THEN c.rule_decision
                    WHEN ref.ref_rule = 'reference work: collapsed' THEN 'reject' ELSE 'accept' END AS rule_decision,
               coalesce(ref.ref_rule, c.rule) AS rule
        FROM cls0 c LEFT JOIN ref USING (cluster_id, work_idx)""")


def _request_key(kind: str, cluster_id: str, items: list[int]) -> str:
    h = hashlib.sha1(",".join(map(str, sorted(items))).encode()).hexdigest()[:12]
    return f"{kind}|{cluster_id}|{h}|{PROMPT_VERSION}"


def requests_table(con) -> pd.DataFrame:
    """One row per Gemini request: request_key, kind, cluster_id, component, work_idxs."""
    r = con.execute(f"""
        SELECT rule AS kind, cluster_id, CASE WHEN rule = 'component' THEN component END AS component,
               list(work_idx ORDER BY work_idx) AS work_idxs
        FROM (SELECT *, CASE WHEN rule = 'works' THEN (row_number() OVER (PARTITION BY cluster_id, rule ORDER BY work_idx) - 1)
                                                       // {MAX_WORKS_PER_REQUEST} END AS chunk
              -- components as the graph made them (cls0), so a rule that overrides single works never changes
              -- a component's request key; works only if still requested and not judged already
              FROM (SELECT cluster_id, work_idx, component, rule FROM cls0 WHERE rule = 'component'
                    UNION ALL
                    SELECT cluster_id, work_idx, component, rule FROM cls WHERE rule = 'works'
                      AND (cluster_id, work_idx) NOT IN (SELECT (cluster_id, work_idx) FROM saved_works)))
        GROUP BY rule, cluster_id, CASE WHEN rule = 'component' THEN component END, chunk""").fetchdf()
    r["request_key"] = [_request_key(k, c, list(w)) for k, c, w in zip(r.kind, r.cluster_id, r.work_idxs)]
    return r


def _profiles(con, req: pd.DataFrame, acifs_full: pd.DataFrame,
              authorships=OPENALEX_COMPACT_DIR / "authorships", sources=OPENALEX_DIR / "sources.parquet") -> dict:
    """Prompt pieces for the ACIFs in `req`: ARC text, core summary, per-work rows."""
    con.register("req_acifs", pd.DataFrame({"cluster_id": req.cluster_id.unique()}))
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pw AS
        SELECT cw.cluster_id, cw.work_idx, cw.in_core, cw.y, cw.top_field, cw.title, cw.type, cw.authors_count,
               cw.cited_by_count, src.display_name AS venue,
               [a.printed_name FOR a IN cw.authorships] AS printed,
               flatten([[i.name || ' (' || coalesce(i.country, '?') || ')' FOR i IN a.institutions] FOR a IN cw.authorships]) AS insts,
               [a.author_idx FOR a IN cw.authorships] AS own
        FROM cw JOIN req_acifs USING (cluster_id) LEFT JOIN read_parquet('{sources}') src ON src.source_idx = cw.source_id""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pco AS
        SELECT pw.cluster_id, pw.work_idx, list(a.author_name) AS coauthors
        FROM (SELECT DISTINCT work_idx FROM pw) k JOIN read_parquet('{authorships}/*.parquet') a USING (work_idx)
        JOIN pw USING (work_idx)
        WHERE a.author_idx IS NOT NULL AND NOT list_contains(pw.own, a.author_idx) AND pw.authors_count < 50
        GROUP BY 1, 2""")
    w = con.execute("SELECT pw.*, coalesce(pco.coauthors, []) AS coauthors FROM pw LEFT JOIN pco USING (cluster_id, work_idx)").fetchdf()
    a = acifs_full.set_index("cluster_id")
    out = {}
    for cid, d in w.groupby("cluster_id"):
        r = a.loc[cid]
        arc = {"names on grants": list(r.full_names), "ARC grants": f"{len(r.grant_codes)} grants, {int(r.first_year)}-{int(r.last_year)}",
               "ARC fields of research": [f["name"] for f in r.for2020_codes],
               "grant universities": list(r.single_org_universities) or list(r.hep_codes),
               "ARC co-investigators": sorted({c.split("_", 1)[1].replace("_", " ").title() for c in r.coawardee_acif_ids})[:MAX_LIST]}
        out[cid] = {"arc": json.dumps(arc, ensure_ascii=False, indent=1), "works": d.set_index("work_idx"),
                    "core": json.dumps(_summary(d[d.in_core]), ensure_ascii=False, indent=1)}
    return out


def _summary(d: pd.DataFrame) -> dict:
    yrs = d.y.dropna()
    return {"works": len(d), "years": f"{int(yrs.min())}-{int(yrs.max())}" if len(yrs) else "unknown",
            "printed names": dict(Counter(p for ps in d.printed for p in ps).most_common(5)),
            "affiliations": dict(Counter(i for xs in d.insts for i in xs).most_common(MAX_LIST)),
            "fields": dict(Counter(d.top_field.dropna()).most_common(6)),
            "venues": dict(Counter(d.venue.dropna()).most_common(MAX_LIST)),
            "frequent co-authors": dict(Counter(n for cs in d.coauthors for n in cs).most_common(MAX_LIST)),
            "sample titles": [f"{int(r.y) if pd.notna(r.y) else '?'}: {r.title}"
                              for r in d.sort_values("cited_by_count", ascending=False).head(MAX_LIST).itertuples()]}


def build_prompts(con, req: pd.DataFrame, acifs_full: pd.DataFrame) -> pd.DataFrame:
    """Add the prompt text (and, for works requests, the id -> work_idx order) to `req`."""
    prof = _profiles(con, req, acifs_full)
    prompts, orders = [], []
    for r in req.itertuples():
        p = prof[r.cluster_id]
        if r.kind == "component":
            comp = _summary(p["works"].loc[list(r.work_idxs)].reset_index())
            prompts.append(COMPONENT_PROMPT.format(ARC=p["arc"], CORE=p["core"],
                                                   COMPONENT=json.dumps(comp, ensure_ascii=False, indent=1)))
            orders.append([])
        else:
            ws = p["works"].loc[list(r.work_idxs)]
            items = [{"id": k, "year": int(x.y) if pd.notna(x.y) else None, "type": x.type, "title": x.title,
                      "venue": x.venue if isinstance(x.venue, str) else None, "printed name": "; ".join(x.printed),
                      "affiliation": "; ".join(x.insts) or "none recorded", "number of authors": int(x.authors_count),
                      "some co-authors": list(x.coauthors)[:6], "field": x.top_field}
                     for k, x in enumerate(ws.itertuples(), 1)]
            prompts.append(WORKS_PROMPT.format(ARC=p["arc"], CORE=p["core"],
                                               WORKS=json.dumps(items, ensure_ascii=False, indent=1)))
            orders.append(list(ws.index))
    return req.assign(prompt=prompts, id_order=orders)


def saved_work_verdicts(verdicts: dict) -> pd.DataFrame:
    """(cluster_id, work_idx, g_decision, g_reason) from every saved per-work answer (later answers win)."""
    rows = {}
    for v in verdicts.values():
        if v.get("kind") != "works" or not isinstance(v.get("answer"), list):
            continue
        ans = {x.get("id"): x for x in v["answer"] if isinstance(x, dict)}
        for k, w in enumerate(v["id_order"], 1):
            x = ans.get(k, {})
            dec = {"in": "accept"}.get(x.get("verdict"), "reject" if x.get("verdict") == "out"
                                       and x.get("confidence") == "high" else "unsure")
            rows[(v["cluster_id"], int(w))] = (dec, x.get("reason"))
    return pd.DataFrame([(c, w, d, r) for (c, w), (d, r) in rows.items()],
                        columns=["cluster_id", "work_idx", "g_decision", "g_reason"])


def apply_verdicts(con, verdicts: dict, req: pd.DataFrame, out_path) -> None:
    """Write the classed works: decision accept/reject/unsure/pending, decided_by, reason."""
    rows = []
    for r in req[req.kind == "component"].itertuples():
        v = verdicts.get(r.request_key)
        if v is None or not isinstance(v.get("answer"), dict):
            continue
        d = v["answer"]
        dec = {"same": "accept"}.get(d.get("verdict"), "reject" if d.get("verdict") == "different"
                                     and d.get("confidence") == "high" else "unsure")
        rows += [(r.cluster_id, int(w), dec, d.get("reason")) for w in r.work_idxs]
    g = pd.concat([pd.DataFrame(rows, columns=["cluster_id", "work_idx", "g_decision", "g_reason"]),
                   saved_work_verdicts(verdicts)], ignore_index=True)
    con.register("gv_in", g)
    con.execute(f"""
        COPY (
            SELECT cls.cluster_id, cls.work_idx, cls.component, cls.rule,
                   CASE WHEN cls.rule_decision <> 'request' THEN cls.rule_decision
                        ELSE coalesce(g.g_decision, 'pending') END AS decision,
                   CASE WHEN cls.rule_decision <> 'request' THEN 'rule'
                        WHEN g.g_decision IS NOT NULL THEN 'gemini' END AS decided_by,
                   CASE WHEN cls.rule_decision = 'request' THEN g.g_reason END AS reason
            FROM cls LEFT JOIN (SELECT DISTINCT ON (cluster_id, work_idx) * FROM gv_in) g USING (cluster_id, work_idx)
            ORDER BY cls.cluster_id, cls.work_idx
        ) TO '{out_path}' (FORMAT parquet)""")


def classify(con, works_path, graph_path, acifs_full: pd.DataFrame, verdicts: dict, out_path, requests_path,
             orcid_dois: pd.DataFrame | None = None) -> pd.DataFrame:
    """Run the rules, write the Gemini requests (with prompts) and the classed works; returns requests.
    orcid_dois: (cluster_id, doi) on the people's own ORCID records (src/oeuvre/orcid_works.py)."""
    con.register("acif_in", acifs_full[["cluster_id", "first_year"]])
    con.register("orcid_dois", orcid_dois if orcid_dois is not None
                 else pd.DataFrame({"cluster_id": pd.Series(dtype=str), "doi": pd.Series(dtype=str)}))
    sw = saved_work_verdicts(verdicts)[["cluster_id", "work_idx"]]
    con.register("saved_works", sw.astype({"work_idx": "int64"}) if len(sw) else
                 pd.DataFrame({"cluster_id": pd.Series(dtype=str), "work_idx": pd.Series(dtype="int64")}))
    _rules(con, works_path, graph_path)
    req = requests_table(con)
    req = build_prompts(con, req, acifs_full) if len(req) else req.assign(prompt=[], id_order=[])
    req.to_parquet(requests_path, index=False)
    apply_verdicts(con, verdicts, req, out_path)
    return req
