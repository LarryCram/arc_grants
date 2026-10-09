"""
analysis/utils/dossier_build.py -- builds Dossier objects (analysis/utils/dossier.py) from persisted
outputs only (rebuilt 2026-10-09; the earlier builder read the archived pipeline's tables):

  ARC          acifs_arc.parquet, acif_arc_records.parquet (01), grants_flat.parquet (00a)
  linker       processed/oax_link/: accepted_links, orcid_links, name_links, name_decisions,
               works_evidence, works_decisions, scopus_links, scopus_decisions (02)
  works        processed/oeuvre/: acif_works_single, acif_works_classified, acif_work_graph,
               acif_work_citations (03)
  OpenAlex     openalex_authors_prep.parquet (00b) for record names/ORCIDs, sources.parquet for venues

DossierBuilder loads the small tables once; build(cluster_id) reads one person's rows from the large
ones by a filtered scan, so building many dossiers in a loop stays cheap.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb
import pandas as pd

from analysis.utils.dossier import Award, Dossier, LinkedRecord, Work
from config.settings import (ACIF_ARC_RECORDS, ACIFS_ARC, ADMIN_ORGS_CSV, DUCKDB_TMP_DIR, OAX_LINK_DIR, OEUVRE_DIR,
                             OPENALEX_DIR, PROCESSED_DATA)
from src.utils.for_resolve import Resolver

PREP = PROCESSED_DATA / "openalex_authors_prep.parquet"
GRANTS_FLAT = PROCESSED_DATA / "grants_flat.parquet"
STAGE_ORDER = {"orcid": 0, "name": 1, "works": 2, "scopus": 3}


class DossierBuilder:
    def __init__(self, con: duckdb.DuckDBPyConnection | None = None):
        self.con = con or duckdb.connect()
        self.con.execute(f"SET temp_directory='{DUCKDB_TMP_DIR}'")
        self.acifs = pd.read_parquet(ACIFS_ARC).set_index("cluster_id")
        self.records = pd.read_parquet(ACIF_ARC_RECORDS)
        self.grants = pd.read_parquet(GRANTS_FLAT, columns=[
            "grant_code", "scheme_name", "years_funded", "end_year", "funding_announced", "n_eligible_orgs",
            "primary_for_name"]).set_index("grant_code")
        L = OAX_LINK_DIR
        self.accepted = pd.read_parquet(L / "accepted_links.parquet")
        self.orcid_links = pd.read_parquet(L / "orcid_links.parquet", columns=[
            "cluster_id", "author_idx", "author_name", "orcid", "name_relation", "works_share", "status", "in_pool"])
        self.name_links = pd.read_parquet(L / "name_links.parquet")
        self.name_dec = pd.read_parquet(L / "name_decisions.parquet").set_index("cluster_id")
        self.works_ev = pd.read_parquet(L / "works_evidence.parquet")
        self.works_dec = pd.read_parquet(L / "works_decisions.parquet").set_index("cluster_id")
        self.scopus_links = pd.read_parquet(L / "scopus_links.parquet")
        self.scopus_dec = pd.read_parquet(L / "scopus_decisions.parquet").set_index("cluster_id")
        org = pd.read_csv(ADMIN_ORGS_CSV)
        org = org[org.hep_code.notna()].drop_duplicates("hep_code")
        self.hep_name = dict(zip(org.hep_code, org.institution_name))
        self.resolver = Resolver()

    # ---- ARC ------------------------------------------------------------------------------------
    def _division(self, codes) -> str | None:
        prim = [c["code"][:2] for c in codes if c.get("is_primary")] or [c["code"][:2] for c in codes]
        if not prim:
            return None
        d = Counter(prim).most_common(1)[0][0]
        try:
            return f"{d} {self.resolver.resolve(d, 'FOR2020', 'FOR2020').label.capitalize()}"
        except Exception:
            return d

    def _awards(self, cid: str) -> list[Award]:
        rec = self.records[self.records.cluster_id == cid]
        out = []
        for r in rec.drop_duplicates("grant_code").sort_values(["funding_commence_year", "grant_code"]).itertuples():
            g = self.grants.loc[r.grant_code] if r.grant_code in self.grants.index else None
            others = self.records[(self.records.grant_code == r.grant_code) & (self.records.cluster_id != cid)]
            out.append(Award(
                grant_code=r.grant_code, scheme=r.grant_code[:2], scheme_name=None if g is None else g.scheme_name,
                role_code=r.role_code, is_fellowship=bool(r.is_fellowship),
                year=None if pd.isna(r.funding_commence_year) else int(r.funding_commence_year),
                years_funded=None if g is None or pd.isna(g.years_funded) else int(g.years_funded),
                end_year=None if g is None or pd.isna(g.end_year) else int(g.end_year),
                funding_announced=None if g is None or pd.isna(g.funding_announced) else float(g.funding_announced),
                admin_org=r.admin_org, n_eligible_orgs=None if g is None or pd.isna(g.n_eligible_orgs) else int(g.n_eligible_orgs),
                primary_for=None if g is None else g.primary_for_name,
                declined=bool(r.declined) if pd.notna(r.declined) else False,
                ended_early=bool(r.ended_early) if pd.notna(r.ended_early) else False,
                coinvestigators=sorted(f"{o.full_name} ({o.role_code})" for o in others.itertuples())))
        return out

    # ---- linker ---------------------------------------------------------------------------------
    def _links(self, cid: str) -> tuple[list[LinkedRecord], list[str]]:
        acc = self.accepted[self.accepted.cluster_id == cid].copy()
        info = {}
        if len(acc):
            ids = ",".join(str(int(a)) for a in acc.author_idx)
            info = {int(r[0]): (r[1], r[2]) for r in self.con.execute(
                f"SELECT author_idx, full_name, orcid FROM read_parquet('{PREP}') WHERE author_idx IN ({ids})").fetchall()}
        out = []
        for r in acc.sort_values("stage", key=lambda s: s.map(STAGE_ORDER)).itertuples():
            aid = int(r.author_idx)
            name, orcid = info.get(aid, (None, None))
            if r.stage == "orcid":
                o = self.orcid_links[(self.orcid_links.cluster_id == cid) & (self.orcid_links.author_idx == aid)].iloc[0]
                name, orcid = name or o.author_name, orcid or o.orcid
                ev = f"shares the ACIF's ORCID; names: {o.name_relation}; {o.works_share:.0%} of the ACIF's linked works" \
                     + ("" if o.in_pool else "; outside the Australian-context pool")
            elif r.stage == "name":
                n = self.name_links[(self.name_links.cluster_id == cid) & (self.name_links.author_idx == aid)]
                yrs = int(n.years_at_grant_university.iloc[0]) if len(n) else 0
                ev = f"only name-compatible record with OpenAlex affiliation at a grant university in {yrs} grant years"
            elif r.stage == "works":
                e = self.works_ev[(self.works_ev.cluster_id == cid) & (self.works_ev.author_idx == aid)]
                ev = (f"{int(e.coinv_works.iloc[0])} works with linked ARC co-investigators, "
                      f"{int(e.uni_works.iloc[0])} works at a grant university in {int(e.uni_years.iloc[0])} grant years"
                      if len(e) else r.status)
            else:
                s = self.scopus_links[(self.scopus_links.cluster_id == cid) & (self.scopus_links.author_idx == aid)]
                ev = (f"holds {int(s.shared_dois.iloc[0])} of the {int(s.profile_dois_in_openalex.iloc[0])} OpenAlex DOIs of "
                      f"Scopus profile {s.scopus_id.iloc[0]}" + (" (nickname)" if r.status.endswith("nickname") else "")
                      if len(s) else r.status)
            out.append(LinkedRecord(aid, name, orcid, r.stage, r.status,
                                    None if pd.isna(r.works_count_global) else int(r.works_count_global), ev))
        route = []
        a = self.acifs.loc[cid]
        if len(a.orcids):
            refused = self.orcid_links[self.orcid_links.cluster_id == cid]
            route.append(f"ORCID {a.orcids[0]}: " + ("no OpenAlex record carries it" if not len(refused)
                                                     else "records carrying it not accepted (" + ", ".join(refused.status) + ")"))
        if cid in self.name_dec.index:
            route.append(f"name + institution: {self.name_dec.loc[cid, 'status']}")
        if cid in self.works_dec.index:
            route.append(f"works-first: {self.works_dec.loc[cid, 'status']}")
        if cid in self.scopus_dec.index:
            route.append(f"Scopus DOI bridge: {self.scopus_dec.loc[cid, 'status']}")
        return out, route

    # ---- works ----------------------------------------------------------------------------------
    def _works(self, cid: str) -> tuple[list[Work], dict, dict]:
        O = OEUVRE_DIR
        q = f"""
            SELECT s.work_idx, s.publication_year, s.type, s.title, src.display_name AS venue, s.doi,
                   s.cited_by_count, s.authors_count, s.fields[1].name AS field, s.n_versions,
                   k.decision, k.rule, k.decided_by, k.reason, k.on_scopus_profile, coalesce(g.in_core, false) AS in_core
            FROM read_parquet('{O}/acif_works_single.parquet') s
            JOIN read_parquet('{O}/acif_works_classified.parquet') k USING (cluster_id, work_idx)
            LEFT JOIN read_parquet('{O}/acif_work_graph.parquet') g USING (cluster_id, work_idx)
            LEFT JOIN read_parquet('{OPENALEX_DIR}/sources.parquet') src ON src.source_idx = s.source_id
            WHERE s.cluster_id = ?"""
        d = self.con.execute(q, [cid]).fetchdf()
        rejected = Counter(d.loc[d.decision == "reject", "rule"])
        keep = d[d.decision != "reject"]
        works = [Work(int(r.work_idx), None if pd.isna(r.publication_year) else int(r.publication_year), r.type, r.title,
                      r.venue if isinstance(r.venue, str) else None, r.doi, int(r.cited_by_count or 0),
                      None if pd.isna(r.authors_count) else int(r.authors_count),
                      r.field if isinstance(r.field, str) else None, r.decision, r.rule, r.decided_by,
                      r.reason if isinstance(r.reason, str) else None, bool(r.in_core),
                      None if pd.isna(r.on_scopus_profile) else bool(r.on_scopus_profile),
                      int(r.n_versions) if pd.notna(r.n_versions) else 1)
                 for r in keep.itertuples()]
        cites: dict[int, dict[int, int]] = {}
        if len(keep):
            con = self.con
            con.register("dz_w", keep[["work_idx"]].astype("int64"))
            for w, y, n in con.execute(f"""SELECT c.work_idx, c.year, c.citations FROM read_parquet('{O}/acif_work_citations.parquet') c
                                           JOIN dz_w USING (work_idx)""").fetchall():
                cites.setdefault(int(w), {})[int(y)] = int(n)
        return works, dict(rejected), cites

    def build(self, cid: str) -> Dossier:
        a = self.acifs.loc[cid]
        names = list(a.full_names)
        name = max(names, key=len) if names else cid
        links, route = self._links(cid)
        works, rejected, cites = self._works(cid)
        codes = [dict(c) for c in (a.for2020_codes if a.for2020_codes is not None else [])]
        return Dossier(
            cluster_id=cid, name=name, name_variants=[n for n in names if n != name],
            orcids=list(a.orcids), orcid_sources=list(a.orcid_sources),
            for_codes=[f"{c['code']} {c['name']}" + (" (primary)" if c.get("is_primary") else "") for c in codes],
            main_division=self._division(codes),
            universities=[f"{self.hep_name.get(h, h)} ({h})" for h in sorted(a.hep_codes)],
            excluded=bool(a.excluded), excluded_reason=a.excluded_reason if isinstance(a.excluded_reason, str) else None,
            awards=self._awards(cid), links=links, link_route=route if not any(l.stage == "orcid" for l in links) else [],
            works=works, rejected=rejected, citations=cites)


def find_acifs(text: str) -> list[str]:
    """cluster_ids whose id or any recorded full name contains `text` (case-insensitive)."""
    a = pd.read_parquet(ACIFS_ARC, columns=["cluster_id", "full_names"])
    t = text.lower()
    return [c for c, ns in zip(a.cluster_id, a.full_names) if t in c.lower() or any(t in n.lower() for n in ns)]
