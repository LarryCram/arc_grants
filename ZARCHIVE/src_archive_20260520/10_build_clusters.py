"""
10_build_clusters.py  —  Layer 0

Build the initial cluster set from ARC investigator data.

For every unique (arc_first, arc_family) pair:
  1. Construct arc and norm_np ParsedName representations.
  2. Build a name-compatibility graph (same norm_family key + first_names_compatible).
  3. Connected components → clusters.

Each cluster is loaded with all its InvestigatorRecords (one per person × grant),
aggregated orcids / institutions (HEP codes) / for_2d sets, and co-awardee links.

INPUT:
  PROCESSED_DATA/investigators.parquet
  PROCESSED_DATA/grants.parquet            (admin_org, scheme_code, dates)
  PROCESSED_DATA/for_codes_wrangled.parquet
  PROCESSED_DATA/institution_concordance.parquet

OUTPUT:
  PROCESSED_DATA/clusters.jsonl                 — full cluster store
  PROCESSED_DATA/clusters_after_layer0.jsonl    — checkpoint copy
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.pipeline.models import (
    Cluster, ClusterStatus, InvestigatorRecord, NameForm,
    ParsedName, save_clusters,
)
from src.utils.io import setup_stdout_utf8
from src.utils.names import (
    norm_alpha, strip_diacriticals, strip_parens, strip_postnominals,
)


# ── Name helpers ──────────────────────────────────────────────────────────────

def _build_arc_parsed(arc_first: str, arc_family: str) -> ParsedName:
    """arc form: parse first_name alone; set last directly from arc_family."""
    n = HumanName(arc_first)
    given = [t for t in [n.first] + (n.middle.split() if n.middle else []) + [n.last] if t]
    first  = given[0] if given else ""
    middle = tuple(given[1:])
    return ParsedName(
        title=n.title, first=first, middle=middle,
        last=arc_family,
        suffix=n.suffix, nickname=n.nickname,
    )


def _build_norm_np_parsed(arc_first: str, arc_family: str) -> ParsedName:
    """norm_np form: normalise both fields, then parse combined string through HumanName."""
    first_clean  = strip_diacriticals(strip_parens(arc_first))
    family_clean = strip_diacriticals(strip_parens(arc_family))
    n = HumanName(f"{first_clean} {family_clean}")
    def _norm(s: str) -> str:
        return re.sub(r"[^a-z ]", "", strip_diacriticals(s).lower()).strip()
    middle = tuple(p for p in (_norm(t) for t in n.middle.split()) if p) if n.middle else ()
    return ParsedName(
        title=_norm(n.title), first=_norm(n.first), middle=middle,
        last=_norm(n.last),
        suffix=_norm(n.suffix), nickname=_norm(n.nickname),
    )


def first_names_compatible(a: str, b: str) -> bool:
    """True if first names cannot be ruled out as the same person."""
    def _toks(s):
        return re.findall(r"[a-z]+", strip_diacriticals(strip_parens(s)).lower())
    ta, tb = _toks(a), _toks(b)
    if not ta or not tb:
        return True
    for x in ta:
        for y in tb:
            if x == y:
                return True
            if len(x) == 1 and y.startswith(x):
                return True
            if len(y) == 1 and x.startswith(y):
                return True
    return False


# ── Union-Find ────────────────────────────────────────────────────────────────

class UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank   = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: int, y: int) -> None:
        px, py = self.find(x), self.find(y)
        if px == py:
            return
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()

    # ── Load source tables ────────────────────────────────────────────────────
    print("Loading source tables...")
    df_inv  = pd.read_parquet(PROCESSED_DATA / "investigators.parquet")
    df_grts = pd.read_parquet(PROCESSED_DATA / "grants.parquet",
                              columns=["grant_code", "admin_org",
                                       "scheme_code", "funding_commence_year",
                                       "years_funded"])
    df_for  = pd.read_parquet(PROCESSED_DATA / "for_codes_wrangled.parquet",
                              columns=["grant_code", "for_code"])
    df_conc = pd.read_parquet(PROCESSED_DATA / "institution_concordance.parquet",
                              columns=["arc_admin_org", "hep_code"])

    df_for["for_2d"] = df_for["for_code"].str[:2]
    grant_for2d: dict[str, list[str]] = (
        df_for.groupby("grant_code")["for_2d"]
        .apply(lambda s: sorted(set(s.dropna())))
        .to_dict()
    )

    # HEP code lookup: admin_org → list of hep_codes (deduplicated, non-null)
    hep_lookup: dict[str, list[str]] = {}
    for org, grp in df_conc[df_conc["hep_code"].notna()].groupby("arc_admin_org"):
        hep_lookup[org] = sorted(set(grp["hep_code"].tolist()))

    df_inv = df_inv.merge(df_grts, on="grant_code", how="left")
    print(f"  Investigators: {len(df_inv):,}")

    # ── Strip post-nominals from family_name ──────────────────────────────────
    df_inv["family_name"] = df_inv["family_name"].apply(
        lambda x: strip_postnominals(str(x)) if pd.notna(x) else x
    )

    # ── Build unique (arc_first, arc_family) pairs and NameForms ─────────────
    print("Building NameForms...")
    persons = (
        df_inv[["first_name", "family_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    persons["_fam_key"] = persons["family_name"].apply(norm_alpha)

    name_forms: list[NameForm] = []
    for _, row in persons.iterrows():
        nf = NameForm(
            arc=_build_arc_parsed(str(row["first_name"]), str(row["family_name"])),
            norm_np=_build_norm_np_parsed(str(row["first_name"]), str(row["family_name"])),
        )
        name_forms.append(nf)

    print(f"  Unique name pairs: {len(persons):,}")

    # ── Build name-compatibility graph → connected components ─────────────────
    print("Building compatibility graph...")
    uf = UnionFind(len(persons))
    n_edges = 0
    for _, grp in persons.groupby("_fam_key"):
        idx = grp.index.tolist()
        for i in range(len(idx)):
            for j in range(i + 1, len(idx)):
                a = persons.iloc[idx[i]]
                b = persons.iloc[idx[j]]
                if first_names_compatible(str(a["first_name"]), str(b["first_name"])):
                    uf.union(idx[i], idx[j])
                    n_edges += 1

    persons["cluster_root"] = [uf.find(i) for i in range(len(persons))]
    n_clusters = persons["cluster_root"].nunique()
    print(f"  Compatibility edges: {n_edges:,}")
    print(f"  Clusters:            {n_clusters:,}")

    # Stable integer cluster IDs (0-based, sorted by first occurrence)
    root_order = {r: i for i, r in enumerate(persons["cluster_root"].unique())}
    persons["cluster_id"] = persons["cluster_root"].map(root_order)

    # Map (first_name, family_name) → (cluster_id, NameForm index)
    pair_to_cluster: dict[tuple[str, str], int] = {
        (row["first_name"], row["family_name"]): row["cluster_id"]
        for _, row in persons.iterrows()
    }
    cluster_to_nameforms: dict[int, list[NameForm]] = defaultdict(list)
    for i, row in persons.iterrows():
        cluster_to_nameforms[row["cluster_id"]].append(name_forms[i])

    # ── Build InvestigatorRecords ─────────────────────────────────────────────
    print("Building InvestigatorRecords...")
    inv_records: list[tuple[int, InvestigatorRecord]] = []
    for _, row in df_inv.iterrows():
        first  = str(row["first_name"])
        family = str(row["family_name"])
        cid    = pair_to_cluster.get((first, family))
        if cid is None:
            continue
        rec = InvestigatorRecord(
            grant_code=str(row["grant_code"]),
            arc_first=first,
            arc_family=family,
            orcid=str(row["orcid"]).strip() if pd.notna(row["orcid"]) else None,
            grant_heps=hep_lookup.get(str(row["admin_org"]), []),
            for_2d=grant_for2d.get(str(row["grant_code"]), []),
            role_code=str(row["role_code"]) if pd.notna(row.get("role_code")) else "",
        )
        inv_records.append((cid, rec))

    print(f"  InvestigatorRecords built: {len(inv_records):,}")

    # ── Assemble Clusters ─────────────────────────────────────────────────────
    print("Assembling clusters...")
    cluster_records: dict[int, list[InvestigatorRecord]] = defaultdict(list)
    for cid, rec in inv_records:
        cluster_records[cid].append(rec)

    clusters: list[Cluster] = []
    for cid in range(n_clusters):
        recs = cluster_records.get(cid, [])
        orcids       = sorted({r.orcid for r in recs if r.orcid})
        institutions = sorted({h for r in recs for h in r.grant_heps})
        for_2d       = sorted({f for r in recs for f in r.for_2d})
        c = Cluster(
            cluster_id=cid,
            name_forms=cluster_to_nameforms[cid],
            records=recs,
            orcids=orcids,
            institutions=institutions,
            for_2d=for_2d,
            co_awardee_cluster_ids=[],
        )
        clusters.append(c)

    # ── Build co-awardee links ────────────────────────────────────────────────
    print("Building co-awardee links...")
    grant_to_clusters: dict[str, set[int]] = defaultdict(set)
    for cid, rec in inv_records:
        grant_to_clusters[rec.grant_code].add(cid)

    co_awardees: dict[int, set[int]] = defaultdict(set)
    for grant_code, cids in grant_to_clusters.items():
        for cid in cids:
            co_awardees[cid].update(cids - {cid})

    for c in clusters:
        c.co_awardee_cluster_ids = sorted(co_awardees[c.cluster_id])

    # ── Summary ───────────────────────────────────────────────────────────────
    n_multi_orcid = sum(1 for c in clusters if len(c.orcids) > 1)
    n_with_orcid  = sum(1 for c in clusters if len(c.orcids) == 1)
    n_no_orcid    = sum(1 for c in clusters if len(c.orcids) == 0)
    print(f"\n{'─'*60}")
    print(f"LAYER 0 SUMMARY")
    print(f"{'─'*60}")
    print(f"  Clusters total:              {n_clusters:>7,}")
    print(f"  With exactly 1 ORCID:        {n_with_orcid:>7,}")
    print(f"  With >1 ORCID (flag):        {n_multi_orcid:>7,}")
    print(f"  With no ORCID:               {n_no_orcid:>7,}")
    print(f"  Total investigator records:  {len(inv_records):>7,}")

    # ── Save ─────────────────────────────────────────────────────────────────
    out_path       = PROCESSED_DATA / "clusters.jsonl"
    checkpoint     = PROCESSED_DATA / "clusters_after_layer0.jsonl"
    save_clusters(clusters, out_path)
    save_clusters(clusters, checkpoint)
    print(f"\nSaved:")
    print(f"  {out_path}")
    print(f"  {checkpoint}")


if __name__ == "__main__":
    main()
