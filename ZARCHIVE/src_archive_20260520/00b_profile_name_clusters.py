"""
00b_profile_name_clusters.py

Cluster ARC investigator names that could refer to the same person.

For every unique (first_name, family_name) pair in investigators.parquet, three
name representations are retained:
  actual      — as stored in ARC data
  nameparser  — HumanName normalisation (strips titles, handles née, aliases)
  norm        — diacriticals + exotic hyphens stripped, punctuation removed, lowercase

Clustering rules:
  1. Normalised family names must be identical (key = strip all non-alpha).
  2. Within each family-name group, two names are *compatible* if their
     first-name token sets are mutually non-contradicting:
       any exact token match, or an initial in either name covers a full token
       in the other.
  3. Clusters are connected components of the compatibility graph
     (union-find, no external graph library required).

Per-cluster report columns:
  cluster_id, n_name_forms, n_grants, n_orcid_grants,
  n_distinct_orcids, n_distinct_institutions, n_distinct_for2d, name_forms

OUTPUT:
  PROCESSED_DATA/name_clusters_summary.csv   — one row per cluster, sorted by n_grants desc
  PROCESSED_DATA/name_clusters_long.parquet  — one row per (cluster, name form) with all three reps
"""

import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.utils.io import setup_stdout_utf8


# ── Name helpers ──────────────────────────────────────────────────────────────

_EXOTIC_HYPHENS = re.compile(r"[­‐‑‒–—―−－]")


def _strip_diacriticals(s: str) -> str:
    s = _EXOTIC_HYPHENS.sub("-", s)
    s = s.replace("ı", "i").replace("İ", "I")
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def _tokens(name: str) -> list[str]:
    return re.findall(r"[a-z]+", _strip_diacriticals(name).lower())


def _clean_parens(s: str) -> str:
    return re.sub(r"\s*\(.*?\)", "", s).strip()


def _norm_family_key(family: str) -> str:
    """All-alpha lowercase key for family name grouping."""
    return re.sub(r"[^a-z]", "", _strip_diacriticals(_clean_parens(family)).lower())


def _norm_full(first: str, family: str) -> str:
    """Punct/diacritic-stripped lowercase full name."""
    full = f"{_clean_parens(first)} {_clean_parens(family)}"
    s = _strip_diacriticals(full).lower()
    s = re.sub(r"[^a-z\s]", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _nameparser_form(first: str, family: str) -> str:
    """Reconstruct name via HumanName (handles titles, née, parens)."""
    n = HumanName(f"{first} {family}")
    parts = [n.first, n.middle, n.last]
    return " ".join(p for p in parts if p).strip()


def first_names_compatible(a: str, b: str) -> bool:
    """True if first names a and b could belong to the same person."""
    ta = _tokens(_clean_parens(a))
    tb = _tokens(_clean_parens(b))
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

    df_inv = pd.read_parquet(PROCESSED_DATA / "investigators.parquet")
    df_grts = pd.read_parquet(PROCESSED_DATA / "grants.parquet",
                              columns=["grant_code", "admin_org"])
    df_inv = df_inv.merge(df_grts, on="grant_code", how="left")

    df_for = pd.read_parquet(PROCESSED_DATA / "for_codes_wrangled.parquet",
                             columns=["grant_code", "for_code"])
    df_for["for_2d"] = df_for["for_code"].str[:2]

    print(f"Investigators (grant rows):  {len(df_inv):>8,}")

    # ── Build unique-person table with all three name representations ─────────
    persons = (
        df_inv[["first_name", "family_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    print(f"Unique (first, family) pairs: {len(persons):>7,}")

    persons["actual"]     = persons["first_name"] + " " + persons["family_name"]
    persons["nameparser"] = persons.apply(
        lambda r: _nameparser_form(r["first_name"], r["family_name"]), axis=1
    )
    persons["norm"]       = persons.apply(
        lambda r: _norm_full(r["first_name"], r["family_name"]), axis=1
    )
    persons["_fam_key"]   = persons["family_name"].apply(_norm_family_key)

    # ── Build compatibility graph and find connected components ───────────────
    print("Building compatibility graph...")
    uf = UnionFind(len(persons))

    n_edges = 0
    for fam_key, grp in persons.groupby("_fam_key"):
        idx = grp.index.tolist()
        for i in range(len(idx)):
            for j in range(i + 1, len(idx)):
                a = persons.iloc[idx[i]]
                b = persons.iloc[idx[j]]
                if first_names_compatible(a["first_name"], b["first_name"]):
                    uf.union(idx[i], idx[j])
                    n_edges += 1

    persons["cluster_id"] = [uf.find(i) for i in range(len(persons))]
    n_clusters = persons["cluster_id"].nunique()
    n_multi    = (persons.groupby("cluster_id").size() > 1).sum()
    print(f"  Compatibility edges:          {n_edges:>7,}")
    print(f"  Clusters (connected components): {n_clusters:>5,}")
    print(f"  Multi-name clusters:          {n_multi:>7,}")

    # ── Join back to investigators for per-grant stats ────────────────────────
    df_aug = df_inv.merge(
        persons[["first_name", "family_name", "cluster_id"]],
        on=["first_name", "family_name"],
        how="left"
    )
    grant_for2d = df_for.groupby("grant_code")["for_2d"].apply(set).to_dict()

    # ── Aggregate per cluster ─────────────────────────────────────────────────
    print("Aggregating cluster statistics...")

    cluster_stats = (
        df_aug.groupby("cluster_id")
        .apply(lambda g: pd.Series({
            "n_grants":           g["grant_code"].nunique(),
            "n_orcid_grants":     g["orcid"].notna().sum(),
            "n_distinct_orcids":  g["orcid"].dropna().nunique(),
            "n_distinct_insts":   g["admin_org"].nunique(),
            "n_distinct_for2d":   len({
                f for gc in g["grant_code"].unique()
                for f in grant_for2d.get(gc, set())
            }),
        }), include_groups=False)
        .reset_index()
    )

    name_forms = (
        persons.groupby("cluster_id")
        .apply(lambda g: pd.Series({
            "n_name_forms":   len(g),
            "name_forms":     " | ".join(sorted(g["actual"])),
            "nameparser_forms": " | ".join(sorted(g["nameparser"].unique())),
            "norm_forms":     " | ".join(sorted(g["norm"].unique())),
        }), include_groups=False)
        .reset_index()
    )

    df_clusters = name_forms.merge(cluster_stats, on="cluster_id")
    df_clusters = df_clusters.sort_values("n_grants", ascending=False).reset_index(drop=True)

    # ── Summary stats ─────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"NAME CLUSTER SUMMARY")
    print(f"{'─'*60}")
    print(f"  Total clusters:                  {n_clusters:>7,}")
    print(f"  Singleton (1 name form):         {n_clusters - n_multi:>7,}")
    print(f"  Multi-name clusters:             {n_multi:>7,}")
    print(f"  Clusters with >1 ORCID:          {(df_clusters['n_distinct_orcids'] > 1).sum():>7,}  ← likely collisions")
    print(f"  Clusters with >4 FoR 2d fields:  {(df_clusters['n_distinct_for2d'] > 4).sum():>7,}  ← check for collisions")

    print(f"\nTop 20 clusters by grant count:")
    print(f"{'grants':>7} {'orcids':>6} {'insts':>6} {'for2d':>6}  name forms")
    print(f"{'─'*7} {'─'*6} {'─'*6} {'─'*6}  {'─'*40}")
    for _, r in df_clusters.head(20).iterrows():
        print(f"{r['n_grants']:>7,} {r['n_distinct_orcids']:>6} {r['n_distinct_insts']:>6} {r['n_distinct_for2d']:>6}  {r['name_forms'][:70]}")

    print(f"\nClusters with multiple distinct ORCIDs (likely name collisions):")
    collisions = df_clusters[df_clusters["n_distinct_orcids"] > 1].sort_values(
        "n_distinct_orcids", ascending=False
    )
    print(f"  {len(collisions):,} clusters")
    for _, r in collisions.head(20).iterrows():
        print(f"  orcids={r['n_distinct_orcids']}  grants={r['n_grants']}  for2d={r['n_distinct_for2d']}  {r['name_forms'][:70]}")

    # ── Long format (one row per name form) ───────────────────────────────────
    df_long = persons[["cluster_id", "first_name", "family_name",
                        "actual", "nameparser", "norm"]].copy()
    df_long = df_long.merge(
        df_clusters[["cluster_id","n_name_forms","n_grants","n_orcid_grants",
                     "n_distinct_orcids","n_distinct_insts","n_distinct_for2d"]],
        on="cluster_id"
    )

    # ── Save ─────────────────────────────────────────────────────────────────
    summary_path = PROCESSED_DATA / "name_clusters_summary.csv"
    long_path    = PROCESSED_DATA / "name_clusters_long.parquet"

    df_clusters.to_csv(summary_path, index=False)
    df_long.to_parquet(long_path, index=False)

    print(f"\nSaved:")
    print(f"  {summary_path}  ({len(df_clusters):,} clusters)")
    print(f"  {long_path}  ({len(df_long):,} name forms)")


if __name__ == "__main__":
    main()
