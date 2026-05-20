"""
17b_parquet_oax.py  —  Layer 5 (parquet edition): OAX snapshot author disambiguation

Identical gate logic to 17_oax_search.py but driven from the local OAX snapshot
instead of the live REST API.  No rate limits; all 2,973 UNDECIDABLE clusters
complete in under a minute.

Strategy:
  1. Normalise ARC family names → norm_family_token (same as oax_name_index build)
  2. One bulk DuckDB query: name_index JOIN author parquets, filter to AU-affiliated
  3. Apply name-consistency + two-signal gate per cluster in Python
  4. Identical output: oax_candidates on cluster, layer5_candidates.csv, checkpoint

Snapshot date: Feb 2026.  Recent affiliation changes not reflected.

Usage:
  python 17b_parquet_oax.py              # all UNDECIDABLE clusters
  python 17b_parquet_oax.py --limit 128  # first N only (spot-check)
  python 17b_parquet_oax.py --limit 128 --dry-run

INPUT:  PROCESSED_DATA/clusters.jsonl
        PROCESSED_DATA/oax_name_index.parquet
        OPENALEX_DIR/authors/*.parquet
        PROCESSED_DATA/institution_concordance.parquet
        DATA_ROOT/for_oax_concordance.csv
OUTPUT: PROCESSED_DATA/clusters.jsonl
        PROCESSED_DATA/clusters_after_layer5.jsonl
        PROCESSED_DATA/layer5_candidates.csv
"""

import argparse
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import FOR_OAX_CSV, OAX_AUTHORS, PROCESSED_DATA
from src.pipeline.models import Cluster, ClusterStatus, load_clusters, save_clusters
from src.utils.io import setup_stdout_utf8

FELLOWSHIP_CODES = {"FT", "FL", "DE", "DECRA", "APD", "APF", "ARF", "QEII", "FF", "APDI", "IRF", "ARFI", "LE"}


# ── Name normalisation (mirrors src/11_oax_name_index.py) ─────────────────────

def _strip_diacriticals(s: str) -> str:
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")

def _strip_parens(s: str) -> str:
    return re.sub(r"\(.*?\)", "", s).strip()

def _norm(s: str) -> str:
    return re.sub(r"[^a-z ]", "", _strip_diacriticals(s).lower()).strip()

def _norm_family(arc_family: str) -> str:
    clean = _strip_diacriticals(_strip_parens(arc_family))
    return _norm(HumanName(clean).last or clean)


# ── HEP certainty (same as 17_oax_search.py) ──────────────────────────────────

def _build_grant_ci_count(clusters: list) -> dict[str, int]:
    grant_to_cids: dict[str, set] = defaultdict(set)
    for c in clusters:
        for r in c.records:
            m = re.search(r"grant_code='([^']+)'", str(r))
            if m:
                grant_to_cids[m.group(1)].add(c.cluster_id)
    return {gc: len(cids) for gc, cids in grant_to_cids.items()}


def _certain_heps(cluster: Cluster, grant_ci_count: dict[str, int]) -> set[str]:
    certain: set[str] = set()
    for r in cluster.records:
        rs = str(r)
        m_code = re.search(r"grant_code='([^']+)'", rs)
        m_role = re.search(r"role_code='([^']+)'", rs)
        m_heps = re.search(r"grant_heps=\[([^\]]*)\]", rs)
        if not (m_code and m_role and m_heps):
            continue
        grant_code = m_code.group(1)
        role_code  = m_role.group(1)
        grant_heps = [h.strip("' ") for h in m_heps.group(1).split(",") if h.strip("' ")]
        if role_code in FELLOWSHIP_CODES or grant_ci_count.get(grant_code, 1) == 1 or len(grant_heps) == 1:
            certain.update(grant_heps)
    return certain


# ── Gate functions ─────────────────────────────────────────────────────────────

def _name_consistent(arc_first: str, oax_display_name: str) -> bool:
    oax_tokens = oax_display_name.split()
    if not oax_tokens:
        return False
    oax_f = oax_tokens[0].strip(".,").lower().replace("‐", "-")
    arc_f = arc_first.strip().lower().replace("‐", "-")
    if not arc_f or not oax_f:
        return False
    if arc_f == oax_f:
        return True
    if len(arc_f) == 1 or len(oax_f) == 1:
        return arc_f[0] == oax_f[0]
    if arc_f.startswith(oax_f) or oax_f.startswith(arc_f):
        return True
    return False


def _passes_gate(
    cluster: Cluster,
    inst_ids: list[str],       # pre-extracted from SQL
    field_names: list[str],    # pre-extracted from SQL
    inst_id_to_hep: dict[str, str],
    for2d_to_fields: dict[str, set[str]],
    certain_heps: set[str],
) -> bool:
    cand_heps   = {inst_id_to_hep[iid] for iid in inst_ids if iid in inst_id_to_hep}
    inst_filter = certain_heps if certain_heps else set(cluster.institutions)
    if not (cand_heps & inst_filter):
        return False
    expected: set[str] = set()
    for code in cluster.for_2d:
        expected.update(for2d_to_fields.get(code, set()))
    if not expected:
        return True
    return bool(set(field_names) & expected)


# ── Bulk parquet query ─────────────────────────────────────────────────────────

def _fetch_candidates(
    all_targets: list[Cluster],
    name_index: Path,
    author_glob: str,
) -> dict[int, list[dict]]:
    """
    Two-step DuckDB fetch for ALL UNDECIDABLE clusters (not limited by --limit).

    Step 1: query name index (small file) → candidate OAX IDs per family token
    Step 2: targeted fetch from author parquets for just those OAX IDs

    The name index was built from AU-only authors so no AU filter needed.
    Returns {cluster_id: [author_row, ...]} where each author_row has:
      oax_id, display_name, orcid, works_count, cited_by_count,
      inst_ids (list[str]), field_names (list[str])
    """
    token_to_cids: dict[str, list[int]] = defaultdict(list)
    for c in all_targets:
        nf  = c.name_forms[0]
        tok = _norm_family(nf.arc.last)
        if tok:
            token_to_cids[tok].append(c.cluster_id)

    tokens = list(token_to_cids.keys())
    if not tokens:
        return {}

    con = duckdb.connect()
    con.execute("CREATE TEMP TABLE _tokens AS SELECT unnest(?::VARCHAR[]) AS tok", [tokens])

    # ── Step 1: name index → candidate OAX IDs ───────────────────────────────
    print(f"  Step 1: name index lookup — {len(all_targets):,} clusters, "
          f"{len(tokens):,} distinct family tokens...")
    ni_rows = con.execute(f"""
        SELECT DISTINCT ni.norm_family_token AS tok, ni.oax_id
        FROM _tokens
        JOIN read_parquet('{name_index}') ni ON ni.norm_family_token = _tokens.tok
    """).fetchall()
    print(f"    → {len(ni_rows):,} candidate (token, oax_id) pairs")

    token_to_oax: dict[str, list[str]] = defaultdict(list)
    all_oax_ids: set[str] = set()
    for tok, oax_id in ni_rows:
        token_to_oax[tok].append(oax_id)
        all_oax_ids.add(oax_id)

    if not all_oax_ids:
        return {}

    # ── Step 2: fetch author records for candidate OAX IDs ───────────────────
    import glob as glob_module
    oax_id_list = sorted(all_oax_ids)
    con.execute("CREATE TEMP TABLE _oax_ids AS SELECT unnest(?::VARCHAR[]) AS oax_id", [oax_id_list])
    print(f"  Step 2: author parquet fetch — {len(oax_id_list):,} distinct OAX IDs...")

    _AUTHOR_SQL = """
        SELECT
            a.id                                                        AS oax_id,
            a.display_name,
            a.works_count,
            a.cited_by_count,
            a.orcid,
            list_filter(
                list_transform(a.affiliations, x -> x.institution.id),
                x -> x IS NOT NULL
            )                                                           AS inst_ids,
            list_filter(
                list_transform(a.topics, x -> x.field.display_name),
                x -> x IS NOT NULL
            )                                                           AS field_names
        FROM read_parquet('{src}') a
        SEMI JOIN _oax_ids ON a.id = _oax_ids.oax_id
    """

    try:
        au_rows = con.execute(_AUTHOR_SQL.format(src=author_glob)).fetchall()
    except duckdb.InvalidInputException:
        print("  Warning: batch fetch failed (bad encoding); retrying per partition...")
        au_rows = []
        for fpath in sorted(glob_module.glob(author_glob)):
            try:
                au_rows.extend(con.execute(_AUTHOR_SQL.format(src=fpath)).fetchall())
            except duckdb.InvalidInputException:
                print(f"  Warning: skipping {Path(fpath).name} (invalid encoding)")

    print(f"    → {len(au_rows):,} author records fetched")

    oax_id_to_author: dict[str, dict] = {}
    for oax_id, display_name, works_count, cited_by_count, orcid, inst_ids, field_names in au_rows:
        oax_id_to_author[oax_id] = {
            "oax_id":         oax_id,
            "display_name":   display_name,
            "works_count":    works_count,
            "cited_by_count": cited_by_count,
            "orcid":          (orcid or "").replace("https://orcid.org/", "") or None,
            "inst_ids":       inst_ids or [],
            "field_names":    [f for f in (field_names or []) if f],
        }

    # ── Group by cluster ──────────────────────────────────────────────────────
    result: dict[int, list[dict]] = defaultdict(list)
    for tok, cids in token_to_cids.items():
        authors = [oax_id_to_author[oid] for oid in token_to_oax.get(tok, []) if oid in oax_id_to_author]
        for cid in cids:
            result[cid].extend(authors)

    return dict(result)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",   type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    path = PROCESSED_DATA / "clusters.jsonl"
    print("Loading clusters...")
    clusters = load_clusters(path)
    targets  = [c for c in clusters if c.status == ClusterStatus.UNDECIDABLE.value]
    print(f"  Total: {len(clusters):,}  |  UNDECIDABLE: {len(targets):,}")

    # Resume: skip already-processed
    already_done = sum(1 for c in targets if c.oax_candidates)
    targets = [c for c in targets if not c.oax_candidates]
    if already_done:
        print(f"  Resuming: skipping {already_done} already-processed clusters")

    # Concordances
    df_conc = pd.read_parquet(
        PROCESSED_DATA / "institution_concordance.parquet",
        columns=["institution_id", "hep_code"],
    )
    inst_id_to_hep: dict[str, str] = {
        row.institution_id: row.hep_code
        for row in df_conc[df_conc["institution_id"].notna() & df_conc["hep_code"].notna()].itertuples()
    }

    df_for = pd.read_csv(FOR_OAX_CSV, sep=";")
    df_for["for_2d"] = df_for["for_2digit"].apply(lambda x: f"{int(x):02d}")
    for2d_to_fields: dict[str, set[str]] = (
        df_for.groupby("for_2d")["oax_field"].apply(set).to_dict()
    )

    print("Building HEP certainty index...")
    grant_ci_count = _build_grant_ci_count(clusters)

    # ── Bulk fetch (always covers all pending targets, regardless of --limit) ──
    name_index  = PROCESSED_DATA / "oax_name_index.parquet"
    author_glob = str(OAX_AUTHORS / "*.parquet")

    print("\nFetching candidates from parquet snapshot...")
    candidates_by_cluster = _fetch_candidates(targets, name_index, author_glob)

    # Apply --limit only to the gate loop, not the fetch
    gate_targets = targets[:args.limit] if args.limit else targets
    if args.limit:
        print(f"  (gate limited to first {args.limit} clusters)")

    # ── Gate per cluster ──────────────────────────────────────────────────────
    print(f"\nApplying gate to {len(gate_targets):,} clusters...\n")
    n_matched = n_ambiguous = n_no_inst = n_no_field = n_zero = 0
    rows: list[dict] = []
    candidate_rows: list[dict] = []

    for i, cluster in enumerate(gate_targets, 1):
        nf        = cluster.name_forms[0]
        name      = f"{nf.arc.first} {nf.arc.last}".strip()
        arc_first = nf.arc.first
        cert_heps = _certain_heps(cluster, grant_ci_count)
        results   = candidates_by_cluster.get(cluster.cluster_id, [])

        if not results:
            n_zero += 1
            outcome = "zero results"
        else:
            name_ok = [r for r in results if _name_consistent(arc_first, r["display_name"])]
            passing = [
                r for r in name_ok
                if _passes_gate(cluster, r["inst_ids"], r["field_names"],
                                inst_id_to_hep, for2d_to_fields, cert_heps)
            ]

            if len(passing) == 1:
                r = passing[0]
                if not args.dry_run:
                    cluster.status = ClusterStatus.MATCHED.value
                    cluster.oax_id = r["oax_id"]
                    cluster.tier   = 6
                n_matched += 1
                outcome = f"MATCHED → {r['oax_id']}  orcid={r['orcid']}  name={r['display_name']}"
            elif len(passing) > 1:
                n_ambiguous += 1
                names_same = len({r["display_name"].lower() for r in passing}) == 1
                tag = " [same-name fragment?]" if names_same else ""
                outcome = (f"ambiguous ({len(passing)} pass gate of "
                           f"{len(name_ok)} name-ok / {len(results)} AU results){tag}")
                if i % 100 == 0:
                    for r in passing:
                        print(f"           {r['oax_id'].split('/')[-1]:<14} "
                              f"works={r.get('works_count') or 0:>4}  "
                              f"cited={r.get('cited_by_count') or 0:>5}  "
                              f"orcid={r['orcid']}  name={r['display_name']}")
            else:
                inst_ok = [
                    r for r in name_ok
                    if {inst_id_to_hep[iid] for iid in r["inst_ids"] if iid in inst_id_to_hep}
                       & set(cluster.institutions)
                ]
                if not inst_ok:
                    n_no_inst += 1
                    outcome = f"no inst match ({len(name_ok)} name-ok / {len(results)} AU results)"
                else:
                    n_no_field += 1
                    outcome = (f"no field match ({len(name_ok)} name-ok / "
                               f"{len(results)} AU results, {len(inst_ok)} pass inst)")

            if passing and not args.dry_run:
                cluster.oax_candidates = [
                    {
                        "oax_id":         r["oax_id"],
                        "display_name":   r["display_name"],
                        "orcid":          r["orcid"],
                        "works_count":    r.get("works_count"),
                        "cited_by_count": r.get("cited_by_count"),
                    }
                    for r in passing
                ]

            names_same_csv = len({r["display_name"].lower() for r in passing}) == 1 if len(passing) > 1 else None
            for r in passing:
                candidate_rows.append({
                    "cluster_id":     cluster.cluster_id,
                    "arc_name":       name,
                    "heps":           "|".join(cluster.institutions),
                    "for_2d":         "|".join(cluster.for_2d),
                    "oax_id":         r["oax_id"],
                    "oax_name":       r["display_name"],
                    "orcid":          r["orcid"],
                    "works_count":    r.get("works_count"),
                    "cited_by_count": r.get("cited_by_count"),
                    "n_passing":      len(passing),
                    "same_name_frag": names_same_csv,
                    "outcome":        "MATCHED" if len(passing) == 1 else "AMBIGUOUS",
                })

        if i % 100 == 0:
            cert_tag = f"  certain={sorted(cert_heps)}" if cert_heps != set(cluster.institutions) else ""
            print(f"  [{i:>4}] {name:<30} heps={cluster.institutions}{cert_tag}")
            print(f"         {outcome}")

        rows.append({"cluster_id": cluster.cluster_id, "name": name, "outcome": outcome})

        if i % 250 == 0:
            print(f"\n  ── progress {i}/{len(gate_targets)} ──  "
                  f"matched={n_matched}  ambiguous={n_ambiguous}  "
                  f"no_inst={n_no_inst}  no_field={n_no_field}  zero={n_zero}")
            if not args.dry_run and not args.limit:
                save_clusters(clusters, path)
                print(f"  checkpoint saved → {path}\n")
            else:
                print()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"LAYER 5b SUMMARY  {'(DRY RUN)' if args.dry_run else ''}")
    print(f"{'─'*60}")
    print(f"  Matched (tier 6):        {n_matched:>6,}")
    print(f"  Ambiguous (>1 pass):     {n_ambiguous:>6,}")
    print(f"  No institution match:    {n_no_inst:>6,}")
    print(f"  No field match:          {n_no_field:>6,}")
    print(f"  Zero AU results:         {n_zero:>6,}")

    if candidate_rows:
        detail_path = PROCESSED_DATA / "layer5_candidates.csv"
        pd.DataFrame(candidate_rows).to_csv(detail_path, index=False)
        n_clu = pd.DataFrame(candidate_rows)["cluster_id"].nunique()
        print(f"\nAll gate-passing candidates: {detail_path}  ({len(candidate_rows)} rows, {n_clu} clusters)")

    if not args.dry_run and not args.limit:
        save_clusters(clusters, path)
        checkpoint = PROCESSED_DATA / "clusters_after_layer5.jsonl"
        save_clusters(clusters, checkpoint)
        print(f"\nSaved: {path}")
        print(f"       {checkpoint}")
    elif not args.dry_run and args.limit:
        print(f"\n(--limit set: changes applied in memory but not saved)")


if __name__ == "__main__":
    main()
