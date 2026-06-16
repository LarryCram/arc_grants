"""
src/01_prepare_arc.py

Prepare ARC investigators for Splink, then deduplicate grant rows into person clusters.

Phase 1 – Prep: parse and normalise ARC name/institution/FOR fields
    → arc_investigators_prep.parquet

Phase 2 – Dedupe: Splink dedupe_only on the prep output
    → arc_persons.parquet  (62k grant rows → ~22,819 persons)
"""

import sys
import csv
import re
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, ADMIN_ORGS_CSV, GRANT_SUMMARIES_CSV
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import make_expanded_for_tokens, name_part_tokens, strip_diacriticals, for_name_tokens
from src.utils.lookup_for_topic import ForTopicLookup

MANUAL_SPLITS_CSV = Path(__file__).resolve().parents[1] / "config" / "manual_splits.csv"
FOR_DIVISIONS_CSV = Path(__file__).resolve().parents[1] / "config" / "for_divisions.csv"
FOR_ADJACENT_CSV  = Path(__file__).resolve().parents[1] / "config" / "for_adjacent_divisions.csv"

RARE_NAME_TF      = 5e-5
CLUSTER_THRESHOLD = 0.9


# ── FOR code normalisation (2008 → 2020) ─────────────────────────────────────

def upgrade_for_code(lu: ForTopicLookup, code: str | None) -> str | None:
    """Upgrade a 2008 FOR group code to its 2020 equivalent; 2020 codes pass through.
    Returns None for unmappable codes (SQL COALESCE keeps the original in that case).
    """
    if not code:
        return None
    return lu.upgrade_for_code(code)


def upgrade_for_name(lu: ForTopicLookup, code: str | None, name: str | None) -> str | None:
    """Return the official ANZSRC 2020 group name when a 2008 code is upgraded,
    or the original name when already 2020 or no mapping exists.
    """
    if not code:
        return name
    code20 = lu.upgrade_for_code(code)
    if code20 is None or code20 == code:
        return name
    sf_row = lu.group_to_subfield(code20)
    return sf_row["group_name"] if sf_row else name


# ── Phase 1 helpers ───────────────────────────────────────────────────────────

def _initials(toks: list[str]) -> list[str]:
    return [t[0] for t in toks if t]


def arc_name_arrays(first_name: str, family_name: str) -> dict:
    full = f"{first_name or ''} {family_name or ''}".strip()
    hn = HumanName(full)

    if not hn.last and hn.first:
        hn.last = hn.first

    f_toks = name_part_tokens(hn.first) + name_part_tokens(hn.middle)
    fam_norm = strip_diacriticals(hn.last).lower().strip() if hn.last else ""

    first_names = list(set(f_toks + _initials(f_toks)))
    family_names = [fam_norm] if fam_norm else []

    if not f_toks and fam_norm:
        first_names.append(fam_norm[0])

    return {"first_names": list(set(first_names)), "family_names": family_names}


# ── Phase 2 helpers ───────────────────────────────────────────────────────────

def _load_for_divisions() -> tuple[dict[str, str], set[frozenset]]:
    div: dict[str, str] = {}
    with open(FOR_DIVISIONS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            div[row["for_name"]] = row["division"]
    adj: set[frozenset] = set()
    with open(FOR_ADJACENT_CSV, newline="") as f:
        for row in csv.DictReader(f):
            adj.add(frozenset([row["div_a"], row["div_b"]]))
    return div, adj


def _load_all(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    print(f"  {len(df)} grant rows, {df['unique_id'].nunique()} unique IDs")
    return df


def _first_initial(toks) -> str | None:
    if toks is None or len(toks) == 0:
        return None
    full = [t for t in toks if len(t) > 1]
    return full[0][0] if full else toks[0][0]


def _first_name_canonical(full_toks, initials) -> str | None:
    if full_toks is not None and len(full_toks) > 0:
        return max(full_toks, key=len)
    if initials is not None and len(initials) > 0:
        return initials[0]
    return None


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["inst_arr"] = df["institution_oax_id"].apply(
        lambda x: [x] if (pd.notna(x) and x) else []
    )
    df["family_name_main"] = df["family_names"].apply(
        lambda x: max(x, key=len) if (x is not None and len(x) > 0) else None
    )
    df["first_initial"] = df["first_names"].apply(_first_initial)
    df["for_name_tokens"] = df["for_name_tokens"].apply(
        lambda x: list(x) if x is not None else []
    )
    df["first_name_full"] = df["first_name_full"].apply(
        lambda x: list(x) if x is not None else []
    )
    df["first_initials"] = df["first_initials"].apply(
        lambda x: list(x) if x is not None else []
    )
    df["first_name_canonical"] = df.apply(
        lambda r: _first_name_canonical(r["first_name_full"], r["first_initials"]),
        axis=1,
    )
    df["full_name_key"] = df.apply(
        lambda r: f"{r['first_name_canonical']}_{r['family_name_main']}"
        if (pd.notna(r["first_name_canonical"]) and pd.notna(r["family_name_main"]))
        else None,
        axis=1,
    )
    return df[[
        "unique_id", "full_name",
        "first_name_canonical", "full_name_key",
        "family_name_main", "first_initial",
        "orcid", "inst_arr", "for_name_tokens",
    ]]


def _norm_full(name: str) -> str:
    return re.sub(r'[^a-z ]', '', name.lower()).strip()


def _split_multi_name_clusters(
    df_cluster_ids: pd.DataFrame,
    df_prep: pd.DataFrame,
    orcid_enrichment: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    For clusters containing 2+ genuinely distinct full name forms, use three
    signals to detect mis-merged different people and split them:

      1. Co-investigator sets  — build a defaultdict(set) keyed on normalised
         full name; each set contains every other investigator who appeared on
         any grant bearing that name form.  Disjoint sets = no shared network.

      2. FOR field sets — union of for_names across all grants for each name
         form.  Disjoint sets = different research areas.

      3. ORCID enrichment cache — if two name forms resolve to different ORCIDs
         the split is definitive regardless of the other signals.

    Split rule:
      - Different ORCIDs                                → definitive split
      - Disjoint co-investigator sets AND disjoint FOR  → definitive split

    Abbreviated / common name forms (first token ≤ 2 chars, e.g. "Chun Li")
    are not used to drive splits.  After full-name forms are split into
    sub-clusters, each abbreviated grant is assigned to the sub-cluster whose
    FOR set overlaps most with that grant's FOR.  If no overlap → own cluster.
    """
    # Load raw investigators filtered to in-scope schemes and roles only
    raw_inv_path = PROCESSED_DATA / "investigators_raw.parquet"
    raw_inv = pd.read_parquet(raw_inv_path, columns=["grant_code", "first_name", "family_name", "role_code"])
    raw_inv = raw_inv[
        raw_inv["grant_code"].str[:2].isin(KEEP_SCHEMES) &
        raw_inv["role_code"].isin(KEEP_ROLES)
    ]
    raw_inv["coinv_name"] = (raw_inv["first_name"] + " " + raw_inv["family_name"]).str.strip()

    # Build ORCID enrichment lookup: (norm_full_name) → orcid
    enriched_orcid: dict[str, str] = {}
    if orcid_enrichment is not None and len(orcid_enrichment):
        for _, er in orcid_enrichment.iterrows():
            if pd.notna(er.get("orcid")):
                key = _norm_full(f"{er['first_name']} {er['family_name']}")
                enriched_orcid[key] = er["orcid"]

    merged = df_prep[["unique_id", "full_name", "for_names"]].merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])
    merged["norm_name"] = merged["full_name"].apply(_norm_full)
    # Grant code is the prefix of unique_id before the last underscore block
    merged["grant_code"] = merged["unique_id"].apply(lambda u: u.rsplit("_", 1)[0])

    out = []
    n_splits = 0

    for cid, grp in merged.groupby("cluster_id"):
        # Distinct normalised full name forms that have a full first name (>2 chars)
        def _first_tok(norm):
            parts = norm.split()
            return parts[0] if parts else ""

        full_forms = {n for n in grp["norm_name"].unique() if len(_first_tok(n)) > 2}
        abbrev_rows = grp[grp["norm_name"].apply(lambda n: len(_first_tok(n)) <= 2)]

        if len(full_forms) <= 1:
            out.append(grp[["unique_id", "cluster_id"]])
            continue

        # ── Build co-investigator and FOR sets per name form ──────────────────
        coinv: dict[str, set] = defaultdict(set)
        for_sets: dict[str, set] = defaultdict(set)

        for norm_name in full_forms:
            form_rows = grp[grp["norm_name"] == norm_name]
            for _, row in form_rows.iterrows():
                gc = row["grant_code"]
                # co-investigators on this grant (everyone else)
                others = raw_inv[
                    (raw_inv["grant_code"] == gc) &
                    (raw_inv["coinv_name"] != row["full_name"])
                ]["coinv_name"]
                coinv[norm_name].update(others)
                # FOR fields for this grant row
                fors = row["for_names"]
                if fors is not None:
                    for_sets[norm_name].update(fors)

        # ── Check pairwise for split signals ─────────────────────────────────
        form_list = sorted(full_forms)
        split_pairs: set[tuple] = set()

        for i, a in enumerate(form_list):
            for b in form_list[i + 1:]:
                oa = enriched_orcid.get(a)
                ob = enriched_orcid.get(b)
                different_orcid = oa and ob and oa != ob
                disjoint_coinv  = len(coinv[a] & coinv[b]) == 0
                disjoint_for    = len(for_sets[a] & for_sets[b]) == 0
                if different_orcid or (disjoint_coinv and disjoint_for):
                    split_pairs.add((a, b))

        if not split_pairs:
            out.append(grp[["unique_id", "cluster_id"]])
            continue

        # ── Assign each full-name form to a sub-cluster ───────────────────────
        # Use union-find to group compatible name forms together
        parent = {f: f for f in form_list}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        # Forms NOT in split_pairs are compatible → union them
        for i, a in enumerate(form_list):
            for b in form_list[i + 1:]:
                if (a, b) not in split_pairs:
                    union(a, b)

        root_to_forms: dict[str, list] = defaultdict(list)
        for f in form_list:
            root_to_forms[find(f)].append(f)

        if len(root_to_forms) <= 1:
            out.append(grp[["unique_id", "cluster_id"]])
            continue

        n_splits += 1
        print(f"  Multi-name split: {cid}")
        for root, forms in root_to_forms.items():
            print(f"    sub-cluster: {forms}")

        # Build sub-cluster FOR sets (union across forms in each sub-cluster)
        sub_for: dict[str, set] = {}
        for root, forms in root_to_forms.items():
            sub_for[root] = set().union(*(for_sets[f] for f in forms))

        # Assign full-name rows
        norm_to_root = {f: find(f) for f in form_list}
        sub = grp.copy()

        def assign(row):
            nn = row["norm_name"]
            if nn in norm_to_root:
                return f"{cid}_nm_{norm_to_root[nn][:8]}"
            # Abbreviated form: assign by FOR overlap with sub-clusters
            fors = set(row["for_names"]) if row["for_names"] is not None else set()
            if fors:
                scores = {root: len(fors & sf) for root, sf in sub_for.items()}
                best_root, best_score = max(scores.items(), key=lambda x: x[1])
                if best_score > 0:
                    return f"{cid}_nm_{best_root[:8]}"
            return row["unique_id"]  # no FOR overlap → own cluster

        sub["cluster_id"] = sub.apply(assign, axis=1)
        out.append(sub[["unique_id", "cluster_id"]])

    print(f"  Multi-name split: {n_splits} cluster(s) split")
    return pd.concat(out, ignore_index=True)


def _split_orcid_conflicts(
    df_cluster_ids: pd.DataFrame, df_original: pd.DataFrame
) -> pd.DataFrame:
    merged = df_original[["unique_id", "orcid"]].merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])

    out = []
    for cid, grp in merged.groupby("cluster_id"):
        distinct_orcids = grp["orcid"].dropna().unique()
        if len(distinct_orcids) <= 1:
            out.append(grp[["unique_id", "cluster_id"]])
        else:
            sub = grp.copy()
            sub["cluster_id"] = sub.apply(
                lambda r: f"orcid_{r['orcid']}" if pd.notna(r["orcid"]) else r["unique_id"],
                axis=1,
            )
            out.append(sub[["unique_id", "cluster_id"]])

    return pd.concat(out, ignore_index=True)


def _aggregate_clusters(df_original: pd.DataFrame, df_cluster_ids: pd.DataFrame) -> pd.DataFrame:
    merged = df_original.merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])

    def union_lists(series):
        out = set()
        for lst in series:
            if lst is not None:
                out.update(lst)
        return sorted(out)

    def distinct_nonempty(series):
        return sorted({v for v in series if pd.notna(v) and v})

    return (
        merged.groupby("cluster_id")
        .agg(
            full_names=("full_name", lambda s: sorted(set(s))),
            first_names=("first_names", union_lists),
            family_names=("family_names", union_lists),
            orcids=("orcid", distinct_nonempty),
            inst_arr=("inst_arr", union_lists),
            for_names=("for_names", union_lists),
            for_codes=("for_codes", union_lists),
            grant_ids=("unique_id", list),
            n_grants=("unique_id", "count"),
        )
        .reset_index()
    )


def _load_inst_names() -> dict[str, str]:
    lookup = {}
    if not ADMIN_ORGS_CSV.exists():
        return lookup
    with open(ADMIN_ORGS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            iid = (row.get("institution_id") or "").strip()
            name = (row.get("organisationName") or "").strip()
            if iid and name:
                lookup[iid] = name
    return lookup


def _inst_label(inst_id: str, lookup: dict) -> str:
    return lookup.get(inst_id, inst_id.split("/")[-1] if inst_id else "?")


def _division_mismatch(for_names: list, div_map: dict, adj: set) -> bool:
    divs = list({div_map.get(n, "?") for n in for_names if n and div_map.get(n)})
    if len(divs) <= 1:
        return False

    parent = list(range(len(divs)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(len(divs)):
        for j in range(i + 1, len(divs)):
            if divs[i] == divs[j] or frozenset([divs[i], divs[j]]) in adj:
                ri, rj = find(i), find(j)
                if ri != rj:
                    parent[ri] = rj

    return len({find(i) for i in range(len(divs))}) > 1


def _is_case_a(full_names: list) -> bool:
    firsts = set()
    for name in full_names:
        parts = name.strip().split()
        if parts:
            firsts.add(parts[0].lower())
    return len(firsts) <= 1


def _is_suspicious(
    row: pd.Series,
    div_map: dict,
    adj: set,
    tf_lookup: dict,
) -> bool:
    if row["orcids"]:
        return False
    fnk = row.get("full_name_key")
    if fnk and tf_lookup.get(fnk, 1.0) < RARE_NAME_TF:
        return False
    if not _division_mismatch(row["for_names"], div_map, adj):
        return False
    return True


def _diagnostic_report(
    persons: pd.DataFrame,
    inst_lookup: dict,
    div_map: dict,
    adj: set,
    tf_lookup: dict,
) -> None:
    multi = persons[persons["n_grants"] > 1].sort_values("n_grants", ascending=False)
    singletons = (persons["n_grants"] == 1).sum()
    print(f"\n  Clusters: {len(persons)}  ({singletons} singletons, {len(multi)} multi-grant)")

    one_inst   = multi[multi["inst_arr"].apply(len) == 1]
    multi_inst = multi[multi["inst_arr"].apply(len) > 1]
    no_inst    = multi[multi["inst_arr"].apply(len) == 0]
    print(f"    1 institution:   {len(one_inst):>4}  (same person, multiple grants)")
    print(f"    2+ institutions: {len(multi_inst):>4}  (career moves or check for mis-merge)")
    print(f"    0 institutions:  {len(no_inst):>4}")

    suspect = multi_inst[multi_inst.apply(
        lambda r: _is_suspicious(r, div_map, adj, tf_lookup), axis=1
    )]
    case_a = suspect[suspect["full_names"].apply(_is_case_a)]
    case_b = suspect[~suspect["full_names"].apply(_is_case_a)]

    auto_committed = len(multi_inst) - len(suspect)
    print(f"\n  Multi-inst auto-committed (ORCID / rare name / compatible divisions): {auto_committed}")
    print(f"  Needs review (multi-inst, common name, no ORCID, cross-division FOR): {len(suspect)}")
    print(f"    Case A — single name: {len(case_a)}")
    print(f"    Case B — multiple names: {len(case_b)}")

    def _show(df, label, n=20):
        print(f"\n  {label} (top {n} by grant count):")
        for _, row in df.head(n).iterrows():
            names  = "; ".join(row["full_names"][:4])
            orcids = ", ".join(row["orcids"]) if row["orcids"] else "—"
            insts  = "; ".join(_inst_label(i, inst_lookup) for i in row["inst_arr"][:4])
            divs   = sorted({div_map.get(fn, "?") for fn in row["for_names"] if fn})
            fors   = "; ".join(row["for_names"]) if row["for_names"] else "—"
            print(f"    n={row['n_grants']:>3}  [{orcids}]  {names}")
            print(f"           insts: {insts}")
            print(f"           FOR:   {fors}")
            print(f"           divs:  {' '.join(divs)}")

    _show(case_a, "Case A — single name, needs review")
    _show(case_b, "Case B — multiple names, needs review")


def _export_manual_splits_template(
    persons: pd.DataFrame,
    inst_lookup: dict,
    div_map: dict,
    adj: set,
    tf_lookup: dict,
) -> None:
    multi_inst = persons[
        (persons["n_grants"] > 1)
        & (persons["inst_arr"].apply(len) > 1)
        & (persons["full_names"].apply(_is_case_a))
        & persons.apply(lambda r: _is_suspicious(r, div_map, adj, tf_lookup), axis=1)
    ].sort_values("n_grants", ascending=False)

    existing: dict[str, dict] = {}
    if MANUAL_SPLITS_CSV.exists():
        with open(MANUAL_SPLITS_CSV, newline="") as f:
            for row in csv.DictReader(f):
                existing[row["cluster_id"]] = row

    rows = []
    for _, p in multi_inst.iterrows():
        cid = p["cluster_id"]
        prev = existing.get(cid, {})
        rows.append({
            "cluster_id":               cid,
            "n_grants":                 p["n_grants"],
            "names":                    "; ".join(p["full_names"]),
            "orcids":                   "; ".join(p["orcids"]) if p["orcids"] else "",
            "institutions":             "; ".join(_inst_label(i, inst_lookup) for i in p["inst_arr"]),
            "for_names":                "; ".join(p["for_names"]),
            "grant_ids":                "; ".join(p["grant_ids"]),
            "confirmed_different_people": prev.get("confirmed_different_people", ""),
            "notes":                    prev.get("notes", ""),
        })

    with open(MANUAL_SPLITS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  Case A template written → {MANUAL_SPLITS_CSV}")
    coded = sum(1 for r in rows if r["confirmed_different_people"].strip().lower() == "true")
    print(f"  {coded} of {len(rows)} clusters hand-coded as 'confirmed_different_people'")


def _apply_manual_splits(
    df_cluster_ids: pd.DataFrame, df_original: pd.DataFrame
) -> pd.DataFrame:
    if not MANUAL_SPLITS_CSV.exists():
        return df_cluster_ids

    split_clusters: set[str] = set()
    with open(MANUAL_SPLITS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("confirmed_different_people", "").strip().lower() == "true":
                split_clusters.add(row["cluster_id"])

    if not split_clusters:
        return df_cluster_ids

    print(f"  Applying manual splits for {len(split_clusters)} cluster(s)...")

    merged = df_original[["unique_id", "institution_oax_id"]].merge(
        df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left"
    )
    merged["cluster_id"] = merged["cluster_id"].fillna(merged["unique_id"])

    out = []
    for cid, grp in merged.groupby("cluster_id"):
        if cid not in split_clusters:
            out.append(grp[["unique_id", "cluster_id"]])
            continue
        sub = grp.copy()
        sub["cluster_id"] = sub["institution_oax_id"].apply(
            lambda inst: f"{cid}_inst_{inst}" if (pd.notna(inst) and inst) else sub["unique_id"]
        )
        out.append(sub[["unique_id", "cluster_id"]])

    return pd.concat(out, ignore_index=True)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    con = duckdb.connect()

    # ── Phase 1: Prepare ARC investigators ───────────────────────────────────

    print("=== Phase 1: ARC prep ===")
    con.create_function("arc_names", arc_name_arrays,
                        ['VARCHAR', 'VARCHAR'],
                        'STRUCT(first_names VARCHAR[], family_names VARCHAR[])')

    concordance_csv = Path(__file__).resolve().parents[1] / "config" / "for_concordance.csv"
    expanded_for_tokens = make_expanded_for_tokens(str(concordance_csv))
    con.create_function("for_tokens", expanded_for_tokens,
                        ['VARCHAR'],
                        'VARCHAR[]')

    _lu = ForTopicLookup()
    con.create_function("upgrade_for_code",
                        lambda code: upgrade_for_code(_lu, code),
                        ['VARCHAR'], 'VARCHAR',
                        null_handling='special')
    con.create_function("upgrade_for_name",
                        lambda code, name: upgrade_for_name(_lu, code, name),
                        ['VARCHAR', 'VARCHAR'], 'VARCHAR',
                        null_handling='special')

    out_arc = PROCESSED_DATA / "arc_investigators_prep.parquet"

    roles_sql   = ", ".join(f"'{r}'" for r in KEEP_ROLES)
    schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)

    print(f"  Saving to {out_arc}...")
    con.execute(f"""
        COPY (
            WITH arc_raw AS (
                SELECT
                    i.unique_id,
                    i.grant_code as grant_id,
                    i.first_name,
                    i.family_name,
                    g.admin_org as AdminOrg,
                    i.role_code as role,
                    i.orcid,
                    upgrade_for_name(
                        regexp_extract(s.primary_field_of_research, '^\\d{{4}}'),
                        g.primary_for_name
                    ) as for_name,
                    COALESCE(
                        upgrade_for_code(regexp_extract(s.primary_field_of_research, '^\\d{{4}}')),
                        regexp_extract(s.primary_field_of_research, '^\\d{{4}}')
                    ) as for_code
                FROM '{PROCESSED_DATA}/investigators_raw.parquet' i
                LEFT JOIN '{PROCESSED_DATA}/grants_flat.parquet' g
                    ON i.grant_code = g.grant_code
                LEFT JOIN read_csv_auto('{GRANT_SUMMARIES_CSV}') s
                    ON i.grant_code = s.grant_id
                WHERE i.role_code IN ({roles_sql})
                  AND substring(i.grant_code, 1, 2) IN ({schemes_sql})
            )
            SELECT
                a.unique_id,
                CONCAT_WS(' ', a.first_name, a.family_name) AS full_name,
                arc_names(a.first_name, a.family_name).first_names AS first_names,
                list_filter(arc_names(a.first_name, a.family_name).first_names, x -> len(x) = 1)  AS first_initials,
                list_filter(arc_names(a.first_name, a.family_name).first_names, x -> len(x) > 1)  AS first_name_full,
                arc_names(a.first_name, a.family_name).family_names AS family_names,
                a.orcid,
                o.institution_id as institution_oax_id,
                [a.for_name] as for_names,
                list_filter([a.for_code], x -> x != '') as for_codes,
                for_tokens(a.for_name) as for_name_tokens,
                'AU' as country_code
            FROM arc_raw a
            INNER JOIN read_csv_auto('{ADMIN_ORGS_CSV}') o
                ON a.AdminOrg = o.organisationName_alias
        ) TO '{out_arc}' (FORMAT PARQUET)
    """)
    print("  ARC prep complete.")

    # ── Phase 2: Deduplicate ARC persons ─────────────────────────────────────

    print("\n=== Phase 2: ARC dedupe ===")
    arc_path = PROCESSED_DATA / "arc_investigators_prep.parquet"
    out_path  = PROCESSED_DATA / "arc_persons.parquet"

    print("[1/5] Loading full ARC grant rows...")
    df_raw = _load_all(con, arc_path)

    print("[2/5] Preparing Splink input columns...")
    df = _prep(df_raw)
    print(f"  first_initial coverage: "
          f"{df['first_initial'].notna().sum()} / {len(df)} rows")

    settings = SettingsCreator(
        unique_id_column_name="unique_id",
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("family_name_main", "first_initial"),
            "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        ],
        comparisons=[
            cl.CustomComparison(
                output_column_name="first_name_canonical",
                comparison_description="First name: exact / initial-match / full-mismatch",
                comparison_levels=[
                    {
                        "sql_condition": "first_name_canonical_l IS NULL OR first_name_canonical_r IS NULL",
                        "label_for_charts": "null",
                        "is_null_level": True,
                    },
                    {
                        "sql_condition": "first_name_canonical_l = first_name_canonical_r",
                        "label_for_charts": "Exact match",
                        "tf_adjustment_column": "first_name_canonical",
                        "tf_adjustment_weight": 1.0,
                    },
                    {
                        "sql_condition": (
                            "(length(first_name_canonical_l) = 1"
                            " AND length(first_name_canonical_r) > 1"
                            " AND first_name_canonical_l = substr(first_name_canonical_r, 1, 1))"
                            " OR"
                            " (length(first_name_canonical_r) = 1"
                            " AND length(first_name_canonical_l) > 1"
                            " AND first_name_canonical_r = substr(first_name_canonical_l, 1, 1))"
                        ),
                        "label_for_charts": "Initial matches full name",
                    },
                    {
                        "sql_condition": (
                            "length(first_name_canonical_l) > 1"
                            " AND length(first_name_canonical_r) > 1"
                            " AND first_name_canonical_l != first_name_canonical_r"
                        ),
                        "label_for_charts": "Full name mismatch",
                        "m_probability": 0.02,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other",
                    },
                ],
            ),
            cl.CustomComparison(
                output_column_name="family_name_main",
                comparison_levels=[
                    cll.NullLevel("family_name_main"),
                    cll.ExactMatchLevel("family_name_main").configure(
                        tf_adjustment_column="family_name_main",
                        tf_adjustment_weight=1.0,
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.CustomComparison(
                output_column_name="full_name_key",
                comparison_levels=[
                    cll.NullLevel("full_name_key"),
                    cll.ExactMatchLevel("full_name_key").configure(
                        tf_adjustment_column="full_name_key",
                        tf_adjustment_weight=1.0,
                    ),
                    cll.ElseLevel(),
                ],
            ),
            cl.ExactMatch("orcid").configure(
                m_probabilities=[0.85, 0.15]
            ),
            cl.ArrayIntersectAtSizes("inst_arr", [1]),
            cl.ArrayIntersectAtSizes("for_name_tokens", [2, 1]).configure(
                m_probabilities=[0.35, 0.45, 0.20]
            ),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker(df, settings, db_api=db_api)

    print("  Registering OAX name frequency tables...")
    for fname, col in [
        ("oax_tf_family_name.parquet",  "family_name_main"),
        ("oax_tf_first_name.parquet",   "first_name_canonical"),
        ("oax_tf_full_name.parquet",    "full_name_key"),
    ]:
        tf = pd.read_parquet(PROCESSED_DATA / fname)
        linker.table_management.register_term_frequency_lookup(tf, col)

    print("[3/5] Training EM model...")
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("family_name_main")],
        recall=0.8,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        fix_u_probabilities=True,
    )

    print("[4/5] Predicting matches and clustering...")
    df_pred = linker.inference.predict(threshold_match_probability=0.5)
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_pred, threshold_match_probability=CLUSTER_THRESHOLD
    )
    df_cluster_ids = df_clusters.as_pandas_dataframe()

    print("  Applying ORCID conflict split...")
    df_cluster_ids = _split_orcid_conflicts(df_cluster_ids, df_raw)
    print("  Applying multi-name cluster split...")
    orcid_enrichment = None
    enrichment_path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if enrichment_path.exists():
        orcid_enrichment = pd.read_parquet(enrichment_path)
    df_cluster_ids = _split_multi_name_clusters(df_cluster_ids, df_raw, orcid_enrichment)
    print("  Applying manual splits (config/manual_splits.csv)...")
    df_cluster_ids = _apply_manual_splits(df_cluster_ids, df_raw)

    n_clusters = df_cluster_ids["cluster_id"].nunique()
    n_records  = len(df_cluster_ids)
    print(f"  {n_clusters} clusters from {n_records} records "
          f"({n_records/n_clusters:.2f} grants/person avg)")

    print("[5/5] Aggregating to person-level records...")
    df_raw["inst_arr"] = df_raw["institution_oax_id"].apply(
        lambda x: [x] if (pd.notna(x) and x) else []
    )
    df_raw["for_names"] = df_raw["for_names"].apply(
        lambda x: list(x) if x is not None else []
    )
    df_raw["for_codes"] = df_raw["for_codes"].apply(
        lambda x: list(x) if x is not None else []
    )
    persons = _aggregate_clusters(df_raw, df_cluster_ids)

    persons.to_parquet(out_path, index=False)
    print(f"  Saved {len(persons)} person records → {out_path}")

    map_path = PROCESSED_DATA / "arc_grant_cluster_map.parquet"
    df_cluster_ids[["unique_id", "cluster_id"]].to_parquet(map_path, index=False)
    print(f"  Saved grant→cluster map → {map_path}")

    fnk = (
        df[["unique_id", "full_name_key"]]
        .merge(df_cluster_ids[["unique_id", "cluster_id"]], on="unique_id", how="left")
    )
    fnk["cluster_id"] = fnk["cluster_id"].fillna(fnk["unique_id"])
    fnk_mode = (
        fnk.dropna(subset=["full_name_key"])
        .groupby("cluster_id")["full_name_key"]
        .agg(lambda s: s.mode().iloc[0] if len(s) else None)
        .reset_index()
        .rename(columns={"full_name_key": "full_name_key"})
    )
    persons = persons.merge(fnk_mode, on="cluster_id", how="left")

    div_map, adj = _load_for_divisions()
    tf_df = pd.read_parquet(PROCESSED_DATA / "oax_tf_full_name.parquet")
    tf_lookup = dict(zip(tf_df["full_name_key"], tf_df["tf_full_name_key"]))

    inst_lookup = _load_inst_names()
    _diagnostic_report(persons, inst_lookup, div_map, adj, tf_lookup)
    _export_manual_splits_template(persons, inst_lookup, div_map, adj, tf_lookup)


if __name__ == "__main__":
    main()
