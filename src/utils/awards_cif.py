"""
src/awards_cif.py

AwardsCIF() -- the ARC-side identity object: a provisional grouping of Award-CI/F items
(one row per grant x ARC Chief Investigator/Fellow, already role/scheme-filtered to
KEEP_ROLES ∩ KEEP_SCHEMES) under evidence that they are potentially the same real person.

This module holds the data model only (AwardCIFItem, AwardsCIF). Construction -- the
functions that load items, cluster them, and refine those clusters into resolved
AwardsCIF() instances -- lives alongside this module and is planned separately; see
/home/lc/.claude/plans/review-this-code-base-groovy-key.md for the full design and the
reasoning behind treating this as the primary object with today's 01_prepare_arc.py logic
rebuilt onto it, rather than a wrapper constructed around that logic.

Field provenance: AwardsCIF's fields mirror arc_persons.parquet's real output columns
(01_prepare_arc.py's Phase 2), not a guessed shape -- including gap_candidates and
orcid_for_codes, which an earlier planning draft of this object missed. Aggregated fields
(inst_arr, for_names, for_codes) are sorted deduplicated lists, matching how
_aggregate_clusters() actually produces them, not sets -- kept this way so a rebuilt
AwardsCIF can be diffed field-for-field against today's arc_persons.parquet.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import diskcache
import duckdb
import pandas as pd
from nameparser import HumanName
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

from config.settings import PROCESSED_DATA, ADMIN_ORGS_CSV, GRANT_SUMMARIES_CSV, DISKCACHE_DIR
from config.scope import KEEP_ROLES, KEEP_SCHEMES
from src.utils.names import make_expanded_for_tokens, name_part_tokens, strip_diacriticals, for_name_tokens
from src.utils.for_resolve import upgrade_for_code, upgrade_for_name

_DATA_PERSISTED = Path(__file__).resolve().parents[2] / "data_persisted"
_FOR_CONCORDANCE_CSV = _DATA_PERSISTED / "for_concordance.csv"
_MANUAL_NAME_CORRECTIONS_CSV = _DATA_PERSISTED / "manual_name_corrections.csv"
_MANUAL_SPLITS_CSV = _DATA_PERSISTED / "manual_splits.csv"
_MANUAL_ORCIDS_CSV = _DATA_PERSISTED / "manual_orcids.csv"
_MANUAL_MERGES_CSV = _DATA_PERSISTED / "manual_merges.csv"
_ENRICHMENT_BLOCKLIST_CSV = _DATA_PERSISTED / "enrichment_blocklist.csv"

CLUSTER_THRESHOLD = 0.9  # same value as 01_prepare_arc.py -- high precision, prefer splitting over merging


@dataclass(frozen=True)
class AwardCIFItem:
    """One row per (grant x ARC CI/Fellow) -- the atomic unit AwardsCIF() clusters.

    Raw ARC facts plus the normalized/derived name forms Splink's dedupe_only blocking and
    comparisons need (family_name_main, first_initial, first_name_canonical, full_name_key,
    for_name_tokens) -- computed once at load time by arc_name_arrays()/_prep()'s current
    logic, not left to be recomputed lazily on every access.
    """

    # raw ARC facts (investigators_raw.parquet joined to grants_flat.parquet)
    unique_id: str
    grant_code: str
    first_name: str
    family_name: str
    role_code: str
    orcid: str | None
    admin_org: str | None
    institution_oax_id: str | None
    funding_commence_year: int | None
    for_name: str | None  # ANZSRC name for this grant, upgraded to 2020 series
    for_code: str | None  # ANZSRC code for this grant, upgraded to 2020 series

    # derived, normalized forms (mirrors 01_prepare_arc.py's arc_name_arrays()/_prep())
    full_name: str
    first_names: list[str] = field(default_factory=list)
    family_names: list[str] = field(default_factory=list)
    family_name_main: str | None = None
    first_initial: str | None = None
    first_name_canonical: str | None = None
    full_name_key: str | None = None
    for_name_tokens: list[str] = field(default_factory=list)


@dataclass
class AwardsCIF:
    """A provisional grouping of AwardCIFItems under evidence they are potentially the same
    real ARC CI/Fellow. Fields mirror arc_persons.parquet's real output columns exactly --
    see module docstring -- so a rebuilt AwardsCIF can be diffed against it field-for-field.

    Deliberately not yet present: any works/oeuvre data, or a definitive-gate status/score.
    Those are added by later roadmap steps (see the plan file), not this object as built here.
    """

    cluster_id: str  # = arc_id, unchanged key
    items: list[AwardCIFItem] = field(default_factory=list)

    # aggregated across items -- sorted deduplicated lists, matching _aggregate_clusters()
    full_names: list[str] = field(default_factory=list)
    first_names: list[str] = field(default_factory=list)
    family_names: list[str] = field(default_factory=list)
    orcids: list[str] = field(default_factory=list)
    inst_arr: list[str] = field(default_factory=list)
    for_names: list[str] = field(default_factory=list)
    for_codes: list[str] = field(default_factory=list)
    full_name_key: str | None = None  # modal full_name_key across items

    grant_ids: list[str] = field(default_factory=list)  # = [item.unique_id for item in items]
    n_grants: int = 0

    orcid_status: str = "NO_ORCID"  # HAS_ORCID | NO_ORCID | MULTI_ORCID
    orcid_for_codes: list[dict] = field(default_factory=list)  # ERA FOR codes via for_cache
    gap_candidates: list[str] = field(default_factory=list)  # other cluster_ids not ruled out
    reliability_tier: str | None = None  # 1a | 1b | 1c | 2 | 3 | 4 | 4u
    resolution_status: str = "UNRESOLVED"  # RESOLVED | UNRESOLVED

    # candidate OpenAlex author_idx set -- {oax_id} ∪ secondary_oax_ids, populated later
    oax_candidates: list[str] = field(default_factory=list)

    # renamed from cluster_history; same JSON-event-log shape and existing event types
    # (splink_cluster, orcid_merge, manual_split, ...) -- ARC-side identity events only
    # in this pass; oeuvre-level exclusion provenance is appended by later work-set
    # operations, not a different owner.
    provenance: list[dict] = field(default_factory=list)

    def record_event(self, event: str, **details) -> None:
        """Append a structured provenance event. Every construction/refinement operation
        should call this rather than appending to `provenance` directly, so event shape
        stays consistent."""
        self.provenance.append({"event": event, **details})


# ── construction: load_award_cif_items ──────────────────────────────────────────

def _name_forms(first_name: str, family_name: str) -> tuple[list[str], list[str]]:
    """Mirrors 01_prepare_arc.py's arc_name_arrays(): given-name tokens (+ their initials)
    and a single normalized family-name form, from ARC's raw first_name/family_name fields."""
    full = f"{first_name or ''} {family_name or ''}".strip()
    hn = HumanName(full)
    if not hn.last and hn.first:
        hn.last = hn.first

    f_toks = name_part_tokens(hn.first) + name_part_tokens(hn.middle)
    fam_norm = strip_diacriticals(hn.last).lower().strip() if hn.last else ""

    first_names = list(set(f_toks + [t[0] for t in f_toks if t]))
    family_names = [fam_norm] if fam_norm else []

    if not f_toks and fam_norm:
        first_names.append(fam_norm[0])

    return list(set(first_names)), family_names


def _first_initial(first_names: list[str]) -> str | None:
    if not first_names:
        return None
    full = [t for t in first_names if len(t) > 1]
    return full[0][0] if full else first_names[0][0]


def _first_name_canonical(first_names: list[str]) -> str | None:
    full_toks = [t for t in first_names if len(t) > 1]
    initials = [t for t in first_names if len(t) == 1]
    if full_toks:
        return max(full_toks, key=len)
    if initials:
        return initials[0]
    return None


def _load_manual_name_corrections() -> dict[str, dict]:
    """data_persisted/manual_name_corrections.csv -- hand-confirmed fixes for ARC-source typos
    (e.g. LP0211975_MarieMalherbe: ARC's own investigators-at-announcement record has "Marie"
    where investigators-current and 4 other grants sharing the same ORCID all say "François" --
    confirmed against raw_json.csv directly, not a guess). Keyed on unique_id, not cluster_id,
    since the correction applies to one raw item before any clustering happens."""
    if not _MANUAL_NAME_CORRECTIONS_CSV.exists():
        return {}
    import csv as _csv
    corrections: dict[str, dict] = {}
    with open(_MANUAL_NAME_CORRECTIONS_CSV, newline="") as f:
        for row in _csv.DictReader(f):
            uid = row["unique_id"].strip()
            if uid:
                corrections[uid] = row
    return corrections


def load_award_cif_items(
    con: duckdb.DuckDBPyConnection | None = None,
) -> tuple[list[AwardCIFItem], dict[str, dict]]:
    """Load Award-CI/F items: investigators_raw.parquet joined to grants_flat.parquet and
    (for the FOR-code upgrade) grant_summaries.csv, filtered to KEEP_ROLES ∩ KEEP_SCHEMES,
    with normalized name forms and the upgraded ANZSRC FOR name/code attached per item.

    Mirrors 01_prepare_arc.py's Phase 1 (the arc_raw CTE + arc_names()/for_tokens() UDFs +
    final SELECT) -- same source tables and filters, typed objects instead of a parquet file.

    Returns (items, corrections_applied) -- corrections_applied maps unique_id to the
    manual_name_corrections.csv row that was applied, if any. AwardCIFItem itself carries no
    provenance (it's a raw atomic unit, not a resolved identity) -- cluster_items() is
    responsible for recording a provenance event on any AwardsCIF whose items were corrected,
    once that cluster actually exists.
    """
    corrections = _load_manual_name_corrections()
    own_con = con is None
    con = con or duckdb.connect()
    try:
        roles_sql = ", ".join(f"'{r}'" for r in KEEP_ROLES)
        schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)

        rows = con.execute(f"""
            SELECT
                i.unique_id,
                i.grant_code,
                i.first_name,
                i.family_name,
                i.role_code,
                i.orcid,
                g.admin_org,
                o.institution_id AS institution_oax_id,
                g.funding_commence_year,
                g.primary_for_name,
                regexp_extract(s.primary_field_of_research, '^\\d{{4}}') AS for2008_code
            FROM read_parquet('{PROCESSED_DATA}/investigators_raw.parquet') i
            LEFT JOIN read_parquet('{PROCESSED_DATA}/grants_flat.parquet') g
                ON i.grant_code = g.grant_code
            LEFT JOIN read_csv_auto('{GRANT_SUMMARIES_CSV}') s
                ON i.grant_code = s.grant_id
            LEFT JOIN read_csv_auto('{ADMIN_ORGS_CSV}') o
                ON g.admin_org = o.organisationName_alias
            WHERE i.role_code IN ({roles_sql})
              AND substring(i.grant_code, 1, 2) IN ({schemes_sql})
        """).fetchall()
        col_names = [d[0] for d in con.description]
    finally:
        if own_con:
            con.close()

    expanded_for_tokens = make_expanded_for_tokens(str(_FOR_CONCORDANCE_CSV))

    items: list[AwardCIFItem] = []
    for row in rows:
        r = dict(zip(col_names, row))

        first_name = r["first_name"]
        correction = corrections.get(r["unique_id"])
        if correction is not None:
            first_name = correction["correct_first_name"]

        for_name = upgrade_for_name(r["for2008_code"], r["primary_for_name"])
        for_code = upgrade_for_code(r["for2008_code"]) or r["for2008_code"]

        first_names, family_names = _name_forms(first_name, r["family_name"])
        family_name_main = max(family_names, key=len) if family_names else None
        first_initial = _first_initial(first_names)
        first_name_canonical = _first_name_canonical(first_names)
        full_name_key = (
            f"{first_name_canonical}_{family_name_main}"
            if first_name_canonical and family_name_main
            else None
        )

        items.append(AwardCIFItem(
            unique_id=r["unique_id"],
            grant_code=r["grant_code"],
            first_name=first_name,
            family_name=r["family_name"],
            role_code=r["role_code"],
            orcid=r["orcid"],
            admin_org=r["admin_org"],
            institution_oax_id=r["institution_oax_id"],
            funding_commence_year=r["funding_commence_year"],
            for_name=for_name,
            for_code=for_code,
            full_name=f"{first_name} {r['family_name']}",
            first_names=first_names,
            family_names=family_names,
            family_name_main=family_name_main,
            first_initial=first_initial,
            first_name_canonical=first_name_canonical,
            full_name_key=full_name_key,
            for_name_tokens=expanded_for_tokens(for_name),
        ))

    return items, corrections


# ── construction: cluster_items ─────────────────────────────────────────────────

def _build_awards_cif(cluster_id: str, items: list[AwardCIFItem]) -> AwardsCIF:
    """Aggregate a group of items into one AwardsCIF -- mirrors 01_prepare_arc.py's
    _aggregate_clusters(), sorted deduplicated lists per field, modal full_name_key."""
    orcids = sorted({it.orcid for it in items if it.orcid})
    fnk_counts = Counter(it.full_name_key for it in items if it.full_name_key)

    return AwardsCIF(
        cluster_id=cluster_id,
        items=items,
        full_names=sorted({it.full_name for it in items}),
        first_names=sorted({fn for it in items for fn in it.first_names}),
        family_names=sorted({fn for it in items for fn in it.family_names}),
        orcids=orcids,
        inst_arr=sorted({it.institution_oax_id for it in items if it.institution_oax_id}),
        for_names=sorted({it.for_name for it in items if it.for_name}),
        for_codes=sorted({it.for_code for it in items if it.for_code}),
        full_name_key=fnk_counts.most_common(1)[0][0] if fnk_counts else None,
        grant_ids=[it.unique_id for it in items],
        n_grants=len(items),
        orcid_status=(
            "HAS_ORCID" if len(orcids) == 1 else
            "MULTI_ORCID" if len(orcids) > 1 else
            "NO_ORCID"
        ),
    )


def cluster_items(
    items: list[AwardCIFItem],
    corrections: dict[str, dict] | None = None,
) -> list[AwardsCIF]:
    """Cluster Award-CI/F items into provisional AwardsCIF() groupings via Splink `dedupe_only`
    -- the exact comparison/blocking configuration from 01_prepare_arc.py's Phase 2, reused
    unchanged as a tool (Splink itself isn't being redesigned, only what's built from its output
    -- see the tool/architecture split in the plan). What changes: instead of writing a
    cluster_id column onto a DataFrame, the clustering result directly constructs one
    AwardsCIF() per group, recording a splink_cluster provenance event on each, plus a
    name_typo_correction event on any cluster containing an item load_award_cif_items()
    corrected.
    """
    corrections = corrections or {}

    df = pd.DataFrame([{
        "unique_id": it.unique_id,
        "first_name_canonical": it.first_name_canonical,
        "family_name_main": it.family_name_main,
        "full_name_key": it.full_name_key,
        "first_initial": it.first_initial,
        "orcid": it.orcid,
        "inst_arr": [it.institution_oax_id] if it.institution_oax_id else [],
        "for_name_tokens": it.for_name_tokens,
    } for it in items])

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

    for fname, col in [
        ("oax_tf_family_name.parquet", "family_name_main"),
        ("oax_tf_first_name.parquet", "first_name_canonical"),
        ("oax_tf_full_name.parquet", "full_name_key"),
    ]:
        tf = pd.read_parquet(PROCESSED_DATA / fname)
        linker.table_management.register_term_frequency_lookup(tf, col)

    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("family_name_main")],
        recall=0.8,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        fix_u_probabilities=True,
    )

    df_pred = linker.inference.predict(threshold_match_probability=0.5)
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_pred, threshold_match_probability=CLUSTER_THRESHOLD
    )
    df_cluster_ids = df_clusters.as_pandas_dataframe()

    item_by_uid = {it.unique_id: it for it in items}
    groups: dict[str, list[AwardCIFItem]] = defaultdict(list)
    for row in df_cluster_ids.itertuples():
        groups[row.cluster_id].append(item_by_uid[row.unique_id])

    clusters: list[AwardsCIF] = []
    for cluster_id, group_items in groups.items():
        cif = _build_awards_cif(cluster_id, group_items)
        cif.record_event("splink_cluster")
        for it in group_items:
            correction = corrections.get(it.unique_id)
            if correction is not None:
                cif.record_event(
                    "name_typo_correction",
                    unique_id=it.unique_id,
                    wrong_first_name=correction["wrong_first_name"],
                    correct_first_name=correction["correct_first_name"],
                    notes=correction.get("notes"),
                )
        clusters.append(cif)

    return clusters


# ── construction: refine_clusters ───────────────────────────────────────────────
#
# Nine operations ported from 01_prepare_arc.py's Phase 2, same order as its main(). Each
# takes and returns list[AwardsCIF] -- merges/splits re-aggregate via _build_awards_cif and
# carry provenance forward; ORCID-injection steps mutate in place (AwardsCIF isn't frozen).
# Validated logic, reused unchanged; only the representation (typed objects, not DataFrame
# columns) is new -- see the plan file's tool/architecture split.

def _norm_full(name: str) -> str:
    return re.sub(r"[^a-z ]", "", name.lower()).strip()


def _merge_awards_cifs(canonical: AwardsCIF, absorbed: list[AwardsCIF], event: str, **details) -> AwardsCIF:
    """Merge `absorbed` clusters into `canonical`: re-aggregate over the union of items,
    concatenate provenance, then append a new event describing the merge."""
    all_items = canonical.items + [it for c in absorbed for it in c.items]
    merged = _build_awards_cif(canonical.cluster_id, all_items)
    merged.provenance = canonical.provenance + [ev for c in absorbed for ev in c.provenance]
    merged.record_event(event, merged_from=[c.cluster_id for c in absorbed], **details)
    return merged


def merge_by_orcid(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Deterministically merge clusters sharing an ORCID -- mirrors _merge_by_orcid(). Splink's
    own EM-trained clustering, even with ORCID-exact blocking, doesn't always fully unite
    same-ORCID records (confirmed empirically on Malherbe/Nakata in cluster_items()) -- this
    deterministic pass is what actually guarantees it."""
    orcid_to_ids: dict[str, list[str]] = defaultdict(list)
    for c in clusters:
        for orcid in c.orcids:
            orcid_to_ids[orcid].append(c.cluster_id)

    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            keep, drop = (ra, rb) if ra < rb else (rb, ra)
            parent[drop] = keep

    by_id = {c.cluster_id: c for c in clusters}
    for c in clusters:
        find(c.cluster_id)
    for ids in orcid_to_ids.values():
        for cid in ids[1:]:
            union(ids[0], cid)

    groups: dict[str, list[str]] = defaultdict(list)
    for c in clusters:
        groups[find(c.cluster_id)].append(c.cluster_id)

    out = []
    for root, ids in groups.items():
        if len(ids) == 1:
            out.append(by_id[root])
            continue
        canonical_id = min(ids)
        canonical = by_id[canonical_id]
        absorbed = [by_id[cid] for cid in ids if cid != canonical_id]
        shared_orcids = sorted({o for cid in ids for o in by_id[cid].orcids})
        out.append(_merge_awards_cifs(canonical, absorbed, "orcid_merge", orcid=shared_orcids))
    return out


def split_orcid_conflicts(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Split any cluster containing 2+ distinct ORCIDs -- mirrors _split_orcid_conflicts().
    No-ORCID items become singletons; each distinct-ORCID group becomes its own cluster."""
    out = []
    for c in clusters:
        distinct_orcids = sorted({it.orcid for it in c.items if it.orcid})
        if len(distinct_orcids) <= 1:
            out.append(c)
            continue

        groups: dict[str, list[AwardCIFItem]] = defaultdict(list)
        group_orcid: dict[str, str | None] = {}
        for it in c.items:
            key = it.orcid if it.orcid else f"__singleton__{it.unique_id}"
            groups[key].append(it)
            group_orcid[key] = it.orcid

        for key, its in groups.items():
            new_id = min(i.unique_id for i in its)
            new_cif = _build_awards_cif(new_id, its)
            new_cif.provenance = list(c.provenance)
            new_cif.record_event("orcid_conflict_split", split_from=c.cluster_id, orcid=group_orcid[key])
            out.append(new_cif)
    return out


def _load_coinvestigator_names() -> dict[str, list[str]]:
    """grant_code -> ['First Last', ...] for every KEEP_ROLES/KEEP_SCHEMES investigator on that
    grant -- used by split_multi_name_clusters() to find co-investigator overlap."""
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    roles_sql = ", ".join(f"'{r}'" for r in KEEP_ROLES)
    schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)
    rows = con.execute(f"""
        SELECT grant_code, first_name || ' ' || family_name AS coinv_name
        FROM read_parquet('{PROCESSED_DATA}/investigators_raw.parquet')
        WHERE role_code IN ({roles_sql}) AND substring(grant_code, 1, 2) IN ({schemes_sql})
    """).fetchall()
    con.close()
    out: dict[str, list[str]] = defaultdict(list)
    for grant_code, name in rows:
        out[grant_code].append(name.strip())
    return out


def _load_enriched_orcid_by_name() -> dict[str, str]:
    """norm_full_name -> orcid, from orcid_enrichment.parquet (if present)."""
    path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    out: dict[str, str] = {}
    for _, r in df.iterrows():
        if pd.notna(r.get("orcid")):
            out[_norm_full(f"{r['first_name']} {r['family_name']}")] = r["orcid"]
    return out


def split_multi_name_clusters(
    clusters: list[AwardsCIF],
    coinv_by_grant: dict[str, list[str]] | None = None,
    enriched_orcid: dict[str, str] | None = None,
) -> list[AwardsCIF]:
    """For clusters containing 2+ genuinely distinct full name forms, use co-investigator
    overlap, FOR-name overlap, and enriched-ORCID disagreement to detect mis-merged different
    people -- mirrors _split_multi_name_clusters(). Abbreviated name forms (first token <=2
    chars, e.g. "Chun Li") don't drive splits; after full-name forms are split, each
    abbreviated item is assigned to the sub-cluster with the best FOR overlap, or left
    singleton if none overlaps."""
    if coinv_by_grant is None:
        coinv_by_grant = _load_coinvestigator_names()
    if enriched_orcid is None:
        enriched_orcid = _load_enriched_orcid_by_name()

    def _first_tok(norm: str) -> str:
        parts = norm.split()
        return parts[0] if parts else ""

    out = []
    for c in clusters:
        distinct_orcids = {it.orcid for it in c.items if it.orcid}
        if len(distinct_orcids) == 1:
            out.append(c)
            continue

        norm_names = {it.unique_id: _norm_full(it.full_name) for it in c.items}
        full_forms = {n for n in norm_names.values() if len(_first_tok(n)) > 2}
        if len(full_forms) <= 1:
            out.append(c)
            continue

        coinv: dict[str, set] = defaultdict(set)
        for_sets: dict[str, set] = defaultdict(set)
        for it in c.items:
            nn = norm_names[it.unique_id]
            if nn not in full_forms:
                continue
            others = [n for n in coinv_by_grant.get(it.grant_code, []) if _norm_full(n) != nn]
            coinv[nn].update(others)
            if it.for_name:
                for_sets[nn].add(it.for_name)

        form_list = sorted(full_forms)
        split_pairs: set[tuple] = set()
        for i, a in enumerate(form_list):
            for b in form_list[i + 1:]:
                oa, ob = enriched_orcid.get(a), enriched_orcid.get(b)
                different_orcid = bool(oa and ob and oa != ob)
                disjoint_coinv = len(coinv[a] & coinv[b]) == 0
                disjoint_for = len(for_sets[a] & for_sets[b]) == 0
                if different_orcid or (disjoint_coinv and disjoint_for):
                    split_pairs.add((a, b))

        if not split_pairs:
            out.append(c)
            continue

        parent = {f: f for f in form_list}

        def find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for i, a in enumerate(form_list):
            for b in form_list[i + 1:]:
                if (a, b) not in split_pairs:
                    union(a, b)

        root_to_forms: dict[str, list] = defaultdict(list)
        for f in form_list:
            root_to_forms[find(f)].append(f)

        if len(root_to_forms) <= 1:
            out.append(c)
            continue

        sub_for = {
            root: set().union(*(for_sets[f] for f in forms))
            for root, forms in root_to_forms.items()
        }
        norm_to_root = {f: find(f) for f in form_list}

        def get_root(it: AwardCIFItem) -> str | None:
            nn = norm_names[it.unique_id]
            if nn in norm_to_root:
                return norm_to_root[nn]
            if it.for_name:
                scores = {root: (1 if it.for_name in sf else 0) for root, sf in sub_for.items()}
                best_root, best_score = max(scores.items(), key=lambda x: x[1])
                if best_score > 0:
                    return best_root
            return None

        assigned: dict[str, list[AwardCIFItem]] = defaultdict(list)
        for it in c.items:
            root = get_root(it)
            key = root if root is not None else f"__singleton__{it.unique_id}"
            assigned[key].append(it)

        for items in assigned.values():
            new_id = min(it.unique_id for it in items)
            new_cif = _build_awards_cif(new_id, items)
            new_cif.provenance = list(c.provenance)
            new_cif.record_event(
                "name_split", split_from=c.cluster_id,
                name_forms=sorted({norm_names[it.unique_id] for it in items}),
            )
            out.append(new_cif)

    return out


def apply_manual_splits(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Split clusters confirmed as containing 2+ different people in
    data_persisted/manual_splits.csv, dividing by institution_oax_id -- mirrors
    _apply_manual_splits()."""
    if not _MANUAL_SPLITS_CSV.exists():
        return clusters
    split_ids: set[str] = set()
    with open(_MANUAL_SPLITS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("confirmed_different_people", "").strip().lower() == "true":
                split_ids.add(row["cluster_id"])
    if not split_ids:
        return clusters

    out = []
    for c in clusters:
        if c.cluster_id not in split_ids:
            out.append(c)
            continue
        groups: dict[str, list[AwardCIFItem]] = defaultdict(list)
        for it in c.items:
            key = it.institution_oax_id or f"__singleton__{it.unique_id}"
            groups[key].append(it)
        for its in groups.values():
            new_id = min(it.unique_id for it in its)
            new_cif = _build_awards_cif(new_id, its)
            new_cif.provenance = list(c.provenance)
            new_cif.record_event(
                "manual_split", split_from=c.cluster_id, institution=its[0].institution_oax_id,
            )
            out.append(new_cif)
    return out


def apply_enriched_orcids(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Promote high/au_match-confidence ORCIDs from orcid_enrichment.parquet -- mirrors
    _apply_enriched_orcids(). Only promotes when exactly 1 distinct ORCID is found across all
    enriched name forms in a cluster, and the cluster currently has no ORCID."""
    enrichment_path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if not enrichment_path.exists():
        return clusters
    enrichment = pd.read_parquet(enrichment_path)
    enrichment = enrichment[
        enrichment["confidence"].isin(["high", "au_match"]) & enrichment["orcid"].notna()
    ]
    if len(enrichment) == 0:
        return clusters

    blocklist: set[tuple[str, str]] = set()
    if _ENRICHMENT_BLOCKLIST_CSV.exists():
        with open(_ENRICHMENT_BLOCKLIST_CSV, newline="") as f:
            for row in csv.DictReader(f):
                cid, orcid = row["cluster_id"].strip(), row["orcid"].strip()
                if cid and orcid:
                    blocklist.add((cid, orcid))

    enrich_by_name: dict[tuple[str, str], set[str]] = defaultdict(set)
    conf_by_name: dict[tuple[str, str], str] = {}
    for _, r in enrichment.iterrows():
        key = (r["first_name"], r["family_name"])
        enrich_by_name[key].add(r["orcid"])
        conf_by_name[key] = r["confidence"]

    for c in clusters:
        if c.orcids:
            continue
        found: set[str] = set()
        conf = None
        for it in c.items:
            key = (it.first_name, it.family_name)
            if key in enrich_by_name:
                found |= enrich_by_name[key]
                conf = conf_by_name[key]
        if len(found) != 1:
            continue
        orcid = next(iter(found))
        if (c.cluster_id, orcid) in blocklist:
            continue
        c.orcids = [orcid]
        c.orcid_status = "HAS_ORCID"
        c.record_event("enriched_orcid", orcid=orcid, confidence=conf)
    return clusters


def promote_low_by_for(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Promote low-confidence enrichment candidates when FOR-token overlap uniquely picks one
    AU candidate -- mirrors _promote_low_by_for(). Compares each AU candidate's ORCID-derived
    ERA FOR (from for_cache, populated by 00b_enrich_orcid.py) against the cluster's ARC
    for_names."""
    enrichment_path = PROCESSED_DATA / "orcid_enrichment.parquet"
    if not enrichment_path.exists():
        return clusters
    enrichment = pd.read_parquet(enrichment_path)
    enrichment = enrichment[
        (enrichment["confidence"] == "low")
        & enrichment["au_candidates"].notna()
        & (enrichment["au_candidates"] != "[]")
    ]
    if len(enrichment) == 0:
        return clusters

    candidates_by_name: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for _, r in enrichment.iterrows():
        key = (r["first_name"], r["family_name"])
        raw = r["au_candidates"]
        for cand in (json.loads(raw) if isinstance(raw, str) else raw):
            if cand.get("orcid"):
                candidates_by_name[key].append(cand)

    with diskcache.Cache(str(DISKCACHE_DIR / "orcid_for")) as for_cache:
        for c in clusters:
            if c.orcids:
                continue
            seen: set[str] = set()
            candidates: list[dict] = []
            for it in c.items:
                for cand in candidates_by_name.get((it.first_name, it.family_name), []):
                    if cand["orcid"] not in seen:
                        seen.add(cand["orcid"])
                        candidates.append(cand)
            if len(candidates) < 2:
                continue
            arc_toks = {tok for name in c.for_names for tok in for_name_tokens(name)}
            if not arc_toks:
                continue
            scores = []
            for cand in candidates:
                oid = cand["orcid"]
                orcid_for = for_cache.get(oid, [])
                orcid_toks = {tok for e in orcid_for for tok in for_name_tokens(e["name"])}
                scores.append((oid, len(arc_toks & orcid_toks)))
            max_score = max(s for _, s in scores)
            if max_score == 0:
                continue
            winners = [oid for oid, s in scores if s == max_score]
            if len(winners) != 1:
                continue
            winner = winners[0]
            c.orcids = [winner]
            c.orcid_status = "HAS_ORCID"
            c.record_event(
                "enriched_orcid", orcid=winner, confidence="low_for_disambiguated", for_score=max_score,
            )
    return clusters


def apply_manual_orcids(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Inject verified ORCIDs for clusters ARC data has none for --
    data_persisted/manual_orcids.csv. Mirrors _apply_manual_orcids()."""
    if not _MANUAL_ORCIDS_CSV.exists():
        return clusters
    overrides: dict[str, str] = {}
    with open(_MANUAL_ORCIDS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            cid, orcid = row["cluster_id"].strip(), row["orcid"].strip()
            if cid and orcid:
                overrides[cid] = orcid
    if not overrides:
        return clusters
    by_id = {c.cluster_id: c for c in clusters}
    for cid, orcid in overrides.items():
        c = by_id.get(cid)
        if c is None:
            continue
        c.orcids = [orcid]
        c.orcid_status = "HAS_ORCID"
        c.record_event("manual_orcid", orcid=orcid)
    return clusters


def merge_persons_by_orcid(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Merge clusters sharing an ORCID after enrichment/manual additions -- mirrors
    _merge_persons_by_orcid(). Skips merges where first-name initials are incompatible
    (signals a wrong enrichment hit)."""
    orcid_to_ids: dict[str, list[str]] = defaultdict(list)
    for c in clusters:
        for orcid in c.orcids:
            orcid_to_ids[orcid].append(c.cluster_id)
    conflicts = {o: sorted(ids) for o, ids in orcid_to_ids.items() if len(ids) > 1}
    if not conflicts:
        return clusters

    by_id = {c.cluster_id: c for c in clusters}
    remapping: dict[str, str] = {}
    for orcid, ids in conflicts.items():
        canonical = min(ids)
        absorbed = [cid for cid in ids if cid != canonical]
        can_initials = {x for x in by_id[canonical].first_names if len(x) == 1}
        skip = False
        for cid in absorbed:
            abs_initials = {x for x in by_id[cid].first_names if len(x) == 1}
            if can_initials and abs_initials and not (can_initials & abs_initials):
                skip = True
                break
        if not skip:
            for cid in absorbed:
                remapping[cid] = canonical

    if not remapping:
        return clusters

    def resolve(cid: str) -> str:
        seen = set()
        while cid in remapping and cid not in seen:
            seen.add(cid)
            cid = remapping[cid]
        return cid

    groups: dict[str, list[str]] = defaultdict(list)
    for cid in by_id:
        groups[resolve(cid)].append(cid)

    out = []
    for root, ids in groups.items():
        if len(ids) == 1:
            out.append(by_id[root])
            continue
        canonical = by_id[root]
        absorbed = [by_id[cid] for cid in ids if cid != root]
        out.append(_merge_awards_cifs(canonical, absorbed, "post_enrichment_merge"))
    return out


def apply_manual_merges(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Merge cluster pairs listed in data_persisted/manual_merges.csv -- mirrors
    _apply_manual_merges(). Applied unconditionally; cluster_keep survives, cluster_drop is
    absorbed into it."""
    if not _MANUAL_MERGES_CSV.exists():
        return clusters
    by_id = {c.cluster_id: c for c in clusters}
    remapping: dict[str, str] = {}
    with open(_MANUAL_MERGES_CSV, newline="") as f:
        for row in csv.DictReader(f):
            keep, drop = row["cluster_keep"].strip(), row["cluster_drop"].strip()
            if keep and drop and drop in by_id:
                remapping[drop] = keep
    if not remapping:
        return clusters

    def resolve(cid: str) -> str:
        seen = set()
        while cid in remapping and cid not in seen:
            seen.add(cid)
            cid = remapping[cid]
        return cid

    groups: dict[str, list[str]] = defaultdict(list)
    for cid in by_id:
        groups[resolve(cid)].append(cid)

    out = []
    for root, ids in groups.items():
        if root not in by_id:
            continue
        if len(ids) == 1:
            out.append(by_id[root])
            continue
        canonical = by_id[root]
        absorbed = [by_id[cid] for cid in ids if cid != root]
        out.append(_merge_awards_cifs(canonical, absorbed, "manual_merge"))
    return out


def merge_same_grant_coinvestigators(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Auto-merge same-blocking-key clusters on the same single-org grant -- mirrors
    _merge_same_grant_coinvestigators(). If a grant's only eligible organisation is one
    university, two clusters sharing (family_name_main, first_initial) on that grant are the
    same person. Skips pairs whose clusters already carry distinct non-empty ORCIDs."""
    grants = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet")
    if "n_eligible_orgs" not in grants.columns:
        return clusters
    single_org = set(grants.loc[grants["n_eligible_orgs"] == 1, "grant_code"])
    if not single_org:
        return clusters

    key_groups: dict[tuple, set[str]] = defaultdict(set)
    for c in clusters:
        fam = max(c.family_names, key=len) if c.family_names else None
        ini = _first_initial(c.first_names)
        if fam is None or ini is None:
            continue
        for gid in c.grant_ids:
            grant_code = gid.rsplit("_", 1)[0]
            if grant_code in single_org:
                key_groups[(grant_code, fam, ini)].add(c.cluster_id)

    groups = [ids for ids in key_groups.values() if len(ids) >= 2]
    if not groups:
        return clusters

    by_id = {c.cluster_id: c for c in clusters}
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        keep, drop = (ra, rb) if ra < rb else (rb, ra)
        parent[drop] = keep

    for ids in groups:
        ids = sorted(ids)
        non_empty = [set(by_id[cid].orcids) for cid in ids if by_id[cid].orcids]
        if len(non_empty) >= 2:
            union_all = set.union(*non_empty)
            shared = set.intersection(*non_empty)
            if len(union_all) > len(shared):
                continue  # ORCID conflict -- skip this group
        for cid in ids[1:]:
            union(ids[0], cid)

    merge_groups: dict[str, list[str]] = defaultdict(list)
    for c in clusters:
        merge_groups[find(c.cluster_id)].append(c.cluster_id)

    out = []
    for root, ids in merge_groups.items():
        if len(ids) == 1:
            out.append(by_id[root])
            continue
        canonical = by_id[root]
        absorbed = [by_id[cid] for cid in ids if cid != root]
        out.append(_merge_awards_cifs(canonical, absorbed, "same_grant_merge"))
    return out


def refine_clusters(clusters: list[AwardsCIF]) -> list[AwardsCIF]:
    """Apply the full Phase-2 refinement sequence to Splink's provisional clusters -- same
    step order as 01_prepare_arc.py's main(). See the plan file for why this order and
    decomposition is preserved (validated logic, rebuilt architecture, not a redesign)."""
    clusters = merge_by_orcid(clusters)
    clusters = split_orcid_conflicts(clusters)
    clusters = split_multi_name_clusters(clusters)
    clusters = apply_manual_splits(clusters)
    clusters = apply_enriched_orcids(clusters)
    clusters = promote_low_by_for(clusters)
    clusters = apply_manual_orcids(clusters)
    clusters = merge_persons_by_orcid(clusters)
    clusters = apply_manual_merges(clusters)
    clusters = merge_same_grant_coinvestigators(clusters)
    return clusters
