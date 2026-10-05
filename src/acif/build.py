"""
Cyclic ACIF construction engine.

Per /home/lc/.claude/plans/plan-that-in-tiny-immutable-heron.md. Built incrementally, one
reviewable piece at a time -- src/utils/awards_cif.py is being archived, so nothing here imports
from it; logic worth keeping is ported and condensed, not wrapped.

Built so far: load_items() + seed() -- raw ARC data in, one singleton AwardsCIF per item out
(the plan's "Explicit seeding (stage 1)"). Nothing else yet: no name parsing, no FOR2020/HEP/
institution derivation, no merge tests, no nested loop. Each of those is its own step, reviewed
before the next lands.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.scope import KEEP_ROLES, KEEP_SCHEMES, admin_orgs_canonical as _admin_orgs_canonical
from config.settings import PROCESSED_DATA, GRANT_SUMMARIES_CSV, ADMIN_ORGS_CSV, ARC_GRANTS_CSV
from src.utils.for_resolve import upgrade_for_name, upgrade_for_code, resolve_arc_for_entry, for2020_group_name
from src.acif.models import AwardCIFItem, AwardsCIF


DATA_PERSISTED = Path(__file__).resolve().parents[2] / "data_persisted"


def load_manual_orcid_corrections(
    path: Path | None = None,
) -> dict[str, tuple[str, str | None]]:
    """unique_id -> (wrong_orcid, correct_orcid_or_None) from
    data_persisted/manual_orcid_corrections.csv. Pure I/O, kept separate from
    apply_manual_orcid_corrections() so the actual correction logic is directly testable against
    a small hand-built dict, not real file I/O."""
    path = path or (DATA_PERSISTED / "manual_orcid_corrections.csv")
    if not path.exists():
        return {}
    out: dict[str, tuple[str, str | None]] = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            uid = row["unique_id"].strip()
            wrong = row["wrong_orcid"].strip()
            correct = row["correct_orcid"].strip() or None
            if uid and wrong:
                out[uid] = (wrong, correct)
    return out


def apply_manual_orcid_corrections(
    items: list[AwardCIFItem], corrections: dict[str, tuple[str, str | None]],
) -> list[AwardCIFItem]:
    """Apply data_persisted/manual_orcid_corrections.csv: for any item whose unique_id is a key
    and whose CURRENT orcid matches the recorded wrong_orcid, replace it with correct_orcid (or
    None if blank -- "field nulled, not substituted," this project's own established convention
    when the true correct ORCID for a wrongly-labeled person isn't independently known).

    Applied inside load_items(), before any merge test ever runs, so the bad evidence never gets
    a chance to cause a wrong merge in the first place -- confirmed necessary directly, not just
    in theory: without this, merge_by_orcid() silently merges two different real "Wei Liu"s
    (DP150102405, wrongly carrying the RMIT Wei Liu's own ORCID) into one ACIF, since a
    family-name compatibility check has no way to distinguish two different people who happen to
    share an identical full name."""
    out = []
    for item in items:
        correction = corrections.get(item.unique_id)
        if correction and item.orcid == correction[0]:
            item = replace(item, orcid=correction[1])
        out.append(item)
    return out


def load_items(con: duckdb.DuckDBPyConnection | None = None) -> list[AwardCIFItem]:
    """Raw ARC facts only -- investigators_raw.parquet joined to grants_flat.parquet and
    grant_summaries.csv, filtered to KEEP_ROLES / KEEP_SCHEMES / genuine-HEP admin_org (the same
    three scope rules src/utils/awards_cif.py::load_award_cif_items() applies, ported and
    condensed rather than imported since that module is being archived).

    Populates every AwardCIFItem field that has no default -- unique_id, grant_code, first_name,
    family_name, role_code, orcid, admin_org, institution_oax_id, funding_commence_year, for_name,
    for_code, full_name -- plus is_fellowship, plus admin_orgs (see below), plus full_name_keys.
    Deliberately leaves every other DERIVED field at its dataclass default (first_names,
    family_names, family_name_main, first_initial, first_name_canonical, full_name_key,
    for_name_tokens, parsed, for2020_codes, hep_codes, inst_ids, for_name_rarity,
    single_institution_grant, institution_rarity) -- FOR2020 resolution and HEP/institution-set
    derivation are each their own later step, not folded in here. manual_orcid_corrections.csv IS
    applied here, though (see apply_manual_orcid_corrections()), since it must happen before any
    merge test ever sees the data.

    Names: nothing here parses a name. full_name_keys is read from arc_names.parquet, which
    00a_extract_arc.py writes from its one NameParser() pass -- and announcement/current renames
    are already merged there (the surviving id carries both forms' keys), so there is no rename
    handling here either (2026-09-29; previously this function parsed names itself and applied
    00c_extract_propensities.py's award_rename_map).

    admin_orgs (2026-09-28 finding, confirmed by the grant's own former CI): admin_org alone
    silently drops the at-award institution whenever it differs from the current one (a fellow
    moved institutions mid-grant -- DE120101452 is admin_org=Sydney now, was ANU at
    announcement). admin_org itself stays "the current one," but admin_orgs retains both.
    """
    own_con = con is None
    con = con or duckdb.connect()
    try:
        roles_sql = ", ".join(f"'{r}'" for r in KEEP_ROLES)
        schemes_sql = ", ".join(f"'{s}'" for s in KEEP_SCHEMES)
        rows = con.execute(f"""
            SELECT
                i.unique_id, i.grant_code, i.first_name, i.family_name, i.role_code,
                i.orcid, i.is_fellowship, g.admin_org, g.announcement_admin_org,
                o.institution_id AS institution_oax_id,
                g.funding_commence_year, g.primary_for_name,
                regexp_extract(s.primary_field_of_research, '^\\d{{4}}') AS for2008_code,
                n.full_name_keys
            FROM read_parquet('{PROCESSED_DATA}/investigators_raw.parquet') i
            LEFT JOIN read_parquet('{PROCESSED_DATA}/arc_names.parquet') n
                ON i.unique_id = n.unique_id
            LEFT JOIN read_parquet('{PROCESSED_DATA}/grants_flat.parquet') g
                ON i.grant_code = g.grant_code
            LEFT JOIN read_csv_auto('{GRANT_SUMMARIES_CSV}') s
                ON i.grant_code = s.grant_id
            LEFT JOIN read_csv_auto('{ADMIN_ORGS_CSV}') o
                ON g.admin_org = o.organisationName_alias
            WHERE i.role_code IN ({roles_sql})
              AND substring(i.grant_code, 1, 2) IN ({schemes_sql})
            ORDER BY i.unique_id
        """).fetchall()
        col_names = [d[0] for d in con.description]
    finally:
        if own_con:
            con.close()

    hep_admin_orgs, _alias_to_hep_code, _alias_to_institution_id = _admin_orgs_canonical()

    items: list[AwardCIFItem] = []
    for row in rows:
        r = dict(zip(col_names, row))
        if r["admin_org"] not in hep_admin_orgs:
            continue
        if r["full_name_keys"] is None:
            raise ValueError(f"{r['unique_id']} has no row in arc_names.parquet -- rerun 00a_extract_arc.py")
        full_name = f"{r['first_name']} {r['family_name']}"
        full_name_keys = list(r["full_name_keys"])
        items.append(AwardCIFItem(
            unique_id=r["unique_id"],
            grant_code=r["grant_code"],
            first_name=r["first_name"],
            family_name=r["family_name"],
            role_code=r["role_code"],
            orcid=r["orcid"],
            admin_org=r["admin_org"],
            admin_orgs=sorted({n for n in (r["admin_org"], r["announcement_admin_org"]) if n}),
            institution_oax_id=r["institution_oax_id"],
            funding_commence_year=r["funding_commence_year"],
            for_name=upgrade_for_name(r["for2008_code"], r["primary_for_name"]),
            for_code=upgrade_for_code(r["for2008_code"]) or r["for2008_code"],
            full_name=full_name,
            full_name_keys=full_name_keys,
            is_fellowship=bool(r["is_fellowship"]),
        ))
    return apply_manual_orcid_corrections(items, load_manual_orcid_corrections())


def load_grant_for2020_codes() -> dict[str, list[dict]]:
    """grant_code -> every ARC field-of-research entry for that grant, resolved to FOR2020 group
    (4-digit) precision -- {code, name, is_primary, confidence}, ordered primary-first then
    alphabetically. Ported from src/utils/awards_cif.py's function of the same name (that logic
    was already clean, single-purpose, and validated -- not part of what needed condensing).

    Scoped to KEEP_SCHEMES before resolving anything: raw_json.csv covers every ARC scheme ever
    run, most out of scope for this project (e.g. "LE" Linkage-Equipment grants, which fund
    shared lab equipment for a whole department and carry FOR-code spreads unrelated to any one
    CI's own research identity)."""
    df = pd.read_csv(ARC_GRANTS_CSV)
    out: dict[str, dict[str, dict]] = defaultdict(dict)  # grant_code -> {code4: entry}
    for _, row in df.iterrows():
        try:
            rec = json.loads(row["single_grant"])
        except (TypeError, ValueError):
            continue
        grant_code = rec.get("data", {}).get("id")
        if not grant_code or grant_code[:2] not in KEEP_SCHEMES:
            continue
        for f in rec.get("data", {}).get("attributes", {}).get("field-of-research") or []:
            resolved = resolve_arc_for_entry(f.get("code"), f.get("type"))
            if resolved is None:
                continue
            code20, name, confidence = resolved
            code4 = code20[:4]
            if code20 != code4:
                name = for2020_group_name(code4) or name
            is_primary = bool(f.get("isPrimary"))
            existing = out[grant_code].get(code4)
            if existing is None:
                out[grant_code][code4] = {
                    "code": code4, "name": name, "is_primary": is_primary, "confidence": confidence,
                }
            else:
                existing["is_primary"] = existing["is_primary"] or is_primary
                existing["confidence"] = max(existing["confidence"], confidence)
    return {
        grant_code: sorted(entries.values(), key=lambda e: (not e["is_primary"], e["name"].lower()))
        for grant_code, entries in out.items()
    }


def load_grant_org_facts() -> dict[str, dict]:
    """grant_code -> {hep_codes, inst_ids} -- both genuinely GRANT-level facts (every investigator
    on the same grant shares the same eligible_orgs/admin_org/announcement_admin_org), so computed
    once per grant here rather than re-derived per item.

    hep_codes: every HEP-eligible organisation formally on the grant (eligible_orgs UNION
    admin_org) -- the broader set, used for subfield/HEP corroboration downstream.
    inst_ids: this grant's own ARC-org set, narrower and deliberately NOT including eligible_orgs
    -- exactly {admin_org, announcement_admin_org} (partner/collaborating orgs aren't reliably
    THIS investigator's own institution any more than admin_org is)."""
    df = pd.read_parquet(
        PROCESSED_DATA / "grants_flat.parquet",
        columns=["grant_code", "admin_org", "announcement_admin_org", "eligible_orgs"],
    )
    _hep_admin_orgs, alias_to_hep_code, alias_to_institution_id = _admin_orgs_canonical()

    out: dict[str, dict] = {}
    for row in df.itertuples(index=False):
        # eligible_orgs is a numpy array (parquet list column via pyarrow) -- `x or []` is
        # invalid, ambiguous truth value for an array with 2+ elements (same pitfall already
        # documented in 00c_extract_propensities.py's build_institution_rarity()).
        eligible = set(row.eligible_orgs) if row.eligible_orgs is not None else set()
        if row.admin_org:
            eligible.add(row.admin_org)
        hep_codes = sorted({alias_to_hep_code[n] for n in eligible if n in alias_to_hep_code})

        arc_orgs = {n for n in (row.admin_org, row.announcement_admin_org) if n}
        inst_ids = sorted({alias_to_institution_id[n] for n in arc_orgs if n in alias_to_institution_id})

        out[row.grant_code] = {"hep_codes": hep_codes, "inst_ids": inst_ids}
    return out


def enrich_items(
    items: list[AwardCIFItem],
    for2020: dict[str, list[dict]],
    org_facts: dict[str, dict],
) -> list[AwardCIFItem]:
    """Attach the three grant-level derived facts (for2020_codes, hep_codes, inst_ids) computed
    once per grant_code, not per item -- each of an item's grant-mates gets the identical value.
    Items whose grant_code has no facts (should not happen post-scope-filter, checked rather than
    assumed) keep the dataclass default (empty list), not a KeyError.

    Takes the two lookups as parameters rather than loading them itself (2026-09-28) -- pure
    transformation, no I/O of its own, so it's testable against small hand-built dicts instead of
    real parquet/CSV data. Production callers pass load_grant_for2020_codes()/
    load_grant_org_facts()'s real output; see build_stage_zero() below for the actual wiring."""
    return [
        replace(
            item,
            for2020_codes=for2020.get(item.grant_code, []),
            hep_codes=org_facts.get(item.grant_code, {}).get("hep_codes", []),
            inst_ids=org_facts.get(item.grant_code, {}).get("inst_ids", []),
        )
        for item in items
    ]


def build_stage_zero() -> list[AwardsCIF]:
    """Production entry point: load real ARC data, enrich it with the real grant-level lookups,
    seed one singleton AwardsCIF per item. The only call site that wires enrich_items() to real
    I/O -- kept separate so enrich_items() itself stays a pure, directly-testable function."""
    items = load_items()
    items = enrich_items(items, load_grant_for2020_codes(), load_grant_org_facts())
    return seed(items)


def seed(items: list[AwardCIFItem]) -> list[AwardsCIF]:
    """Explicit stage-1 seeding (the plan's own term): one singleton AwardsCIF per item, never an
    empty list. cluster_id = item.unique_id needs no tie-break rule yet -- a singleton's id is
    trivially just its own one member; the year/scheme/remainder tie-break only matters once a
    merge has to pick a survivor between two or more."""
    return [
        AwardsCIF(cluster_id=item.unique_id, items=[item], cycle_stages=[1])
        for item in items
    ]


class UnionFind:
    """Path-compressing union-find over cluster_id strings. `parent` is meant to be a
    PERSISTED, ever-accumulating map (2026-09-29 design decision, plan-that-in-tiny-immutable-
    heron.md's "One index, not two" section): once an id has been absorbed, find() recovers its
    current representative in one or two hops, no population-wide grant_ids scan ever needed --
    the old pipeline's resolve_cluster_id() does NOT need porting into this package because of
    this. Pass the same `parent` dict across every stage/every merge test to keep it complete."""

    def __init__(self, parent: dict[str, str] | None = None):
        self.parent: dict[str, str] = parent if parent is not None else {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> str:
        """Survivor = min() of the two representatives (2026-09-29 settled tie-break -- any
        deterministic, order-independent rule is equally correct, plain min() is simplest)."""
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return ra
        survivor, absorbed = (ra, rb) if ra < rb else (rb, ra)
        self.parent[absorbed] = survivor
        return survivor


def compute_orcids(acif: AwardsCIF) -> None:
    """Recompute orcids/orcid_status fresh from an ACIF's CURRENT items, mutating in place --
    never cached across stages. No individual item's own orcid ever changes, but which items
    currently belong to this ACIF can, so the aggregate must be recomputed every time it's
    needed, per the plan's "recompute bottom-up, never patch" principle."""
    distinct = sorted({it.orcid for it in acif.items if it.orcid})
    acif.orcids = distinct
    acif.orcid_status = (
        "MULTI_ORCID" if len(distinct) > 1 else
        "HAS_ORCID" if len(distinct) == 1 else
        "NO_ORCID"
    )


def _full_name_keys(acif: AwardsCIF) -> set[str]:
    """Every full_name_key across an ACIF's current items -- values straight from
    arc_names.parquet (00a_extract_arc.py's one NameParser() pass), no name handling here."""
    return {k for it in acif.items for k in it.full_name_keys}


def _item_orcids(acif: AwardsCIF) -> set[str]:
    """Every ORCID tied to an ACIF's current items: ARC's own, any found through Scopus
    (item.scopus_orcid) and any given by hand (item.hand_orcid). merge_by_key()'s default
    orcids_of."""
    return {o for it in acif.items for o in (it.orcid, it.scopus_orcid, it.hand_orcid) if o}


def _keys(k) -> set[str]:
    if not k:
        return set()
    return {k} if isinstance(k, str) else {x for x in k if x}


def key_components(acifs: list[AwardsCIF], key_of) -> list[tuple[list[str], list[AwardsCIF]]]:
    """Connected components of ACIFs that share an identity key. key_of(acif) returns None, one
    key, or several (an ACIF that carries several Scopus profiles, say); two ACIFs are in one
    component when a chain of shared keys connects them. Only components of 2+ ACIFs are
    returned, as (sorted keys, ACIFs sorted by cluster_id), ordered by their first cluster_id --
    the result does not depend on the order of `acifs`."""
    comp = UnionFind()
    first_holder: dict[str, str] = {}
    keys_of: dict[str, set[str]] = {}
    by_id = {a.cluster_id: a for a in acifs}
    for a in acifs:
        ks = _keys(key_of(a))
        if not ks:
            continue
        keys_of[a.cluster_id] = ks
        comp.find(a.cluster_id)
        for k in ks:
            if k in first_holder:
                comp.union(first_holder[k], a.cluster_id)
            else:
                first_holder[k] = a.cluster_id
    groups: dict[str, list[str]] = defaultdict(list)
    for cid in keys_of:
        groups[comp.find(cid)].append(cid)
    out = []
    for cids in groups.values():
        if len(cids) < 2:
            continue
        cids = sorted(cids)
        out.append((sorted(set().union(*(keys_of[c] for c in cids))), [by_id[c] for c in cids]))
    return sorted(out, key=lambda kg: kg[1][0].cluster_id)


def merge_by_key(
    acifs: list[AwardsCIF], uf: UnionFind, key_of, orcids_of=_item_orcids, check=None,
) -> tuple[list[AwardsCIF], list[dict]]:
    """Group ACIFs by identity key (key_components(): key_of(acif) -> None, a key, or several
    keys; ACIFs chained by shared keys form one group) and merge each group only when all its
    ACIFs are linked by shared full_name_keys (A shares a key with B, B with C, ...; keys are the
    parser's own output, from arc_names.parquet -- no name handling here). A group that splits
    into unlinked parts is left unmerged entirely and reported instead -- "don't guess, defer to
    human review" (the real conflicts this catches: Wang/Duan, Bunda/Lasczik, Curran/Gallagher).

    ORCID veto: a group whose ACIFs between them carry 2+ different ORCIDs (orcids_of(acif) ->
    set; default: the ARC and Scopus ORCIDs on its items) is never merged -- the whole group is
    left as it is and reported, whatever the key, so the outcome doesn't depend on order. The
    check is on whole groups of whole ACIFs, so it holds across calls too: A merged with C by one
    key carries A's ORCID into the next call, and C can't then bring in B with a different ORCID
    by another key. When the key is itself an ORCID the veto can't fire (one ORCID per group).

    check(keys, group) -> None or a reason string: a caller's own extra test, run after the name
    and ORCID tests; a group it gives a reason for is left unmerged and reported with that reason.

    Survivor cluster_id = min(unique_id) of the merged items; absorbed ids resolve through `uf`.
    orcids/orcid_status of each survivor are recomputed from its items.

    Returns (updated_acifs, mismatches): one entry per group left unmerged, with `reason`
    ("names_do_not_link", "orcid_veto" or the check's reason), `orcid` (the group's first key),
    `keys`, the sub-groups, names and ORCIDs needed to review it."""
    mismatches: list[dict] = []
    to_merge: list[tuple[str, str]] = []

    for keys, group in key_components(acifs, key_of):
        names = {a.cluster_id: a.items[0].full_name for a in group}
        local = UnionFind()  # throwaway, scoped to partitioning just this group
        fnk = [_full_name_keys(a) for a in group]
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                if fnk[i] & fnk[j]:
                    local.union(group[i].cluster_id, group[j].cluster_id)

        sub_groups: dict[str, list[AwardsCIF]] = defaultdict(list)
        for acif in group:
            sub_groups[local.find(acif.cluster_id)].append(acif)

        if len(sub_groups) > 1:
            mismatches.append({
                "reason": "names_do_not_link", "orcid": keys[0], "keys": keys,
                "groups": sorted(sorted(a.cluster_id for a in sub) for sub in sub_groups.values()),
                "names": names,
            })
            continue

        orcids = {a.cluster_id: sorted(orcids_of(a)) for a in group}
        reason = "orcid_veto" if len({o for os in orcids.values() for o in os}) > 1 else None
        if reason is None and check is not None:
            reason = check(keys, group)
        if reason:
            mismatches.append({
                "reason": reason, "orcid": keys[0], "keys": keys,
                "groups": sorted([a.cluster_id] for a in group),
                "names": names, "orcids": orcids,
            })
            continue

        for a in group[1:]:
            to_merge.append((group[0].cluster_id, a.cluster_id))

    return apply_unions(acifs, uf, to_merge), mismatches


def apply_unions(acifs: list[AwardsCIF], uf: UnionFind, to_merge) -> list[AwardsCIF]:
    """Union each (cluster_id, cluster_id) pair in `uf` and rebuild the merged ACIFs: survivor
    cluster_id = min(unique_id) of the merged items; orcids/orcid_status recomputed."""
    if not to_merge:
        return acifs
    for a, b in to_merge:
        uf.union(a, b)

    grouped: dict[str, list[AwardsCIF]] = defaultdict(list)
    for acif in acifs:
        grouped[uf.find(acif.cluster_id)].append(acif)

    survivors: list[AwardsCIF] = []
    for root, members in grouped.items():
        if len(members) == 1:
            survivors.append(members[0])
            continue
        merged_items = [it for m in members for it in m.items]
        cluster_id = min(it.unique_id for it in merged_items)
        cycle_stages = sorted({s for m in members for s in m.cycle_stages})
        survivor = AwardsCIF(cluster_id=cluster_id, items=merged_items, cycle_stages=cycle_stages)
        compute_orcids(survivor)
        survivors.append(survivor)

    return survivors


def merge_by_orcid(
    acifs: list[AwardsCIF], uf: UnionFind,
) -> tuple[list[AwardsCIF], list[dict]]:
    """First real merge test: group ACIFs sharing an identical ARC ORCID and merge them through
    merge_by_key() (names must link; unlinked groups are reported, not merged).

    orcids/orcid_status are recomputed fresh for every ACIF first (see compute_orcids()) --
    required even though ORCID itself never changes, since which items an ACIF currently holds
    can change between stages."""
    for acif in acifs:
        compute_orcids(acif)
    return merge_by_key(acifs, uf, lambda a: a.orcids[0] if a.orcid_status == "HAS_ORCID" else None)


def build_acifs(scopus: bool = True, hand: bool = True, names: bool = True,
                ) -> tuple[list[AwardsCIF], UnionFind, dict]:
    """The build so far: stage zero -> ARC ORCID merge -> (scopus=True) Scopus pass one (ORCIDs
    found through Scopus) -> Scopus pass two (shared Scopus profile); see src/acif/scopus.py ->
    (hand=True) the hand-confirmed ORCIDs and merges from data_persisted (src/acif/hand.py) ->
    (names=True) the clean name groups (src/acif/name_merge.py).
    One UnionFind runs through every stage. Returns (acifs, uf, report): report holds each
    stage's ACIF count and unmerged groups, and pass one's per-ACIF decisions."""
    uf = UnionFind({})
    seeds = build_stage_zero()
    acifs, m0 = merge_by_orcid(seeds, uf)
    report = {"n_seed": len(seeds), "n_arc_orcid": len(acifs), "arc_orcid_mismatches": m0}
    if scopus:
        from src.acif.scopus import load_scopus_extract, scopus_pass_one, scopus_pass_two
        ext = load_scopus_extract()
        acifs, decisions, m1 = scopus_pass_one(acifs, uf, ext)
        report.update(n_scopus_pass_one=len(acifs), pass_one_decisions=decisions, pass_one_mismatches=m1)
        acifs, m2, n_claimed = scopus_pass_two(acifs, uf, ext)
        report.update(n_scopus_pass_two=len(acifs), pass_two_mismatches=m2, pass_two_orcid_from_claims=n_claimed)
    if hand:
        from src.acif.hand import hand_stage
        acifs, hand_report = hand_stage(acifs, uf)
        report.update(n_hand=len(acifs), hand=hand_report)
    if names:
        from src.acif.name_merge import name_merge
        acifs, name_report = name_merge(acifs, uf)
        report.update(n_names=len(acifs), names=name_report)
    return acifs, uf, report


def render_orcid_mismatch_report(mismatches: list[dict]) -> str:
    """Plain-text rendering of merge_by_orcid()'s own mismatch report -- for human review, not
    a separate diagnostic script re-deriving the same grouping logic."""
    if not mismatches:
        return "No ORCID/name mismatches found."
    lines = [f"{len(mismatches)} group(s) left unmerged:\n"]
    for m in mismatches:
        lines.append(f"key {m['orcid']} ({m.get('reason', 'names_do_not_link')}):")
        for group in m["groups"]:
            names = ", ".join(
                f"{cid} ({m['names'][cid]}"
                + (f"; ORCID {', '.join(m['orcids'][cid]) or '-'}" if "orcids" in m else "")
                + ")"
                for cid in group
            )
            lines.append(f"  - {names}")
        lines.append("")
    return "\n".join(lines)
