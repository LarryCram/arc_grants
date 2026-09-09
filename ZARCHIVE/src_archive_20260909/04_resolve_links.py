"""
src/04_resolve_links.py

Disambiguate ARC persons with multiple high-confidence OAX matches, AND produce the final
OAX-enriched AwardsCIF population (awards_cif.parquet) -- this script now absorbs the job
03b_enrich_awards_cif.py used to do separately (archived 2026-08-25, see
ZARCHIVE/src_archive_20260825/03b_enrich_awards_cif.py and CLAUDE.md's "03b/04 consolidation"
session notes for the incident this fixes: two independently-computed outputs reading the same
inputs -- 03b's own dedup_oax_candidates() call and this file's own former Steps 0/0b, an
independent reimplementation of the same OAX-side split-record dedup logic over a narrower,
HC-only population -- with nothing reconciling them. Measured before the fix: 165/22,563
disagreements between awards_cif.parquet's oax_candidates and this file's own resolved oax_id.

Input:  arc_oax_links.parquet       (all candidate pairs >= OAX_CANDIDATE_THRESHOLD, 0.5)
        awards_cif_arc_only.parquet (the full ARC-only population -- 01_prepare_arc.py's output)
        openalex_authors_prep.parquet (orcid, inst_ids per OAX author)
        OAX raw authors parquet (works_count)

Output: awards_cif.parquet
            The full ARC-only population + oax_candidates (the canonical, deduped candidate
            pool -- see step 0 below). Replaces 03b_enrich_awards_cif.py's own output; every
            other field is a pass-through of awards_cif_arc_only.parquet's own columns.

        arc_oax_resolved.parquet
            arc_id, oax_id, match_probability, resolved_by, secondary_oax_ids
            One row per ARC person (only those with a resolved HC match).
            secondary_oax_ids: other HC candidates not chosen (e.g. split OAX records).

        arc_ambiguous_deferred.parquet
            arc_id, oax_id, match_probability, inst_overlap
            All HC candidate rows for ARC persons that remain ambiguous after all steps.

        arc_manual_unlinked.parquet

Pipeline, in order:
  0. enrich_with_oax_candidates() (populate_oax_candidates -> dedup_oax_candidates, both from
     awards_cif.py, UNCHANGED) -- the canonical candidate pool, over ALL candidate pairs
     >= OAX_CANDIDATE_THRESHOLD, not just high-confidence. Collapses OpenAlex's own split-
     record duplicates (same ORCID, or same specific topic with compatible names, dominant
     works_count) and removes any candidate a human has confirmed via manual_resolutions.csv's
     "unlink" rows. THIS is the single place OAX-side split-record dedup happens now -- 04's
     own former Steps 0/0b are retired; everything below operates on the deduped pool, so a
     resolution can never point at something dedup already excluded, by construction.
  1. Disambiguation, applied to whatever HC pairs survive within the deduped pool:
     1a. Name-character mismatch filter (drop clear non-matches, e.g. Peter vs Patricia).
     1b. ORCID exact match: if exactly 1 surviving HC candidate shares the ARC person's ORCID.
     1c. Single-org institution gate: if every contributing ARC grant has n_eligible_orgs==1,
         exclude candidates with zero institution overlap when at least one candidate has some.
     2.  Institution overlap: restrict to candidates with maximum overlap (if any > 0).
     2b. Field match: restrict by OAX topics/subfields vs ARC FOR codes.
     3.  Unique highest match_probability among remaining candidates.
     4.  Highest works_count: one candidate holds >TOP_CUT share of combined works_count.
     5.  Still tied -> defer (genuine common-name collisions).
  2. Sub-HC rescue: arc_ids with zero surviving HC candidates but exactly one compatible
     sub-HC candidate (0.7 <= p < 0.9) within the deduped pool.
  3. Manual resolve/unlink (manual_resolutions.csv) applied last, as before -- a manual
     "resolve" pointing at an oax_id NOT already in the cluster's deduped oax_candidates
     extends the pool to include it (so awards_cif.parquet stays consistent with the human's
     own confirmed answer -- the BrienNorton/FrederickRavenhill class of case, found via manual
     full-OAX surname search, never a Splink candidate in the first place). Manual "unlink" rows
     were already applied inside dedup_oax_candidates() above (removed before disambiguation
     ever ran); the only unlink handling needed here is removing any resolved/deferred row that
     still names an unlinked arc_id, same as before.

ARC persons with 0 surviving candidates in the deduped pool (HC or sub-HC) are in neither
arc_oax_resolved.parquet nor arc_ambiguous_deferred.parquet -- accounted for as "Unlinked".
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, OAX_AUTHORS, TOP_CUT
from src.utils.for_resolve import oax_subfield_name
from src.utils.pipeline_freshness import assert_fresh
from src.utils.awards_cif import (
    load_awards_cif,
    enrich_with_oax_candidates,
    persist_awards_cif,
    resolve_cluster_id,
    ARC_ONLY_PARQUET,
    AWARDS_CIF_PARQUET,
)

LINK_THRESHOLD = 0.9
SUBHC_MIN = 0.7

_MANUAL_RESOLUTIONS_CSV = Path(__file__).resolve().parents[1] / "data_persisted" / "manual_resolutions.csv"
_LINK_ARC_OAX_SOURCE = Path(__file__).resolve().parent / "03_link_arc_oax.py"


def main():
    arc_path  = ARC_ONLY_PARQUET
    oax_path  = PROCESSED_DATA / "openalex_authors_prep.parquet"
    link_path = PROCESSED_DATA / "arc_oax_links.parquet"
    out_resolved  = PROCESSED_DATA / "arc_oax_resolved.parquet"
    out_ambiguous = PROCESSED_DATA / "arc_ambiguous_deferred.parquet"
    out_manual_unlinked = PROCESSED_DATA / "arc_manual_unlinked.parquet"

    # Pre-flight -- verify arc_oax_links.parquet (this script's real dependency, along with
    # awards_cif_arc_only.parquet) isn't older than ITS OWN sources. Absorbed from
    # 03b_enrich_awards_cif.py's own former check, along with its job. No self-check on this
    # script's OWN outputs (arc_oax_resolved.parquet / awards_cif.parquet), which are
    # unconditionally rebuilt below regardless -- see CLAUDE.md's "self-blocking freshness-gate"
    # note for why checking a producer's own output against its own source is always wrong.
    assert_fresh(
        "04_resolve_links (arc_oax_links.parquet)",
        outputs=[link_path],
        inputs=[arc_path, oax_path, _LINK_ARC_OAX_SOURCE],
    )

    con = duckdb.connect()

    print("[1/5] Loading data...")
    links = con.execute(f"SELECT * FROM read_parquet('{link_path}')").fetchdf()
    arc   = con.execute(f"SELECT cluster_id, orcids, inst_arr, for_codes, first_names, grant_ids FROM read_parquet('{arc_path}')").fetchdf()
    oax   = con.execute(f"SELECT unique_id, orcid, inst_ids, topic_names, subfield_names, first_name, family_name_main FROM read_parquet('{oax_path}')").fetchdf()

    arc["orcid"] = arc["orcids"].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    arc_orcid     = dict(zip(arc["cluster_id"], arc["orcid"]))
    arc_inst      = dict(zip(arc["cluster_id"], arc["inst_arr"]))
    arc_for_codes = dict(zip(arc["cluster_id"], arc["for_codes"]))
    arc_firstnames = {r["cluster_id"]: [str(fn).lower().strip() for fn in (r["first_names"] if r["first_names"] is not None else [])]
                      for _, r in arc.iterrows()}

    # Single-institution gate (2026-08-25 rewrite): len(inst_arr)==1 on the ACIF's own
    # aggregate institution set, not the old per-grant "n_eligible_orgs==1 on EVERY contributing
    # grant" check. These are NOT equivalent -- the old check could be true for someone whose
    # grants are each individually single-org but point to DIFFERENT institutions across their
    # career (never looks across grants), wrongly certifying them as "single org" when they
    # genuinely have two. inst_arr is now the union of admin_org + announcement_admin_org
    # across every grant this ACIF has (see AwardCIFItem.inst_ids' own docstring) -- a person's
    # whole footprint, not one grant at a time -- so len==1 is a stricter, more correct test:
    # fewer clusters qualify, but the ones that do are more trustworthy.
    arc_all_single_org = {cid: len(inst) == 1 for cid, inst in arc_inst.items()}
    oax_orcid     = dict(zip(oax["unique_id"],  oax["orcid"]))
    oax_inst      = dict(zip(oax["unique_id"],  oax["inst_ids"]))
    oax_topics    = dict(zip(oax["unique_id"],  oax["topic_names"]))
    oax_subfields = dict(zip(oax["unique_id"],  oax["subfield_names"]))
    oax_firstname = {r["unique_id"]: str(r["first_name"] or "").lower().strip()
                     for _, r in oax.iterrows()}
    oax_familyname= {r["unique_id"]: str(r["family_name_main"] or "").lower().strip()
                     for _, r in oax.iterrows()}

    print("[2/5] Computing canonical OAX candidate pool (populate_oax_candidates -> dedup_oax_candidates)...")
    clusters = load_awards_cif(arc_path)
    clusters = enrich_with_oax_candidates(clusters, con)
    oax_candidates_by_arc: dict[str, set[str]] = {c.cluster_id: set(c.oax_candidates) for c in clusters}
    n_with_candidates = sum(1 for s in oax_candidates_by_arc.values() if s)
    print(f"  {n_with_candidates:,} / {len(clusters):,} clusters have >=1 OAX candidate (deduped)")

    # Restrict every candidate pair to the deduped pool -- the actual fix: anything this
    # script resolves to is guaranteed to already be a member of the pool persisted to
    # awards_cif.parquet below, by construction, not by two separately-computed outputs
    # happening to agree.
    links = links[links.apply(
        lambda r: r["oax_id"] in oax_candidates_by_arc.get(r["arc_id"], set()), axis=1
    )]

    hc = links[links["high_confidence"]].copy()
    per_arc = hc.groupby("arc_id").size()

    print("[3/5] Fetching OAX works_count...")
    hc_oax_ids = hc["oax_id"].unique().tolist()
    oax_works = {}
    if hc_oax_ids:
        idxs_sql = ", ".join(i.replace("https://openalex.org/A", "") for i in hc_oax_ids)
        wc_df = con.execute(f"""
            SELECT author_idx, works_count
            FROM read_parquet('{OAX_AUTHORS}/*.parquet')
            WHERE author_idx IN ({idxs_sql})
        """).fetchdf()
        oax_works = {
            f"https://openalex.org/A{idx}": wc
            for idx, wc in zip(wc_df["author_idx"], wc_df["works_count"])
        }
    print(f"  Retrieved works_count for {len(oax_works):,} / {len(hc_oax_ids):,} OAX IDs")

    # Persons with exactly 1 HC match within the deduped pool -- already resolved. A cluster
    # whose pool dedup_oax_candidates() already collapsed to one HC survivor (what used to need
    # this file's own Step 0/0b) correctly lands here now as unique_hc, not a regression --
    # the true reason for resolution moved upstream, into the shared dedup step, from a
    # duplicate reimplementation of the same logic that used to live only here.
    single_ids = per_arc[per_arc == 1].index
    resolved_single = hc[hc["arc_id"].isin(single_ids)][
        ["arc_id", "oax_id", "match_probability"]
    ].copy()
    resolved_single["resolved_by"] = "unique_hc"
    resolved_single["secondary_oax_ids"] = [[] for _ in range(len(resolved_single))]

    # Persons with 2+ HC matches (post-dedup) — need disambiguation
    ambig_ids = per_arc[per_arc >= 2].index
    ambig = hc[hc["arc_id"].isin(ambig_ids)].copy()

    print(f"  ARC persons with 1 HC match (post-dedup):    {len(single_ids):,}")
    print(f"  ARC persons with 2+ HC matches (post-dedup): {len(ambig_ids):,}")

    print("[4/5] Disambiguating...")

    def _lst(v):
        return list(v) if v is not None else []

    def _field_score(arc_id, oax_id):
        # Derive the set of OAX subfield names implied by the ARC person's FOR codes.
        # Codes with no OAX subfield mapping (e.g. 1701 pre-conversion) are skipped and
        # contribute 0 — the filter guard (max_fs >= 1 and min_fs == 0) then doesn't fire,
        # leaving disambiguation unchanged for those persons.
        target_subfields = set()
        for code in _lst(arc_for_codes.get(arc_id)):
            sf_name = oax_subfield_name(code)
            if sf_name:
                target_subfields.add(sf_name)
        if not target_subfields:
            return 0
        oax_sfs = set(_lst(oax_subfields.get(oax_id)))
        return len(target_subfields & oax_sfs)

    def _inst_overlap(arc_id, oax_id):
        a = arc_inst.get(arc_id)
        o = oax_inst.get(oax_id)
        if a is None or o is None or len(a) == 0 or len(o) == 0:
            return 0
        return len(set(a) & set(o))

    def _names_compat(arc_id, oax_id):
        """False only when every ARC first name AND the OAX first name are all
        ≥4 chars but none share the same 3-char prefix — a clear character
        mismatch (e.g. Peter vs Patricia).  Short/initial names pass through."""
        arc_fns = arc_firstnames.get(arc_id, [])
        o = oax_firstname.get(oax_id, "")
        if not arc_fns or len(o) < 4:
            return True
        for a in arc_fns:
            if len(a) < 4 or a[:3] == o[:3]:
                return True   # short ARC name, or prefix matches → compatible
        return False           # all ARC first names clearly differ from OAX

    ambig["inst_overlap"] = ambig.apply(
        lambda r: _inst_overlap(r["arc_id"], r["oax_id"]), axis=1
    )
    ambig["orcid_match"] = ambig.apply(
        lambda r: (
            arc_orcid.get(r["arc_id"]) is not None
            and arc_orcid.get(r["arc_id"]) == oax_orcid.get(r["oax_id"])
        ),
        axis=1,
    )
    ambig["works_count"] = ambig["oax_id"].map(oax_works).fillna(0).astype(int)

    resolved_rows = []
    deferred_rows = []

    for arc_id, group in ambig.groupby("arc_id"):
        # sorted, not set() -- set() iteration order is randomized per-process
        # (PYTHONHASHSEED), and this feeds max(wcs, key=wcs.get) below, whose tie-break (two
        # candidates with equal works_count) would otherwise silently differ run to run. Same
        # class of bug found and fixed 2026-08-23 in awards_cif.py's _name_forms() and
        # dedup_oax_candidates().
        all_oax = sorted(set(group["oax_id"]))

        # Step 1a: first-name character mismatch filter.
        # If at least one candidate has a compatible first name, drop those that
        # clearly don't.  "Compatible" = either name is <4 chars (initial/short),
        # OR both names share their first 3 chars.  Only fires when the filter
        # would actually reduce the candidate set.
        compat = group["oax_id"].apply(lambda oid: _names_compat(arc_id, oid))
        if compat.any() and not compat.all():
            group = group[compat]
        if len(group) == 1:
            r = group.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"],
                "resolved_by": "name_filter",
                "secondary_oax_ids": [x for x in all_oax if x != r["oax_id"]],
            })
            continue

        # Step 1b: unique ORCID match
        orcid_matches = group[group["orcid_match"]]
        if len(orcid_matches) == 1:
            r = orcid_matches.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"], "resolved_by": "orcid",
                "secondary_oax_ids": [x for x in all_oax if x != r["oax_id"]],
            })
            continue

        # Step 1c: single-org institution gate
        # All contributing ARC grants have n_eligible_orgs == 1 → the ARC inst_arr
        # is the person's definitive institution. Exclude candidates with no overlap
        # when at least one candidate does overlap.
        if arc_all_single_org.get(arc_id, False):
            max_ov_gate = group["inst_overlap"].max()
            if max_ov_gate > 0:
                gated_out = set(group.loc[group["inst_overlap"] == 0, "oax_id"])
                if gated_out:
                    group = group[group["inst_overlap"] > 0]
            if len(group) == 1:
                r = group.iloc[0]
                resolved_rows.append({
                    "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                    "match_probability": r["match_probability"], "resolved_by": "inst_gate",
                    "secondary_oax_ids": [x for x in all_oax if x != r["oax_id"]],
                })
                continue

        # Step 2: restrict to max institution overlap
        max_ov = group["inst_overlap"].max()
        if max_ov > 0:
            candidates = group[group["inst_overlap"] == max_ov]
            by = "inst_overlap"
        else:
            candidates = group
            by = "probability"

        # Step 2b: restrict by field match (OAX topics/subfields vs ARC FOR codes)
        field_scores = {r["oax_id"]: _field_score(arc_id, r["oax_id"])
                        for _, r in candidates.iterrows()}
        max_fs = max(field_scores.values())
        min_fs = min(field_scores.values())
        if max_fs >= 1 and min_fs == 0:
            field_filtered = candidates[candidates["oax_id"].map(field_scores) == max_fs]
            if len(field_filtered) < len(candidates):
                candidates = field_filtered
                by = "field"

        # Step 3: unique highest probability
        max_prob = candidates["match_probability"].max()
        best = candidates[candidates["match_probability"] == max_prob]
        if len(best) == 1:
            r = best.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"], "resolved_by": by,
                "secondary_oax_ids": [x for x in all_oax if x != r["oax_id"]],
            })
            continue

        # Step 4: one OAX record holds >90% of combined works → split record, take dominant
        sum_wc = best["works_count"].sum()
        max_wc = best["works_count"].max()
        if sum_wc > 0 and max_wc / sum_wc > TOP_CUT:
            top = best[best["works_count"] == max_wc]
            if len(top) == 1:
                r = top.iloc[0]
                resolved_rows.append({
                    "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                    "match_probability": r["match_probability"], "resolved_by": "works_count",
                    "secondary_oax_ids": [x for x in all_oax if x != r["oax_id"]],
                })
                continue

        # Step 5: defer
        for _, r in group.iterrows():
            deferred_rows.append({
                "arc_id":            r["arc_id"],
                "oax_id":            r["oax_id"],
                "match_probability": r["match_probability"],
                "inst_overlap":      r["inst_overlap"],
            })

    resolved_ambig = pd.DataFrame(resolved_rows)
    deferred       = pd.DataFrame(deferred_rows) if deferred_rows else pd.DataFrame(
        columns=["arc_id", "oax_id", "match_probability", "inst_overlap"]
    )

    # Sub-HC rescue: arc_ids with zero HC candidates (within the deduped pool) that have at
    # least one sub-HC pair (0.5 ≤ p < 0.9, also within the deduped pool) surviving the
    # name-compatibility filter. If exactly one candidate survives, resolve it as "name_filter".
    hc_arc_ids = set(hc["arc_id"])
    sub_hc_rescue = links[
        (~links["high_confidence"])
        & (links["match_probability"] >= SUBHC_MIN)
        & (~links["arc_id"].isin(hc_arc_ids))
    ].copy()

    rescue_rows = []
    if len(sub_hc_rescue):
        sub_idxs_sql = ", ".join(
            i.replace("https://openalex.org/A", "") for i in sub_hc_rescue["oax_id"].unique().tolist()
        )
        sub_wc = con.execute(f"""
            SELECT author_idx, works_count FROM read_parquet('{OAX_AUTHORS}/*.parquet')
            WHERE author_idx IN ({sub_idxs_sql})
        """).fetchdf()
        sub_oax_works = {
            f"https://openalex.org/A{idx}": wc
            for idx, wc in zip(sub_wc["author_idx"], sub_wc["works_count"])
        }
        sub_hc_rescue["works_count"] = sub_hc_rescue["oax_id"].map(sub_oax_works).fillna(0).astype(int)

        for arc_id, grp in sub_hc_rescue.groupby("arc_id"):
            all_sub = sorted(set(grp["oax_id"]))  # sorted, not set() -- see all_oax above
            compat = grp["oax_id"].apply(lambda oid: _names_compat(arc_id, oid))
            grp = grp[compat]
            if len(grp) == 0:
                continue
            # Unique highest probability among compatible survivors
            max_p = grp["match_probability"].max()
            best  = grp[grp["match_probability"] == max_p]
            if len(best) == 1:
                r = best.iloc[0]
                rescue_rows.append({
                    "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                    "match_probability": r["match_probability"],
                    "resolved_by": "name_filter",
                    "secondary_oax_ids": [x for x in all_sub if x != r["oax_id"]],
                })

    resolved_rescue = pd.DataFrame(rescue_rows)
    resolved = pd.concat([resolved_single, resolved_ambig, resolved_rescue], ignore_index=True)

    print("[5/5] Applying manual resolutions and saving...")
    manual_path = _MANUAL_RESOLUTIONS_CSV
    manual_unlinked = pd.DataFrame(columns=["arc_id", "note"])
    n_manual_resolve = n_manual_unlink = 0
    if manual_path.exists():
        manual_df = pd.read_csv(manual_path).dropna(subset=["arc_id"])
        for _, row in manual_df.iterrows():
            # resolve_cluster_id() raises StaleClusterIdError rather than letting a drifted
            # arc_id silently write a phantom resolved/unlinked row for a person who no
            # longer exists under that id -- see CLAUDE.md's 2026-08-26 stale-reference risk
            # audit (this loop was the one already confirmed to be doing exactly that: 43 of
            # 463 manual_resolutions.csv rows didn't match any current cluster_id).
            aid    = resolve_cluster_id(row["arc_id"], clusters)
            action = row["action"]
            note   = row.get("note", "")
            if action == "resolve":
                auto_match = resolved.loc[resolved["arc_id"] == aid, "oax_id"]
                if not auto_match.empty and auto_match.iloc[0] != row["oax_id"]:
                    print(f"  WARN manual override: {aid} pipeline→{auto_match.iloc[0]} manual→{row['oax_id']}")
                others = list(
                    (set(deferred.loc[deferred["arc_id"] == aid, "oax_id"])
                     | set(resolved.loc[resolved["arc_id"] == aid, "oax_id"]))
                    - {row["oax_id"]}
                )
                deferred = deferred[deferred["arc_id"] != aid]
                resolved  = resolved[resolved["arc_id"] != aid]
                resolved  = pd.concat([resolved, pd.DataFrame([{
                    "arc_id": aid, "oax_id": row["oax_id"],
                    "match_probability": 1.0, "resolved_by": "manual",
                    "secondary_oax_ids": others,
                }])], ignore_index=True)
                n_manual_resolve += 1
                # Keep awards_cif.parquet's own oax_candidates consistent with a manual
                # resolution even when it points outside the automated candidate pool (the
                # BrienNorton/FrederickRavenhill class of case -- found via manual full-OAX
                # surname search, never a Splink candidate in the first place, see CLAUDE.md).
                oax_candidates_by_arc.setdefault(aid, set()).add(row["oax_id"])
            elif action == "unlink":
                deferred = deferred[deferred["arc_id"] != aid]
                resolved  = resolved[resolved["arc_id"] != aid]
                manual_unlinked = pd.concat([manual_unlinked, pd.DataFrame([{
                    "arc_id": aid, "note": note,
                }])], ignore_index=True)
                n_manual_unlink += 1
            # defer_keep: no data change — recorded for reference only
    if n_manual_resolve or n_manual_unlink:
        print(f"  manual resolve: {n_manual_resolve}  manual unlink: {n_manual_unlink}")
    else:
        print("  (none)")

    resolved.to_parquet(out_resolved, index=False)
    deferred.to_parquet(out_ambiguous, index=False)
    manual_unlinked.to_parquet(out_manual_unlinked, index=False)

    # Write the final, resolution-consistent oax_candidates back onto the AwardsCIF population
    # and persist awards_cif.parquet -- absorbs 03b_enrich_awards_cif.py's own job (archived
    # 2026-08-25), guaranteeing this file and arc_oax_resolved.parquet can never disagree about
    # which OAX candidates exist for a given ARC person, since both now come from one pass.
    for c in clusters:
        c.oax_candidates = sorted(oax_candidates_by_arc.get(c.cluster_id, set()))
    persist_awards_cif(clusters, AWARDS_CIF_PARQUET)

    all_arc = len(clusters)
    by_counts = resolved["resolved_by"].value_counts() if len(resolved) else pd.Series(dtype=int)
    print(f"\n  Total ARC persons:              {all_arc:,}")
    print(f"  Resolved (1 HC match):          {by_counts.get('unique_hc', 0):,}")
    print(f"  Resolved (disambiguated):        {len(resolved) - by_counts.get('unique_hc', 0):,}")
    for label in ["orcid", "inst_gate", "inst_overlap", "field", "probability", "works_count", "name_filter", "manual"]:
        n = by_counts.get(label, 0)
        if n:
            print(f"    of which by {label+':':16s} {n:,}")
    print(f"  Resolved total:                  {len(resolved):,}  ({100*len(resolved)/all_arc:.1f}%)")
    print(f"  Ambiguous deferred:              {deferred['arc_id'].nunique():,}")
    print(f"  Manual unlinked:                 {len(manual_unlinked):,}")
    print(f"  Unlinked (no candidate resolved): {all_arc - len(resolved) - deferred['arc_id'].nunique() - len(manual_unlinked):,}")
    print(f"\n  → {out_resolved}")
    print(f"  → {out_ambiguous}")
    print(f"  → {out_manual_unlinked}")
    print(f"  → {AWARDS_CIF_PARQUET}")


if __name__ == "__main__":
    main()
