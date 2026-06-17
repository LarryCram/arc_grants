"""
src/04_resolve_links.py

Disambiguate ARC persons with multiple high-confidence OAX matches.

Input:  arc_oax_links.parquet  (all HC + sub-HC candidate pairs)
        arc_persons.parquet    (orcids, inst_arr per cluster)
        openalex_authors_prep.parquet (orcid, inst_ids per OAX author)
        OAX raw authors parquet (works_count)

Output: arc_oax_resolved.parquet
            arc_id, oax_id, match_probability, resolved_by, secondary_oax_ids
            One row per ARC person (only those with a resolved HC match).
            secondary_oax_ids: other HC candidates not chosen (e.g. split OAX records).

        arc_ambiguous_deferred.parquet
            arc_id, oax_id, match_probability, inst_overlap
            All HC candidate rows for ARC persons that remain ambiguous after all steps.

Resolution strategy (applied to HC matches only):
  0. OAX same-ORCID pre-dedup: two OAX candidates sharing an ORCID are split records;
     keep the one with more works, collapse others into secondary_oax_ids.
  1. ORCID exact match: if exactly 1 HC candidate shares the ARC person's ORCID → resolve.
  2. Institution overlap: restrict to candidates with maximum overlap (if any > 0).
  3. Unique highest match_probability among remaining candidates → resolve.
  4. Highest works_count: among remaining ties, pick the OAX record with most indexed works.
  5. Still tied → defer (genuine common-name collisions).

ARC persons with 0 HC matches are in arc_unlinked_deferred.parquet (step 03).
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, OAX_AUTHORS, TOP_CUT
from src.utils.lookup_for_topic import ForTopicLookup

LINK_THRESHOLD = 0.9


def main():
    arc_path  = PROCESSED_DATA / "arc_persons.parquet"
    oax_path  = PROCESSED_DATA / "openalex_authors_prep.parquet"
    link_path = PROCESSED_DATA / "arc_oax_links.parquet"
    out_resolved  = PROCESSED_DATA / "arc_oax_resolved.parquet"
    out_ambiguous = PROCESSED_DATA / "arc_ambiguous_deferred.parquet"

    con = duckdb.connect()

    print("[1/4] Loading data...")
    links = con.execute(f"SELECT * FROM read_parquet('{link_path}')").fetchdf()
    arc   = con.execute(f"SELECT cluster_id, orcids, inst_arr, for_codes, first_names, grant_ids FROM read_parquet('{arc_path}')").fetchdf()
    oax   = con.execute(f"SELECT unique_id, orcid, inst_ids, topic_names, subfield_names, first_name, family_name_main FROM read_parquet('{oax_path}')").fetchdf()

    grants_flat_path = PROCESSED_DATA / "grants_flat.parquet"
    gf = con.execute(f"SELECT grant_code, n_eligible_orgs FROM read_parquet('{grants_flat_path}')").fetchdf()
    grant_n_orgs = dict(zip(gf["grant_code"], gf["n_eligible_orgs"]))

    arc["orcid"] = arc["orcids"].apply(lambda x: x[0] if x is not None and len(x) > 0 else None)
    arc_orcid     = dict(zip(arc["cluster_id"], arc["orcid"]))
    arc_inst      = dict(zip(arc["cluster_id"], arc["inst_arr"]))
    arc_for_codes = dict(zip(arc["cluster_id"], arc["for_codes"]))
    arc_firstnames = {r["cluster_id"]: [str(fn).lower().strip() for fn in (r["first_names"] if r["first_names"] is not None else [])]
                      for _, r in arc.iterrows()}

    # Precompute: does every grant contributing to this cluster have n_eligible_orgs == 1?
    # When true, inst_arr is definitively the person's own institution(s) and
    # non-overlap with an OAX candidate is strong evidence against that candidate.
    def _grant_codes(grant_ids):
        if grant_ids is None or len(grant_ids) == 0:
            return []
        return [g.split("_")[0] for g in grant_ids]

    arc_all_single_org = {
        r["cluster_id"]: all(
            grant_n_orgs.get(c, 999) == 1 for c in _grant_codes(r["grant_ids"])
        )
        for _, r in arc.iterrows()
    }
    oax_orcid     = dict(zip(oax["unique_id"],  oax["orcid"]))
    oax_inst      = dict(zip(oax["unique_id"],  oax["inst_ids"]))
    oax_topics    = dict(zip(oax["unique_id"],  oax["topic_names"]))
    oax_subfields = dict(zip(oax["unique_id"],  oax["subfield_names"]))
    oax_firstname = {r["unique_id"]: str(r["first_name"] or "").lower().strip()
                     for _, r in oax.iterrows()}
    oax_familyname= {r["unique_id"]: str(r["family_name_main"] or "").lower().strip()
                     for _, r in oax.iterrows()}

    hc = links[links["high_confidence"]].copy()
    per_arc = hc.groupby("arc_id").size()

    # Fetch works_count for all HC candidate OAX IDs
    print("[2/4] Fetching OAX works_count...")
    hc_oax_ids = hc["oax_id"].unique().tolist()
    ids_sql = ", ".join(f"'{i}'" for i in hc_oax_ids)
    wc_df = con.execute(f"""
        SELECT id, works_count
        FROM read_parquet('{OAX_AUTHORS}/*.parquet')
        WHERE id IN ({ids_sql})
    """).fetchdf()
    oax_works = dict(zip(wc_df["id"], wc_df["works_count"]))
    print(f"  Retrieved works_count for {len(oax_works):,} / {len(hc_oax_ids):,} OAX IDs")

    # Persons with exactly 1 HC match — already resolved
    single_ids = per_arc[per_arc == 1].index
    resolved_single = hc[hc["arc_id"].isin(single_ids)][
        ["arc_id", "oax_id", "match_probability"]
    ].copy()
    resolved_single["resolved_by"] = "unique_hc"
    resolved_single["secondary_oax_ids"] = [[] for _ in range(len(resolved_single))]

    # Persons with 2+ HC matches — need disambiguation
    ambig_ids = per_arc[per_arc >= 2].index
    ambig = hc[hc["arc_id"].isin(ambig_ids)].copy()

    print(f"  ARC persons with 1 HC match:    {len(single_ids):,}")
    print(f"  ARC persons with 2+ HC matches: {len(ambig_ids):,}")

    print("[3/4] Disambiguating...")

    def _lst(v):
        return list(v) if v is not None else []

    _lu = ForTopicLookup()

    def _field_score(arc_id, oax_id):
        # Derive the set of OAX subfield names implied by the ARC person's FOR codes.
        # 2008 codes are upgraded to 2020 group codes via the conversion table;
        # codes with no 4-digit 2020 equivalent (e.g. 1701 pre-conversion) are skipped
        # and contribute 0 — the filter guard (max_fs >= 1 and min_fs == 0) then
        # doesn't fire, leaving disambiguation unchanged for those persons.
        target_subfields = set()
        for code in _lst(arc_for_codes.get(arc_id)):
            code20 = _lu.upgrade_for_code(code)
            if code20:
                sf_row = _lu.group_to_subfield(code20)
                if sf_row:
                    target_subfields.add(sf_row["oax_subfield_name"])
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

    def _oax_names_compat(oax_ids):
        """True when all OAX candidates in a group could plausibly be the same person.
        Requires identical family_name_main AND mutually compatible first names
        (same 3-char prefix among all full names; initials <4 chars pass through).
        Used to guard oax_topic_dedup against collapsing genuinely different people."""
        fams = {oax_familyname.get(oid, "") for oid in oax_ids}
        if len(fams) != 1:
            return False
        full_firsts = [oax_firstname.get(oid, "") for oid in oax_ids if len(oax_firstname.get(oid, "")) >= 4]
        if len(full_firsts) < 2:
            return True   # at most one full first name → no conflict possible
        prefix = full_firsts[0][:3]
        return all(n[:3] == prefix for n in full_firsts[1:])

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
        all_oax = set(group["oax_id"])

        # Step 0: OAX same-ORCID pre-dedup — two OAX IDs sharing an ORCID are
        # split records of the same person; keep the dominant one (>80% of group
        # works_count). If no single record is dominant, leave the group intact.
        orcid_to_oax_ids = defaultdict(list)
        for oax_id in all_oax:
            orcid = oax_orcid.get(oax_id)
            if orcid:
                orcid_to_oax_ids[orcid].append(oax_id)
        split_secondaries = set()
        for ids in orcid_to_oax_ids.values():
            if len(ids) > 1:
                wcs = {oid: oax_works.get(oid, 0) for oid in ids}
                total = sum(wcs.values())
                best = max(wcs, key=wcs.get)
                if total > 0 and wcs[best] / total > TOP_CUT:
                    split_secondaries.update(oid for oid in ids if oid != best)
        if split_secondaries:
            group = group[~group["oax_id"].isin(split_secondaries)]
        if len(group) == 1:
            r = group.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"],
                "resolved_by": "oax_orcid_dedup",
                "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
            })
            continue

        # Step 0b: same-topic pre-dedup — two OAX candidates sharing ≥1 specific topic
        # are likely split records of the same person; keep the one with more works.
        # Limitation: this collapses the photonics-related splits of a common surname
        # (e.g. Tucker) correctly, but leaves unrelated namesakes with different topics
        # in the group. If those namesakes have even slight field overlap with the ARC
        # FOR codes, step 2b's min_fs==0 guard won't fire and the case defers.
        # A future improvement: treat a topic-sharing cluster as a confirmed OAX split
        # when one member carries an ORCID (even if the ARC person lacks one), and
        # use the ORCID-bearing member's identity to exclude out-of-field namesakes.
        topic_to_oax_ids = defaultdict(list)
        for oax_id in set(group["oax_id"]):
            for t in _lst(oax_topics.get(oax_id)):
                topic_to_oax_ids[t].append(oax_id)
        # Protect any OAX record that matches the ARC person's own ORCID.
        arc_person_orcid = arc_orcid.get(arc_id)
        orcid_protected = {
            oid for oid in set(group["oax_id"])
            if arc_person_orcid and oax_orcid.get(oid) == arc_person_orcid
        }
        topic_secondaries = set()
        for ids in topic_to_oax_ids.values():
            if len(ids) > 1:
                # Name-compatibility guard: only treat as split records when all
                # candidates share the same family name and compatible first names.
                # Prevents collapsing two different people who share a research topic.
                if not _oax_names_compat(ids):
                    continue
                wcs = {oid: oax_works.get(oid, 0) for oid in ids}
                best = max(wcs, key=wcs.get)
                topic_secondaries.update(
                    oid for oid in ids if oid != best and oid not in orcid_protected
                )
        if topic_secondaries:
            group = group[~group["oax_id"].isin(topic_secondaries)]
            split_secondaries.update(topic_secondaries)
        if len(group) == 1:
            r = group.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"],
                "resolved_by": "oax_topic_dedup",
                "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
            })
            continue

        # Step 0c: first-name character mismatch filter.
        # If at least one candidate has a compatible first name, drop those that
        # clearly don't.  "Compatible" = either name is <4 chars (initial/short),
        # OR both names share their first 3 chars.  Only fires when the filter
        # would actually reduce the candidate set.
        compat = group["oax_id"].apply(lambda oid: _names_compat(arc_id, oid))
        if compat.any() and not compat.all():
            name_excluded = set(group.loc[~compat, "oax_id"])
            group = group[compat]
            split_secondaries.update(name_excluded)
        if len(group) == 1:
            r = group.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"],
                "resolved_by": "name_filter",
                "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
            })
            continue

        # Step 1: unique ORCID match
        orcid_matches = group[group["orcid_match"]]
        if len(orcid_matches) == 1:
            r = orcid_matches.iloc[0]
            resolved_rows.append({
                "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                "match_probability": r["match_probability"], "resolved_by": "orcid",
                "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
            })
            continue

        # Step 1b: single-org institution gate
        # All contributing ARC grants have n_eligible_orgs == 1 → the ARC inst_arr
        # is the person's definitive institution. Exclude candidates with no overlap
        # when at least one candidate does overlap.
        if arc_all_single_org.get(arc_id, False):
            max_ov_gate = group["inst_overlap"].max()
            if max_ov_gate > 0:
                gated_out = set(group.loc[group["inst_overlap"] == 0, "oax_id"])
                if gated_out:
                    group = group[group["inst_overlap"] > 0]
                    split_secondaries.update(gated_out)
            if len(group) == 1:
                r = group.iloc[0]
                resolved_rows.append({
                    "arc_id": r["arc_id"], "oax_id": r["oax_id"],
                    "match_probability": r["match_probability"], "resolved_by": "inst_gate",
                    "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
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
                "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
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
                    "secondary_oax_ids": list(all_oax - {r["oax_id"]}),
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

    # Sub-HC rescue: arc_ids with zero HC candidates that have at least one
    # sub-HC pair (0.5 ≤ p < 0.9) surviving the name-compatibility filter.
    # If exactly one candidate survives, resolve it as "name_filter".
    SUBHC_MIN = 0.7
    hc_arc_ids = set(hc["arc_id"])
    sub_hc_rescue = links[
        (~links["high_confidence"])
        & (links["match_probability"] >= SUBHC_MIN)
        & (~links["arc_id"].isin(hc_arc_ids))
    ].copy()

    rescue_rows = []
    if len(sub_hc_rescue):
        sub_ids_sql = ", ".join(f"'{i}'" for i in sub_hc_rescue["oax_id"].unique().tolist())
        sub_wc = con.execute(f"""
            SELECT id, works_count FROM read_parquet('{OAX_AUTHORS}/*.parquet')
            WHERE id IN ({sub_ids_sql})
        """).fetchdf()
        sub_oax_works = dict(zip(sub_wc["id"], sub_wc["works_count"]))
        sub_hc_rescue["works_count"] = sub_hc_rescue["oax_id"].map(sub_oax_works).fillna(0).astype(int)

        for arc_id, grp in sub_hc_rescue.groupby("arc_id"):
            all_sub = set(grp["oax_id"])
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
                    "secondary_oax_ids": list(all_sub - {r["oax_id"]}),
                })

    resolved_rescue = pd.DataFrame(rescue_rows)
    resolved = pd.concat([resolved_single, resolved_ambig, resolved_rescue], ignore_index=True)

    print("[4/4] Applying manual resolutions...")
    manual_path = Path(__file__).resolve().parents[1] / "config" / "manual_resolutions.csv"
    manual_unlinked = pd.DataFrame(columns=["arc_id", "note"])
    n_manual_resolve = n_manual_unlink = 0
    if manual_path.exists():
        manual_df = pd.read_csv(manual_path).dropna(subset=["arc_id"])
        for _, row in manual_df.iterrows():
            aid    = row["arc_id"]
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

    out_manual_unlinked = PROCESSED_DATA / "arc_manual_unlinked.parquet"

    print("[5/5] Saving outputs...")
    resolved.to_parquet(out_resolved, index=False)
    deferred.to_parquet(out_ambiguous, index=False)
    manual_unlinked.to_parquet(out_manual_unlinked, index=False)

    all_arc = con.execute(f"SELECT count(*) FROM read_parquet('{arc_path}')").fetchone()[0]

    by_counts = resolved["resolved_by"].value_counts() if len(resolved) else pd.Series(dtype=int)
    print(f"\n  Total ARC persons:              {all_arc:,}")
    print(f"  Resolved (1 HC match):          {by_counts.get('unique_hc', 0):,}")
    print(f"  Resolved (disambiguated):        {len(resolved) - by_counts.get('unique_hc', 0):,}")
    for label in ["oax_orcid_dedup", "oax_topic_dedup", "orcid", "inst_gate", "inst_overlap", "field", "probability", "works_count", "name_filter", "manual"]:
        n = by_counts.get(label, 0)
        if n:
            print(f"    of which by {label+':':16s} {n:,}")
    print(f"  Resolved total:                  {len(resolved):,}  ({100*len(resolved)/all_arc:.1f}%)")
    print(f"  Ambiguous deferred:              {deferred['arc_id'].nunique():,}")
    print(f"  Manual unlinked:                 {len(manual_unlinked):,}")
    print(f"  Unlinked (no HC match):          {all_arc - len(resolved) - deferred['arc_id'].nunique() - len(manual_unlinked):,}")
    print(f"\n  → {out_resolved}")
    print(f"  → {out_ambiguous}")
    print(f"  → {out_manual_unlinked}")


if __name__ == "__main__":
    main()
