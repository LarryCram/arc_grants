# Full TODO list — ARC Grants → OpenAlex pipeline

## Context

The user asked for the complete outstanding TODO list from this project, compiled with
integer labels and a recommended execution sequence, sequence first, with item numbers
matching the sequence order. Everything below is drawn directly from `CLAUDE.md`'s "Next
Priority" section plus explicitly-flagged open decision points elsewhere in the file (the
"Known Issues in 02 Output" section, and end-of-session notes that say something is
queued/not-yet-applied/pending a decision). Items marked `~~closed~~` in CLAUDE.md itself
(already fixed/superseded/dropped) are excluded — this is the *live* list only.

Items 1 and 2 have since been worked and closed (see their entries below) — investigating
them on request surfaced that both were already stale: the underlying pipeline had already
fixed or correctly resolved each case, and only CLAUDE.md's own tracking text hadn't been
updated to say so. CLAUDE.md has been corrected accordingly for both. Item 4 has since been
worked and closed too — the user chose to keep reviewing rather than stop at 35, and the `4u`
under-merge population is now 0. Item 5 has since been worked and closed too — the original
44-candidate list turned out to be genuinely unrecoverable, but the population containing it
was independently driven to UNRESOLVED=0 by later, more thorough sessions, so nothing from
it could still be unaddressed.

This file lives in `docs/` (moved here from a local Claude Code plan file, which isn't
version-controlled or shared across machines) so it persists as a real, checked-in project
artifact rather than session-local state.

## Recommended sequence

Fast, low-risk cleanups first (they close out review threads already in flight and cost
little), then the structural work that unlocks better automation (merge operator), then the
oeuvre/piling correctness work, then the roadmap continuation, then the larger/optional items
last.

1. ~~Record the 3 confirmed wrong-merges in `manual_splits.csv`~~ — **DONE, no action was
   needed.** Investigated 2026-08-26: all three (Wang/Duan, Bunda/Lasczik, Curran/Gallagher)
   were already fixed on 2026-08-21 via `manual_orcid_corrections.csv`, and Curran/Gallagher
   were never actually merged (Gallagher's `PI` role is out of `KEEP_ROLES` scope). CLAUDE.md's
   stale "Next Priority" bullet corrected to record this.
2. ~~Investigate/resolve Raymond Gilbert / Robert Gilbert~~ — **DONE.** Investigated
   2026-08-26: already split into two clean `RESOLVED` clusters by the automatic pipeline
   (no manual CSV entry existed) — `DP0210039_RaymondGilbert` (15 grants, Civil Engineering,
   UNSW) and `DP0210446_RobertGilbert` (19 grants, Materials Chemistry → Food Sciences,
   Sydney→UQ). User-supplied external evidence (Raymond's ORCID matched exactly; Robert G.
   Gilbert's real biography/ORCID matched the institution timeline and field trajectory)
   confirmed the split is correct. Robert's real ORCID (`0000-0001-6988-114X`) added to
   `manual_orcids.csv`; CLAUDE.md's stale "Known Issues" bullet corrected.
3. ~~Clean up the 42 stale `arc_id` rows in `manual_resolutions.csv`~~ — **DONE, and grew into
   much more than a cleanup.** Investigating *why* rows go stale led to a full session-long
   arc: diagnosed `cluster_id`'s `min()`-derivation as the root cause, refined the sort key
   (drop the scheme-letter prefix, sort on the year-first numeric suffix), audited all 7
   `cluster_id`-keyed `manual_*.csv` mechanisms for risk, then built and wired a shared
   `resolve_cluster_id()`/`StaleClusterIdError` utility (raises loudly rather than the old
   ad hoc silent skip/fallback/delete behaviour) into every one of them plus `01a_diagnose.py`
   and `04_resolve_links.py`. Of the original 43 stale rows, 8 were archived (genuinely
   out-of-scope people — non-HEP admin_org or the dropped DI scheme, not drift) to
   `ZARCHIVE/data/manual_resolutions_retired_out_of_scope_20260826.csv`, 1 was corrected
   (`orcid_...` sentinel → its real current `cluster_id`), and the remaining 34 now self-heal
   automatically at runtime. 473/473 tests passing throughout.
4. ~~Decision gate: continue manual review of the remaining 35 `4u` under-merge clusters, or
   proceed to OAX linking~~ — **DONE, driven to zero.** The user chose to keep reviewing rather
   than stop at 35. Across two sessions (2026-08-26): 8 more confirmed via `manual_merges.csv`
   (Robertson, Wang/FT120100612, Bonniface, Anthony Harris, Su Ming Zhu, Richard Jones ×2, John
   Evans ×2, Ying Zhu, Jennifer Smith/Smith-Merry, Jun Li 5-way, David Evans 3-way, Mark Baker),
   17 confirmed distinct via the new `manual_confirmed_distinct.csv`. A real process failure
   happened mid-session — 9 pairs were batch-merged on the user's own summary characterization
   without checking co-investigators/institution/year individually, caught immediately by the
   user ("you did not show me the full story... Jing Li os tow people"), fully reversed, then
   redone one case at a time. `4u` population: 479 → 35 (2026-08-24) → 43 (after the diacritic
   fix rerun regenerated the population) → **0** (2026-08-26, this pass). Full detail in
   CLAUDE.md's "`4u` review continued..." and "`4u` population driven to zero..." session
   entries. Final `01`→`03`→`04` rerun: 22,893 AwardsCIF, 98.6% resolved.
5. ~~Verify whether the 44 ORCID-institution-mismatch split candidates were actually subsumed
   by the 2026-08-23/24 UNRESOLVED-to-zero pass, or still need recording~~ — **DONE.** The
   original list turned out unrecoverable (confirmed 2026-08-25), but the population
   containing it was independently driven to UNRESOLVED=0 by later, more thorough sessions —
   structurally nothing could remain unaddressed. Verified 2026-08-26: 0 UNRESOLVED, 0 `4u`,
   370/370 tests passing. One real side-finding: 2 clusters briefly regressed to UNRESOLVED
   purely as an artifact of this session's own merges (broadened FOR-code spread tripping the
   division-mismatch heuristic) — fixed via `manual_confirmed_not_suspicious.csv`.
6. **Build the `merge()` operator + candidate-pair proposer** (incl. B3 signal) — the structural fix that makes under-merge detectable at all, and de-risks items like #1/#2 above from ever recurring
7. **Re-derive `ACCEPTABLE_DIVISION_PAIRS` properly** (slow, named-case-first this time — 3 prior attempts failed)
8. **Close the two known oeuvre/piling gaps**: mega-pool false bridging, within-pile contamination (Hayward-style)
9. **HDBSCAN vs DBSCAN comparison** (directly feeds #8)
10. **Spot-check the "multi-pile, 2+ confirmed" bucket growth**
11. **Build the person-relative implausible-year filter**
12. **Roadmap step 3**: drop zero-work `author_idx` from `oax_candidates`
13. **Roadmap step 4**: definitive-evidence gate / `Dossier()` construction by selection
14. **Plan Part B**: `Dossier()` ARC-story header, `dossier_build_arc.py`/`dossier_build_oax.py` split
15. (optional/low-value) **Frequency-plurality fallback for `family_name_main`** — set-overlap fix already shipped, this is a residual refinement only
16. **Extract `author_position`** — purely opportunistic, do only when the OpenAlex snapshot is next re-converted for an unrelated reason
17. **Work through the `accuracy_checks.md` checklist** (largest, most multi-part, least urgent — a standing backlog, not a single task)

---

## Full TODO (detail)

### 1 — Three confirmed wrong-merges (Wang/Duan, Bunda/Lasczik, Curran/Gallagher) — CLOSED
Originally: found via a full-population same-grant/same-ORCID screen, thought to need
`manual_splits.csv` entries. Actually already fixed 2026-08-21 via `manual_orcid_corrections.csv`
(nulling the shared/wrong ORCID pre-clustering), verified 2026-08-26 directly against
`awards_cif_arc_only.parquet` — `DP1095466_WenhuiDuan` (10 grants, single ORCID) and
`DP170104546_ChienMingWang` (3 grants, ORCID correctly nulled) are separate resolved clusters;
`DP240100968_AlexandraLasczik` (1 grant) and `DP240100968_TraceyBunda` (2 grants, her own
later-enriched ORCID) are separate resolved clusters; Georgia Curran and Enid Gallagher were
never merged at all (Gallagher's `PI` role is out of scope). No further action.

### 2 — Raymond Gilbert / Robert Gilbert (n=34, no ORCID) — CLOSED
Originally: different first names + 3 different fields, suspected mis-merge of 2–3 people,
never investigated. Actually already split into two clean `RESOLVED` clusters by the pipeline's
own name/FOR/institution clustering, no manual CSV entry involved: `DP0210039_RaymondGilbert`
(15 grants, Civil Engineering, mostly UNSW, ORCID `0000-0001-8540-6517`, tier 1b) and
`DP0210446_RobertGilbert` (19 grants, Materials/Macromolecular Chemistry → Food Sciences,
University of Sydney 2002–2006 → University of Queensland 2005–2025, tier 3). User-supplied
external evidence confirmed both sides: Raymond's real ORCID matched exactly what the pipeline
already had; Robert G. Gilbert's real biography and ORCID (`0000-0001-6988-114X`) matched the
institution timeline and field trajectory. Robert's ORCID added to `manual_orcids.csv`.

### 3 — Clean up stale `arc_id` rows in `manual_resolutions.csv` — CLOSED, expanded scope
Originally: 42 (later recounted as 43) rows reference an `arc_id` no longer matching any
current `cluster_id`, presumed stale from an ARC-side rename/merge/split.

What actually happened this session, in order:
1. **Root cause diagnosed**: `cluster_id = min(unique_ids in the cluster)` is recomputed
   fresh on every `01_prepare_arc.py` run — it isn't a stable per-person handle, it's a
   derived label that can shift on any membership change (a merge, a split, a new grant
   added under an earlier-sorting scheme letter), independent of whether the person's real
   identity changed at all.
2. **Sort key refined**: the scheme-letter prefix (`DP`/`LP`/`DE`/...) sorts alphabetically,
   not chronologically, so it was a pure source of arbitrary volatility. Confirmed via real
   data that ARC's own grant-code convention puts the 2-digit award year immediately after
   the scheme prefix, at a fixed width, before a variable-length sequence number — so
   sorting on `grant_id[2:]` (keeping the full string as the display name, just changing the
   *sort key*) is chronologically correct by construction, not by coincidence.
3. **Full risk audit across all 7 `cluster_id`-keyed `manual_*.csv` mechanisms**
   (`manual_resolutions.csv`, `manual_orcids.csv`, `manual_splits.csv` +
   `manual_splits_by_grant.csv`, `manual_merges.csv`, `manual_confirmed_not_suspicious.csv`,
   `enrichment_blocklist.csv`) — each had its own ad hoc silent-failure behaviour on a stale
   reference: `manual_merges.csv` was the worst, silently **deleting** a real, current person
   from the whole population outright if `cluster_keep` alone had drifted; others silently
   fell back to a known-inferior method, silently dropped confirmed work, or silently
   reopened an already-closed review loop.
4. **Built `resolve_cluster_id()` / `StaleClusterIdError`** (`src/utils/awards_cif.py`) —
   accepts either `list[AwardsCIF]` or a `DataFrame` (both shapes are genuinely used side by
   side in this codebase), resolves via exact match or unique_id-membership search, raises
   loudly on anything unresolvable or ambiguous. Wired into all 7 mechanisms plus
   `01a_diagnose.py`'s `check_A()` and `04_resolve_links.py`'s manual-override loop. 5 new
   tests added, plus 2 `autouse` fixtures so existing synthetic-fixture tests aren't broken
   by real on-disk CSV content. 473/473 passing.
5. **The 9 rows this then surfaced as genuinely unresolvable were resolved by hand**: 8
   archived to `ZARCHIVE/data/manual_resolutions_retired_out_of_scope_20260826.csv` (each
   traced to a specific, pre-existing scope rule — non-HEP admin_org for 7 of them: QIMR,
   WEHI, NICTA, CSIRO; the 2026-08-17 DI-scheme removal for the 8th — confirmed via checking
   *every* grant each person holds, not just the one named in the stale row, so there was no
   live alternate cluster to redirect to); 1 corrected directly (`orcid_0000-0001-9646-670X`
   → `DE190101078_XiangLi`, its real current `cluster_id`). The remaining 34 (of the original
   43) now resolve automatically at runtime — no CSV edit needed, confirmed via direct
   verification against `awards_cif_arc_only.parquet` (0 truly unresolvable rows remain).

### 4 — Decision gate: continue `4u` (under-merge risk) review or proceed to OAX linking — CLOSED
After driving `4u` 479 → 35 via targeted manual verification (2026-08-24), the remaining 35
were predominantly weak-signal cases (no-institution-overlap, bare-initial) plus
same-institution cases previously checked and found unconfirmable (Jun Li, David Evans, John
Evans). The user chose to keep reviewing rather than stop at 35.

A subsequent diacritic-table-removal rerun regenerated the population at 43 `4u` clusters
(2026-08-26) — worked down in two passes the same day: first pass closed 4 pairs (Robertson,
Wang/FT120100612, Bonniface, Anthony Harris — with a real methodological correction along the
way, switching from institution-overlap evidence to shared-co-investigator-identity evidence
once it was noticed multi-investigator grants can't reliably attribute an institution to any
one specific person) and left Mark Baker as an open, unconfirmed lead. Second pass (same day,
different session) worked through the remaining pool: **a real process failure occurred and
was caught** — 9 pairs were merged based on the user's own batch summary without individually
checking co-investigators/institution/year, caught immediately by the user, fully reversed via
file restoration, then redone one pair at a time against actual `investigators_raw.parquet`
rows. Final outcomes: 2 of the 9 were genuinely the same person (Ying Zhu — shared
co-investigator pair confirmed it; Jennifer Smith/Smith-Merry — ORCID-confirmed), the other 7
confirmed distinct. Two more genuinely new cases were resolved via user-supplied ORCID
searches: Jun Li (5-way merge for the UniSA fragments, decisively confirmed by a real
co-authored paper matching the inferred co-investigator chain exactly; the DECRA/UTS Jun Li in
the same original pool confirmed as a different, younger person) and David Evans (3-way
merge, same PI across all three UTAS grants, confirmed via a privacy-locked ORCID whose public
works list — brewing/malting chemistry — matched the grants' Crop and pasture production FOR
code). Mark Baker's open lead was then confirmed by the user directly. Li Li and Peter
James/James Smith closed out the pool as confirmed-distinct.

**`4u` population: 479 → 35 → 43 (regenerated) → 0.** Final `01`→`03`→`04` rerun: 22,893
AwardsCIF, 98.6% resolved (22,573), 176 ambiguous deferred, 31 manual unlinked, 113 unlinked.
Full case-by-case detail in CLAUDE.md's "`4u` review continued post-diacritic-fix rerun..."
and "`4u` population driven to zero; a batch-merge mistake caught and fully reversed..."
session entries (both 2026-08-26).

### 5 — Verify the 44 ORCID-institution-mismatch split candidates were recorded — CLOSED
Originally: the 2026-08-19/20 "ORCID Public API integration" session (this item's own text
mislabeled it "2026-08-21" — corrected here) found 44 real, institution-corroborated split
candidates via a systematic biography+employment check over the 82 ORCID-bearing UNRESOLVED
clusters remaining at that checkpoint, explicitly "queued for recording, not yet applied."

No literal diff was possible — a 2026-08-25 session ("`inst_arr` widened...") already tried
to re-derive the specific 44-cluster list and confirmed directly that **the original list was
never persisted anywhere** (no scratch file, no `cluster_id`s recorded in CLAUDE.md or any
CSV), only discussed in a conversation that later ended — genuinely unrecoverable by name.

**Resolved structurally instead, 2026-08-26**: the population those 44 belonged to
(UNRESOLVED, standing at 120 right when they were found) was subsequently driven through
several more sessions all the way to **0** (the 2026-08-23/24 "UNRESOLVED population driven
to zero" session, later reconfirmed after this session's own merges/splits). Since every
single UNRESOLVED cluster — a strict superset of whichever ones were among the original 44 —
has since been individually reviewed and resolved (split, confirmed, or manually overridden)
by that more thorough exhaustive pass, nothing from the 44 could still be sitting unaddressed
in the live population, even though the specific pointer back to "these were part of the 44"
is lost. Verified directly: `resolution_status` UNRESOLVED = 0 and `reliability_tier=='4u'`
= 0 on the current `awards_cif_arc_only.parquet` (22,893 AwardsCIF), 370/370 tests passing.

One real, unrelated side-finding from this verification pass: 2 clusters (`LP0669061_JunLi`,
`LP0454996_RichardJones`) briefly showed `UNRESOLVED` immediately after this session's own
merges, purely because merging genuinely broadened their FOR-code spread enough to trip the
division-mismatch heuristic — the same "reviewed forever" loop pattern
`manual_confirmed_not_suspicious.csv` exists for. Both already had decisive evidence backing
their merge (Jun Li: ORCID + matching co-authored paper; Richard Jones: ORCID, already
documented). Recorded in `manual_confirmed_not_suspicious.csv`; rerun confirmed 0/0 clean.

### 6 — No standalone `AwardsCIF` merge() operator; under-merge structurally undetectable
`awards_cif.py` has a low-level `_merge_awards_cifs()` primitive but every call site fires
from one fixed, early-pipeline sequence before OAX candidates/oeuvre/piling exist —
nothing downstream can invoke a merge based on evidence discovered later. Needs: a real
`merge(acif_a, acif_b, evidence) -> AwardsCIF` operator, callable post-hoc from any
pipeline stage, plus a candidate-pair proposer (cross-ACIF similarity via shared OAX
candidates / shared coauthors / etc.) that specifically includes the cross-grant B3 signal
(`01a_diagnose.py`'s existing informational-only same-blocking-key + shared-co-investigator
+ same-admin_org check) — folded in here rather than as a separate line item since it
would duplicate logic the general operator needs anyway.

### 7 — `ACCEPTABLE_DIVISION_PAIRS` re-derivation
Needed because `division_mismatch_for2020()` was switched to all-codes divisions (not
just primary), so the existing 41-pair whitelist is now tested against a broader
population than it was calibrated for. Three re-derivation attempts each surfaced a new
problem instead of converging (unexplained 2–3x inflation; a lift correction that didn't
fix it; a version that regressed by whitelisting the exact division pair from the
confirmed Wei Wang false-merge). Reverted to the original 41-pair list, which over-flags
for manual review (safe direction) rather than under-flags. The original derivation script
was never persisted, so there's no way to diff against it directly. Needs a slower pass
grounded in real, named-case validation from the start, not aggregate statistics alone.

### 8 — Group-level ACIF-membership gate: two remaining real gaps
Design done, channeling + persistence + `Dossier()` wiring done (2026-08-18). Two
confirmed, unresolved gaps:
- **Mega-pool false bridging**: in large candidate pools (WeiWang/MohammadIslam scale),
  DBSCAN at the eps needed to unify a genuinely coherent career can also merge two
  confirmed-different people via an indirect chain through *other* candidates in the same
  pool — even when the two aren't directly similar. HDBSCAN identified as the likely fix
  (see #9), not yet tried.
- **Within-pile contamination**: a nominally "correct" dominant pile can still be
  majority-wrong internally (Hayward's main pile was only 59% the ORCID-confirmed correct
  candidate) — piling's own clustering doesn't guarantee purity within a pile, and the
  pile-to-ACIF channeling step wasn't confirmed to catch this. Not resolved.

### 9 — HDBSCAN vs DBSCAN systematic comparison
Requested 2026-08-18, not yet done — user-deferred until other in-flight work finished.
Test `sklearn.cluster.HDBSCAN` (already available, no new dependency) against the same
cross-section used to validate DBSCAN (Hessel for "does it still unify a clean career",
WeiWang/MohammadIslam for "does it stop mega-pool false bridging").

### 10 — "Multi-pile, 2+ confirmed" bucket growth spot-check
The Stage 3 ORCID/small-pool gates roughly doubled this bucket population-wide (ECR
cohort: 1,133 → 1,413 → 2,094 across two rounds of gate changes). Plausibly genuine
fragment-splitting now visible because more real data reaches piling, but could also be
over-permissive corroboration letting wrong piles through — the same failure mode
subfield-level matching was originally adopted to prevent. Not checked either way.

### 11 — Person-relative implausible-year filter
Stage 1's `implausible_year` check only catches globally-implausible years (outside
1950–2026) — it can't catch a work whose year is merely impossible *for this specific
person* (e.g. Adam Hulme's contaminated 1960/1971/1985 works sit comfortably inside the
global range). Flagged as possibly as effective as field-based filtering, and safer (a
wrong-decade work is a cleaner signal than a wrong-field one). Not implemented.

### 12 — Roadmap step 3: drop zero-work `author_idx` from `oax_candidates`
Named directly in the `oeuvre_build.py` section as not built this pass. Prunes candidates
that never contributed any oeuvre work.

### 13 — Roadmap step 4: definitive-evidence gate / `Dossier()` construction by selection
Named directly alongside step 3 as not built this pass — the actual selection mechanism
that turns a scored/piled candidate set into a `Dossier()`.

### 14 — Plan Part B: `Dossier()` ARC-story header + `dossier_build_arc.py`/`dossier_build_oax.py` split
Part A (verification infrastructure: `01a_diagnose.py`'s structured result type,
`--sample-4u`, `cluster_detail_data()`/`cluster_detail_text()` relocation) is done. Part B
was not started in the 2026-08-24 session; plan file
`/home/lc/.claude/plans/how-do-yo-know-linked-diffie.md` is still current for it.

### 15 — `family_name_main` frequency/plurality fallback (residual, optional)
The set-overlap blocking fix (option 2) already shipped 2026-08-25 and is confirmed
working. Option 1 — picking `family_name_main` itself by frequency/plurality rather than
raw string length — was never built. Same class of bug already independently fixed in the
two ARC-internal Python grouping functions. Low priority: the core over-merge/under-merge
risk this was protecting against is already addressed by the shipped fix.

### 16 — Extract `author_position` on next OpenAlex snapshot conversion
Check the raw native snapshot carries the field, re-extract as an explicit persisted
column (never inferred from row order — found unreliable both ways in this project's own
investigation, see `docs/author_position_investigation.md`), then verify against real
known works before trusting it downstream. Not urgent standalone — do opportunistically
next time the snapshot conversion runs for any other reason.

### 17 — `accuracy_checks.md` checklist
Kept as a live TODO (2026-08-25), not parked; none of its items are built yet. Covers: a
repair-reporting mechanism for any OAX author id mapped to 2+ ARC persons; earliest-year-
on-duplicate-title logic; a flag/hide rule for implausible 2026+ publication years; a
work-subfield-vs-ARC-FOR-code consistency cross-check; an academic-age bound (first pub
after 1950, age under 60); an "oax_id problem vs work problem" decision rule; a
year-continuity check; and an ECR/MCR/senior academic-age-at-award check keyed to a
not-yet-fully-compiled fellowship-scheme-to-career-stage list, with real undefined
exceptions (career breaks).

---

## Verification
Items 1 and 2: no pipeline rerun was needed since both turned out to already be correctly
resolved in current output — verified directly by querying `awards_cif_arc_only.parquet`.
`manual_orcids.csv` gained one new row (`DP0210446_RobertGilbert`); CLAUDE.md's "Next
Priority" and "Known Issues in 02 Output" sections were corrected to stop describing closed
issues as open. Remaining items still need their own individual verification once
undertaken (each already describes, in CLAUDE.md, what a clean rerun/test-suite pass looks
like for that specific fix).
