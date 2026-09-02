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

**2026-09-01 resequencing**: former #9 (HDBSCAN vs DBSCAN comparison) dropped outright — user
decision, DBSCAN is good enough, not worth the comparison effort. Former #10 ("multi-pile,
2+ confirmed" bucket spot-check) folded into #9/piling-gaps (now #9 below) — same
investigation, no reason to track separately. Former #19 (audit `analysis/` for stale
hardcoded literals) moved up to #6 — user's own call, agreed: it's cheap, fully decoupled
from everything else on this list, and the one bug it already found (`03_annual_metrics.py`'s
hardcoded 2000/2025 window) was a real, silent, substantial data-loss bug, so there's a real
chance another one is sitting undetected while other work builds on top of it. Everything from
former #11 onward renumbered down accordingly (net: two items dropped/merged, one moved up,
final count 19).

This file lives in `docs/` (moved here from a local Claude Code plan file, which isn't
version-controlled or shared across machines) so it persists as a real, checked-in project
artifact rather than session-local state.

## Recommended sequence

Fast, low-risk cleanups first (they close out review threads already in flight and cost
little), then a cheap decoupled audit, then the structural work that unlocks better
automation (merge operator), then the oeuvre/piling correctness work, then the roadmap
continuation, then the larger/optional items last.

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
6. ~~Audit `analysis/` for other stale hardcoded literals~~ — **DONE, same session.** Real
   fixes in `04_au_baseline.py`, `04b_citation_quantiles.py`, `02_accuracy_check.py`,
   `05_explore.py` (hardcoded year windows narrower than `MIN_PUB_YEAR`/`MAX_PUB_YEAR`, and
   "academic age" hardcoded against a fixed year rather than computed live); consolidated
   4 duplicate `MIN_PUB_YEAR`/`MAX_PUB_YEAR` definitions down to 1. 462/462 tests passing.
7. **Build the `merge()` operator + candidate-pair proposer** (incl. B3 signal) — the structural fix that makes under-merge detectable at all, and de-risks items like #1/#2 above from ever recurring
8. **Re-derive `ACCEPTABLE_DIVISION_PAIRS` properly** (slow, named-case-first this time — 3 prior attempts failed)
9. **Close the two known oeuvre/piling gaps**: mega-pool false bridging, within-pile contamination (Hayward-style); includes the "multi-pile, 2+ confirmed" bucket growth spot-check (folded in — same investigation). HDBSCAN comparison dropped — DBSCAN is good enough
10. **Build the person-relative implausible-year filter**
11. **Roadmap step 3**: drop zero-work `author_idx` from `oax_candidates`
12. **Roadmap step 4**: definitive-evidence gate / `Dossier()` construction by selection
13. **Plan Part B**: `Dossier()` ARC-story header, `dossier_build_arc.py`/`dossier_build_oax.py` split
14. (optional/low-value) **Frequency-plurality fallback for `family_name_main`** — set-overlap fix already shipped, this is a residual refinement only
15. **Extract `author_position`** — purely opportunistic, do only when the OpenAlex snapshot is next re-converted for an unrelated reason
16. **Work through the `accuracy_checks.md` checklist** (largest, most multi-part, least urgent — a standing backlog, not a single task)
17. **HEP-affiliation-history candidate-pool pruning for common-name mega-pools** — real, partial win, not built (see its own entry below)
18. **Extract `raw_affiliation_string` on the next OpenAlex snapshot conversion, then filter junk-matched authorship institutions** — a real, confirmed OpenAlex-side data-quality bug found via two dossier examples (see its own entry below); needs new data before it's actionable, so opportunistic like the existing `author_position` item, not urgent standalone
19. **Consolidate pre-linking ORCID processing into `00b_enrich_orcid.py`; clean out the scattered/duplicated local-ORCID-source code this session's exploration left behind** — high priority, blocks trusting any further ORCID enrichment work until done

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

### 6 — Audit `analysis/` for other stale hardcoded literals
Found 2026-08-31 while building a dossier-page redesign: `analysis/03_annual_metrics.py`
correctly used `MIN_PUB_YEAR`/`MAX_PUB_YEAR` (1950-2026) for deduping/loading works, but four
separate SQL blocks inside the same script hardcoded a stale `2000`/`2025` literal window for
the actual metrics-generation step — silently dropping any real work outside that narrower
range from `annual_metrics.parquet`, even though `deduped_works` itself already correctly
included it. Confirmed via a real case (Sarah Legge, `DP0210086` — genuine 1996/1997/1999
works and a 2026 one were all missing from what the table used to produce). Fixed (all four
sites now use the same constants), full rerun: 546,076 → 873,887 rows / 22,625 → 22,671
persons — a real, substantial population-wide effect, not a one-person edge case.

`analysis/` (as opposed to `src/`) hasn't been through the same level of scrutiny this project
has repeatedly applied to the identity-resolution pipeline this year — this specific bug was
found by accident (building a chart, not auditing), which means there's no reason to believe
it's the only one. Needs a systematic sweep of `analysis/*.py` and `analysis/utils/*.py` for
other hardcoded year ranges, magic thresholds, or other literals that have quietly drifted out
of sync with the constants/config they should be deriving from — not scoped or started.

**Moved up in sequence 2026-09-01** (was #19) — user's call, agreed: cheap, fully decoupled
from the merge-operator/piling work below, and the one bug already found here was a real,
silent, substantial one, so there's a real chance another is lurking while other work
proceeds. No dependency reason it had to move, just no reason to wait either.

**DONE, same session.** Full grep sweep of every `analysis/*.py`/`analysis/utils/*.py` file
for suspicious year literals, cross-checked each hit against context (real filter dropping
data vs. benign display/sample/documentation) before touching anything. Real findings, all
fixed:
- **`analysis/04_au_baseline.py`** (`au_annual.parquet`/`world_annual.parquet`): both
  `publication_year BETWEEN 2000 AND 2025` filters (building the AU-wide and world-wide
  citation baselines these get compared against) silently dropped any work outside that
  window — same bug class as the original `03_annual_metrics.py` fix, and now doubly stale
  since 2026 has already started. Fixed to `MIN_PUB_YEAR`/`MAX_PUB_YEAR`. One of the two
  occurrences was missed on the first pass (different indentation defeated an exact-string
  `replace_all`) and caught by a re-sweep after the first round of fixes — a reminder that
  even a targeted fix needs its own verification pass, not just a first attempt.
- **`analysis/04b_citation_quantiles.py`** (`citation_quantiles.parquet`, feeds
  `03_annual_metrics.py`'s `n_highly_cited` backfill): `BETWEEN 1990 AND 2025` was narrower
  than what `03_annual_metrics.py` itself covers (1950-2026), so `n_highly_cited` silently
  stayed null for any work before 1990 or in 2026. Fixed to `MIN_PUB_YEAR`/`MAX_PUB_YEAR`.
- **`analysis/02_accuracy_check.py`**: three separate issues — (1) `BETWEEN 1980 AND 2025` /
  `generate_series(1980, 2025)` for career year-gap detection, narrower than the project's
  own 1950-2026 range, fixed; (2) bare `1950`/`2026` literals (matching the real constants by
  coincidence, not by reference) in the implausible/future-year classification, replaced with
  the named constants to remove the coincidence; (3) **academic age hardcoded as "2025"** in
  two places (`avg_academic_age`, `academic_age`) — genuinely stale now, since today's real
  date is 2026-09-01, meaning every academic-age figure this script printed was already a
  year off. Fixed to compute the real current year at runtime (`datetime.date.today().year`),
  not a literal.
- **`analysis/05_explore.py`**: three issues of the same shape — (1) "H-index distribution at
  2024" was hardcoded despite the surrounding comment already saying "(most recent year)",
  contradicting its own stated intent; fixed to actually compute the latest year present in
  the data; (2) "Academic age (2026 − first_pub_year)" hardcoded, same real-current-year fix
  as `02_accuracy_check.py`; (3) three `BETWEEN 2000 AND 2024` windows on the ARC-vs-AU-vs-
  World time-series plot, stale and inconsistent with every other window in the project, now
  a third different hardcoded range on top of the ones already found — aligned to
  `MIN_PUB_YEAR`/`MAX_PUB_YEAR`.
- **Duplicated constant definitions**: `MIN_PUB_YEAR`/`MAX_PUB_YEAR` (1950/2026) were
  independently redefined in four places — `analysis/utils/dedup.py` (the real source),
  `03_annual_metrics.py`, `06_analyse_fellowships.py`, and `analysis/tests/test_metrics.py` —
  all consistent today but a structural drift risk (exactly this bug class, again, the next
  time one gets bumped and the others don't). Consolidated: the three duplicates now import
  from `dedup.py` instead of redefining; `test_metrics.py`'s copy was fully unused dead code,
  removed outright. `04_au_baseline.py`, `04b_citation_quantiles.py`, `02_accuracy_check.py`,
  and `05_explore.py` all gained a fresh import from `dedup.py` rather than yet another local
  redefinition.
- **`06_analyse_fellowships.py`**: one stale line in the module docstring ("academic ages
  (2025 − first_pub_year)") describing an old calculation method the code no longer uses —
  the real, current code correctly computes age at `award_year` (already using
  `MIN_PUB_YEAR`/`MAX_PUB_YEAR` properly throughout its real queries, no bug there) — docstring
  corrected to match. The `award_year >= 2015` filter on the trajectory plot (line ~168) was
  checked and confirmed intentional, already documented in CLAUDE.md's own 2026-06-18 session
  notes — left unchanged.

**Checked and confirmed benign, not touched**: `01_fetch_oeuvres.py`'s `pre2000` count (a pure
diagnostic count, not a filter — the file's own docstring already says "No year filter"),
`02_accuracy_check.py`'s `first_pub_year > 2000`/`< 1990` debut thresholds (fixed historical
markers, not meant to track the current year), sample-year print statements in
`04_au_baseline.py`/`04b_citation_quantiles.py`/`04c_subfield_cooccurrence_baseline.py`
(illustrative spot-checks, not filters), and comment-only date references in
`analysis/utils/dossier.py`/`dossier_build.py`/`07_analyse_ecr_fellowships.py`.

Verified: `.venv/bin/python -m pytest analysis/tests/ tests/ -q` — 462/462 passing throughout,
including after the missed-occurrence catch.

### 7 — No standalone `AwardsCIF` merge() operator; under-merge structurally undetectable
`awards_cif.py` has a low-level `_merge_awards_cifs()` primitive but every call site fires
from one fixed, early-pipeline sequence before OAX candidates/oeuvre/piling exist —
nothing downstream can invoke a merge based on evidence discovered later. Needs: a real
`merge(acif_a, acif_b, evidence) -> AwardsCIF` operator, callable post-hoc from any
pipeline stage, plus a candidate-pair proposer (cross-ACIF similarity via shared OAX
candidates / shared coauthors / etc.) that specifically includes the cross-grant B3 signal
(`01a_diagnose.py`'s existing informational-only same-blocking-key + shared-co-investigator
+ same-admin_org check) — folded in here rather than as a separate line item since it
would duplicate logic the general operator needs anyway.

### 8 — `ACCEPTABLE_DIVISION_PAIRS` re-derivation
Needed because `division_mismatch_for2020()` was switched to all-codes divisions (not
just primary), so the existing 41-pair whitelist is now tested against a broader
population than it was calibrated for. Three re-derivation attempts each surfaced a new
problem instead of converging (unexplained 2–3x inflation; a lift correction that didn't
fix it; a version that regressed by whitelisting the exact division pair from the
confirmed Wei Wang false-merge). Reverted to the original 41-pair list, which over-flags
for manual review (safe direction) rather than under-flags. The original derivation script
was never persisted, so there's no way to diff against it directly. Needs a slower pass
grounded in real, named-case validation from the start, not aggregate statistics alone.

### 9 — Group-level ACIF-membership gate: two remaining real gaps
Design done, channeling + persistence + `Dossier()` wiring done (2026-08-18). Two
confirmed, unresolved gaps:
- **Mega-pool false bridging**: in large candidate pools (WeiWang/MohammadIslam scale),
  DBSCAN at the eps needed to unify a genuinely coherent career can also merge two
  confirmed-different people via an indirect chain through *other* candidates in the same
  pool — even when the two aren't directly similar. An HDBSCAN comparison was considered as a
  possible fix but **dropped 2026-09-01 (user decision)** — DBSCAN is good enough; address
  this instead via candidate-pool pruning shrinking the pool before piling ever sees it (see
  the oeuvre-QA plan's item 2, `write-out-the-full-floofy-starfish.md`).
- **Within-pile contamination**: a nominally "correct" dominant pile can still be
  majority-wrong internally (Hayward's main pile was only 59% the ORCID-confirmed correct
  candidate) — piling's own clustering doesn't guarantee purity within a pile, and the
  pile-to-ACIF channeling step wasn't confirmed to catch this. Not resolved.
- **"Multi-pile, 2+ confirmed" bucket growth** (folded in 2026-09-01, formerly tracked
  separately): the Stage 3 ORCID/small-pool gates roughly doubled this bucket population-wide
  (ECR cohort: 1,133 → 1,413 → 2,094 across two rounds of gate changes). Plausibly genuine
  fragment-splitting now visible because more real data reaches piling, but could also be
  over-permissive corroboration letting wrong piles through — the same failure mode
  subfield-level matching was originally adopted to prevent. Not checked either way — the
  same population-scale measurement pass that addresses the two gaps above should cover this.

Approach: measure population scale first (none of the three above is currently measured
beyond 1–2 named cases), then fix — consistent with this project's own standing rule against
building before profiling.

### 10 — Person-relative implausible-year filter
Stage 1's `implausible_year` check only catches globally-implausible years (outside
1950–2026) — it can't catch a work whose year is merely impossible *for this specific
person* (e.g. Adam Hulme's contaminated 1960/1971/1985 works sit comfortably inside the
global range). Flagged as possibly as effective as field-based filtering, and safer (a
wrong-decade work is a cleaner signal than a wrong-field one). Not implemented.

### 11 — Roadmap step 3: drop zero-work `author_idx` from `oax_candidates`
Named directly in the `oeuvre_build.py` section as not built this pass. Prunes candidates
that never contributed any oeuvre work.

### 12 — Roadmap step 4: definitive-evidence gate / `Dossier()` construction by selection
Named directly alongside step 3 as not built this pass — the actual selection mechanism
that turns a scored/piled candidate set into a `Dossier()`.

### 13 — Plan Part B: `Dossier()` ARC-story header + `dossier_build_arc.py`/`dossier_build_oax.py` split
Part A (verification infrastructure: `01a_diagnose.py`'s structured result type,
`--sample-4u`, `cluster_detail_data()`/`cluster_detail_text()` relocation) is done. Part B
was not started in the 2026-08-24 session; plan file
`/home/lc/.claude/plans/how-do-yo-know-linked-diffie.md` is still current for it.

User's own characterization, 2026-09-01: "the whole `Dossier()` build is a mess" — beyond
just Part B's own scope (the arc/oax split + ARC-story header), this likely also covers
`Dossier.works` currently reading `analysis/01_fetch_oeuvres.py`'s independent, unfiltered
pipeline rather than anything the candidate/work cleaning work produces (see the oeuvre-QA
plan's item 10, `write-out-the-full-floofy-starfish.md`) — not scoped further than that yet.

### 14 — `family_name_main` frequency/plurality fallback (residual, optional)
The set-overlap blocking fix (option 2) already shipped 2026-08-25 and is confirmed
working. Option 1 — picking `family_name_main` itself by frequency/plurality rather than
raw string length — was never built. Same class of bug already independently fixed in the
two ARC-internal Python grouping functions. Low priority: the core over-merge/under-merge
risk this was protecting against is already addressed by the shipped fix.

### 15 — Extract `author_position` on next OpenAlex snapshot conversion
Check the raw native snapshot carries the field, re-extract as an explicit persisted
column (never inferred from row order — found unreliable both ways in this project's own
investigation, see `docs/author_position_investigation.md`), then verify against real
known works before trusting it downstream. Not urgent standalone — do opportunistically
next time the snapshot conversion runs for any other reason.

### 16 — `accuracy_checks.md` checklist
Kept as a live TODO (2026-08-25), not parked; none of its items are built yet. Covers: a
repair-reporting mechanism for any OAX author id mapped to 2+ ARC persons; earliest-year-
on-duplicate-title logic; a flag/hide rule for implausible 2026+ publication years; a
work-subfield-vs-ARC-FOR-code consistency cross-check; an academic-age bound (first pub
after 1950, age under 60); an "oax_id problem vs work problem" decision rule; a
year-continuity check; and an ECR/MCR/senior academic-age-at-award check keyed to a
not-yet-fully-compiled fellowship-scheme-to-career-stage list, with real undefined
exceptions (career breaks).

### 17 — HEP-affiliation-history candidate-pool pruning for common-name mega-pools
Found 2026-08-27 investigating why piling kept crashing on a handful of common-name
mega-pool ACIFs (`LP0777033_WeiZhang` 67,475 Stage-3 survivor works, `DE130100488_YanYan`
80,579, `DP0342641_JunWang` 69,354, `DP120102205_XiaodongLi` 25,635) — see CLAUDE.md's
"Piling wired into `06_build_oeuvre.py`..." session for the full incident (a `MAX_POOL_SIZE`
safety cap was built to stop these crashing piling, but that's a symptom fix, not a cause fix).

Checked whether "has this candidate `author_idx` ever had an Australian HEP affiliation"
could prune these pools *before* piling (or even before Stage 1/3) ever sees them — real,
measured, but mixed: `WeiZhang` (2,365 candidates, only 867/37% ever AU-affiliated) and
`YanYan` (1,278 candidates, 440/34% ever AU-affiliated) are 63-66% prunable this way — most
of their candidates never worked in Australia at all, pure noise from a globally common
name. But `XiaodongLi` (350 candidates, 314/90% ever AU-affiliated) barely shrinks under this
heuristic — its candidates are overwhelmingly already Australian, so this is a genuinely
different failure shape (many real, distinct, Australian-affiliated people who happen to
share a common name), not foreign-noise contamination.

Needs: (1) decide where in the pipeline this pruning belongs — most naturally
`dedup_oax_candidates()`/`populate_oax_candidates()` in `awards_cif.py` (upstream of Stage
1/3/piling entirely, so it shrinks the problem before any downstream stage pays the cost),
(2) a real query for "ever had ANY Australian institutional affiliation" per candidate
`author_idx` at bulk/population scale (the ad hoc per-cluster version used to investigate this
took ~50-60s per cluster scanning the full 119M-row OpenAlex authors table — needs a
proper one-time bulk join, not N ad hoc queries), (3) accept that this only helps the
foreign-noise-contamination shape of mega-pool (confirmed real for WeiZhang/YanYan-style
cases) and won't materially shrink the same-country-common-name shape (XiaodongLi-style) —
the `MAX_POOL_SIZE` cap (or a proper merge-operator/candidate-pool-pruning fix, see item 7)
still does the real work for those.

**A third, different failure mode noted but not pursued** (user's own recollection): a real,
uncommon-name person whose correct OpenAlex identity was never captured as an ARC↔OAX
candidate at all (a missed match, not a collision) — this pruning idea does nothing for that
case. Proposed downstream diagnostic, not built: scan piling results for known high-profile
ARC-funded people with implausibly low total work-count, then chase down their real
`author_idx` from there as a manual follow-up, the same evidence-first pattern as the `4u`
review work.

### 18 — Extract `raw_affiliation_string`, filter junk-matched authorship institutions
Found 2026-08-31 investigating a real, recurring anomaly surfaced by the redesigned dossier
page: "Schlumberger (Ireland)" (an Irish oilfield-services company) appeared in the oeuvre of
two unrelated Charles Darwin University wildlife ecologists (Sarah Legge, Christine
Schlesinger), on works with titles decisively unrelated to oil/gas (fire ecology, feral cats,
threatened mammals, desert lizards). Checked and ruled out: not a person-identity mixup (the
`author_idx` on both is each person's own confirmed OpenAlex identity, not a shared/wrong one).

**Root cause, confirmed via the live OpenAlex API and the original paper's own author-footnote
block** (both user-supplied): the raw
`authorships[].raw_affiliation_string` for Sarah Legge on one of these works is literally
`"KCorresponding author. Email: sarahmarialegge@gmail.com"`. The paper's real footnote block
explains exactly why:
```
Sarah Legge
BResearch Institute of Environment and Livelihoods, Charles Darwin University, Casuarina, NT 0810, Australia.
CFenner School of Environment and Society, The Australian National University, Canberra, ACT 2601, Australia.
KCorresponding author. Email: sarahmarialegge@gmail.com
```
This is a standard lettered-footnote-marker convention (CSIRO-Publishing-style journal): each
author's name carries superscript letters pointing to numbered/lettered footnotes — `B`/`C` are
real institution footnotes (Charles Darwin University, ANU), but `K` is a *different kind* of
footnote (the corresponding-author/email note) that happens to use the same lettering scheme.
OpenAlex's extraction pipeline evidently doesn't distinguish "this lettered footnote is an
institution" from "this lettered footnote is a corresponding-author note" — it feeds the `K`
footnote through the same institution-matching logic as `B` and `C`, which then force-matched
it to a real but wrong institution. Not random parsing noise — a specific, structural
misreading of a common journal footnote convention, which means it will recur on any paper
using this same style, not just Sarah Legge's.

The raw match was to `institution_idx 4210108542` ("Schlumberger (Ireland)"). The same work's
`corresponding_institution_ids` also included **Services Australia** (Australia's
social-security/government-services agency) alongside the two real institutions (ANU, Charles
Darwin University) — user independently confirmed this second one is also a genuine error, not
a real affiliation. Two garbage matches on one paper, not a Schlumberger-
specific quirk.

**Confirmed systemic, not a one-off**: checked the field distribution of all ~41,291 works
tagged with `institution_idx 4210108542` in this project's own authorships extract — Social
Sciences (13,534), Agricultural/Biological Sciences (7,385), and Medicine (6,706) all far
outnumber Engineering (3,671, only 8th) and Earth/Planetary Sciences (1,828), which is the
reverse of what a real oilfield-services company's actual publication footprint would look
like. This points to a systemic weakness in OpenAlex's own affiliation parser — force-matching
garbled non-affiliation text to *some* real institution rather than leaving it unresolved —
not something specific to this one institution ID or these two people.

**Blocked on missing data**: `raw_affiliation_string` does not exist anywhere in this
project's own local OpenAlex extract (checked directly — absent from `authorships`, `works`,
and `work_topics`). By the time data reaches the local snapshot, the junk string has already
been collapsed into a clean-looking `institution_name`, indistinguishable locally from a real
affiliation — so this can't be filtered against current data at all.

**Two-part fix, not yet started**: (1) add `raw_affiliation_string` to the fields pulled on
the next OpenAlex snapshot re-conversion — same "opportunistic, do it next time the snapshot
is touched for another reason" framing as the existing `author_position` item (#15); (2) once
that data exists, add a Stage-1-style exclusion (alongside the existing `corrupt_authorship`
category) dropping an *authorship row's* institution (not necessarily the whole work) whenever
`raw_affiliation_string` matches a junk pattern — candidate regex, derived from the one
confirmed example plus standard academic-paper conventions for this category, not yet
validated against a real corpus: `^.{0,5}corresponding\s+author\b` (case-insensitive; the
leading `.{0,5}` catches garbled footnote-marker prefixes like the "K"), or contains `email:`
/ a bare email-address pattern with little other text, or "To whom correspondence should be
addressed."

### 19 — CLOSED 2026-09-02 — Consolidate pre-linking ORCID processing into `00b_enrich_orcid.py`; clean out the scattered local-ORCID-source mess
Found 2026-09-01 investigating the NO_ORCID population (4,895 ACIFs) via `orcid.db` — the
investigation itself surfaced a real code-organization problem, not just a data finding.
User's framing, taken as the design brief for this item: `00b_enrich_orcid.py` is *the* point
that should determine everything this project is going to know about ORCID records
**before** OAX linking — so it should be refactored to a clear, simple plan that does what
this session ended up doing by hand, rather than leaving that logic scattered across ad hoc
scripts. Any *post*-link ORCID work (using OAX-side evidence) is explicitly out of scope for
`00b_` — a separate, later concern, not to be folded in here.

**What's actually scattered right now, concretely:**
- `src/utils/orcid_bulk_lookup.py` already existed, querying `orcid_persons.parquet`/
  `orcid_affiliations.parquet` — confirmed this session to be a byte-faithful DuckDB-friendly
  conversion of `records_hq.json.gz` (see `/home/lc/s/orcid/convert_to_parquet.py`), covering
  the ~4.8M-person "HQ" subset (ROR-grounded employer/education, or a PubMed-indexed
  publication) with full aliases and dated employment/education history.
- This session's own `_search_bulk_db()`/`find_candidates_by_institution()` additions to
  `00b_enrich_orcid.py`/`orcid_bulk_lookup.py` (mid-session, before being told to stop) are
  scoped *only* to that same ~4.8M HQ parquet source.
- Separately, this session discovered and queried `orcid.db` (the Zenodo release's sqlite
  file, `/home/lc/s/orcid/orcid.db`) directly via ad hoc scratch scripts (DuckDB `ATTACH ...
  TYPE sqlite`) — a genuinely different, broader (17.15M person rows) but shallower (one
  name, one current `ror`, no aliases, no dated affiliation history) source, not touched by
  `orcid_bulk_lookup.py` at all.
- Also queried `records_hq.json.gz` directly via raw `gzip`/`json` in more scratch scripts
  (e.g. to pull Simon Kelly's and Brian Wilson's dated employment record) — duplicating
  exactly what `orcid_bulk_lookup.py`'s existing `find_candidates()` already returns via the
  parquet form of the identical data, just reached a different, uncommitted way.
- Every population-scale number reported this session (71,612 distinct name-candidates
  across the 4,895; the 2,882/4,895 name-match coverage; the 299 clean HEP-corroborated
  hits; the HEP-code → institution-name crosswalk join) was computed in throwaway scratch
  scripts against `orcid.db`, not as real, tested, reusable module code anywhere.

**The refactor, as directed:**
1. Convert `orcid.db` to parquet too (a **preprocessing** step, not a "util" — see the
   file-placement principle below), the same way `convert_to_parquet.py` already did for
   `records_hq.json.gz`. User's explicit reasoning: once both sources are parquet, DuckDB
   query speed is no longer a reason to hit `orcid.db` live via the sqlite attach — it only
   makes sense to keep both sources instead of one wherever their real, different tradeoffs
   (broader-but-shallower vs. narrower-but-richer) actually matter to a specific lookup.
2. Decide, explicitly, which source (or combination) each piece of `00b_enrich_orcid.py`'s
   logic should use — not leave it implicit or split across whichever script happened to
   write it: broad name+institution discovery probably wants the wider `orcid.db` population
   (catches cases like the institution-issued, uncurated accounts found this session — Xu Jia
   Wang, Jianxin Zhao, etc. — that the narrower HQ subset may or may not contain); the
   dated-employment/role corroboration step (the thing that turned "ambiguous" into "clearly
   this one" for Simon Kelly and Brian Wilson) wants the HQ parquet's `orcid_affiliations`
   table, keyed by the specific candidate ORCID once one is already in hand — not a second,
   duplicate raw-JSON read.
3. Rebuild `00b_enrich_orcid.py`'s pipeline around that decision as one clear, ordered plan
   (something like: raw ARC orcid → live-API-cache hits already on file → bulk-DB name match
   → institution-overlap tie-break using the ACIF's own HEP list (already crosswalked via
   `admin_orgs.csv`) → dated-employment corroboration by ORCID for anything still
   ambiguous), replacing the current mix of live-API-first logic, the narrowly-scoped bulk-DB
   addition, and everything this session did instead in scratch scripts.
4. Promote the real, working pieces of this session's scratch exploration into actual
   `src`/`src/utils` code once step 3's plan is settled — the HEP-code-to-institution-name
   join, the name-explode-and-match query, the institution-overlap tie-break, the
   ORCID-keyed employment lookup — rather than leaving them as one-off throwaway scripts that
   would need re-deriving from scratch next time.
5. Leave `widen_names_with_orcid_bulk_db()` (`awards_cif.py` — widens name *forms* for
   clusters that already have a resolved ORCID) alone unless step 3's plan naturally
   subsumes it; it's a different, narrower, already-working job from ORCID *discovery*.

**File-placement principle to apply throughout** (the user's own framing, to settle the
`src/` vs `src/utils/` question this session blurred): an API lookup/cache wrapper
(`orcid_client.py`-style — stateless, reusable, called from multiple places, produces no
persisted checkpoint of its own) is a genuine `src/utils/` **util**. A one-time or
occasional conversion of a raw external dump (a `.json.gz`, a `.db`) into this project's own
clean parquet form is a **preprocessor**, not a util — it belongs alongside this project's
other numbered `src/NN_*.py` preprocessing stages (in spirit like `00_extract_arc.py`/
`02_prepare_oax.py`), since it produces a persisted, checkable artifact the rest of the
pipeline depends on, the same way those do. A stateless query *function* that only ever
reads an already-prepared local parquet/table (no raw-source parsing, no network call) sits
in the middle and is reasonably a util either way — the user was explicit that this specific
distinction (SQL-against-already-local-data) wasn't the confusing part; the conversion step
was.

**2026-09-02 update — `OrcidProcessor` built as the consolidation target; `00b_enrich_orcid.py`
itself not yet rewired onto it (that's the remaining piece of this item).** The plan above
(step 1: convert `orcid.db` to parquet too, keep it alongside the HQ parquet) was superseded by
a better option found by actually checking the data first: `records.jsonl.gz` (the Zenodo
release's FULL population, 17.15M records) turns out to carry the exact same rich per-record
schema (aliases, dated employments/educations/memberships, works) that only the narrower ~4.8M
"HQ" subset used to have — confirmed via a direct full pass, not assumed. So there's no real
broad-vs-rich tradeoff to manage across two sources any more: `orcid.db` (shallow, 17.15M) and
`orcid_persons.parquet`/`orcid_affiliations.parquet` (rich, 4.8M) are BOTH superseded by one
new artifact, `/home/lc/s/orcid/orcid_bulk.parquet` (rich AND full-population, built via
`src/utils/orcid_processor.py::convert_bulk_dump()`) — `orcid.db` was deleted outright, and
`orcid_bulk_lookup.py`'s old HQ-only parquet pair is now dead code (not yet removed, see below).

Built, tested, verified against real cases (Simon Kelly UQ/Macquarie disambiguation, bare-initial
"W Cope" fallback — both reproduce this session's own by-hand findings exactly):
- `src/utils/orcid_processor.py` — `OrcidProcessor.discover()` (pre-link name matching against
  `orcid_bulk.parquet`, returns every candidate with full career `institution_names` attached,
  deliberately does NOT reduce to a winner itself) + `collapse_candidates()` (post-link
  `oax_candidates` pruning once an ACIF is already ORCID-resolved) + `OrcidRecord`/
  `AffiliationEntry` dataclasses + `get_or_fetch()` (cache-first live-record retrieval, `cache`/
  `fetcher` both injected so this module stays free of `requests`/`diskcache`/`config.settings`
  imports — the standalone-module constraint from this file's own "File-placement principle").
- `src/utils/orcid_processor_arc_adapter.py` — the project-specific glue: `arc_name_normalizer()`
  (injects `names.py`'s `HumanNameParser` as `OrcidProcessor`'s pluggable name_normalizer),
  `resolve_institution_overlap()` (the set-to-set institution reduction `discover()` deliberately
  leaves to the caller — this project's own version, against its HEP vocabulary), `get_record()`
  (wires `get_or_fetch()`'s `cache`/`fetcher` hooks to `orcid_client.py`'s existing
  `default_cache()`/`fetch_orcid_record()` — reuses that module's OAuth/HTTP/retry logic rather
  than a second copy of it, same one cache, `DISKCACHE_DIR/orcid_records_authenticated`, as
  every other ORCID consumer in this project).
- `tests/test_orcid_processor.py` (38 tests) + `tests/test_orcid_processor_arc_adapter.py`
  (9 tests), all against small synthetic fixtures — never the real 17.15M-row table or a live
  API call. 527/527 full suite passing.

Along the way, a real Unicode-normalization hardening pass landed in `names.py`/
`name_diacritic_variants.py` first (NFC/NFKC ingestion hygiene, zero-width character stripping,
a soft-hyphen substitution bug, a new `HumanNameParser` class as the sole real implementation of
this project's whole name-parsing chain, with both an ASCII-reduced key for genuine
spelling-convention bridging and a non-ASCII "raw" key so non-Latin-script/uncatalogued-diacritic
names aren't silently dropped) — triggered directly by `orcid_bulk.parquet`'s far greater
linguistic diversity than ARC/OAX's own more curated inputs. Fixed a real pre-existing given-name-
widening asymmetry bug in `00c_prepare_oax.py::_parse_name()` (only the family name was
diacritic-widened, not the given name) as part of the same pass, since every name-parsing call
site was being audited anyway. Full `00c`→`01`→`03`→`04` rerun (user's own hands) confirmed
zero regression against documented baselines throughout.

**2026-09-02 update — callers rewired, legacy source retired. Item 19 closed.**
`00b_enrich_orcid.py::_search_bulk_db()` now calls `OrcidProcessor.discover()` (a lazy
module-level singleton, `_get_orcid_proc()`, built with `arc_name_normalizer` so ORCID-side
matching keys get the same hardened parse as every other ARC-side comparison) instead of
`orcid_bulk_lookup.find_candidates()` — institution corroboration now goes through
`orcid_processor_arc_adapter.institution_matched_candidates()` (a new function, factored out of
`resolve_institution_overlap()` so both a caller wanting the raw matched list, to distinguish
"0 matches" from "2+, a real ambiguity," and a caller wanting the collapsed single winner share
one definition of "matched"). `awards_cif.py::widen_names_with_orcid_bulk_db()` now calls
`OrcidProcessor.lookup_by_orcid()` (new method — the keyed-lookup equivalent of the old
`orcid_bulk_lookup.fetch_by_orcid()`) instead of reading the old HQ parquet pair directly. Both
call sites' own function signatures are unchanged, so neither's existing tests needed
restructuring — only the monkeypatch target/shape in `tests/test_awards_cif.py`'s
`TestWidenNamesWithOrcidBulkDb` changed (dict return instead of a DataFrame, matching the new
method's shape); `tests/test_00b_enrich_orcid.py`'s `TestSearchOrcid` suite needed zero changes
and still passes unmodified against the real, larger `orcid_bulk.parquet` (confirmed: common
names like "John Smith" still correctly return 2+ ambiguous local candidates and fall through to
the mocked live-API path, exactly as before — the population only got bigger, the logic's
behavior didn't change). New tests: `OrcidProcessor.lookup_by_orcid()`
(`tests/test_orcid_processor.py`) and `institution_matched_candidates()`
(`tests/test_orcid_processor_arc_adapter.py`). Full suite: 535/535 passing.

`src/utils/orcid_bulk_lookup.py` deleted outright (`git rm`) — every caller confirmed migrated
first, zero remaining live imports (only historical docstring mentions of the old module name
remain, left as-is as project history). Its two data files
(`/home/lc/s/orcid/orcid_persons.parquet`, `/home/lc/s/orcid/orcid_affiliations.parquet`, the
~4.8M-record "HQ" subset) deleted as redundant with `orcid_bulk.parquet` (17.15M records, full
population, same rich per-record schema). Also deleted as fully superseded, upstream of that
pair: `/home/lc/s/orcid/convert_to_parquet.py` (the one-time conversion script whose only output
was the now-deleted parquet pair, and whose own input format — a single-JSON-object
`records_hq.json.gz` — had already been superseded by the JSON-Lines `records_hq.jsonl.gz`
earlier the same session) and `records_hq.jsonl.gz` itself (632MB — a strict subset of
`records.jsonl.gz`'s population with the identical schema, confirmed via direct inspection
before deleting). `schema.json` (the Zenodo dataset's own published record schema — reference
documentation, not tied to the dead conversion script) kept. The unrelated, much older
`ORCID_2023_10_activities`/`ORCID_2023_10_summaries`/`orcid-conversion-lib-*.jar` (a 146GB+ raw
ORCID XML dump from a separate, earlier investigation, dated 2023-10 — predates this project's
2026 sessions) were left untouched — out of scope for this cleanup, not something this
session's work made redundant.

Real coverage-scale consequence of the rewiring, not yet measured at full population scale:
`00b_enrich_orcid.py`'s no-ORCID search population (several thousand ACIFs/name-pairs) now
matches against 17.15M people instead of 4.8M, with hardened ARC-parity name keys instead of a
bare parse — a real re-run against the current NO_ORCID population (to see how many previously
`not_found`/`too_common`/`live_api`-sourced rows now resolve via the richer local source before
ever reaching the live API) has not been done this session; `search_cache`'s existing entries
are keyed on `(first_name, family_name)` only, so a full re-run needs `--update-name` per pair or
a fresh cache to actually re-attempt anything already cached under the old source.

---

## Verification
Items 1 and 2: no pipeline rerun was needed since both turned out to already be correctly
resolved in current output — verified directly by querying `awards_cif_arc_only.parquet`.
`manual_orcids.csv` gained one new row (`DP0210446_RobertGilbert`); CLAUDE.md's "Next
Priority" and "Known Issues in 02 Output" sections were corrected to stop describing closed
issues as open. Remaining items still need their own individual verification once
undertaken (each already describes, in CLAUDE.md, what a clean rerun/test-suite pass looks
like for that specific fix).
