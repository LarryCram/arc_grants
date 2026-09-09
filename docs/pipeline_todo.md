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
14. ~~Locate and fix every instance of collapsing a name-form set to one scalar before matching~~ — **the ARC-internal blocking half CLOSED 2026-09-08.** `cluster_items()` now has a given-name-side set-overlap blocking rule (mirroring the family-name-side one already there) plus its own comparison level — real nickname cases (Yingzi/Jenny Wang and others) confirmed merging correctly at full population scale. A separate, pre-existing bug found and fixed along the way: the family-name-side comparison level this item's earlier pass claimed to have added was never actually there. The bigger original remainder — a persisted, unnested per-grant full-name-key table — is now understood to be a trivial mechanical export (not built yet, but no longer "the largest piece"); see the item's own entry for the full account.
15. **Extract `author_position`** — purely opportunistic, do only when the OpenAlex snapshot is next re-converted for an unrelated reason
16. **Work through the `accuracy_checks.md` checklist** (largest, most multi-part, least urgent — a standing backlog, not a single task)
17. **HEP-affiliation-history candidate-pool pruning for common-name mega-pools** — real, partial win, not built (see its own entry below)
18. **Extract `raw_affiliation_string` on the next OpenAlex snapshot conversion, then filter junk-matched authorship institutions** — a real, confirmed OpenAlex-side data-quality bug found via two dossier examples (see its own entry below); needs new data before it's actionable, so opportunistic like the existing `author_position` item, not urgent standalone
19. **Consolidate pre-linking ORCID processing into `00b_enrich_orcid.py`; clean out the scattered/duplicated local-ORCID-source code this session's exploration left behind** — high priority, blocks trusting any further ORCID enrichment work until done
20. **Audit `AwardsCIF.coawardees` for a co-awardee key colliding with the ACIF's own name** — deliberately not filtered out at construction (a collision is itself a useful merge-candidate signal, not an error); needs a standing check to actually surface and review these, not just leave them sitting unexamined in the data
21. **`compute_gap_candidates()`'s `orcid_incompat` is an unconditional veto — loosen it, and add a deterministic announcement/current-snapshot auto-merge** — two confirmed real cases (Restubog, Craig) currently invisible to review because of this; see its own entry below
22. ~~Non-circular ARC+OAX_AU vs ORCID-bulk name-frequency reference~~ — **DEMOTED 2026-09-07,
    not a real todo.** User's own correction: this was a design discussed in passing while
    investigating a different finding, never actually committed to as a task — recorded here
    with more weight than it ever had. Kept below only as a shrunk historical note (what was
    discussed, why it was set aside), not as a live, sequenced item.
23. **Rerun the `FetchOrcid` NO_ORCID scan (stale) and run the HAS_ORCID audit for the first time at full population scale** — the tooling (`src/utils/fetch_orcid.py`) is built and fixed; the actual population-scale numbers are either stale (NO_ORCID) or never computed (HAS_ORCID). See its own entry below.
24. **Diagnose why a recorded/matched orcid doesn't resolve to the correct OAX `author_idx`** — two of four candidate causes closed empirically this session; the real remaining work reframes into exactly two post-link questions (single-candidate-link reliability; multi-candidate disambiguation, which is substantially already built but short-circuits around ORCID). See its own entry below.
25. **Build a generic, name-agnostic set-comparison utility for Splink evidence** (blocking/scoring/TF-adjustment over any `{value: count}` set, not just names) — deliberately deferred, not part of `src/utils/name_set_processing.py`; see its own entry below
26. ~~Refactor `orcid_processor.py` to depend on `names.py`/`ParsedName` directly; drop `NameForms` and the adapter layer~~ — **DONE 2026-09-08.** `NameForms`/`default_name_normalizer()`/`all_full_name_keys()` removed outright; `orcid_processor_arc_adapter.py`'s `arc_name_normalizer()` removed; `orcid_bulk.parquet` rebuilt from the raw 17.15M-record snapshot on the new logic. Two real, previously-hidden bugs found and fixed along the way; see its own entry below.
27. ~~`HumanNameParser.parse()` silently drops a quoted/parenthesized nickname~~ — **DONE 2026-09-08.** Landed as a new `nickname_tokens` field (kept separate from `given_tokens`, not folded in as originally specified) plus a `full_name_keys` field that actually consumes it. The originally-specified 3-case incorporation rule was tested against real cases and replaced by a single uniform rule; see its own entry below for what changed and why.
28. **`04_resolve_links.py`'s dedup/disambiguation checks are ad hoc booleans — move toward rarity-weighted ("value_counts") evidence with an explicit veto-in/veto-out framework** — 2026-09-09 status: OAX-side dedup (`_oax_names_compat()`) already fixed (real `full_name_keys` field, comparison-time bare-initial filtering); the disambiguation cascade itself confirmed structurally broken via 7 traced cases (two distinct, confirmed defects in `_names_compat()`), archived, and a rebuild started as `FilterCandidates` (`src/04_filter_candidates.py`) — `orcid_veto()` implemented, `fd_compare()`/`score()`/`resolve()` still stubs. See its own entry below and CLAUDE.md's matching dated session entry.

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

**What `is_suspicious_for2020()`/`resolution_status` actually affects, checked directly
(2026-09-07)**: grepped every read site of `resolution_status` in `src/`/`analysis/`/`tests/`.
It is written in exactly one place (`compute_reliability()`, `awards_cif.py`) and read in
exactly one place downstream of that: `01a_diagnose.py`'s own console report and its A2/A3
cross-check count. Nothing in `03_link_arc_oax.py`, `04_resolve_links.py`,
`oeuvre_build.py`, `dossier.py`/`dossier_build.py`, or any `analysis/*.py` script reads it at
all. So this flag has **no downstream computational effect** on linking, oeuvre-building, or
analysis output — its only real consequence is whether a human gets prompted to review a
cluster (via `01a_diagnose.py`'s report, feeding `manual_splits.csv`/
`manual_confirmed_not_suspicious.csv`). Getting `ACCEPTABLE_DIVISION_PAIRS` wrong therefore
costs *review burden* (too many or too few clusters surfaced for a human to look at), not a
silently wrong pipeline output by itself — a false negative (a real wrongful merge that stays
`RESOLVED`) only becomes a real problem if it's also never caught by any of this project's
other checks (B1 ORCID-collision, `4u` under-merge review, manual case-by-case review), and a
false positive just means one more cluster a human has to glance at and confirm fine.

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

### 14 — Locate and fix every instance of collapsing a name-form SET to one scalar before
matching — RESCOPED 2026-09-07 (twice), scope corrected and narrowed both times

**User's own framing, verbatim, is the actual task**: "everything you found that is an
example of selecting one item from an object that is a set must be located and fixed."
Everything below is the located inventory, confirmed by direct code reading (not grepped-and-
assumed), with two self-corrections along the way recorded plainly rather than smoothed over.

**Search method, so a future pass can extend it rather than repeat it from scratch**: the
naive first search (grep for the literal `_main` name suffix) was too narrow — it only finds
things *named* like the bug, not things that *inherit* it. Broadened twice: (1) grep the root
operation (`max_by_len`/`max(..., key=len)`) instead of a naming convention; (2) grep every
*other* mechanism that can collapse a set to one element — `sorted(...)[0/-1]`, bare
`name[0]`/`[-1]` indexing, `.iloc[0]`/`.head(1)`, `next(iter(...))`, `min(key=len)`,
`.drop_duplicates()`/`.groupby().first()`, and SQL-side `MAX()`/`ANY_VALUE()`/`arg_max()`/
`FIRST_VALUE()` — across the *whole* `src/`+`analysis/` tree, not just a curated file list.

**Confirmed real, needs fixing:**
1. **`family_name_main = max(family_names, key=len)`** — three independent sites:
   `names.py:249` (inside `HumanNameParser.parse()`), `03_link_arc_oax.py:151` (ARC-side
   Splink prep), `00c_prepare_oax.py:364` (OAX-side Splink prep).
2. **`full_name_key`'s construction — the deepest instance, and the one with the biggest
   blast radius.** `03_link_arc_oax.py:153`, ARC side: `df["full_names"].apply(max_by_len)
   .apply(parse_given)` — picks the single longest *raw full-name string* from an ACIF's
   whole recorded set, and *only that one string* ever gets parsed into
   `first_name`/`middle_name`/`first_compound`/`first_initial`/`middle_initial`. Every other
   recorded full-name variant is discarded before the given-name side of the ARC↔OAX link
   even starts — this feeds the entire given-name comparison cascade and both middle-initial
   cross-blocking rules in that script, not just one field. `00c_prepare_oax.py:354-361`,
   OAX side: a different flavour of the same anti-pattern — `_canonical()` picks the single
   longest *token* from an already-exploded given-name-token array, with no first/middle
   distinction.
3. **`dossier_build.py:320`**: `preferred_name = full_names[0]` — display-only (a Dossier's
   header line), no matching-logic consequence, but the same pattern: `full_names` has no
   documented or enforced ordering, so "preferred" really means "whichever form happened to
   be listed first."

**Checked and confirmed FALSE ALARMS — real code was read, not assumed, before ruling these
out** (kept here so they aren't re-flagged and re-investigated next time):
- `awards_cif.py:1495` (`next(iter(found))`) — guarded by `if len(found) != 1: continue`
  immediately above; `found` is always a singleton set when this runs.
- Every `.iloc[0]` in `04_resolve_links.py`, and `awards_cif.py:2268-2271`
  (`grow.iloc[0][...]` in `cluster_detail_data()`) — all either guarded by a prior
  `len(...) == 1` check, or a genuine single-row-per-key lookup (grant-level facts have one
  row per `grant_code` in `grants_flat.parquet`; not the same ambiguity as the `inst_arr`
  announcement/current split, which is two *columns* on one row, not two rows).
- `names.py:254,267,282` and `awards_cif.py:728` — position-based (first-listed, deliberately
  ordered token) or explicit fallback reads of the *already-fixed* `HumanNameParser` output,
  not new arbitrary collapses.
- `awards_cif.py:1938` (`_oax_names_compat`'s `full_firsts[0][:3]`) — a uniformity check ("do
  all candidates share the same 3-char prefix as each other"); which element serves as the
  reference point doesn't change the result.
- `01a_diagnose.py:313-314` (`.mode().iloc[0]`) — a defensible "most common value" choice for
  a diagnostic report column only, not matching logic.
- Every SQL `MAX()`/`drop_duplicates()` hit in `analysis/*.py` — legitimate aggregate
  statistics (h_index, year ranges, a diagnostic dedup count), not identity collapses.

**Two self-corrections, made directly rather than left standing — both matter for scoping
the actual fix:**

1. **`_first_name_canonical()` (`awards_cif.py:376-383`) is dead code, not a live bug.**
   Grepped every call site: it's defined but never invoked anywhere. It was wrongly flagged
   in an earlier pass of this same item as "the ARC-internal cluster-level sibling that never
   got the first-vs-middle fix, feeding `cluster_items()`'s own blocking key" — that claim was
   never actually verified before being written down. `AwardCIFItem.first_name_canonical`
   (the per-*item* field that's actually used) is built at `awards_cif.py:727` directly from
   `parsed.first_name_canonical` — the already-fixed `HumanNameParser` output — so this path
   was never broken.
2. **`AwardsCIF.full_name_key` (the cluster-level field) is not built via `max_by_len()`
   either, and its design is already correct.** Read `_build_awards_cif()` directly
   (`awards_cif.py:802-858`): `full_names`/`first_names`/`family_names` are all
   `sorted({...})` — genuine full sets already, matching exactly what this item is asking
   for, no scalar collapse at the `AwardsCIF` level at all. `full_name_key` itself is
   `fnk_counts.most_common(1)[0][0]` — the **modal** (most-common) form among the cluster's
   own items, tie-broken deterministically by `unique_id` order — a meaningfully more robust
   choice than "longest string wins" (a contaminant would need to appear *more often* than
   the real name to win a mode, not just be longer once).

**So the real, confirmed bug in #2 above is narrower and more specific than "family_name_main
is broken": `03_link_arc_oax.py::_prep_arc()` (and its OAX-side counterpart) don't use
`awards_cif_arc_only.parquet`'s own good, already-computed, already-persisted `full_name_key`
at all — they re-derive a worse one from scratch, via `max_by_len()`, straight from the raw
`family_names`/`full_names` set columns, discarding a better computation that's sitting right
there unused.** This also shrinks the earlier "128 occurrences across 14 files, touches
everything" framing — most of those 128 occurrences are either the already-fine `AwardsCIF`
set fields being read, or dead/false-alarm code; the real fix is localized to the Splink
prep/comparison layer in `03_link_arc_oax.py` and `00c_prepare_oax.py`, not a sweeping
14-file rewrite.

**IMPLEMENTED 2026-09-07/08 (renamed `00c_prepare_oax.py` → `02_prepare_oax.py` in the same
pass, per direct user request, for pipeline-sequence-number consistency — mechanical rename,
all references updated: `01_prepare_arc.py`'s importlib path, `run_pipeline.sh`, and every
comment/docstring referencing the old name):**

1. **`AwardsCIF.family_name_main`** — new field, modal (`Counter.most_common(1)` over items,
   same design as the existing `full_name_key`), computed in `_build_awards_cif()`, persisted/
   loaded in `persist_awards_cif()`/`load_awards_cif()`. `03_link_arc_oax.py::_prep_arc()` no
   longer re-derives `family_name_main` via `max_by_len()` at all — reads this column directly.
2. **OAX-side `family_name_main`** (`02_prepare_oax.py`) — now prefers `family_names_display`
   (OpenAlex's own curated form) over `family_names_alt`, not `max_by_len()` over the flatly-
   merged, alternates-contaminated pool.
3. **OAX-side `first_name_canonical`** — switched from `max(full, key=len)` to `full[0]`
   (first element, in display-name-priority order) — the same position-not-length fix already
   applied to `names.py::HumanNameParser.parse()`, extended here.
4. **`cluster_items()` (ARC-internal Splink dedupe) — a real, previously-unverified gap found
   while implementing this, not just a documentation fix.** Unlike its already-fixed
   `03_link_arc_oax.py` sibling, this comparison had *neither* a set-overlap blocking rule nor
   a matching scoring level — confirmed by actually reading its full `SettingsCreator` block
   (an earlier, wrong claim about this file's state was corrected before landing here). Added
   both, mirroring `03_link_arc_oax.py`'s 2026-08-25 pattern exactly. Verified via a controlled
   A/B rerun (rule enabled vs. disabled) that this addition is *not* the cause of a separately-
   discovered, pre-existing regression (see below) — identical output either way.
5. `dossier_build.py`'s `preferred_name` — was `full_names[0]` (arbitrary, alphabetical-sort
   artifact); now `max(full_names, key=len)` (longest recorded form) — a deliberate, low-stakes,
   *display-only* choice (unlike matching, where "longest wins" was the bug).

**Empirical result, `verify_family_name_blocking.py::empirical_mismatch_check()`** (against
8,362 ORCID-confirmed ARC↔OAX ground-truth pairs — a real regression-check tool that already
existed, itself needed a fix along the way: it was re-deriving `family_name_main` locally via
its own `max(key=len)` instead of reading the pipeline's real column, silently re-introducing
the exact bug being measured):
- **Mismatch rate: 28.05% → 1.72%** — a ~16x reduction. This is the population that would have
  been *structurally invisible* to blocking without a recorded ORCID to force-add the pair; the
  fix makes a large share of it correctly reachable by name alone.
- Full-pipeline effect (`02`→`01`→`03`→`04` rerun): ARC persons with ≥1 high-confidence OAX
  candidate rose to **99.1%** (up from the documented 98.4% baseline); final resolved rate
  **98.5%** (22,550/22,885, up from 98.4%). 443/443 tests passing throughout.
- Residual 1.72% (144 pairs) is a genuinely different, harder category — compound/double-
  barreled surnames (which half is "the surname"), spacing conventions, apparent real name
  changes — not the OpenAlex-alternates contamination this fix targeted. Not pursued further
  here; a reasonable stopping point, not an oversight.

**A separate, pre-existing regression found and deliberately NOT fixed as part of this item**:
`resolution_status` came back 86 UNRESOLVED (was 0 in the last documented baseline) after the
`01_prepare_arc.py` rerun. Confirmed via a controlled A/B test (disabling the new
`cluster_items()` blocking rule and rerunning) that this is **unrelated to any change in this
item** — identical 86 with the rule on or off. Root cause traced for one sample case
(`DP0209363_DAllen`, 8 grants, FOR-code spread spanning medical physiology/banking-finance/
econometrics/zoology/computer vision — an implausible single career): the ARC-internal Splink
dedupe had *already* merged multiple different real "David Allen"s into one cluster (confirmed
via `provenance`: the `splink_cluster` event predates the `enriched_orcid` event), and
`is_suspicious_for2020()` correctly flagged it — working as designed, not a pipeline bug. This
is a backlog of common-name-collision cases needing the same manual-review treatment (via
`manual_splits.csv`/`manual_splits_by_grant.csv`) as the many similar cases already documented
throughout this project's history (Wei Wang, Gilbert, Young, etc.) — tracked as a new,
separate item, not folded into this one.

**Given-name-side blocking gap CLOSED 2026-09-08** (the piece the discussion above still left
open). `cluster_items()`'s ARC-internal Splink dedupe had a family-name-side set-overlap
blocking rule (2026-09-07) but no given-name-side equivalent — a real nickname or informal
name ("Yingzi (Jenny) Wang") changes `first_initial` itself, not just the spelling, so neither
the primary `family_name_main+first_initial` rule nor the family-name-overlap rule (still
anchored on `first_initial`) could ever reach it.

Two designs were tried and measured against the real ~64,830-item population before picking
one, not assumed:
- **Unanchored**, testing `ParsedName.full_name_keys` (given+nickname × family, built
  2026-09-08, see item #27) directly via `list_has_any(l.full_name_keys, r.full_name_keys)`
  with no equality condition at all — logically the most complete option (uses the full
  `family_names` set via the cross-product, not the `family_name_main` scalar), but measured
  at **241.2s** via `deterministic_link()` (not the 25.5s Splink's own `blocking_analysis` tool
  first suggested — that tool's fast path turned out not to be representative of the real
  execution cost). Sampling its own output also surfaced a real, confirmed false positive:
  `Ying Zhu`/`Huai-Yong Zhu` blocked together purely because "Huai-Yong" tokenizes to include a
  bare `y`, coincidentally matching "Ying"'s own initial — nothing to do with either person's
  actual given name.
- **Anchored + multichar-filtered** (shipped): `family_name_main = r.family_name_main AND
  list_has_any(l.given_multichar, r.given_multichar)`, where `given_multichar` is
  `given_tokens + nickname_tokens` with single-character tokens filtered out (the same
  bare-initial-collision guard `03_link_arc_oax.py`'s own `first_names_multichar` already
  uses). Measured at **0.5s** — anchoring on `family_name_main` lets DuckDB hash-join instead of
  scanning every pair. Confirmed directly: the `Ying Zhu`/`Huai-Yong Zhu` false positive is
  excluded; a real-data sample of 15 blocked pairs was all genuine matches.

A matching comparison level was added to `first_name_canonical`'s comparison (`list_has_any
(given_multichar_l, given_multichar_r)`, hand-set `m_probability=0.5`/`u_probability=0.02`,
mirroring `03_link_arc_oax.py`'s own given-name set-overlap level exactly) — without it, a pair
reaching Splink only via the new rule would score as a mismatch anyway, defeating the fix.

**A separate, pre-existing bug found while adding this**: the family-name-side comparison
level this same item's 2026-09-07 pass documented adding ("Added both, mirroring
`03_link_arc_oax.py`'s ... pattern exactly") was never actually there — `family_name_main`'s
comparison had only `NullLevel`/`ExactMatchLevel`/`ElseLevel`, no `CustomLevel` for
`list_has_any(family_names_l, family_names_r)` at all. So the family-name blocking rule had
been generating candidate pairs since 2026-09-07 that then scored as mismatches regardless —
the earlier fix was half-applied and never caught. Fixed alongside the given-name addition,
mirroring `03_link_arc_oax.py`'s "Set overlap (shared spelling variant)" level exactly.

**Verified against real data, not just the sample**: full `cluster_items()` run, 64,830 items →
22,791 clusters in 15.1s (load + full Splink dedupe_only: blocking, EM training, predict,
clustering) — no errors, no meaningful slowdown from either fix. Confirmed real, previously
unreachable merges: `Jenny Yingzi Wang`/`Yingzi (Jenny) Wang` (the exact motivating case, first
initials 'j'/'y'), `Drew Dawson`/`William (Drew) Dawson` (11-item cluster), `Chunhui Yang`/
`Richard (Chunhui) Yang`, `Alison (Sal) Humphreys`/`Sal (Alison) Humphreys` (reciprocal
nicknames both directions), `Elizabeth (Libby) Lester`/`Libby Lester`, `Cheng (vincent) Lee`/
`Vincent Lee`, `Huong Giang (Lily) Nguyen`/`Lily Nguyen`, `Anthony (Tony) Vassallo`/`Tony
Vassallo` — none reachable before this fix. Full test suite 543/543 passing throughout.

**Known, accepted residual gap** (not chased further): a pair needing *both* a family-name
scalar mismatch (Schroder/Schroeder) *and* a given-name/nickname mismatch simultaneously would
still slip through both set-overlap rules — each one's own anchor (the *other* field's exact
scalar) breaks under the other rule's own failure mode. Judged an acceptably rare double
coincidence rather than something worth an unanchored, expensive rule to also catch.

**Also confirmed, a genuinely different and harder boundary, not something this fix
touches**: none of this helps when the two name-forms sit in two *separate* records with no
textual connection at all (e.g. one grant plainly records "Yingzi Wang", a wholly separate
grant plainly records "Jenny Wang", neither ever mentioning the other) — that's
information-theoretically identical to matching two unrelated strangers by name alone, and no
blocking rule, however built, could ever close it. Already correctly documented elsewhere in
this project as manual-review-only, not automatable — this session's work only closes the case
where one occurrence's own recorded string already contains both forms.

**`AwardsCIF.first_names` (feeding `03_link_arc_oax.py`'s own given-name set-overlap rule)
was deliberately NOT touched this pass** — `_name_forms()` still doesn't fold `nickname_tokens`
into the ARC-internal item-level `first_names` field, so nicknames don't yet widen the
cluster-level aggregate that feeds ARC↔OAX linking. Checked directly and found lower-priority
than it first looked: `03_link_arc_oax.py` already has its own given-name set-overlap rule
(`first_names_multichar`), so once `first_names` is fixed at the source, that file needs zero
code changes of its own to benefit — this is a smaller follow-on, not a new design question,
deferred rather than bundled into this pass.

**The bigger original remainder — a persisted, unnested "one row per (grant, full_name_key)"
ARC-side table — is now understood to be far cheaper to build than this item originally
assumed**, once item #26/#27's work landed: `AwardCIFItem.parsed.full_name_keys` (every
combinatorial key for one occurrence, nickname included) already sits on every item alongside
`item.grant_code`, computed live by `load_award_cif_items()` — nothing new to design, just an
export loop, not yet written. Not needed for anything currently planned (the blocking fix above
uses `given_multichar`/`family_names` directly, not this table), so still deferred, but no
longer "the largest piece" of this item.

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

### 20 — Audit `AwardsCIF.coawardees` for self-collisions (candidate merge signal)

**Which pipeline phase this is** (clarifying an ambiguity — this project has TWO Splink runs,
`cluster_items()`'s ARC-internal dedupe and `03_link_arc_oax.py`'s ARC↔OAX link, and calling
either "pre-Splink" without saying which is genuinely unclear): this operates on `AwardsCIF`
objects already produced by `01_prepare_arc.py`'s `build_arc_only_population()` chain — i.e.
*after* the ARC-internal Splink dedupe has already run and been refined, but *before*
`03_link_arc_oax.py`'s ARC↔OAX Splink link step ever sees the population. It's an ARC-internal
candidate-merge signal, not something that touches OAX data or the link step at all.

**Update, 2026-09-06**: the standing check this item asked for now exists —
`find_coawardee_self_collisions(clusters)` (`src/utils/awards_cif.py`, committed `ddcc19d`)
does exactly what's described below (every ACIF's own item-level `full_name_key`/
`full_name_key_raw` checked against every `coawardees` entry's key). **Not yet run at
population scale or reviewed** — what remains of this item is running it across the full
non-excluded population and reviewing whatever it surfaces, the same discipline as
`gap_candidates`.

Built 2026-09-04 (`compute_coawardees()`, `src/utils/awards_cif.py`): every ACIF now carries
`coawardees` — every other investigator on any grant it holds, keyed by their own parsed
`full_name_key`/`full_name_key_raw`, with a count of how many shared grants. Found by hand while
building this: `DP0665337_JocelynCraig`/`DP0665337_JocelynLynCraig` and
`DP0984209_SimonRestubog`/`DP130104138_SimonLloydRestubog` — pairs where one ACIF's own name
collides with a co-awardee entry on the *other* ACIF's grants. Checked individually (not assumed):
Craig looks like a real same-person split (Craig's 4 grants are a strict subset of Lyn Craig's 10;
Lyn Craig's own ORCID self-reports as "Lyn Craig", already present in that cluster's own
`full_names`). **Restubog update, 2026-09-05 — corrected, this entry was itself stale for a few
turns**: the "no subset relationship, ORCIDs equally plausible" verdict above was wrong, caught by
direct user challenge rather than by rechecking on my own initiative. Checked
`investigators_raw.parquet` for the shared grant, `DP130104138`: one row `inv_source='announcement'`
("Simon Lloyd Restubog"), one row `inv_source='current'` ("Simon Restubog"), same `role_code`.
That's the same structural pattern already named elsewhere in this file for `LP0211723_DTNguyen`
— one real investigator slot, name changed between ARC's own snapshots, not two co-investigators.
So Restubog is now also a likely same-person split, by a *different*, more mechanical signature
than Craig's (a literal shared-grant snapshot pair, not a subset-of-grants pattern) — see item #21.

This was found by one manual pass over four clusters. Needed: a real, standing check across the
whole population — for every ACIF, does any `coawardees` entry's key match that ACIF's own
`full_name_key`/`full_name_key_raw`? — surfaced as a reviewable list (same treatment as
`gap_candidates`), not auto-resolved. Each hit needs the same individual check as Craig/Restubog
(subset-of-grants shape, shared-grant announcement/current snapshot pairs, each side's own ORCID
record if present) before treating it as anything more than a candidate.

**Craig case strengthened, 2026-09-05** — the "Jocelyn Craig" side's own enrichment ORCID
(`0000-0002-8288-3307`) was checked directly against a live OpenAlex author record
(user-supplied): it resolves to **"Jocelyn E. Craig," Southern Cross University, affiliated
2013–2015, works_count=2** — zero institutional or temporal overlap with `DP0665337_JocelynCraig`'s
actual grants (UNSW/ANU, 2006–2010). Confirms this ORCID is a wrong, coincidental-name-match
enrichment hit (same failure class as the already-documented `LP0347702_JMcDonald` case), not
evidence of a second real person — strengthening, not just repeating, the original "looks like a
real same-person split" verdict. `0000-0001-9723-7255` ("Lyn Craig," University of Melbourne,
matching the other cluster's own later grants exactly) is the real ORCID for the merged person.
Not yet applied to `manual_merges.csv`/`enrichment_blocklist.csv` — still pending the item #21 fix
(or a direct manual entry, if that's applied first).

### 21 — `orcid_incompat` unconditional veto in `compute_gap_candidates()`; deterministic
announcement/current-snapshot auto-merge

**Which pipeline phase**: same as item #20 — `compute_gap_candidates()` is one of the
ARC-only steps in `01_prepare_arc.py`'s `build_arc_only_population()` chain, so this runs
after the ARC-internal Splink dedupe has already produced and refined the ACIF population,
and before `03_link_arc_oax.py`'s ARC↔OAX Splink link step. Purely an ARC-internal
under-merge-detection mechanism; it never touches OAX data.

Found 2026-09-05, directly motivated by the Restubog/Craig cases in item #20 above.
`compute_gap_candidates()`'s incompatibility test (`src/utils/awards_cif.py`, `orcid_incompat`)
is an unconditional veto joined by `or` with the other checks:

```python
orcid_incompat = (
    len(c1.orcids) > 0 and len(c2.orcids) > 0
    and not set(c1.orcids) & set(c2.orcids)
)
if name_incompat or div_incompat or orcid_incompat or scheme_incompat:
    n_incompat += 1   # pair is dropped, never enters gap_candidates
```

If both sides have a non-overlapping ORCID, the pair is excluded regardless of name compatibility
or any corroborating evidence — structurally invisible to review, not merely low-priority. Both
Restubog and Craig are real, confirmed instances of exactly this: name-compatible pairs, each with
its own (enrichment-derived, not raw-ARC) ORCID, that this check silently drops before a human
ever sees them.

**Two-part fix, not yet built:**

1. **Deterministic auto-merge for the strong, mechanical signal** (mirrors `merge_by_orcid()`'s
   own unconditional-merge pattern, not a review-and-decide one): two clusters holding items that
   share a `grant_code`, where one item has `inv_source='announcement'` and the other
   `'current'` — the exact Restubog/DTNguyen shape. This is close enough to certain (the
   alternative — two different real people, one an announcement-only ghost and one a
   current-only ghost, on the identical single grant — is implausible) that it doesn't need human
   review the way every other merge type in this file does. Should log its own `record_event()`
   (e.g. `"announcement_current_pair"`, naming the shared `grant_code`) for auditability. The
   merge itself should go through the standard `_merge_awards_cifs()` item-union path, not a
   custom one — that machinery already re-aggregates `full_names`/`first_names`/`family_names`
   from the union of items, so both name-forms (e.g. "Simon Restubog" and "Simon Lloyd Restubog")
   are preserved automatically, and `n_grants` already counts distinct `grant_code`, not items, so
   the shared grant doesn't get double-counted. Nothing new needed there — confirmed by tracing
   both mechanisms directly, not assumed.
2. **Loosen `orcid_incompat` for the weaker, corroboration-dependent signal** (Craig's shape —
   no shared-grant snapshot pair, but a subset-of-grants relationship plus the target ACIF's real
   name already sitting in the *other* cluster's `full_names`): let a name-compatible,
   ORCID-conflicting pair through to `gap_candidates` for human review — never auto-merge — when
   corroborated by `coawardees` overlap, institution overlap, or a subset-of-grants relationship.
   **Explicit caution, from evidence generated in this project's own session history**: this
   corroboration combination (name-compatible + shared/overlapping grants) gave *opposite* correct
   answers on Restubog and Craig when first tested — Restubog turned out to need the sharper,
   mechanical announcement/current test above, not this one. So this path is for surfacing a
   candidate for review, exactly like every other `gap_candidates` entry, not for resolving one.

**2026-09-05 update**: Craig's own external evidence got stronger (see item #20's update — the
"Jocelyn Craig" side's enrichment ORCID is now confirmed, via a live OpenAlex lookup, to belong to
an unrelated real person), but neither part of the two-part fix above has been built yet. What
*was* built this session is general-purpose supporting tooling (`FetchOrcid`, item #23) that makes
part 2's corroboration-gathering (name-search, AU/HEP signal, middle-name signal) cheap to run —
it doesn't implement the fix itself, and is deliberately not wired into `compute_gap_candidates()`
or any other pre-Splink stage (same "apply only as a post-clustering promotion" principle as
`apply_enriched_orcids()`).

### 22 — [DEMOTED, background only] Non-circular ARC+OAX_AU vs ORCID-bulk name-frequency
reference — a discussion, not a committed task

**Demoted 2026-09-07**: per direct user correction, this was never actually agreed to as a
task — it surfaced while investigating why 791/4,891 NO_ORCID ACIFs have no name-rarity value
at all (`full_name_key` absent from `oax_tf_full_name.parquet`), and the design below was
sketched out loud in that moment, not committed to. Kept here only as a shrunk note in case the
underlying gap (no rarity value for those 791 names) gets picked up again later — not as a live
item, and not sequenced.

The gist of what was discussed: don't fix the gap by folding ARC's own names into the same
population used to judge how rare those names are (circular — a name existing in exactly one
ACIF and nowhere else would get its rarity computed from itself). Instead, build the reference
from ARC+OAX names matched against `orcid_bulk.parquet` (external to both, no circularity),
using every normalized name-form on both sides, not one scalar pick per name.

**One factual correction to the record while demoting this**: this note used to quote
`arc_name_normalizer()` as `NameForms(p.given_tokens, p.family_name_main,
p.first_name_canonical, p.full_name_key)` with no `family_names` — that quote is now stale.
The 2026-09-06 fix (commit `ddcc19d`) changed it to pass `family_names=p.family_names` through
too, exactly the "set, not scalar" principle this discussion was gesturing at.

### 23 — `FetchOrcid` built (`src/utils/fetch_orcid.py`); NO_ORCID scan is now stale, HAS_ORCID
audit never run at scale

Built 2026-09-05 on direct instruction ("code FetchOrcid as a class without planning"). A
standalone class composed on top of `OrcidProcessor`/`orcid_client` (reuses, doesn't duplicate):
`search_orcid(parsed: ParsedName)` — multi-form name search against `orcid_bulk.parquet` (every
given×family combination, ASCII-reduced and raw forms unioned, not either/or), `orcid_fetch_short()`
— local bulk-table row lookup, `orcid_fetch_long()` — cache-or-live-API full `/record`
(`orcid_processor_arc_adapter.get_record()`, no new cache). Deliberately not wired into anything
upstream of Splink, same "apply after clustering" principle as `apply_enriched_orcids()`.

**Performance**: `_ensure_table()` materializes `orcid_bulk.parquet` into an in-memory DuckDB temp
table once per instance instead of a fresh `read_parquet()` scan per call — measured 640ms/cluster
(52 min for the full 4,891-cluster NO_ORCID population) down to ~90-107ms/cluster (~9 min).

**Selectivity added, both real, both partial**:
- `au_signal` — ORCID's self-reported `countries` field (13% filled population-wide) OR an exact
  match against one of ARC's 42 HEP institution names (`admin_orgs.csv`). Real payoff: "L. Craig"
  (44 same-initial candidates worldwide) → 2 with `au_only=True`.
- `middle_match`/`require_middle_match` — a second, independent narrowing dimension for exactly
  what `au_signal` can't help with (two same-surname, same-first-initial, both-Australian
  candidates). Real payoff: "David Craig" (17 candidates) → 1 (`David L. Craig`) once a middle
  initial is known and required. Semantics matter here, and were wrong on first build: excludes
  only a genuine CONTRADICTION (both sides have a middle name and they differ), not mere absence
  (a candidate whose own record never had a middle name entered is kept, not discarded) — fixed
  2026-09-05 by re-parsing each candidate's own name for its own `middle_tokens` rather than
  testing against the flat, undifferentiated `given_tokens` bag.

**Two real bugs found and fixed via testing this session, both in `search_orcid()`**:
1. **Short-circuit**: used to run the broader `family_name_main`+initial fallback only when the
   exact `full_name_key` match found nothing — so one spurious exact match (e.g. someone's own
   canonical name really does reduce to a bare "d_craig") could silently hide every candidate the
   fallback would have found. Fixed: both passes always run, results unioned (deduped by orcid) —
   confirmed neither pass is a strict subset of the other (an alias match can hit where the
   initial-fallback misses, keyed off the PRIMARY name's own initials, and vice versa), so this is
   a real completeness fix, not just a safety margin.
2. **`require_middle_match` semantics** (see above).

**A real, separate bug found and fixed in `HumanNameParser.parse()`** (`src/utils/names.py`),
surfaced while building `middle_tokens`: `first_name_canonical` picked whichever given-name token
was *longest*, with no regard for first-vs-middle position — so "George Stewart Walker"
canonicalized to `stewart`, "Ben Martin Tsamenyi" to `martin`. Checked directly against real ARC
data: **310 of 851 (36.4%)** distinct multi-token `first_name` values were affected, each one
feeding a wrong `first_initial` straight into Splink's primary ARC-internal blocking key
(`family_name_main + first_initial`) — a real, previously-undetected under-merge risk. Fixed to
prefer the true first name, falling back to the middle name only when the first is itself a bare
initial (e.g. "C. David Thomas" still correctly canonicalizes to `david`). Verified: mismatches
dropped to 64, and every remaining one is the correct fallback firing (a genuine bare-initial
first name). New `ParsedName.middle_tokens` field added (additive, no existing field changed).
535/535 tests passing throughout every step above. `_POSTNOMINALS` also gained "Pharmacist"
(same stacking/comma-separator handling as the existing list).

**Investigation findings using `FetchOrcid`, real numbers**:
- **NO_ORCID population** (4,891 non-excluded ACIFs, `au_only=True`): **0 candidates: 3,179
  (65.0%) / 1: 981 (20.1%) / 2+: 731 (14.9%)**. Latest-grant-year distribution for the
  zero-candidate bucket is heavily pre-2014 (median 2008, 77.6% before 2014) — matches this
  project's own already-documented "post-2014 admin-inserted ORCID" pattern. But dropping
  `au_only` on just that zero-candidate bucket found **2,602 of 3,179 (81.9%) actually do have at
  least one name-matched candidate** — the "zero" result was mostly `au_signal`'s own sparsity, not
  genuine absence. Only **577 (18.1%)** have nothing under name-matching at all, with or without
  the filter — that's the population where "no ORCID reachable from ARC data as it stands" is
  actually well-supported. **This whole NO_ORCID scan predates today's short-circuit fix and is
  now stale** — the 981/1-candidate bucket in particular may understate true ambiguity, since some
  of those cases would union in more candidates from the fallback under the fixed code. Needs
  rerunning.
- **HAS_ORCID audit** (`load_cluster_rows()`/`audit_clusters()`/`summarize_audit()`, same file) —
  compares each ACIF's recorded orcid(s) against a fresh `search_orcid()` over its own name-forms,
  reporting `recorded_missing_from_search` (a recorded orcid the fresh search never found) and
  `extra_candidates` (other plausible people found alongside it) — exactly the signal that would
  have flagged the Jocelyn Craig case (see item #20) automatically. Verified correct on a 20-cluster
  sample only (e.g. `DE120100110_JeeHyunKim`, 6 candidates found) — **never run at full population
  scale (~18K clusters)**, two launch attempts were interrupted before completing.
- **ARC's own `orcid` field is confirmed retroactively backfilled**, same pattern as OAX's
  author_idx back-propagation but a different, much lower risk category: 19,951 of 63,713 (31.3%)
  pre-2013 investigator rows have a non-null `orcid`, including grants from 2001–2011 — years
  before ORCID existed (launched October 2012) — proving the field reflects whatever ARC's system
  has on file NOW, not a point-in-time capture. **User correction, recorded because it changes how
  much this should worry us**: ARC's backfill is self-asserted (the applicant typing their own
  ORCID into their profile), a human-mistake error mode, not an inferred/algorithmic match across
  ambiguous evidence like OAX's — tier 1a still deserves the trust it already gets.
- **ORCID `/history` schema** (checked directly against 33,820 real cached `/record` responses):
  `submission-date`/`last-modified-date` always present; `completion-date` rare (7.5%);
  `deactivation-date` never seen in this cache (0%). Not in the local bulk snapshot at all (only
  in the live `/record`) — confirmed by inspecting a raw `records.jsonl.gz` row directly (top-level
  keys: `orcid, name, locale, employments, educations, memberships` — no `history` section at all).
  `claimed`/`verified-email`/`verified-primary-email` exist as real, cheap trust signals (59
  unclaimed, ~2,200-2,500 unverified-email of ~33,820) — not yet added to `OrcidRecord`, which
  currently only carries person/affiliation/work-year fields ported from the older
  `orcid_client.py` accessors.

**Needed next**: rerun the NO_ORCID scan under the fixed code; run the HAS_ORCID audit for the
first time at full scale (~30 min estimated at current per-cluster cost); decide whether to record
the confirmed Jocelyn Craig merge (`manual_merges.csv` + `enrichment_blocklist.csv` for the wrong
ORCID) now or wait for item #21's fix to handle it structurally.

### 24 — Diagnosing why a recorded/matched orcid doesn't resolve to the correct OAX `author_idx`
— two closed causes, and a reframing of what's actually still open

Found 2026-09-06, starting from a direct, Splink-free SQL orcid-equality join over the 17,898
HAS_ORCID ACIFs (`orcid` exploded from `awards_cif_arc_only.parquet`, joined directly against
OpenAlex `authors.orcid` — no Splink involved at all): **1,376 (7.7%)** ARC-recorded orcids not
found anywhere in OpenAlex's `authors` table at all; **14,491 (81.0%)** exactly one clean OAX
author match; **2,031 (11.3%)** orcid shared across 2+ `author_idx`.

**Four initial candidate causes, and what's settled about each:**

- **ARC-side orcid inconsistency — CLOSED, empirically 0.** Two direct checks against
  `awards_cif_arc_only.parquet` (22,789 non-excluded ACIFs, script: `scratch/
  check_orcid_consistency.py`, not committed — project scratch is gitignored): 0 ACIFs with 2+
  distinct orcid within one ACIF; 0 orcid values shared across 2+ ACIFs. Confirms
  `split_orcid_conflicts()`/`MULTI_ORCID` already enforces this by construction, with no residual
  gap from any later `refine_clusters()` step.
- **OAX-side "orcid inconsistency," reframed.** A single `author_idx` never carries 2+ orcids —
  confirmed 3 independent ways directly against `authors/*.parquet` (119,129,660 rows): zero
  duplicate `author_idx` rows at all; the flat `orcid` column and the nested `ids.orcid` struct
  field agree on every non-null pair; no multi-value string encodings found in the orcid field
  itself (only 3 non-standard-length values population-wide, all truncated single orcids, not
  concatenations). The real, common phenomenon is the *opposite* direction — many `author_idx`
  sharing one real orcid, often small, recently-created fragments that OpenAlex's own
  author-clustering never merged into the established record — exactly the 2,031-bucket above,
  confirmed directly by the user to be a common, not rare, pattern ("There are a lot of small
  recent author_idx for the same orcid"). This is OAX's own author-clustering under-merging
  (fragmentation), the mirror image of the contamination case below, not an internal data
  contradiction.
- **Orcid match misses the true "adopted" `author_idx`** — a known, accepted, real category (the
  Jocelyn Craig case, item #20/#21). Publication-count-based diagnostics don't work here: a low
  count can mean either a genuine OpenAlex coverage gap (correct bucket) or a genuinely wrong/thin
  fragment; a high count can mean either a genuinely correct bucket or contamination — count alone
  can't tell the two apart in either direction, so neither a pre-Splink productivity prefilter nor
  a post-link works_count tie-break can serve as this diagnostic. ORCID-record-dependent
  diagnostics (forward-tracing the person's own `/works` DOIs into OpenAlex's `authorships`;
  cross-checking self-reported employment timeline against candidate `author_idx` affiliations)
  were also considered and set aside as impractical at scale — most ORCID records are too sparse
  in exactly this data (few/no listed works, no employment history) for either to apply broadly.
- **Matched `author_idx` contaminated by other people's work** (over-merge; the known Luxin Chen
  pattern, documented elsewhere in CLAUDE.md) — the mirror image of the fragmentation case above.

**The reframing reached with the user**: these four causes collapse into exactly two real
post-link questions, not four separate things to chase.

- **(a) Reliability of "exactly one candidate" links.** The largest population in the whole
  pipeline (the 14,491 clean single-orcid-equality matches above, every Splink `unique_hc`
  resolution, and `04_resolve_links.py`'s own Step 1b within-group orcid match) currently gets
  zero scrutiny by construction, since nothing else is present in that resolution path to compare
  against. Needs a mechanism analogous to `AwardsCIF.reliability_tier` on the ARC-internal side: a
  confidence grade for a *single* OAX candidate, built from institution/field/timeline
  plausibility checked against the ACIF's own known facts (grant institution(s), FOR-declared
  field, grant years) — deliberately NOT dependent on the ORCID record's own self-reported data,
  confirmed too sparse to rely on for most records.
- **(b) Disambiguation of ACIFs with 2+ linked candidates** (common, expected, given the
  fragmentation pattern above) — substantially already built in `04_resolve_links.py` (institution
  overlap Step 2/line 312-319, field score Step 2b/line 321-330, works_count dominance Step
  4/line 344-356 — verified by direct read this session, not from memory or the CLAUDE.md
  summary). Two confirmed real gaps: (i) Step 1b's ORCID match (line 282-291) short-circuits
  *before* institution/field are ever checked — a better-fitting non-orcid candidate in the same
  ambiguous group is never compared against an orcid winner; (ii) the whole cascade only ever
  operates over whatever candidate set Splink's own high-confidence (≥0.9) scoring already
  produced (`per_arc >= 2`, line 195) — no reach into a true candidate Splink never surfaced at
  all, exactly the population the direct SQL orcid-equality join reaches instead.

**Also still open, unresolved, from the same investigation**: a person-relative implausible-year
check (ARC grant's earliest `funding_commence_year` vs. the OAX candidate's earliest OpenAlex
publication year, over the 22,573 currently-resolved links in `arc_oax_resolved.parquet`) found
only 322/22,570 (1.4%) implausible — far short of the user's own claim that "well over 20%" of
current matches are not possible on the basis of ARC/OAX info alone. Not yet explained by any
signal tested so far; `arc_oax_resolved.parquet` itself is this pipeline's own unverified output,
not ground truth, so any such check only measures "how much would the pipeline's own answer
change," never a real accuracy rate.

**Needed next**: (i) design/build the (a)-reliability signal from ARC-side + OAX-native data only,
no ORCID self-report dependency; (ii) fix the two (b)-cascade gaps identified above; (iii) keep
investigating what signal(s) would actually substantiate the ">20%" claim — pub-count/productivity
thresholds and grant-year-vs-earliest-pub-year have both now been tried and don't explain it.

---

### 25 — Build a generic, name-agnostic set-comparison utility for Splink evidence

Found 2026-09-08 while building `src/utils/name_set_processing.py` (`NameSetProcessing`/
`NameComparison`, item #14's fix). `NameComparison.for_blocking()`/`for_scoring()`/`for_tf_idf()`
answer questions that have nothing to do with names specifically — "do these two sets share a
value," "what's the dominant value," "what's the rarest shared value's population frequency."
This project already needs exactly the same operation for a field that isn't a name at all:
`cl.ArrayIntersectAtSizes("inst_arr", ...)` (institution-set overlap) is the identical kind of
check, and `for_name_tokens`'s own set-overlap comparison is a third instance already live in
`cluster_items()`.

Right now that logic is duplicated by construction — the institution comparison, the FOR-token
comparison, and `NameComparison` each implement their own version of "compare two sets" with no
shared code between them. That's the same failure shape as item #14's original bug (independent
copies of the same logic, able to drift independently), just one level more abstract: instead of
several copies of "pick a scalar from a name-set," it would be several copies of "compare two
evidence-sets for Splink," if this is ever generalized without care.

**Deliberately deferred, not folded into item #14**: building this now would have meant a larger
refactor mid-fix, and `NameComparison` already fully implements the name-specific case correctly
on its own (see `scratch/test_name_set_processing_cases.py`'s real-case coverage). The right
shape when this is eventually built: a generic `SetComparison` (or similar) operating on any
`{value: count}` structure, with `for_blocking`/`for_scoring`/`for_tf_idf` implemented once —
`NameComparison` (and, eventually, an institution/FOR-code equivalent) would *compose* it rather
than reimplement it. The name-specific pieces stay separate: `related()` (edit-distance/substring
spelling-variant coherence — meaningless for institution IDs or FOR codes, genuinely specific to
name strings) and `for_label()`/`for_report()` (name-display/provenance semantics) belong to
`NameSetProcessing`, not the generic comparator.

Not scoped further than this — no design decisions made yet on the generic class's exact shape,
and no call sites (`inst_arr`, `for_name_tokens`) identified as *needing* migration onto it
immediately. This is a real, deferred opportunity, not an active defect.

### 26 — Refactor `orcid_processor.py` to depend on `names.py`/`ParsedName` directly

Found 2026-09-08, following directly from item #25's discovery. `orcid_processor.py` defines its
own `NameForms` type (a narrower shape than `ParsedName`: `given_tokens`, `family_name_main`,
`first_name_canonical`, `full_name_key`, `family_names`) rather than using `ParsedName` itself,
on the reasoning that `orcid_processor.py` is meant to be standalone/extractable to its own repo,
so it shouldn't depend on this project's own `names.py`. Checked directly rather than assumed:
`names.py`'s own import chain is `re`, `dataclasses`, the external `nameparser` package, and
`src.utils.name_diacritic_variants` — which itself imports only `itertools`/`re`/`unicodedata`.
Zero project-specific coupling anywhere in that chain. So `names.py` is *already* standalone, and
depending on it would not have compromised `orcid_processor.py`'s own portability at all — the
`NameForms`/adapter separation was built on a premise that doesn't hold.

**Why this matters, not just as a tidiness concern**: `all_full_name_keys()` (the combinatorial
given×family cross-product function — see item #14/this session's later discussion) was written
against `NameForms`, because that's what `orcid_processor.py`'s own pipeline consumes. Because
`NameForms` and `ParsedName` are different types with no shared code path, that function has
never been reachable from `ParsedName` directly — which is the specific, concrete reason the ARC
and OAX prep pipelines (which use `ParsedName` via `HumanNameParser.parse()` and never touch
`NameForms`) have no access to it at all. This is not a second instance of the original
"independent reimplementations" bug (item #14) — it's a real, deliberate architectural boundary
(drawn for a legitimate-sounding reason) that turned out to block a later capability from
reaching the place that needed it most, once the reason for the boundary was actually checked
and found not to hold.

**The refactor**: `orcid_processor.py` should import and use `ParsedName`/`HumanNameParser`
directly, dropping `NameForms` and `orcid_processor_arc_adapter.py`'s conversion step entirely.
`all_full_name_keys()` (or its replacement) should then operate on `ParsedName` directly, making
it reachable from `01_prepare_arc.py`/`awards_cif.py`/`02_prepare_oax.py` without any adapter —
this is the prerequisite for item #14's still-open combinatorial-full-name-key work (see that
item's later discussion, and the "why doesn't this exist in the ARC/OAX parquets yet" note
recorded there) to actually land in the pipelines that need it. Not yet scoped in detail
(exact migration steps, whether `OrcidProcessor.discover()`'s other pluggable-normalizer callers
outside this project would be affected — there are none today, so low risk).

**IMPLEMENTED 2026-09-08.** `NameForms`, `default_name_normalizer()`, and `all_full_name_keys()`
removed outright from `orcid_processor.py` (not deprecated in place — grepped every call site
across `src`/`tests`/`analysis` first to confirm nothing else depended on them). The module now
imports `HumanNameParser`/`ParsedName` from `names.py` directly, with a module-level
`_default_parser = HumanNameParser()` as its own baked-in default normalizer (still pluggable —
`Callable[[str], ParsedName]` — for a caller wanting a genuinely different parser, but there's no
longer a "bare" vs "hardened" pair to choose between). `orcid_processor_arc_adapter.py`'s
`arc_name_normalizer()` removed entirely; its two other responsibilities
(`institution_matched_candidates()`/`resolve_institution_overlap()`, `get_record()`) untouched.
`00b_enrich_orcid.py` updated to construct `OrcidProcessor()` with no explicit `name_normalizer=`
override, relying on the new default.

**Two real, previously-hidden bugs found and fixed while doing this, not just a mechanical
type swap**:
1. `parse_bulk_record()`/`BULK_SCHEMA` never carried the ASCII/raw dual representation ARC/OAX
   prep has had since 2026-09-02 — `given_tokens_raw`/`family_name_raw`/`full_name_key_raw`
   columns added, real value for the many non-Latin-script names a global 17.15M-person ORCID
   population genuinely contains.
2. `OrcidProcessor._match_by_full_name_key()` only ever checked `full_name_key = ? OR
   list_contains(alias_full_name_keys, ?)` — never the richer `all_full_name_keys` column it
   was already persisting. Since `all_full_name_keys` is a strict superset of both (it already
   unions the primary name's own combinatorial keys, which include `full_name_key`, plus every
   alias's own), the fix is both simpler (one condition) and strictly more complete — and it's
   the specific reason a candidate could never be found purely via their own name's nickname
   before this fix, even once `all_full_name_keys` started containing nickname-crossed keys.
   `_match_by_family_and_initial()` similarly updated to also check a new `nickname_tokens`
   column, not just `given_tokens`.

`fetch_orcid.py`'s own combinatorial key builder (`_candidate_full_name_keys()`) and its two
family+initial fallback paths (`_search_by_family_and_initials()`, `batch_search_orcids()`'s
inline block) updated to include `nickname_tokens` alongside `given_tokens` — same reasoning,
ACIF-side search should reach exactly what the bulk table now offers.

**`orcid_bulk.parquet` rebuilt** from the raw `records.jsonl.gz` snapshot (local-only, no API
calls — the rebuild itself doesn't need `orcid_client`/live network access at all) — 17,152,673
rows, unchanged count confirming a clean like-for-like rebuild, ~15.4 minutes. 9,578 real ORCID
records now carry a genuine detected `nickname_tokens` value. Full test suite 543/543 passing
throughout, including new coverage for the rebuilt schema
(`tests/test_orcid_processor.py::TestParseBulkRecord::test_nickname_widens_all_full_name_keys`,
`test_raw_path_columns_present`, `TestOrcidProcessorDiscover::
test_fallback_when_candidate_record_only_has_bare_initial`).

**Not done, deliberately out of scope for this pass**: re-running `00b_enrich_orcid.py`'s actual
population-scale ARC search against the rebuilt table — that touches the live ORCID API
(rate-limited, produces new enrichment data feeding manual-review workflows) and is a materially
bigger, riskier operation than rebuilding a local reference table; needs its own explicit
go-ahead, not bundled into this refactor.

### 27 — `HumanNameParser.parse()` silently drops a quoted/parenthesized nickname

Found 2026-09-08. Confirmed directly: `nameparser`'s `HumanName` already extracts a nickname
when the input string has one written inline —
`HumanName('John "Johnny" Smith')` and `HumanName('John (Johnny) Smith')` both give
`first=John, last=Smith, nickname=Johnny`. This project's `HumanNameParser.parse()` never reads
`hn.nickname` at all (grepped every name-parsing call site, zero references) — if any raw ARC
name or OAX `display_name`/alternative string happens to contain this pattern, the nickname is
silently discarded at parse time, before anything else in the pipeline ever sees it. Different
failure shape from the already-known Jenny/Yingzi-style alias problem (a completely different
chosen name, genuinely no shared string to find) — this one is recoverable, since the nickname
is sitting right there in the raw string, just never extracted.

**The incorporation rule, specified directly by the user:**
1. If there are no existing first/middle-derived given-name tokens, the nickname becomes *the*
   given-name token.
2. If there is exactly one existing given-name token, the nickname is added as a *second*,
   additional given-name token (both kept, not one replacing the other) — e.g. "Robert (Bob)
   Smith" should yield candidate given forms including both "robert" and "bob".
3. If there are already two given-name tokens (a first and a middle both present), the nickname
   joins onto the second one with a space, forming one combined token, rather than becoming a
   third independent token.

**Not yet resolved before building**: which order the join in rule 3 uses (second-token +
nickname, or nickname + second-token) — needs checking against real examples of this pattern in
ARC/OAX data before picking one, not assumed. Real occurrence rate in this project's own data
not yet measured (how many ARC `first_name`/OAX `display_name`/alternative strings actually
contain a quoted or parenthesized nickname) — should be checked before this is prioritized
against the other open items above.

**IMPLEMENTED 2026-09-08, with the incorporation rule corrected against real data before
building it.**

**Occurrence rate, measured**: 156/46,312 distinct ARC `(first_name, family_name)` pairs
(0.34%) carry a quoted/parenthesized nickname — real, not negligible. 228 matches in the
AU/HEP-context OAX `full_name` pool, but a meaningful share of those are not nicknames at all —
OpenAlex's own disambiguation numeric suffixes (`'Ying Zhang (40767)'`), consortium/
group-authorship strings, birth-year annotations (`'(1946-)'`) — confirmed by checking each
candidate's own `display_name_alternatives`, not assumed from the pattern match alone.

**Rule 3's join order question resolved by finding it was the wrong question.** Four real ARC
cases with two pre-existing given tokens before a nickname (`'Xi Wen (Carys) Chan'`, `'Alan
John (AJ) Mitchell'`, `'Folarin Oluseye (Seye) Abimbola'`, `'Huong Giang (Lily) Nguyen'`) — none
support fusing the nickname onto the second token; every one treats it as a wholly independent
alternate given name (`'Carys'`, not `'Wen Carys'`). All three originally-specified cases
(0/1/2 existing tokens) collapse to one rule: nickname tokens are simply appended to whatever
given-name tokens already exist, including none — appending to an empty list already gives the
0-token case its own correct behaviour for free.

**Design landed differently from the original spec, by direct user correction mid-build**: the
nickname was *not* folded into `given_tokens` as originally planned. `ParsedName` gained its own
separate `nickname_tokens` field instead — kept out of `given_tokens` deliberately, since a
nickname is a different *kind* of relationship to the family name (its own combinatorial axis)
than an interchangeable given/middle name is, and folding it in would let it silently influence
`first_name_canonical`/`full_name_key` selection with no evidence to justify that. Guarded
against the OAX noise found above: `_guarded_nickname()` rejects anything containing a digit or
longer than 30 characters.

**A related, adjacent pattern caught and fixed the same day**: a `"nee"`/`"née"` maiden-name
marker (`'Judy Brown (nee Field)'`) lands in `hn.nickname` structurally identically to a genuine
nickname — nothing distinguishes them except the marker word. Confirmed real in this project's
own data (Leesa Costello (nee Bonniface), already documented elsewhere). `_maiden_name()`
catches this first and routes the captured name into `family_names` instead — a former surname,
not a given-name alternative — leaving `family_name_main`/`full_name_key` anchored to the
current name and `nickname_tokens` empty for these cases.

**`nickname_tokens` was initially dead weight, then actually consumed the same session**:
built as its own field, it had zero production consumers at first (confirmed by grepping every
`.nickname_tokens` call site — test files only). Closed by adding `ParsedName.full_name_keys` —
every given/nickname × family combination one occurrence's own tokens produce, the same
combinatorial logic `orcid_processor.py`'s `all_full_name_keys()` used to compute against the
narrower `NameForms` type (see item #26), now built directly into the canonical parser instead
of a second implementation. `full_name_keys` is what actually gets used downstream (item #26's
ORCID-bulk-table rebuild, item #14's `cluster_items()` blocking fix) — `nickname_tokens` alone
was never the end of the work, `full_name_keys` is.

**Verified**: `Yingzi (Jenny) Wang` → `nickname_tokens=('jenny','j')`, `full_name_keys` includes
both `yingzi_wang` and `jenny_wang`, `first_name_canonical`/`full_name_key` stay `'yingzi'`/
`'yingzi_wang'` (unaffected, as designed). `Judy Brown (nee Field)` → `family_names=('brown',
'field')`, `nickname_tokens=()`. Full test suite passing throughout (`tests/test_names.py`'s new
`TestPostnominalHandling`/`TestNicknameTokens`/`TestMaidenNameHandling` classes).

**Also landed the same session, found while doing this work, not originally part of this
item**: the custom `strip_postnominals()`/`_POSTNOMINALS` regex (`names.py`) removed outright,
replaced by full native registration into `HumanName`'s own `CONSTANTS.suffix_acronyms` (19
acronyms including `Pharmacist`, up from the 4 previously registered there). The custom regex
existed because an early test of the native mechanism registered only 4 of the ~19 needed
acronyms, found a stacked-suffix case broken, and concluded native handling didn't work —
never re-tested with the missing acronyms actually added. Verified directly this session that
once fully registered, native handling correctly strips stacked suffixes on its own
(space- and comma-separated), case-insensitively, benefiting OAX `display_name` parsing for
free (it shares the same `HumanNameParser`). One known, accepted residual, confirmed not to
occur in real ARC data (only 2 rows have an empty `first_name`, neither postnominal-affected):
a bare surname with no given name at all, followed by a suffix (`"Raston AO FAA"`), still
misparses — too few tokens for `nameparser`'s own grammar to tell there's no first name.
Checked on the OAX side too: 24 real records match this shape, but they're independently
already-thin, uncorroborated 1-work/no-ORCID fragments (or, for the one exception with
multiple works, `"Kevin AM"`, OpenAlex's own alternates include `'Kevin Am'` — i.e. OpenAlex
itself hasn't resolved whether this is a name or an acronym either) — not something to
reverse-engineer.

A second, unrelated bug fixed the same pass: `_structural()`'s bare-single-word-name fallback
(`hn.last = hn.first` for e.g. plain `"Smith"`, where `nameparser`'s own default guess is to
treat a lone token as a given name) copied the value into `.last` but never cleared `.first` —
so the same word leaked into `given_tokens` as a spurious given-name candidate alongside the
family name it actually is. Fixed by also setting `hn.first = ""` (confirmed this is
`nameparser`'s own empty-field sentinel, not `None`). Found a matching, already-existing test
(`tests/test_awards_cif.py::test_empty_first_name_falls_back_to_family_name`) that had
explicitly asserted the *old*, leaking behaviour as correct — updated to assert the fix instead,
after confirming with the user this was pinning down an oversight, not a considered design
choice (matching the same "don't fabricate a given name with no evidence" principle already
established for the bare-initial fallback next to it, which still stands).

### 28 — `04_resolve_links.py`'s dedup/disambiguation checks are ad hoc booleans; move toward rarity-weighted ("value_counts") evidence with an explicit veto-in/veto-out framework

Found 2026-09-09 working through `dedup_oax_candidates()`/`_oax_names_compat()` (Step 0) and
`_names_compat()` (cascade Step 1a) case by case against three real 2-OAX-candidate examples.

**Two vocabulary terms, introduced by the user, used throughout this item**: a **veto together**
is a signal strong enough on its own to block two candidates from being merged/treated as one
(e.g. `DP0342459_MarcelJackson`'s two OAX candidates, Marcel [algebra/logic] vs Martin [neurology/
psychiatry], share zero topics — that alone vetoes merging them). A **veto apart** is the
opposite: a signal strong enough to force two candidates together despite other apparent
differences (e.g. a shared ORCID).

**Three real cases traced end-to-end early this session, same layout each time (ARC row / ARC-OAX
link row / both OAX rows), establishing the current mechanics** (four more traced later the same
session — Zeunert, Amati, Giblin, Tele Tan — see the "later the same session" status update
below):
- `DE120100315_BenjaminIsakhan` — OAX candidates A5086064061 ("Ben Isakhan," 9 works) and
  A5091134612 ("Benjamin Isakhan," 209 works) share 2 topics and a genuine (non-bare-initial)
  `full_name_keys` overlap (`benjamin_isakhan`, present because A5086064061's own
  `display_name_alternatives` already lists "Benjamin Isakhan"). Step 0 merges them.
  Separately found: the kept 209-work record itself contains 8/43 piled works (19%) classified
  "Space and Planetary Science" — a subfield with no connection to Middle-East-politics/heritage
  studies — all inheriting `confirmed=True` from the candidate-level ORCID/HEP/field match, since
  piling's channeling checks the whole candidate, not each individual work. **Flagged by the user
  as a "keep both by for" case** — not auto-merged, using FOR-code-vs-subfield mismatch as the
  test — pending this item's broader fix rather than a one-off manual override.
- `FL160100170_MichaelTyers` — A5004507461 ("Michael Tyers," 32 works) and A5057700057 ("Mike
  Tyers," 314 works) also share 2 topics, but `_oax_names_compat()`'s informative-key check finds
  no overlap (`michael_tyers` vs `{mike_tyers, mike_duman, ..., maria_jones}` — genuinely no
  shared token once bare initials are excluded). Step 0 does *not* merge them; the cascade's own
  ORCID step (1b) resolves it instead, since A5057700057 carries the ARC person's recorded ORCID.
  Same real person, same shape as Isakhan, resolved via a different step for a data-availability
  reason (this pair happens to have zero recorded/alternate-name connection between "Michael" and
  "Mike" anywhere in OpenAlex, unlike Ben/Benjamin).
- `DP0342459_MarcelJackson` — zero topic overlap (a clean veto-together), so Step 0 correctly
  doesn't merge. But cascade Step 1a (`_names_compat()`, meant to catch an obviously-wrong given
  name) let "Martin" through — not because of the bare-initial mechanism (an incorrect
  attribution corrected mid-session), but because `"marcel"[:3] == "martin"[:3] == "mar"`, a
  coincidental 3-letter-prefix match on the *full* given names themselves. Only the cascade's
  ORCID step (1b) actually resolved the case correctly; the name filter did no real work.

**The open design question this generalizes into**: none of the three signals currently used
(topic-string exact match, `full_name_keys` exact-string overlap, `_names_compat()`'s 3-char
prefix) account for how *common* the shared value is. A shared institution between two Jackson
candidates was noted by the user as illustrative: raw institution-ID overlap exists (both share
`I196829312`) but is almost certainly weak evidence once weighted by how many other researchers
also share that institution — "not zero, but very small" once properly counted, not a boolean.

**Proposed direction (design only, not built)**: compute `value_counts`-style (rarity-weighted
term-frequency, same shape as the project's existing `oax_tf_*.parquet`/TF-adjustment machinery)
measures per OAX candidate across three dimensions — subfield, institution, and coauthor — rather
than the current mix of exact-string/boolean checks. Then work out how these three combine into
an explicit veto-in (force merge) / veto-out (block merge) framework, replacing today's separate,
ad hoc, differently-implemented checks in `dedup_oax_candidates()`/`_oax_names_compat()` and the
cascade's `_names_compat()`. Not scoped further than this — no decision yet on how the three
dimensions combine, what thresholds apply, or whether this subsumes or sits alongside item #25's
already-deferred generic set-comparison utility (which covers the same "compare two evidence
sets" shape, though without the rarity-weighting angle this item adds).

**Status update, later the same session (2026-09-09): OAX-side dedup fixed; cascade archived;
rebuild started.** Full narrative in CLAUDE.md's own dated entry for this session — summary here:

- **`_oax_names_compat()` (Step 0) fixed, not just diagnosed.** `02_prepare_oax.py::oax_name_arrays()`
  gained a real `full_name_keys` field (every given/nickname x family combination an OAX author's
  own display_name + alternatives produce, kept complete/unfiltered at construction — an explicit
  design rule the user corrected into place after an over-hasty first attempt filtered it at
  construction time instead of at comparison time). `_oax_names_compat()` rewritten to compare on
  this field directly, with a comparison-time-only filter for uninformative bare-initial keys and
  a requirement that *every pairwise* combination in a candidate group share ≥1 informative key
  (not just some common key across the whole group). The old scalar-based version's real bug,
  confirmed on Isakhan: it filtered to given names ≥4 characters, silently dropping "ben" (3
  chars) from the comparison entirely and returning `True` without testing anything — right answer
  for the wrong reason.
- **Four more real cascade cases traced** (Zeunert, Amati [3-way], Giblin [4-way], Tele Tan
  [5-way]), confirming ORCID match (cascade Step 1b), not the name check, does the real
  discriminating work even when the name check looks like it should catch a bad candidate. Giblin
  is the clearest counter-case: candidates 2/3 drop by score, candidate 4 drops because "Ryan" is
  neither "Rachel" nor "Rebecca" — decisive on full given names alone, despite OAX cross-
  contaminating the two real people's work sets.
- **Two distinct, confirmed defects in the old cascade's `_names_compat()`** (separate from the
  Step-0 fix above — this is the ARC-vs-OAX-candidate check, cascade Step 1a): (1) a structural
  always-true bare-initial short-circuit (ARC's own `first_names` always self-adds a bare initial,
  e.g. `'r'` for Rachel Giblin, which the old loop hits first and returns `True` on
  unconditionally, regardless of the OAX candidate's real name); (2) a separate coincidental
  3-character-prefix collision (`"marcel"[:3]=="martin"[:3]=="mar"`, confirmed on
  `DP0342459_MarcelJackson` via live Splink retraining + `model.json` m/u extraction + direct SQL
  tracing) — a genuinely different mechanism from (1), not the same bug twice.
- **FOR2020 taxonomy verified directly** (213 valid 4-digit groups, brute-force enumerated; ARC's
  own population uses 204, all resolve cleanly, 0 already needing `upgrade_for_code()`) — confirms
  ARC's FOR data is fully FOR2020-converted. A real display bug found and fixed alongside this:
  the walkthrough script was printing the legacy primary-only `for_codes` column instead of the
  full `for2020_codes` struct list, making `LP0883400_ReveccaKakavanosPlew` look like a single-FOR
  case when she genuinely holds two.
- **Old cascade archived, rebuild started as `FilterCandidates`** — `src/04_resolve_links.py` →
  `ZARCHIVE/src_archive_20260909/04_resolve_links.py`, explicitly not via Plan Mode (direct user
  instruction). `src/04_filter_candidates.py` now holds a container-first `FilterCandidates`
  class (same methodology precedent as `FetchOrcid`): `orcid_veto()` is real and implemented
  (hard veto on a confirmed ORCID mismatch, evaluated per-candidate so the loop keeps scoring
  remaining candidates — not a loop-terminator, needed for small fragment candidates like Tele
  Tan's candidate 5 to still get evaluated); `fd_compare()`/`score()`/`resolve()` are stubs
  (`NotImplementedError`) pending the FD-comparison utility design this item already flags as
  needed. This pipeline stage currently produces no resolved-links output at all — only the
  review/walkthrough capability (loading deduped candidate pools, printing the 3-part
  ARC/links/OAX display for any `n_candidates` bucket) is working.
- **Not yet investigated**: a final, undeveloped observation from this session — "they all look
  suspect — probably the small works and HASS fields" — is a new lead, not yet chased.

## Verification
Items 1 and 2: no pipeline rerun was needed since both turned out to already be correctly
resolved in current output — verified directly by querying `awards_cif_arc_only.parquet`.
`manual_orcids.csv` gained one new row (`DP0210446_RobertGilbert`); CLAUDE.md's "Next
Priority" and "Known Issues in 02 Output" sections were corrected to stop describing closed
issues as open. Remaining items still need their own individual verification once
undertaken (each already describes, in CLAUDE.md, what a clean rerun/test-suite pass looks
like for that specific fix).

Items 14 (given-name blocking half)/26/27 (2026-09-08): full test suite 543/543 passing
throughout every step. `cluster_items()` re-run against the real, current ARC population
(64,830 items → 22,791 clusters, 15.1s) with real, previously-unreachable nickname merges
confirmed directly in the output (see item 14's own entry for the list). `orcid_bulk.parquet`
rebuilt and schema-verified (17,152,673 rows, new columns present, 9,578 rows with a detected
nickname). **Not yet done**: a full `00→01→03→04` pipeline rerun to materialise these fixes
into `awards_cif_arc_only.parquet`/`arc_persons.parquet` and everything downstream — today's
verification is at the `cluster_items()`/`orcid_processor.py` function level, not a fresh
population-wide pipeline output. `AwardsCIF.first_names` (and therefore `03_link_arc_oax.py`'s
own input) does not yet carry nickname-widened forms either — `_name_forms()` was deliberately
left untouched this pass (see item 14's own entry for why that's a smaller, separate follow-on,
not a gap in this pass's own scope).
