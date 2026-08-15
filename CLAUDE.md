# ARC Grants → OpenAlex Linkage Pipeline

## Working Rules
- **Read before editing**: read every line of a function before proposing a fix — do not propose until you have read everything it touches
- **Do not guess or assume**: if something is unclear, ask the user rather than proceeding on an assumption
- **Stop after each step**: after completing one step, stop and wait for the user to review before moving to the next

## Project Goal
Link ARC Chief Investigators/Fellows (CIFs) to their OpenAlex author records for bibliometric analysis.

## Pipeline Architecture (Splink-based)
```
00_extract_arc.py       → grants_flat.parquet, investigators_raw.parquet
01_prepare_arc.py       → arc_investigators_prep.parquet, arc_persons.parquet
                           (ARC name/inst/FOR prep + Splink dedupe_only: 65k rows → 23,056 persons)
02_prepare_oax.py       → openalex_authors_prep.parquet, oax_tf_*.parquet
03_link_arc_oax.py      → arc_oax_links.parquet (link_only: ARC persons → OAX authors)
04_resolve_links.py     → arc_oax_resolved.parquet, arc_ambiguous_deferred.parquet
```
The Splink pipeline replaces the entire old multi-layer pipeline, archived at
`ZARCHIVE/src_archive_20260520/` (moved there 2026-08-14, nothing references it).
`02_run_splink.py` is the old wrong approach — superseded, can be deleted.

## Key Paths
- Config: `config/settings.py`
- **`data_persisted/`** (repo root, git-tracked): every file that represents costly manual
  work or a hard-to-reacquire external source — hand-curated matching/override CSVs
  (`manual_resolutions.csv`, `manual_splits.csv`, `manual_splits_hand_counts.csv`,
  `manual_orcids.csv`, `manual_merges.csv`, `manual_name_corrections.csv`, `enrichment_blocklist.csv`,
  `for_concordance.csv`, `for_divisions.csv`, `for_adjacent_divisions.csv`) plus the ANZSRC/Scopus/OpenAlex/ERA source
  `.xlsx` reference files. Previously split across `config/*.csv` (some git-tracked but
  unlabelled as precious) and `data/*.xlsx` (entirely gitignored, i.e. genuinely unrecoverable
  from a fresh clone) — consolidated 2026-08-07 so nothing irreplaceable can silently fall
  outside version control again. `config/` itself now holds only code (`settings.py`,
  `scope.py`, `scoring.py`).
- Data root: `/home/lc/k/WORKING_ARC_PROJECT/`
- Processed data: `/home/lc/k/WORKING_ARC_PROJECT/processed/`
- OpenAlex data: set via `OPENALEX_DIR` env var in `.env` → `/home/lc/k/openalex_jul26/parquet_converted/`
  (migrated from the old Feb26 snapshot 2026-08-08 — see "OpenAlex Snapshot Migration" below; moved
  from `/home/lc/m/...` to `/home/lc/k/...` 2026-08-14 when the drives were consolidated — `k` and
  `m` are symlinks under `/home/lc/` to `/media/k-drive`/`/media/m-drive`).
  `OPENALEX_DIR` itself holds dimension tables (`authors/`, `institutions.parquet`, etc.);
  `OPENALEX_COMPACT_DIR` (`OPENALEX_DIR / "compact"`) holds the big fact tables (`works/`,
  `authorships/`, `work_topics/`, etc.) — a two-tier split that didn't exist in the old snapshot.

## Data Scale
- ARC CIF rows (after role/scheme filter): 65,087 (37 previously dropped by INNER JOIN on admin_org — now LEFT JOIN)
- ARC person clusters (output of 01): 23,056
- OAX HEP-context authors: 2,453,347 (all authors appearing in AU-context works, not AU-last-institution filter)
- ORCID coverage in ARC CIFs: 44.5%
- ORCID coverage in arc_persons (after enrichment): 73.1%

## Python Environment
**Always use `.venv/bin/python`** — never bare `python`.

## Name Tokenisation (src/utils/names.py)
- `name_part_tokens()`: strips apostrophes before tokenising → `O'Brien → ["obrien"]`
- Hyphens/spaces still split: `Watson-Parker → ["watson","parker"]`
- `strip_diacriticals()`: NFD normalise + ASCII-only, also maps Unicode hyphens to ASCII
- Applied identically on both ARC and OAX sides in `01_prepare_splink.py`

## Name Parsing Design (01_prepare_splink.py)
- **Family names**: use `strip_diacriticals(hn.last).lower()` as a single compound string —
  do NOT split with `name_part_tokens`. HumanName correctly handles compound surnames
  (van der Ent, de Almeida, etc.) and splitting destroys them.
- **OAX name source**: `display_name` is primary (always "First Last", curated by OAX).
  `display_name_alternatives` used only as fallback when display_name yields no family name.
  Alternatives are contaminated with co-author names from OAX entity disambiguation errors.
- **first_initial**: always derived from `first_name_canonical[0]`, never from the
  unordered `first_names` list (set-derived, arbitrary order).

## FOR Code Handling
- Two ANZSRC series (2008 and 2020) use different numeric codes and slightly different names.
  **As of 2026-08-07, cross-series and FOR↔OpenAlex harmonisation is delegated to the
  external `research_classification` package** (`pip install
  git+https://github.com/LarryCram/ResearchClassification.git`), via `src/utils/for_resolve.py`
  — an audited, confidence-scored resolver. This replaces the old, now-deleted
  `src/utils/lookup_for_topic.py` hand-built concordance (fed by
  `config/build_for_oax_field_map.py`'s ~370 hand-typed rows).
  - `for_resolve.upgrade_for_code(code)` / `upgrade_for_name(code, name)`: 2008→2020 group
    upgrade, 2020 passthrough — same signatures/semantics as the old wrappers.
  - `for_resolve.oax_subfield_name(code)`: FOR2008-or-FOR2020 group code → OAX subfield
    label in one `resolve()` call (used by `04_resolve_links.py`'s `_field_score`).
  - `for_resolve.oax_to_for2020(oax_code)`: reverse direction (OAX → FOR2020), reaching
    FOR2020 **field** (6-digit) precision from an OAX topic-level input — finer than the
    ARC→OAX direction ever provides. Not wired into any pipeline step yet; added for future
    ECR/DECRA analysis work.
  - **Do NOT confuse this with `resolve(FOR, "OAX_TOPIC")`** — the package explicitly
    refuses that direction (`ValueError`) since OpenAlex's 4,516 topics are far finer than
    anything honestly derivable from a FOR division; the finest reachable *from* a FOR-family
    input is `OAX_SUBFIELD`. ARC's own `for_code` is 4-digit only anyway (regex
    `^\d{4}` in `01_prepare_arc.py`), so a 6-digit FOR→topic path was never actionable
    against this project's actual data.
- `for_name_tokens()` in `src/utils/names.py`: tokenises FOR names, strips stopwords —
  unrelated to `research_classification` (that package only does exact code/label lookups;
  this handles ARC-internal free-text FOR-name fuzzy bridging for Splink blocking).
- `make_expanded_for_tokens()`: loads `data_persisted/for_concordance.csv` (49 J≥0.5 pairs)
  and unions each name's tokens with its canonical form's tokens — bridges near-synonym names

## 03_link_arc_oax.py Design
- Splink `link_only`: arc_persons → openalex_authors_prep
- Blocking: `(family_name_main, first_initial)` + ORCID exact
- Comparisons: first_name_canonical (exact/initial/mismatch), family_name_main, full_name_key,
  orcid (seeded m/u), inst_arr intersection
- TF adjustment on first_name_canonical, family_name_main, full_name_key
- ORCID force-add: pairs missed by predict (score < 0.5) but sharing exact ORCID → added at p=1.0
- Output: all pairs ≥ 0.5, `high_confidence` flag for ≥ 0.9

## 04_resolve_links.py Design
Disambiguation of ARC persons with 2+ HC OAX matches. Steps in order:
0. **OAX same-ORCID pre-dedup**: two OAX candidates sharing an ORCID → split records;
   keep dominant (`works_count / group_total > TOP_CUT=0.7`).
0b. **OAX same-topic pre-dedup**: two OAX candidates sharing ≥1 specific topic name → split
    records; keep the one with more works. ORCID-matched records are protected from removal.
1. **ARC–OAX ORCID match**: if exactly 1 HC candidate matches the ARC person's ORCID → resolve.
2. **Institution overlap**: restrict to candidates with maximum overlap count (if any > 0).
2b. **Field score filter**: restrict to candidates with maximum field score (`_field_score` —
    a plain set-intersection count between the OAX subfield names implied by the ARC person's
    FOR codes, via `for_resolve.oax_subfield_name()`, and the OAX candidate's own
    `subfield_names`; not token n-gram overlap, despite what this doc used to say). Only fires
    if `max_fs >= 2` AND `min_fs == 0` (at least one candidate has zero overlap — avoids
    within-field false positives).
3. **Unique highest probability**: among remaining → resolve.
4. **works_count dominance**: `max / sum > TOP_CUT` → OAX split record, take dominant.
5. **Defer**: genuine common-name collisions.
Manual overrides: `data_persisted/manual_resolutions.csv` (resolve/unlink actions applied after all steps).

Output columns: `arc_id, oax_id, match_probability, resolved_by, secondary_oax_ids`
`secondary_oax_ids`: all other HC candidates not chosen (split-record duplicates + alternatives).

## Current Linkage Results (2026-06-17, arc_persons 23,056)
- Resolved: 22,599 / 23,056 (98.0%)
  - unique_hc: 9,385 | oax_orcid_dedup: 965 | oax_topic_dedup: 2,337 | orcid: 6,594
  - inst_overlap: 1,846 | field: 679 | probability: 34 | works_count: 299 | name_filter: 36 | manual: 424
- Ambiguous deferred: 179  ← up from 102; larger OAX pool (2.45M) generating more multi-HC candidates
- Manual unlinked: 16
- Unlinked (no HC match): 262
- manual_resolutions.csv: 424 resolve + 16 unlink = 440 rows
- Key change vs previous: orcid +1,616; inst_overlap −1,310 (direct payoff from ORCID enrichment)
- **This baseline assumes ORCID enrichment is complete** (`orcid_enrichment.parquet` present) —
  see the 2026-08-07 verification note below for what happens without it.

### 2026-08-07 verification run (00→01→03→04 full rerun, validating the `research_classification` FOR-code refactor)
Full pipeline rerun from scratch to confirm the FOR-code harmonisation migration (see "FOR Code
Handling") didn't regress linkage quality. Two pre-existing, unrelated gaps surfaced and are
noted here so they aren't mistaken for refactor-caused regressions:
- `orcid_enrichment.parquet` **does not exist in this environment** — `00b_enrich_orcid.py`
  (a slow, ORCID-API-bound job) has never been run to completion and saved here. Its promotion
  step (`_apply_enriched_orcids`) skips gracefully when missing, so this run's ORCID coverage
  reflects raw ARC data + manual overrides only, not the enriched 73.1% baseline above.
- `grants_flat.parquet`/`investigators_raw.parquet` on disk were themselves stale (missing
  `n_eligible_orgs`, 62,747 vs 65,087 rows) — `00_extract_arc.py` was rerun first to fix this.

Results (23,065 persons; -2.2pp ORCID coverage vs baseline is the enrichment gap above, not a
regression):
- Resolved: 22,668 / 23,065 (98.3%) — **up** from 98.0% baseline despite the missing enrichment
  - oax_orcid_dedup: 939 | oax_topic_dedup: 2,375 | orcid: 5,108 (↓ from 6,594 — enrichment gap)
  - inst_gate: 907 (new bucket, unrelated to this refactor) | inst_overlap: 2,070
  - **field: 1,204 (↑ from 679 — the bucket this refactor directly touches)** | probability: 50
  - works_count: 431 | name_filter: 29 | manual: 426
- Ambiguous deferred: 172 | Manual unlinked: 16 | Unlinked (no HC match): 209
- **`field` bucket spot-checked** (8 random samples): every case shows a semantically correct
  ARC-FOR ↔ OAX-subfield match (e.g. Organic chemistry↔Organic Chemistry, Geology↔Geology,
  Philosophy↔Philosophy) with independently high Splink match_probability (0.979–1.0) —
  `research_classification`'s fuller, audited coverage is rescuing genuine matches the old
  ~370-row hand-typed CSV missed, not introducing false positives.
- **Conclusion: FOR-code refactor validated, no regression.** The overall resolution rate
  *improved* (98.0%→98.3%) despite running with less ORCID enrichment than the documented
  baseline, which is strong evidence the field-score change is a net positive. Full apples-to-apples
  comparison against the 2026-06-17 baseline requires re-running `00b_enrich_orcid.py` first
  (separate, large, out of scope for this verification).

### 2026-08-08 follow-up: `00b_enrich_orcid.py` full run — partial (ORCID daily quota hit)
Ran `00b_enrich_orcid.py` from a cold cache (`DISKCACHE_DIR` was genuinely empty — no prior
progress existed in this environment despite the "7,084 still need fetching" note above, which
referred to a different session/machine's state). Result: **ORCID's public API has a hard daily
anonymous-usage quota** (`429 Too Many Requests — exceeded the daily quota for anonymous usage`;
their own error message points to registering a Public API client for a higher limit). The
`/record` fetch phase (for people who already have an ARC ORCID) completed essentially cleanly —
10,413 unique ORCIDs cached, only 3 errors. The name-*search* phase (for people with no ARC
ORCID) only got **41.5%** through before every subsequent call started failing: 4,979/11,989
succeeded, the remaining 7,010 cached as `confidence='error'`.
- **Gotcha for next time**: failed searches are cached under the same key a successful search
  would use, so a naive re-run will **not** retry them — `_search_orcid`'s cache-check only asks
  "was this key attempted," not "did it succeed." Before resuming, evict `confidence='error'`
  entries from the `orcid_searches` diskcache store (`DISKCACHE_DIR/orcid_searches`) first.
- Re-ran `01→03→04` on this partial enrichment anyway (user decision — a real improvement over
  no enrichment, not worth blocking on a multi-hour wait for the quota to reset): ORCID coverage
  49.3%→**64.0%** (14,755/23,060), resolved 22,682/23,060 (**98.4%**, up again from 98.3%).
  `field` bucket: 890 (moved from 1,204 as more candidates now resolve earlier via `orcid`/
  `oax_orcid_dedup` before reaching the field-score step — expected interaction, not a concern).
- **To reach the full 73.1%-ORCID-coverage baseline**: evict the error-cached entries, wait for
  ORCID's daily quota to reset, re-run `00b_enrich_orcid.py` to finish the remaining ~7,010
  searches, then re-run `01→03→04` once more.

## OpenAlex Snapshot Migration (2026-08-08)
Migrated the OpenAlex data source from an old "Feb26" extract (built from OpenAlex's now
discontinued gzipped-JSON dumps) to a "Jul26" extract of OpenAlex's own native parquet
snapshot — a one-time source swap, not a recurring update mechanism. Full detail in git history
(commit "Migrate to Jul26 OpenAlex snapshot..."); summary here for anyone picking this up later.

**Layout change**: Jul26 splits fact tables from dimension tables — `compact/` holds the big
scan-heavy tables (`works/`, `authorships/`, `work_topics/`, `references/`, `work_abstracts/`,
`work_sdgs/`); the parent level holds dimension/lookup tables (`authors/`, `institutions.parquet`,
`sources.parquet`, etc.). `config/settings.py` now has `OPENALEX_DIR` (parent level) and
`OPENALEX_COMPACT_DIR` (`OPENALEX_DIR / "compact"`) to match.

**Extract reshaped to match Feb26's schema**, rather than rewriting downstream query logic —
since the extraction/conversion process is under the user's own control, three adjustments were
requested and confirmed present (verified against real rows, 0 nulls) before any pipeline code
changed:
1. `authorships` keeps `author_name` and gained `institution_name` (both denormalized inline,
   as Feb26 had them — Jul26's native shape dropped both).
2. `work_topics` denormalized from Jul26's native nested `LIST<STRUCT(topic_idx, score)>` (no
   names) back to Feb26's flat one-row-per-(work,topic) shape with `subfield_idx/name,
   field_idx/name, domain_idx/name` inline.
3. `works` needed no change — already compatible.

**Explicitly not changed**: `authors.author_idx` (integer) stays the primary key, not reverted
to a string `id` column — deliberate design on the new snapshot for speed/compactness. Code that
used to join ARC-side `oax_id` (a `"https://openalex.org/A<digits>"` string, used throughout
`arc_oax_resolved.parquet`/`manual_resolutions.csv`/anywhere a human reads or pastes an OpenAlex
URL) against the old `authors.id` string column now converts to `author_idx` first and filters on
that native column — `src/02_prepare_oax.py` (`au.author_idx = sf.author_idx`) and
`src/04_resolve_links.py` (two `works_count` lookups). **The string form is correct and
necessary at the human-facing/output boundary (`oax_id` in `arc_oax_resolved.parquet` etc.) —
only internal joins against the raw `authors` table needed to switch to `author_idx`.**

**Performance gotcha, caught during verification**: the first attempt at the `04_resolve_links.py`
fix filtered on the nested `ids.openalex` struct field (still holds the string form) instead of
converting to `author_idx` — cost a **30x slowdown** (44m44s vs the expected ~1m30s for a full
`04_resolve_links.py` run) at real pipeline scale (102,006 candidate IDs), apparently because
filtering on a field nested inside a STRUCT defeats DuckDB's predicate pushdown in a way a flat
integer column doesn't. Fixed by converting `oax_id` strings to `author_idx` integers before the
`WHERE ... IN (...)` filter, matching the pattern already used elsewhere in the codebase — this
dropped the run back to 1m28s. **Lesson for next time**: always filter/join raw OpenAlex tables
on `author_idx`/`work_idx`/`topic_idx` (native surrogate keys), never on a string field reached
through a struct, even when it's technically correct — nest-field filtering appears to scale very
badly in DuckDB's parquet reader.

**`SQL.md`** had three fully hardcoded Feb26 paths (the one place in the repo where an OpenAlex
path lived outside `.env`/`config/settings.py`) — used to manually regenerate
`works_intermediate_hep.parquet` → `authorships_hep.parquet` / `works_hep.parquet`, the
HEP-institution-prefiltered intermediates `src/02_prepare_oax.py` depends on (see
`AUTHORSHIPS_HEP.md`). Updated and re-run against Jul26.

**End-to-end verification**: full `00→01→03→04` pipeline rerun holds at 98.4% resolved
(22,675/23,049), matching the pre-migration baseline despite the OAX author pool growing from
2.49M to 2.78M authors. `analysis/01_fetch_oeuvres.py --full` and `02_accuracy_check.py --full`
also verified (see "Analysis Pipeline Status" below) — no data-quality regression detected.
Confirmed via full diff review that none of the 9 migration-touched files added any
role/scheme/fellowship filtering — the migration is scope-neutral, it doesn't carve out or
exclude any cohort (DECRA, APF, CI, or otherwise).

## Early-Career Fellowship Cohort Review (2026-08-08)
Corrected the role-code scope for "early career fellowship" analysis (e.g. the ECR/DECRA
"prompt paper" study) and manually reviewed every unlinked case in scope at the time.
- **Scope correction**: `config/scope.py` had 3 wrong role-code labels, sourced from ARC's own
  `role_name` field (not guessed) — **APF = "Australian Professorial Fellowship" (senior, NOT
  postdoctoral)**, ARFI = "Australian Research Fellowship (**Indigenous**)" not "Industry", IRF =
  "Indigenous Research(er) Fellowship" not "Industry Research Fellowship". Comments fixed;
  `KEEP_ROLES` membership unchanged (all three were already included, just mislabeled).
- **True early-career fellowship cohort = DECRA + APD + APDI only** (APF excluded — it's senior).
  4,247 persons, 4,228 linked to OpenAlex (99.6%). APF (240 persons, 239 linked, 99.6%) is a
  separate, senior cohort — do not include in ECR-focused analysis.
- **27-person unlinked-cohort manual review** (before the scope correction, so included 1 APF
  case — Peter Saunders, correctly excluded now rather than resolved): resulted in 20 new
  `data_persisted/manual_resolutions.csv` entries (5 resolve, 15 unlink) + 2 new
  `data_persisted/manual_merges.csv` entries (Veronica Lauck/McBain — announcement→current name
  change, confirmed via `inv_source`; Ya-Feng Yang/Yafeng Yang — hyphenation false split, the
  no-hyphen twin already had the correct ORCID). Notable patterns: several "resolve" targets are
  explicitly flagged as OAX-side contaminated entities (OpenAlex merging 2+ real people under one
  author id — e.g. Peter O'Sullivan, Jung-Ho Yun) resolved or rejected on a case-by-case basis
  after checking institution/topic corroboration, not resolved_by score alone. See
  `manual_resolutions.csv` notes dated 2026-08-08 for full reasoning per case.
- Re-ran `01→03→04` after these changes: 23,049 persons, resolved 22,679 (98.4%).

## Fellowship Cohort Status (2026-06-13)
- **FF** (Federation Fellows): 141/141 resolved ✓
- **FL** (Australian Laureate Fellows): 277/277 resolved ✓
- **FT** (Future Fellows): 277/277 resolved ✓ — 23 manual entries added; blocking failure categories catalogued: Unicode apostrophe (O'Neil→oneill), space-vs-hyphen surnames (gonzalez tokman), ø stripping (krabbenhft), ue-transliteration (rueger), given-name alias (Jenny/Yingzi), first_name_canonical token-length (xu-feng→feng), name-order reversal (Swaminathan/Vishwanathan), patronymic-vs-publication-name (Lakshminarasimha/Gubbi)
- **APDI** (ARC Postdoctoral Industry): 80/80 resolved ✓ — 3 manual entries added
- **CI-DORA**: 80/80 resolved ✓ — 1 manual entry (Jennifer Hocking, ORCID not in OAX)
- **APF**: 239/240 — 1 unresolvable (art theorist turned drama director, no OAX presence)
- **APD**: ~480/495 — 31 unlinked investigated: 18 resolved, 13 genuine unlinks; 15 manual unlink entries

## Important: cluster_id vs scheme membership
Most FF/FL fellows also hold DP grants; their `cluster_id` starts with "DP" not "FF"/"FL".
To find all clusters for a scheme, search `grant_ids` in arc_persons, or use
`arc_grant_cluster_map.parquet` (output of 01) which maps every grantID_personName → cluster_id.

## 03_link_arc_oax.py Design (updated 2026-05-25)
- Given-name comparison uses HumanName cascade: compound (f+m) → first → cross (f=other's m) → initials
- Blocking: `(family_name_main, first_initial)` + two middle-initial cross-blocking rules + ORCID
- Cross-blocking catches cases like ARC "Z Smith" vs OAX "Herb Z Smith" (middle initial match)
- HumanName on `full_names` (ARC longest) and `full_name` (OAX display_name) for consistent parsing
- Compound match "shi xue" = "shi xue" fixes Chinese compound given names (e.g. Shi Xue Dou)
- TF adjustment on `first_name` (hn.first) exact level only

## 01_prepare_arc.py Design (current, 2026-06-17)
Post-Splink steps in order:
1. `_merge_by_orcid`: collapse clusters sharing same ORCID → canonical = `min(cluster_ids)`
2. `_split_orcid_conflicts`: split any cluster with 2+ distinct ORCIDs
3. `_split_multi_name_clusters`: split on disjoint coinvestigator+FOR signals; skips single-ORCID clusters
4. `_apply_manual_splits`: apply `data_persisted/manual_splits.csv` confirmed splits
5. `_apply_enriched_orcids`: promote high/au_match ORCIDs from `orcid_enrichment.parquet`; checks `enrichment_blocklist.csv`
6. `_promote_low_by_for`: promote low-confidence enrichment candidates via ERA FOR token overlap
7. `_apply_manual_orcids`: inject verified ORCIDs from `data_persisted/manual_orcids.csv`
8. `_merge_persons_by_orcid`: post-enrichment dedup — merge clusters sharing same ORCID; skips first-initial mismatch
9. `_apply_manual_merges`: apply `data_persisted/manual_merges.csv` confirmed same-person pairs
10. `_merge_same_grant_coinvestigators`: **NEW** — same blocking key on same single-org grant → auto-merge;
    uses `grants_flat.n_eligible_orgs`; ORCID-conflict guard; 61 absorbed in current run

Output columns in `arc_persons.parquet`:
- `orcid_status`: `HAS_ORCID` | `NO_ORCID` | `MULTI_ORCID`
- `resolution_status`: `RESOLVED` | `UNRESOLVED` (driven by `_is_suspicious`)
- `orcid_for_codes`: ERA FOR codes derived from ORCID works via for_cache
- `gap_candidates`: compatible same-blocking-key clusters not yet merged (for review)
- `reliability_tier`: `1a`/`1b`/`1c`/`2`/`3`/`4`/`4u`
- `cluster_history`: JSON list of events — `splink_cluster`, `orcid_merge`, `orcid_conflict_split`,
  `name_split`, `manual_split`, `enriched_orcid`, `manual_orcid`, `same_grant_merge`, `manual_merge`

**data_persisted/manual_orcids.csv** — 12 entries (verified ORCIDs for clusters missing one):
- DP0451043_SusanOConnor, DP0343994_TerenceONeill, DP0452137_JefferyMalpas,
  DP110100881_TheodorusSloots, DP170102529_PhilipBland, DP0209045_FrankPate,
  DP130101651_LanfengDong, DP0774201_StephenBell, DP110100091_MichaelAdams,
  DP0556160_MichaelHooker, DE220100417_JathanSadowski, DE200100121_BenScheele

**data_persisted/manual_merges.csv** — 11 entries (same-grant nickname pairs + cross-grant confirmed same-person)
**data_persisted/enrichment_blocklist.csv** — 2 entries: LP0220171_JNichols, DP0452211_RobertMarks

## `AwardsCIF()` rebuild (2026-08-11 → 2026-08-13, roadmap step 1 complete)
`src/utils/awards_cif.py` — a dataclass-first rebuild of the above design, not a wrapper around it.
`AwardCIFItem`/`AwardsCIF` dataclasses defined first; the ten Phase-2 steps above are reimplemented
as functions taking/returning `list[AwardsCIF]` (`merge_by_orcid`, `split_orcid_conflicts`,
`split_multi_name_clusters`, `apply_manual_splits`, `apply_enriched_orcids`, `promote_low_by_for`,
`apply_manual_orcids`, `merge_persons_by_orcid`, `apply_manual_merges`,
`merge_same_grant_coinvestigators`, composed by `refine_clusters()`), same validated logic and step
order, new representation. Splink itself (both the ARC-internal `dedupe_only` run and the ARC↔OAX
`link_only` run) is reused unchanged as a tool — only the surrounding pandas architecture is
rebuilt. Full design and rationale: `/home/lc/.claude/plans/review-this-code-base-groovy-key.md`.

**Roadmap step 1 (ARC-side identity + candidate `author_idx` set) is DONE.** Full chain:
`load_award_cif_items() -> cluster_items() -> refine_clusters() -> set_aside_indigenous_research()
-> populate_oax_candidates() -> dedup_oax_candidates() -> compute_orcid_for() ->
compute_gap_candidates() -> compute_reliability()`. `populate_oax_candidates()` reads
`arc_oax_links.parquet` directly (all Splink candidates ≥0.5, not just high-confidence ≥0.9) —
avg 6.64 candidates/person vs. the old hardcoded-empty-`secondary_oax_ids` view's 5.02 —
`dedup_oax_candidates()` then lexically strips OpenAlex's own split-record duplicates (manual
`unlink` + same-ORCID/same-topic collapsing, 10,258/22,852 AwardsCIF touched). `tests/test_awards_cif.py`
(38 tests) covers every pure function. Field-for-field diff against production `arc_persons.parquet`:
≥99.15% match on every field (100% on `family_names`/`resolution_status`), residual traced to
Splink's own documented run-to-run clustering stochasticity, not a logic gap.

**Roadmap step 2 (each AwardsCIF's own work-set) is DONE** — see "oeuvre_build.py" section below.

**data_persisted/manual_name_corrections.csv** — new file, same hand-curated-override convention as
`manual_orcids.csv`/`manual_merges.csv`, keyed on `unique_id` (item-level, applied before
clustering) rather than `cluster_id`. Two confirmed ARC-source typos so far, both found via the same
pattern (a minority name-form disagreeing with an otherwise-unanimous majority, confirmed directly
against `raw_json.csv`'s `investigators-at-announcement` vs `investigators-current` fields for the
one grant where the disagreement lives, not guessed): `LP0211975_MarieMalherbe` (→ François,
ORCID 0000-0001-6127-4169) and `DP0451513_MartinNakata` (→ Nicholas, ORCID 0000-0002-0979-8253).
**Explicitly not the fix for every name-variant found this way** — a broader scan (147 candidate
ORCIDs with incompatible name forms under the same family name) turned out to be overwhelmingly
genuine variation (married/maiden names, hyphenation, nicknames, compound-name field-order
inconsistency — e.g. Trevor→David Waite is a real, ARC-confirmed name change spanning a whole career,
not a typo) that `AwardsCIF.full_names`/`first_names` already retain correctly as sets — no
single-field correction needed or wanted for those. Only genuine, single-record, ARC-self-corrected
typos belong in this file.

**Two real gaps found while investigating `populate_oax_candidates()` (unresolved, see the plan file
for full detail)**: (1) `04_resolve_links.py`'s `secondary_oax_ids` is hardcoded to `[]` for any
`arc_id` resolved via `unique_hc` (the single largest resolution bucket), regardless of what else
scored 0.5–0.89 in `arc_oax_links.parquet` for that same person — quantified at 2,174/7,960 (27%)
`unique_hc` persons having at least one such hidden candidate, 5,901 hidden pairs total. Whether
these are mostly genuine or mostly noise needs OpenAlex-side characterization, not yet done.
(2) Both `01_prepare_arc.py` and `03_link_arc_oax.py` block on a single reduced `first_initial`
value even when a cluster's own evidence contains a full given name — and first-initial preservation
across genuine nicknames is close to a coin flip (Michael/Mike preserves it, William/Bill and
Anthony/Tony and Elizabeth/Libby do not), so a real nickname match lacking a shared ORCID can be
structurally unreachable by blocking, never merely filtered by scoring. Proposed fix (not built):
an additional, tighter blocking rule on the fuller given-name form when available, alongside (not
replacing) the existing `first_initial` fallback.

## FOR-code handling rebuilt on Resolver()-derived FOR2020 data; a real production bug fixed (2026-08-13)

Triggered by `apply_subfield_filter()` (see "oeuvre_build.py" below) being badly over-aggressive
on real data — became a much larger fix once traced to its root cause.

**The bug**: `01_prepare_arc.py`'s `is_suspicious()` cross-division check (feeding
`resolution_status`/`gap_candidates`) had been finding **zero** mismatches across the entire ARC
population — verified directly (old approach=0 flagged, new approach=179, same population) — not
because researchers rarely span multiple divisions, but because its `for_names` lookup against
`data_persisted/for_divisions.csv` silently failed on **casing alone** for ~79% of clusters (ARC's
raw data is sentence-case, e.g. "Health services and systems"; the CSV was title-case). Even where
it resolved, the CSV's letter-keyed divisions (A-W) didn't correspond 1:1 to FOR2020's own numeric
divisions (10/23 letters each spanned ≥2 different FOR2020 divisions). And even ANZSRC's own
numeric divisions were the wrong granularity for this check's purpose: "Legal systems" (division
48) and "Political science" (division 44) are administratively distinct but resolve to the same
OpenAlex **field** "Social Sciences" — OAX_FIELD (~25 categories, content-derived) is what the
check actually needed, not ANZSRC's own division (~22 categories, funding-administration
boundaries). `data_persisted/for_divisions.csv`/`for_adjacent_divisions.csv` archived to
`ZARCHIVE/data/` (`git mv`) once nothing referenced them.

**The fix, applied to both `awards_cif.py` and production `01_prepare_arc.py`** (the bug was live
in what `03`/`04`/every analysis script consume today, not just the parallel rebuild):
- `AwardCIFItem.for2020_codes`/`AwardsCIF.for2020_codes` (+ the equivalent `01_prepare_arc.py`
  `persons` column): every field-of-research entry ARC recorded per grant (`raw_json.csv`'s full
  list — 2 to 16 entries/grant — not just `grant_summaries.csv`'s single primary), resolved via new
  `for_resolve.resolve_arc_for_entry()` (maps ARC's own `RFCD98`/`FOR08`/`FOR20` type labels to the
  package's `FOR1998`/`FOR2008`/`FOR2020` scheme names — confirmed `RFCD98` = the package's
  `FOR1998` by resolving real codes through it, not assumed). Codes stay strings throughout (~44K
  real entries have a leading zero `int()` would silently drop). Ordered primary-first then
  alphabetically; deduped by resolved 4-digit FOR2020 group per grant (6-digit codes truncate
  cleanly to their parent group — verified against the package's own group table).
- `src/utils/cluster_checks.py` rebuilt around this — `for2020_primary_fields()`,
  `division_mismatch_for2020()`/`_pairwise()`, `is_suspicious_for2020()`, `aggregate_for2020_codes()`
  — shared by `01_prepare_arc.py`, `01a_diagnose.py`, and `awards_cif.py` so the two pipelines can't
  drift apart. Old `load_for_divisions()`/`division_mismatch()`/`is_suspicious()` removed outright.
- **Indigenous-focused research is set aside from the working population** (FOR2020 division 45 —
  "Indigenous Studies" — as a *primary* code on any grant): culturally important, not well
  portrayed by the bibliometric methods this pipeline uses, so kept out entirely rather than run
  through them — `AwardsCIF.excluded`/`.excluded_reason` in the rebuild, a companion
  `arc_persons_excluded_indigenous.parquet` in production (107 persons).
- `_load_grant_for2020_codes()`/`load_grant_for2020_codes()` scoped to `KEEP_SCHEMES` before
  resolving anything — `raw_json.csv` covers every ARC scheme ever run, including out-of-scope
  ones like "LE" (Linkage-Equipment, funds shared lab equipment across a whole department, so
  carries person-irrelevant FOR-code spreads — one such grant had 16 different codes).
- `01_prepare_arc.py` fully migrated, not left on the old broken check: Phase 1 attaches
  `for2020_codes` per grant; four separate hardcoded merge-aggregation call sites needed the new
  field added to their field lists (a real bug the first full run caught directly —
  `TypeError: 'float' object is not iterable` — not anticipated in advance).
  `arc_persons.parquet` regenerated: **22,942 persons** (was 23,049 — mainly the 107 Indigenous
  exclusions). `01a_diagnose.py`'s A2/A3 cross-check now genuinely agrees (172=172, identical
  cluster lists) instead of accidentally agreeing at zero. `03_link_arc_oax.py`/`04_resolve_links.py`
  confirmed schema-compatible by direct column-usage inspection — **not re-run**, since neither
  consumes `resolution_status`/`gap_candidates`/`reliability_tier` at all.

Committed and pushed (`0db3f64`).

## `oeuvre_build.py` — roadmap step 2, giving each AwardsCIF its own work-set (2026-08-13)

`src/utils/oeuvre_build.py` — sibling to `awards_cif.py`, mirrors the `dossier.py`/`dossier_build.py`
split (data model vs. construction). Unions the OpenAlex works reached by every candidate
`author_idx` in `AwardsCIF.oax_candidates`, then decides per work whether it belongs in this
person's oeuvre. Only over-merge is addressed (under-merge stays explicitly out of scope, as
throughout this project); no `AwardsCIF` is ever compared against another.

Pipeline: `fetch_candidate_oeuvre -> apply_deterministic_filters -> dedup_oeuvre ->
apply_subfield_filter -> score_institution_coherence -> score_coauthor_arc_corroboration ->
score_identity_clusters`, composed by `build_oeuvre()`. New `CandidateWork` dataclass on
`AwardsCIF.oeuvre` (one list, included-or-not, not two parallel lists — mirrors the
`AwardCIFItem`-inside-`items` precedent).

- **Deterministic filters** (hard excludes, never scored): disallowed `type`/filename-artifact
  titles (`exclusions.py`, reused unchanged), implausible/future/missing year
  (`dedup.MIN_PUB_YEAR..MAX_PUB_YEAR` = 1950–2026, **not** 2000–2024), missing DOI, corrupt
  authorship (any authorship row on the work — own candidate's or a coauthor's — with a null
  `author_idx`/`institution_idx`), missing source for `type='article'`. A `raw_orcid`-based
  corruption filter was directed but dropped this pass — checked directly, no authorships table
  variant this pipeline reads (`compact`/`xpac`/`xpac_raw`/the manually-regenerated
  `authorships_hep.parquet`) carries a `raw_orcid` column at all; future work needs new extraction.
- **Title-dedup**: `dedup.create_deduped_works()` reused unmodified (preprint/published-version
  merges); a *different* case — 2+ of a cluster's own candidate `author_idx` claiming the *same*
  `work_idx` — is collapsed earlier, at fetch time, before dedup ever runs.
- **Field filter** (`apply_subfield_filter()`, name kept for history): compares
  `CandidateWork.field_name` (OpenAlex's own field-level classification, no resolution needed)
  against `cluster_checks.for2020_primary_fields()` (OAX_FIELD, from the person's own primary
  FOR2020 codes) — **not** the original design (literal subfield-string match against a single
  primary `for_code`), which was built, found badly over-aggressive on real data (one geochronologist
  lost 158/160 works), and led directly to the FOR2020/OAX_FIELD rebuild above.
- **Soft signals** (`institution_arc_match`, `coinvestigator_match`, `identity_cluster` via
  `analysis/utils/identity_clustering.py`, widened to `own_author_idxs: set[int]` with a real
  correctness fix — own-candidate co-occurrence on one work must not masquerade as an external
  corroborating coauthor edge — plus a new in-memory entry point avoiding one DuckDB query per
  cluster): recorded on every work, **not** used to gate inclusion this pass — combining them into
  a weighted score needs empirical calibration against known contamination cases, not done.

**Verified** (stratified random sample, 40 clusters by candidate-pool size — not the specific
historically-documented contamination cases, whose exact `cluster_id`s weren't reliably available
this session, an acknowledged gap): per-work inclusion rate drops monotonically with pool size —
**38.1%** (1 candidate) / **19.5%** (2-5) / **9.3%** (6+) — the expected signature of stripping
wrong-candidate noise, not uniform over-exclusion. Stage breakdown on the combined sample: 53.9%
survive deterministic filters, 52.2% additionally survive dedup, 12.3% survive the field filter
(the dominant exclusion source — even on the 1-candidate stratum it costs ~17.3% of raw works, a
real false-positive rate from OAX_FIELD coarsening, e.g. a genuine psychology+linguistics career
still splits across two fields — accepted per this project's precision-over-recall stance, not
hidden). `WeiWang`-scale mega-pools (374-436 candidates) measured at ~17.5s for
`score_identity_clusters()`'s O(n²) step on the single worst case — slow but survivable; total cost
across every such outlier at full 23K-cluster scale not measured, a real open risk before an
unattended full run. `tests/test_oeuvre_build.py` (27 tests, the four pure-logic functions) +
`analysis/tests/test_identity_clustering.py` (14 tests, `identity_clustering.py`'s first ever).

Not built this pass, deliberately: combining the soft signals into a weighted inclusion score
(needs calibration), roadmap step 3 (dropping zero-work `author_idx`), step 4 (the
definitive-evidence gate, `Dossier()` construction by selection).

## Analysis Pipeline Status (2026-06-18)
- `analysis/01_fetch_oeuvres.py` ✓ — 4,236,839 rows, 22,599 persons
- `analysis/02_accuracy_check.py --full` ✓ — 0 over/under-coverage flags; 105 shared author_idx (B3 cross-grant, not errors); 11,604 year flags + 41,835 domain outliers written to work_flags_full.csv
- `analysis/03_annual_metrics.py` ✓ — annual_metrics 544,627 rows/22,586 persons; collab_metrics 4,915,067 rows/22,250 persons
- `analysis/06_analyse_fellowships.py` — plot 2 updated: median of active publishers (not mean+zeros); DECRA bug fixed (role_code `DECRA` not `DE`); award_year >= 2015 filter added to trajectory plot

### 2026-08-08 rerun, post Jul26-snapshot migration
Re-ran `01_fetch_oeuvres.py --full`, `02_accuracy_check.py --full`, and `03_annual_metrics.py`
against the new Jul26 OpenAlex data (see "OpenAlex Snapshot Migration" above) to confirm each
script's path/schema fixes work end-to-end and that data quality held up against the newer,
larger source.
- `01_fetch_oeuvres.py` ✓ — 4,263,042 rows, 22,672 persons, 3,149,390 unique works (48s) —
  comparable scale to the 2026-06-18 baseline (4,236,839 rows / 22,599 persons), as expected
  given the corrected 23,049-person cohort and the larger Jul26 author pool.
- `02_accuracy_check.py --full` ✓ (10s) — 0 over/under-coverage flags (unchanged); 116 shared
  author_idx (was 105 — same expected common-name-collision pattern, e.g. Andrew Martin/Wei
  Wang/Paul Thomas/Jun Li, not new contamination); 10,920 year flags + 45,679 domain outliers
  (was 11,604 + 41,835, comparable scale). New in this run: 143,122 duplicate title+year groups
  written to `title_dupes_full.csv` (a diagnostic not present in the 2026-06-18 run's notes).
- `03_annual_metrics.py` ✓ (22s) — annual_metrics 546,076 rows/22,625 persons (was 544,627/22,586);
  collab_metrics 5,116,507 rows/22,288 persons (was 4,915,067/22,250) — comparable scale,
  confirms the `AUTH_GLOB` → `OPENALEX_COMPACT_DIR` path fix works for the raw authorships scan.
- `04_au_baseline.py` ✓ (9s) — 3,382,340 AU-affiliated works found; sanity check AU n_pubs
  86,187→164,835 and WLD n_pubs 8,084,109→11,439,994 (2010→2020) both look sensible in shape
  and magnitude — full-corpus (non-ARC-filtered) scan of `compact/authorships` + `compact/works`
  confirms the path fix works at whole-snapshot scale, not just the ARC-filtered joins above.
- `04b_citation_quantiles.py` ✓ (47s) — 936 (field × year) rows, 26 fields; then re-ran
  `03_annual_metrics.py` to backfill — `n_highly_cited` now populated for all 546,076 rows
  (was stale from a 2026-06-15 run against the old Feb26 snapshot).
- **Conclusion: no data-quality regression from the OpenAlex migration.**

## Next Priority (start of next session)
Analysis pipeline complete as of 2026-06-18. Pipeline improvement TODOs below.

**Pending code TODOs:**
- 03 Splink inst comparison: when all ARC grants are single-org (`all_single_org` bool in arc_persons), give strong negative weight to inst_arr mismatch (requires conditioning Splink comparison level weights on this flag)
- ~~6-digit FOR → OAX topic field score~~ — **dropped, not applicable**: `research_classification`
  explicitly refuses FOR-family → `OAX_TOPIC` (raises `ValueError`; the finest reachable from a
  FOR input is `OAX_SUBFIELD`, which `_field_score` already uses via `for_resolve.oax_subfield_name()`).
  Also moot against actual data — ARC's own `for_code` is 4-digit only (`^\d{4}` regex in
  `01_prepare_arc.py`), so no 6-digit FOR code exists here to resolve from.
- Refactor cluster to dataclass with stable opaque id and explicit provenance fields
- Refactor 00b to target arc_persons (resolution_status==RESOLVED, orcid_status==NO_ORCID)
- Cross-grant B3 rule: same blocking key + shared co-i + same admin_org → auto-merge (catches Jun Li)
- Complete 00b run: 7,084 ARC-ORCID records still need fetching (running 2026-06-18; was 11,566)
- Strengthen reliability_tier: add ARC for_names vs orcid_for_codes agreement signal for HAS_ORCID clusters
- **Decide on `xpac`/`is_xpac`** (2026-08-08): `/home/lc/k/openalex_jul26/parquet_converted/`
  (path as of the 2026-08-14 drive consolidation; was `/home/lc/m/...` when this note was written)
  has `xpac/` and `xpac_raw/` directories alongside `compact/`, mirroring the same table set
  (authorships, references, work_abstracts, works, work_sdgs, work_topics) — **not currently
  read by anything in this codebase** (`analysis/01_fetch_oeuvres.py` and everything else only
  reads `OPENALEX_COMPACT_DIR`). Row counts differ meaningfully (`compact/works` 317.8M vs
  `xpac/works` 192.6M — not a duplicate), and `xpac/work_topics` was modified *after*
  `compact/work_topics` on the same day, suggesting it may be a newer or still-in-progress
  extraction batch. Needs a decision: merge into `compact/`, read alongside it, or leave
  untouched — deferred, revisit before treating any future oeuvres fetch as complete/final.

## Manual Resolution Techniques (Not Yet Automated in Pipeline)

### Nickname / informal-name variants
Many Australian researchers publish under informal given names not recorded in ARC data.
The pipeline has no lookup table for these. Common patterns seen:
- Bill = William, Tony = Anthony, Beth = Elizabeth
- Geoff = Geoffrey, Greg = Gregory, Cris = Christiaan
- Tim = Timothy, Chris = Christiaan, Rob = Robert
**Potential pipeline addition**: a nickname expansion table applied to `first_name_canonical`
during blocking (add both canonical and common nicknames as candidate first_initials).

### Full-OAX surname search for no-affiliation records
Researchers absent from `openalex_authors_prep.parquet` (AU-filtered) because their
`last_known_institutions` is empty in the Feb26 snapshot. Manual path: scan raw OAX authors
parquet by family name + field topic to find the record, then add to manual_resolutions.csv.
Cases this session: BrienNorton (A5111895832), FrederickRavenhill (A5002864862).
The pipeline cannot rescue these automatically — they are simply not Splink candidates.

### Chinese compound given-name parsing failure
OAX `display_name` "Wing Kong Chiu" → HumanName parses `first='Wing', middle='Kong'` but the
prep code extracts `first_name='kong'` (middle used as first) → `first_initial='k'` not `'w'` →
wrong blocking key → no Splink candidates generated.
**Pattern**: OAX display_names of form "GivenA GivenB Surname" where GivenA+GivenB is a Chinese
compound given name. HumanName may vary in which part it treats as first.
**Potential fix**: add a cross-blocking rule on middle_initial in 03_link_arc_oax.py (partially
done already) or detect compound-given-name patterns and index both initials.

### ORCID mismatch / OAX entity-disambiguation errors
In rare cases OAX has merged two real people into one record (or reassigned an ORCID to a wrong
record). Symptom: OAX `display_name` doesn't match the ORCID holder's actual name.
Example: A5091677854 had display_name="Randal Douc" but ORCID 0000-0003-3910-9495 belongs to
Arnaud Doucet. Cross-check via ORCID.org or raw `raw_author_names` list in OAX.

### Sub-HC rescue (now automated)
The name-filter step in `04_resolve_links.py` rescues arc_ids with only sub-HC candidates
(0.7–0.9) when character-mismatch filtering leaves exactly one compatible candidate.
`resolved_by='name_filter'` (24 cases in current run). Already in pipeline.

## Splink Design Decisions
- **`arrays_to_explode` is NOT supported** in EM training sessions
- **Prior inflation risk**: second EM session on family-name block causes false merges.
  Fix: one ORCID-based EM session + `estimate_probability_two_random_records_match(recall=0.8)`
- **Cluster threshold**: 0.9 (high precision, prefer splitting over merging)
- **Seeded m_probabilities** for orcid comparison (can't train from ORCID-blocked EM)

## Known Issues in 02 Output
- **Raymond Gilbert / Robert Gilbert** (n=34, no ORCID): different first names + 3 different
  fields — suspected mis-merge of 2–3 people
- **Paul Young** (n=28, no ORCID): added to `data_persisted/manual_splits.csv` (confirmed_different_people=True);
  splits by institution into UQ virologist / USyd pharmacologist / Monash engineer / UNSW (crop) groups.
  Re-run 01_prepare_arc.py to materialise sub-clusters, then add per-sub-cluster manual resolutions.
