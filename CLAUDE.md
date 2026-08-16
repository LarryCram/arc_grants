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
- **Group-level ACIF-membership gate for oeuvre_build.py, not yet built** (2026-08-16, see
  "Group-level ACIF-membership gate design" below for the full derivation): (1) when an ACIF has
  an ARC-recorded ORCID, check it directly against each candidate `author_idx`'s own OpenAlex
  `orcid` field *before* any signal-based reasoning -- a direct identity match settles which
  group is correct far more decisively than `group_coherent`/`hep_match` inference, and should be
  the first check attempted, not a fallback. (2) For groups without a direct ORCID match, a
  size-conditioned rule: a *large* group (exact threshold not yet fixed -- discussion landed near
  20 works as a starting candidate, not empirically finalized) with zero `hep_match=True` works
  is strong evidence of being the wrong candidate for this ACIF, independent of how internally
  coherent that group is with itself. (3) `group_coherent`, once a group's identity is otherwise
  confirmed (via ORCID or the HEP-based group gate), still has a real, separate job: catching
  individual misassigned works *within* an already-correctly-identified group (OpenAlex's own
  disambiguation can misattribute specific papers into an otherwise-correct, ORCID-verified
  `author_idx` -- ORCID confirms the group, not every work in it). Two confirmed real contamination
  cases motivate this (Wei Wang, documented separately above; Mohammad Tariqul Islam, see below) --
  `LP0560280_XiaolinWang` and `LP120200066_XinhuaWu` are suggestive but not yet confirmed either
  way, worth investigating with the same discipline once this gate exists.

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

## `is_suspicious_for2020()` ORCID-bypass bug found and fixed; empirical division-pairs whitelist added (2026-08-16)

Found while investigating whether roadmap step 3 addressed anything real (it doesn't, see
`oeuvre_build.py`'s notes) — a spot-check of the largest OAX-candidate-pool `AwardsCIF`
(`DE230100180_WeiWang`, 374 candidates) turned out to itself be a confirmed **ARC-side mis-merge**
of at least 4 different real people, same class as the Raymond/Robert Gilbert and Paul Young cases
above but far more consequential to find, since it exposed a real, previously-undetected bug.

**The false merge**: 9 grants (`DE230100180`, 2023 DECRA + 8 `DP`/`LP` grants spanning 2008–2021)
were clustered as one person under the literal name "Wei Wang." Only 1 of the 9 grants carries an
ORCID (`0000-0001-5788-6314`); the other 8 have none. External biography + PhD-year confirmation
(PhD 2019, City University of Hong Kong / USTC, joined UNSW 2019, DECRA 2022/2023, now RMIT —
fire-safety-materials engineering) proves the ORCID-holder cannot be CI on any grant funded before
2019 — ruling out all 8 other grants definitively. Grant-by-grant breakdown resolves cleanly into
**4 people** using nothing but ARC's own admin_org/funding-year/FOR-division data: the real DECRA
holder (UNSW→RMIT, div 40 Civil Engineering); a 6-grant 2008–2021 UNSW Information-Systems senior
CI (div 46); an isolated 2009 UQ grant (also div 46, different institution — likely a 5th person or
an early pre-UNSW career step, unconfirmed); and a 2016 Curtin Chemical-Engineering grant (div 40,
different institution). Root cause of the original merge: all 9 records share the byte-identical
name "Wei Wang," so Splink's name-comparison features had zero discriminating power — the entire
merge decision rested on institution/FOR-token overlap being weighted too weakly to overcome a
perfect name match (same class of gap as the existing pending TODO re: conditioning Splink's `03`
institution comparison on `all_single_org`, here showing up in the ARC-internal `01` dedupe too).

**The bug**: `cluster_checks.is_suspicious_for2020()` had `if len(orcids) > 0: return False` as its
*first* condition — any cluster with even one ORCID, on even one of many grant records, was fully
exempted from the cross-division suspicion check, regardless of how many other records (with no
ORCID at all) it contained. This conflated two different claims: "two clusters sharing an ORCID
should merge" (the project's genuine, correct, long-standing rule) vs. "a cluster containing even
one ORCID-bearing record is verified as a whole" (never a justified inference — the other 8 records
were clustered by Splink's name/institution/FOR-token blocking alone, independent of the ORCID).
`01a_diagnose.py`'s A3 check calls the same shared function, so it inherited the identical blind
spot. **Fix: the ORCID bypass was removed entirely**, not narrowed to a coverage threshold — ARC's
own investigator-level `orcid` field turns out to be a *current, backfilled* snapshot rather than a
point-in-time capture (confirmed directly: 11,095 of 33,835 in-scope ORCID-bearing investigator
records, 32.8%, sit on grants funded before 2012, ORCID's actual launch date — spot-checked several
real cases, e.g. Michael Kalish's DP0208035 (UWA, 2002) cross-references cleanly against his own
real ORCID employment history showing UWA through May 2002 then a move overseas — so a coverage
threshold would have been the wrong fix, chasing a false signal). `is_suspicious_for2020()` no
longer takes an `orcids` parameter at all (dead after the removal; all 5 call sites across
`01_prepare_arc.py`/`01a_diagnose.py`/`awards_cif.py` updated).

**Removing the bypass outright was only safe because of a second, larger fix alongside it**:
`division_mismatch_for2020()` gained `ACCEPTABLE_DIVISION_PAIRS` — an empirically-derived whitelist
of FOR2020 2-digit division pairs that legitimately co-occur in real single-person careers, added
after the user raised a concrete confound: ARC applicants sometimes deliberately spread proposals
across different FOR divisions (e.g. PHYS/ENG, CHEM/ENG, MATH/IT) specifically to be assessed by
different panels — a real, named tactic, not hypothetical — so a naive "any cross-division spread is
suspicious" rule would over-flag many genuine people. Rather than hand-curate a whitelist, it was
computed: from every ORCID-confirmed (single ORCID present on 100% of the cluster's own grant
records — the only trustworthy ground-truth subset) multi-grant, non-excluded cluster's own
primary-FOR2020-division pairs, tested against a null model of "two people, each drawn from the
population's own single-division base rate, happened to be merged" (z = (observed − expected) /
sqrt(expected), expected = 2 × p(A) × p(B) × n_confirmed — the population base rate p(div) computed
from *all* in-scope grants, unconditioned on clustering outcome, not from the already-selected
cross-division subsample, which would have biased common divisions upward). Threshold z ≥ 3.4
approximates a Bonferroni correction for the ~171 possible division pairs tested at 0.05
family-wise. **41 pairs cleared the bar** — see `cluster_checks.py`'s `ACCEPTABLE_DIVISION_PAIRS`
for the full list and module docstring for the full derivation. All three of the user's named
example pairs came out clearly above threshold (Chemical Sciences/Engineering z=9.25,
Engineering/Physical Sciences z=6.51, Information-Computing-Sciences/Mathematical Sciences z=5.98),
independently validating the method against real domain knowledge; the pair implicated in the Wei
Wang case itself (Engineering/Information-Computing-Sciences, 40/46) did **not** clear the
threshold — the tolerance list does not undermine the very case that motivated building it.

**A real methodological bug was caught and fixed mid-derivation**: the first computation of this
list accidentally included the 107 Indigenous-focused `AwardsCIF` that
`set_aside_indigenous_research()` marks `excluded=True` but deliberately leaves in the returned
list (callers must filter on `.excluded` themselves, per that function's own docstring) — the
survey's own SQL query never added that filter. Caught because the user was surprised division 45
appeared at all ("it was meant to be filtered away at the ARC stage"). Fixed by restricting both the
confirmed-cluster population and the base-rate population to non-excluded grants; division 45 is
now absent from every whitelisted pair, as it should be.

**`division_mismatch_for2020()` widened to 3+ divisions** (same day, same session): the
exactly-2-division-only version was flagging real, single, highly prolific researchers purely for
having a broad research career spanning 3+ divisions — e.g. a 27-grant plant ecophysiologist
(divisions 30/31/40/41, Agricultural/Biological/Engineering/Environmental Sciences) — even though
most of their individual division pairs (30-31, 30-41, 31-41) were each independently whitelisted.
Widened to check that *every* pairwise combination among a cluster's divisions is whitelisted, not
just requiring exactly 2 divisions total. **Known, accepted residual limitation, explicitly flagged
by the user**: this doesn't fully rescue cases where even one division among several doesn't
pairwise-whitelist with the rest (in the plant-ecophysiologist example, Engineering (40) doesn't
pairwise-whitelist with 30/31/41 at all, so that case is still flagged even after widening) — and
more fundamentally, checking "all pairwise combinations whitelisted" cannot distinguish one real
person from two *different* people wrongly merged whose individual division-sets happen to combine
into an all-pairwise-whitelisted union (person A in {30,31}, person B in {31,41}, merged union
{30,31,41} would pass). This exemption only protects against one specific false-positive pattern; a
high grant count is not itself evidence of one person (a common name can accumulate many grants from
several real people as easily as one prolific one) — the check does not replace the rare-name gate,
ORCID-based checks, or manual review as the actual mechanism for catching a wrongful merge, and
`is_suspicious_for2020`/`division_mismatch_for2020` are never the last word regardless: "suspicious"
only ever means "surfaced for human review" (via `01a_diagnose.py`/`_export_manual_splits_template()`),
never an automatic split — only an explicit `confirmed_different_people=True` entry in
`data_persisted/manual_splits.csv` can actually change a cluster's structure.

**Population-level impact** (full `build_awards_cif_population()` re-run, 23,054 `AwardsCIF`):
`resolution_status` UNRESOLVED count went **178 → 231** (RESOLVED 22,869 → 22,823) — 53 net
additional clusters now correctly surfaced for review that the ORCID bypass previously silently
passed through as RESOLVED. (An intermediate exactly-2-division-only version produced 251
UNRESOLVED before the 3+-division widening rescued 20 of those as genuine false positives.)

**Not yet done, deliberately parked** (user: "let us park that — it can happen right at the end"):
`apply_manual_splits()` (both `01_prepare_arc.py`'s `_apply_manual_splits` and `awards_cif.py`'s
mirror) only splits a confirmed cluster by `institution_oax_id` — this is insufficient for Wei Wang
specifically, since 2 of the 4 real people (the DECRA holder and the info-systems senior CI) are
both at UNSW, so an institution-only split would still leave them merged together. No mechanism
currently exists for an explicit, finer-than-institution per-`grant_id` split assignment (the
existing `manual_splits.csv`/`manual_splits_hand_counts.csv` schemas don't carry one — the code
hardcodes the institution split key regardless of CSV content beyond the boolean
`confirmed_different_people` flag). Extending this, and actually splitting the Wei Wang cluster
itself, is deferred until later per the user's own explicit direction.

Tests: `tests/test_cluster_checks.py` — 18 new tests (`TestDivisionMismatchFor2020`,
`TestAcceptableDivisionPairs`, `TestIsSuspiciousFor2020`), 23 total in the file, all passing.
`tests/test_awards_cif.py`'s existing 49 tests unaffected and still passing.

## Group-level ACIF-membership gate design (2026-08-16, design only, not yet implemented)

Following the `group_coherent` redesign (commit above), random-sample spot-checks surfaced a
second, distinct problem: `group_coherent` correctly answers "is this work consistent with the
rest of its own candidate's work-set" (test b), but nothing in the pipeline actually tests
"does this candidate's whole work-set belong to this ACIF at all" (test a) at the *group* level --
only Stage 3's per-*work* field-level filter runs, which is far too permissive to catch a
genuinely coherent but entirely wrong group. `group_coherent` is not at fault here -- a wrong
group can be, and often is, real and internally coherent (it's someone else's real career), which
is exactly what it correctly reports.

**Confirmed case: `DP230101204_MohammadIslam`** (no ORCID, single 2023 UNSW grant, 85 candidates
in pool, 40 groups, dominant group 309 works). ACIF's declared primary FOR2020 code is "Materials
engineering" (4016), correct for the real grant-holder (Dr Mohammad S Islam, UNSW, composite
materials for cryogenic fuel tanks, fracture mechanics -- from the user's direct knowledge of his
current bio). The dominant 309-work group's `author_idx` (5100748842) was identified precisely:
it belongs to **Professor Mohammad Tariqul Islam, Universiti Kebangsaan Malaysia** (antenna/RF/
microwave engineering, his own verified ORCID 0000-0002-4929-3209, 1,308 works, 72 recorded
institutional affiliations with zero UNSW mentions anywhere) -- a real, different, fully-formed
OpenAlex identity pulled into the candidate pool because his name sometimes collapses to
"Mohammad Islam" in some papers' author-list formatting (an ARC<->OAX *linking* problem, a
different mechanism from the Wei Wang case, which was an ARC-internal Splink dedupe error).
`group_coherent` rated this wrong group 677/706 True overall -- correctly, since it *is* a
coherent body of work, just not this ACIF's. Checked directly: of all 40 candidate groups in this
mega-pool, every single one has zero `hep_match=True` works except one -- group `[5076009177]`,
36 works, 19 `hep_match=True` (53%) -- very likely the real Dr Mohammad S Islam.

**Suggestive but not confirmed: `LP0560280_XiaolinWang`** (no ORCID, 35 candidates, 24 groups).
Its 570-work dominant group mixes condensed-matter physics with pharmaceutical/biomedical
materials by title inspection; within it, `hep_match=True` works are disproportionately
Condensed Matter Physics (184) versus `hep_match=False/None` works being disproportionately
Biomedical Engineering/Polymers/Biomaterials -- consistent with a similar contamination pattern,
but a real ORCID found for "Xiaolin Wang" (institutions matching this grant's UOW) resolves to a
*separate* 2-work `author_idx` not in this cluster's candidate pool at all (an unrelated
under-merge gap, out of scope for this pipeline) -- doesn't itself confirm or rule out the
570-work group's own internal mix. `LP120200066_XinhuaWu` shares the same risk profile from the
same random sample, not investigated further.

**Design conclusion, reached through direct back-and-forth correction, not a single clean
derivation** -- recorded here precisely because it wasn't obvious in advance:

1. **When the ACIF has an ARC-recorded ORCID, check it directly against each candidate
   `author_idx`'s own OpenAlex `orcid` field first**, before any `group_coherent`/`hep_match`
   signal reasoning. This is a direct identity match, not an inference from a pattern, and
   settles which group is correct far more decisively when it's available. Verified on
   `DP220100261_VolkerHessel`: the ACIF's recorded ORCID (0000-0002-9494-1519) matches the
   dominant 379-work group's own `author_idx` (5003448404) exactly, at the OpenAlex author-record
   level, independent of any group-size or HEP-ratio inference. A population-level 2x2 of
   `group_coherent` x `hep_match` presence (54,859 evaluable groups) was explored as a possible
   general rule before landing here -- kept as background context, not a recommended mechanism:
   the relationship is real (incoherent groups show 75% zero-HEP vs. coherent groups' 47%
   population-wide) but far too diluted at small group sizes/low competition to be a clean gate on
   its own; it strengthens specifically in large, high-competition mega-pools (17.0% vs. 9.4% HEP
   presence at 16+ competing groups) -- exactly where a direct ORCID check, when available, is
   preferable anyway.
2. **For groups without a direct ORCID match** (the harder case -- no shortcut available), a
   *large* group with zero `hep_match=True` works is strong, close-to-structural evidence of being
   the wrong candidate: holding an ARC grant requires HEP institutional affiliation at the time,
   so a genuinely large real oeuvre should show *some* HEP overlap somewhere. This isn't a
   population-statistics claim needing extensive calibration -- it only needs a size threshold
   large enough that "zero HEP by chance" becomes implausible. Not yet fixed at a specific number.
3. **ORCID confirmation and `group_coherent` are not substitutes for each other.** An ORCID match
   confirms *which group* is correct; it does not confirm that *every work* OpenAlex attributed
   to that `author_idx` is correctly assigned (OpenAlex's own disambiguation can still misattribute
   individual papers into an otherwise-correct, ORCID-verified record). `group_coherent`, applied
   *within* an already-identity-confirmed group, is the tool for catching that -- confirmed
   consistent on Hessel's group (379/379 works coherent, no internal outliers detected).

Nothing above is implemented in `oeuvre_build.py` yet -- see the matching entry in "Pending code
TODOs" above.
