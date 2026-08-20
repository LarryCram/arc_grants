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
  `manual_orcids.csv`, `manual_merges.csv`, `manual_name_corrections.csv`, `manual_orcid_corrections.csv`,
  `enrichment_blocklist.csv`,
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

### 2026-08-20 full rebuild — name-parsing fixes + completed ORCID enrichment promoted end-to-end
Full `01→02→03→04→06_build_oeuvre.py→`piling rebuild, to actually apply everything landed this
session (see "ORCID Public API integration..." below for the name-parsing fixes themselves) rather
than leave it sitting in code with stale persisted output. Also finally promotes the completed
`orcid_enrichment.parquet` search phase (7,010/7,010, zero errors, see the follow-up above) into
production — the 73.1%-ORCID-coverage baseline this file has referenced as a target since
2026-06-17 was never actually reached until this run.

**Real pre-existing bug found and fixed along the way**: `02_prepare_oax.py`'s `oax_names` DuckDB
UDF registration declared a 2-field `STRUCT(first_names, family_names)` return type, never updated
when `oax_name_arrays()` gained `family_names_display`/`family_names_alt` (2026-08-18) — meant the
script has been unable to complete a real run since that date (`InvalidInputException` on the
first row past whichever author happened to have alternates). Fixed by widening the registered
STRUCT to all 4 fields and persisting `family_names_display`/`family_names_alt` as real columns on
`openalex_authors_prep.parquet` (they'd only ever existed as an in-memory intermediate before).

**Results**:
- `01_prepare_arc.py`: **22,820 persons** (down from 23,049/22,942 in prior baselines — mainly
  enrichment-driven merges: 6,485 enriched ORCIDs promoted via `_apply_enriched_orcids`, 174 more
  via low-confidence ERA-FOR matching).
- `02_prepare_oax.py`: 2,779,882 AU-context authors reparsed with the fixed name logic.
- `03_link_arc_oax.py`: 22,401/22,820 (98.2%) got ≥1 high-confidence match; 150,829 candidate
  pairs ≥0.5.
- `04_resolve_links.py`: **22,303/22,820 resolved (97.7%)** — `orcid`-bucket resolutions jumped to
  **8,048** (previous documented baselines: 5,108–6,594), the direct, intended payoff of finally
  promoting the completed enrichment. Overall resolved rate sits modestly below the most recent
  prior baseline (98.4%), plausibly from blocking-key shifts caused by the y-vowel/postnominal/
  diacritic name-parsing fixes changing some `family_name_main`/`first_initial` values — not
  investigated further, flagged as an open question rather than assumed benign.
- `06_build_oeuvre.py`: 22,916 AwardsCIF; Stage 1 → 5,874,508 survivors; Stage 3 → 3,769,100
  survivors; subfield/HEP signals recomputed (`group_coherent` 3,655,188 True).
- Full piling rerun (IDF tables regenerated over the fresh Stage-3 population, including the
  year-bucket block, then `persist_piling_results()` run unscoped across all 22,812 non-excluded
  ACIFs): **3,769,100 rows** persisted (up from 2,549,619 pre-session).

**A pre-existing, unrelated test gap caught by the first full `pytest` run since 2026-08-18**:
`tests/test_oeuvre_build.py::test_mismatched_field_excluded_with_count` silently stopped testing
anything real once the small-pool Stage-3 gate (`<10` candidates skips the field filter entirely)
was added — the test's `_cluster()` helper defaults to 0 `oax_candidates`, so the gate suppressed
the filter and the test's "mismatched field gets excluded" assertion passed only because nothing
was actually being filtered. Fixed: `_cluster()` gained an `oax_candidates` param; the test now
gives its cluster 11 candidates so the gate doesn't mask the behavior under test. 307/307 passing.

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
- **No standalone `AwardsCIF` merge() operator exists — under-merge is structurally
  undetectable today** (2026-08-20, found while auditing the 76 ARC clusters with 2+ distinct
  `family_names` values). `awards_cif.py` has a low-level `_merge_awards_cifs(canonical,
  absorbed, event, **details)` primitive, but every call site (`merge_by_orcid`,
  `merge_persons_by_orcid`, `apply_manual_merges`, `merge_same_grant_coinvestigators`) fires
  from one fixed, early-pipeline sequence inside `refine_clusters()`, before OAX candidates
  exist, let alone oeuvre-building or piling. Nothing downstream (`04_resolve_links.py`,
  `oeuvre_build.py`, `work_piling.py`, `dossier.py`) can invoke a merge based on evidence
  discovered later (e.g. piling noticing two separate ACIFs' candidate pools both dominated by
  the same OAX author). This matters because the 76-cluster audit could only ever see
  **over-merge** risk (two records *did* get connected, so there's something to check) — the
  **under-merge** case (two ACIFs that are really one person, e.g. a pre-2012 grant with no
  ORCID at all paired with a post-name-change grant under a different/new ORCID never
  retroactively applied to the old one) produces two perfectly ordinary-looking, single-surname
  ACIFs with nothing connecting them. Even `gap_candidates` doesn't help here — it also only
  groups on shared surname (soon shared `family_names` overlap, per the Stage 1 rework below),
  so a genuine name-change case with zero surname or ORCID overlap is invisible to every
  mechanism currently in this pipeline. Needs: a real `merge(acif_a, acif_b, evidence) ->
  AwardsCIF` operator, callable post-hoc from any pipeline stage, plus something to actually
  *propose* candidate pairs to feed it (cross-ACIF similarity via shared OAX candidates, shared
  coauthors, or similar) — neither exists yet.
- **Three confirmed candidate wrong-merges found via a full-population same-grant/same-ORCID
  screen, needing human review via `manual_splits.csv`** (2026-08-20). Method that actually
  worked, after two false starts: naive raw-string comparison of `first_name + family_name`
  across same-grant/same-ORCID investigator rows found 980 "different name" groups population-
  wide, almost all noise (`"R Corkish"` vs `"Richard Corkish"`, `"Anthony Thomas"` vs `"Anthony
  Thomas AC"` — postnominal/abbreviation formatting, not identity). Re-running with actual
  parsing (`arc_name_arrays()` + `cluster_checks.first_names_compatible()`, both already built)
  collapsed it to 19 rows / 8 distinct ORCIDs. Of those 8: **3 are real, confirmed conflicts**
  (below); **5 are genuine same-person cases**, each independently confirmed via that ORCID's own
  self-reported `other_names` (`Preethichandra Gamage`/`Daluwathu Mulla Gamage Preethichandra`;
  `Pathegama Ranjith`/`Ranjith Pathegama`; `Kashem Muttaqi`/`Mohammad Kashem` — the weakest of the
  three, no direct ORCID corroboration, but recurs identically across 4 independent grants, which
  argues against a one-off data-entry error) or via the announcement/current snapshot structure
  itself (`Vu To`/`Joseph Tonien` — a single APD-role slot renamed between snapshots, not two
  people in one snapshot; `Kotagiri Ramamohanarao`/`Ramamohanarao Kotagiri` — externally verified
  by the user directly against the real ORCID page).

  The three real conflicts, all with the same signature (two different, real, externally-verified
  people, same grant, same ORCID, different first names — the one signature a genuine name change
  can't produce):
  - `DP1095466_WenhuiDuan` — recurs across **5 grants**, not 1 as first found:
    `DP110101095`, `DP130100109`, `DP160100119`, `DP170104546`, `IH150100006`. `Chien Ming Wang`
    (UQ, TMR Chair Professor in Structural Engineering, real published specialty in floating
    breakwater structures — externally confirmed, a precise topical fit for `DP170104546`'s
    "Floating Forest" breakwater project) and `Wenhui Duan` (Monash, steel/composites structural
    engineering) both carry ORCID `0000-0002-8147-7673`. Recurring across 5 independent
    applications makes a one-off copy-paste less likely than a persistently-wrong record in
    ARC's own investigator database.
  - `DP240100968_AlexandraLasczik` — grant `DP240100968` (Southern Cross University, Indigenous
    child/youth climate-education project): `Tracey Bunda` (CI) and `Alexandra Lasczik` (CI) both
    carry ORCID `0000-0001-7013-2090`. Both real, distinct people (Tracey Bunda a known Indigenous-
    studies scholar; Lasczik's own ORCID only ever shows "Alexandra"/"Lexi Cutcher" forms, never
    Bunda).
  - Georgia Curran's cluster — grant `LP220200211` (Warlpiri cultural-heritage/songlines project,
    University of Sydney admin): `Georgia Curran` (CI) and `Enid Gallagher` (PI, community-partner
    role) both carry ORCID `0000-0003-4290-9186`. Externally confirmed as two distinct real people
    via a co-authored publication with separate institutional emails
    (`georgia.curran@sydney.edu.au` / `enid.gallagher1@education.nt.gov.au`). **User confirmed
    Gallagher has no ORCID of her own at all** — unlike the other two cases (where both people
    plausibly have genuine ORCIDs and the question is which belongs on which row), this one is
    unambiguous: her `orcid` field should simply be null, not corrected to a different value.
    Likely mechanism: ARC's application form may require an ORCID for every investigator, and a
    community-partner PI without one had the CI's copied onto her row — worth watching for as a
    pattern on other Indigenous-community-engaged grants, not confirmed beyond this one case.

  All of this traced against ARC's own raw NCGP API JSON (`raw/raw_json.csv`, the authoritative
  source — `investigators_raw.parquet` is this pipeline's own extraction of it and was confirmed
  faithful, not the source of the error) and cross-checked against the ORCID Public API
  (`src/utils/orcid_client.py`) for each ORCID's own self-reported name(s). Everything else in the
  original 76-cluster population (`family_names` with 2+ values) checked out as genuine name
  variation (maiden/married names, compound surnames, diminutives, one explicit `"(nee X)"`
  marker) — confirmed via each grant's own announcement-vs-current investigator snapshot (same
  first name on the same grant_code = genuine name change; different first name on the same
  grant/ORCID = the red flag), not external lookup, once that became the reliable check.

  **Two small, unrelated postnominal-parsing bugs found and fixed along the way**
  (`src/utils/names.py`'s `_POSTNOMINALS`/`strip_postnominals()`): `"OL"` (Officer of the Order of
  Logohu, PNG — plausible given Glenn Summerhayes' PNG-focused archaeology work) wasn't in the
  postnominal list at all, leaking `"ol"` into his parsed family name; `"Anthony Kinloch FRS,
  FREng"` wasn't stripped because `FREng` wasn't in the list *and* the old regex only handled
  whitespace-separated stacking, not commas. Fixed: `OL`/`FREng` added, separator widened from
  `\s+` to `[\s,]+`. Verified against both real cases plus the existing `"Anthony Thomas AC FAA"`
  regression case; `tests/test_names.py`/`tests/test_prepare_arc_udfs.py` (48 tests) still pass.

  **A genuinely distinct, unfixed issue surfaced by the same investigation, not a data error**:
  reversed given-name/family-name order is a real cultural naming-convention feature for some
  names (Kotagiri Ramamohanarao / Ramamohanarao Kotagiri — Telugu; Pathegama Ranjith / Ranjith
  Pathegama — Sri Lankan), not something with a "correct" order to normalize toward — ARC's
  database forces every name into fixed `firstName`/`familyName` fields regardless of whether
  that convention applies. This breaks strict slot-respecting matching (family-to-family,
  given-to-given) silently: if every ARC record for someone uses one order and OAX's own
  `display_name` uses the other, the shared token sits in `family_names` on one side and
  `first_names` on the other, and no comparison that only ever compares matching slots would see
  them as related at all — same silent-failure shape as the Clarke contamination bug, from
  ordering rather than collapsing. Needs an order-agnostic supplementary comparison/blocking rule
  (pool `first_names ∪ family_names` per side, check for overlap regardless of slot) alongside,
  not instead of, the slot-respecting comparison — not yet designed in detail or scheduled against
  the Stage 1/Stage 2 rework below.
- **Stop collapsing `family_name_main` to a single scalar for Splink blocking — block on set
  overlap instead** (2026-08-20, high priority, found via two independently-confirmed real cases:
  `DE220100680_SarahMonazamErfani` and `DE130100970_TraceyClarke`). Root cause: OpenAlex's
  `display_name_alternatives` is sometimes contaminated with an unrelated co-author's name (Clarke's
  own record carries 7 genuine "Clarke" variants plus exactly 1 contaminant, `'Mariano
  Campoy-Quiles'` — a real, different researcher, plausibly pulled in from a shared-paper byline
  parsing error). `family_name_main = max_by_len(family_names)` (the combined display+alternatives
  set) picks by raw string length with zero semantic justification, so a longer contaminant beats
  the true, shorter surname outright — Clarke's own blocking key ends up `'campoy-quiles'`, Erfani's
  ends up `'montague'`, and the record silently generates zero Splink candidate pairs against its
  own correct ARC cluster (indistinguishable from a genuinely OpenAlex-absent person — no score, no
  flag, nothing surfaces it). The same `max_by_len()`-on-combined-list pattern is repeated ~13 times
  across `01_prepare_arc.py`/`02_prepare_oax.py`/`03_link_arc_oax.py`/`awards_cif.py` (both ARC-side
  and OAX-side, all mirrored pairs) — this is systemic, not a one-off. **Scale, measured via a
  standing diagnostic** (does an OAX record's own `family_name_main` even appear in its own
  `family_names_display`?): 318,858 of 2,779,559 AU-context OAX records (11.5%) are self-inconsistent
  this way; 4,636 non-excluded ACIFs (≈20% of the population) have ≥1 *candidate* affected (a
  different, milder harm — a wrong/contaminated record polluting a pool it shouldn't be in); the
  Erfani/Clarke-pattern harm (a *correct* record structurally missing from its own person's pool)
  turns out **not measurable by a cheap query** — tried twice (a loose token-overlap join and a
  tight exact-family-name+shared-initial join), both landed at ~12,500 ARC clusters / ~1.9M pairs,
  which is suspiciously close to reproducing Splink's own raw *blocking*-pool scale (9.6M pairs
  population-wide per this session's earlier baseline) rather than a real match count — blocking
  is deliberately loose (generates every plausible pair for scoring to narrow down), and no SQL
  join can substitute for Splink's actual probabilistic scoring step. Sizing this side properly
  requires actually building fix (1) below and re-running Splink, not a diagnostic query.
  Two fixes discussed, not yet built, in order of robustness: (1) cheap — replace `max_by_len()`
  with a frequency/plurality pick (count how many alternates resolve to each candidate surname,
  take the majority — Clarke's 7:1 would resolve trivially), with `family_names_display` preferred
  as an exact-tie fallback; fixes all ~13 call sites at once since they're the same operation.
  (2) deeper, more robust against any *future* contamination pattern — change the Splink blocking
  rule itself to match on "shares ≥1 family-name token with the *set* of candidate names" rather
  than exact-equality on one collapsed representative string; removes the "need to choose somehow"
  step entirely at the point it actually causes harm, since ARC's own side (`family_names`) is
  already multi-valued and only the OAX side currently forces a lossy reduction. Not yet decided
  which to build first; (1) is a pure data-prep change, (2) touches Splink's actual comparison/
  blocking configuration.
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
- **Group-level ACIF-membership gate for oeuvre_build.py** (2026-08-16 design, 2026-08-17 partial
  build, **2026-08-18: channeling + persistence + Dossier wiring done**) -- see "Group-level
  ACIF-membership gate design", "`src/utils/work_piling.py` — Phase 2 piling infrastructure", and
  "`oeuvre_piling_results.parquet` persisted; Dossier() wired to piling; Stage 3 gates" below.
  `channel_piles()` (ORCID-first, then HEP-overlap, then FOR-grounded field) is real production
  code now, `persist_piling_results()` runs it across the full population and persists
  `PROCESSED_DATA/oeuvre_piling_results.parquet`, and `Dossier()` reads it directly. Two real,
  unresolved gaps found during cross-section testing remain open: mega-pool false bridging (DBSCAN
  can merge two confirmed-different people via an indirect chain through *other* candidates in a
  large pool, even when they aren't directly similar -- HDBSCAN identified as the likely fix,
  not yet tried, see below), and within-pile contamination hiding inside a nominally "correct"
  dominant pile (Hayward's main pile was only 59% the right person, not re-investigated since).
- **HDBSCAN vs DBSCAN systematic comparison** (2026-08-18, requested, not yet done) -- test
  `sklearn.cluster.HDBSCAN` (built into scikit-learn ≥1.3, no new dependency) against the same
  cross-section used to validate DBSCAN (Hessel for "does it still unify a clean career", WeiWang/
  MohammadIslam for "does it stop mega-pool false bridging"). User-deferred until other in-flight
  work finishes; not started.
- **"Multi-pile, 2+ confirmed" growth needs a spot-check** (2026-08-18): the Stage 3 ORCID/
  small-pool gates (see below) roughly doubled this bucket population-wide (ECR cohort alone:
  1,133 → 1,413 after the field-level channeling fix, then 1,413 → 2,094 after the Stage 3 gates) --
  plausibly genuine fragment-splitting now visible because more real data reaches piling, but could
  also be over-permissive corroboration letting wrong piles through (the same failure mode
  subfield-level matching was originally adopted to prevent). Not checked either way.
- **Person-relative implausible-year filter** (2026-08-18, suggested, not built): Stage 1's
  `implausible_year` check only catches globally-implausible years (outside 1950-2026) -- it can't
  catch a work whose year is merely impossible *for this specific person* (see Adam Hulme below,
  where 1960-2002 works sit comfortably inside the global range). Flagged as possibly as effective
  as field-based filtering, and safer (a wrong-decade work is a much cleaner signal than a
  wrong-field one). Not implemented.
- **`ACCEPTABLE_DIVISION_PAIRS` re-derivation** (2026-08-17, see the matching CLAUDE.md section and
  `cluster_checks.py`'s module docstring) -- attempted, not adopted, reverted to the original
  41-pair list after three iterations each surfaced a new problem (unexplained 2-3x inflation, a
  real regression whitelisting the Wei Wang false-merge's own division pair, and a still-unexplained
  gap even after fixing that). Needs a slower pass grounded in real, named-case validation from the
  start, not aggregate statistics alone.
- **`division_mismatch_for2020()` OAX_FIELD-finer-than-division gap** (2026-08-17, found tracing
  `DP230101204_MohammadIslam`): a legitimate single-ANZSRC-division researcher can be wrongly
  flagged as a cross-division mismatch whenever OAX_FIELD splits that one division into 2+ fields --
  confirmed the whitelist's contents are provably irrelevant to this specific failure mode. Fix
  belongs in the function's own gating logic, not the whitelist. Not yet fixed.

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

## `src/utils/work_piling.py` — Phase 2 piling infrastructure built and tested (2026-08-17)

New module (sibling to `oeuvre_build.py`) implementing the "Group-level ACIF-membership gate"
design above via direct categorical/feature-vector clustering, not Splink and not a graph/network
implementation (both considered and rejected -- see the plan file, since removed, for the full
reasoning: Splink's Fellegi-Sunter model is built for matching records with *fixed* identity
attributes, which a work-snapshotted-at-one-career-point isn't; a literal multigraph -- several
distinct edge types between the same work pair -- is poorly supported by any mainstream Python
network package). Built, tested against a 13-ACIF cross-section (`piling_cross_section.csv`,
8 known-ground-truth cases + 5 ordinary small pools), **not yet wired into production** (no
driver script consumes it; Stage 1/3/`06_build_oeuvre.py` are unchanged by this module's
existence).

**Pipeline**: `compute_and_persist_idf_tables()` -> `fetch_cross_section_raw()` ->
`build_feature_matrix()` -> `compute_distance_matrix()` -> `cluster_piles_dbscan()`/
`cluster_piles_agglomerative()`.
- Five IDF-weighted feature blocks per work: coauthor, institution (own-candidate's, not every
  coauthor's), field, subfield, topic (topic read directly from `work_topics` -- not persisted on
  Stage 3 survivor rows, needs its own fetch). Weight `idf = log(1/tf)`, `tf` from five new
  precomputed tables (`work_tf_coauthor/institution/field/subfield/topic.parquet`, same
  `(value, tf=count/n)` shape as the existing `oax_tf_*.parquet` tables), computed once over the
  **Stage 3** survivor population (2,552,260 rows at last run -- not Stage 1, so "how common" is
  measured over the same population Phase 2 actually clusters, not a broader, noisier one).
- Distance: **cosine**, not Jaccard -- weighted/Ruzicka Jaccard has no efficient vectorized form
  (needs a per-pair min/max), confirmed impractical at the largest real pool's scale (WeiWang,
  12,800 works, ~82M pairs); cosine is one sparse matrix multiply and stays fast at that scale.
- Clustering: **DBSCAN**, not agglomerative -- confirmed empirically, not just by design
  preference: agglomerative (average-linkage) badly over-fragments genuinely coherent single-person
  careers (Hessel's 380 confirmed works: 103 clusters at threshold 0.7), because real careers are
  often *chains* of partial evidence (paper A shares a coauthor with B, B shares an institution
  with C, no direct link between A and C) that average-linkage's "keep the whole cluster's average
  distance low" requirement can't tolerate but DBSCAN's density-reachability chaining can (same
  threshold: 6 clusters, mostly one 345-work dominant pile). Working eps range found empirically:
  **0.85-0.95** correctly unifies a known-coherent career (Hessel: 374-380/380 in one cluster) while
  still separating two confirmed-different real people (MohammadIslam's 36-work correct vs. 309-work
  wrong pile) *when tested in isolation* -- see the mega-pool caveat below.

**Confirmed real finding, not yet resolved: mega-pool false bridging.** In WeiWang/MohammadIslam-
scale pools (many candidates), DBSCAN at the eps needed to correctly unify one coherent career also
sometimes merges two confirmed-different real people -- not because they're directly similar
(MohammadIslam's two confirmed groups have minimum pairwise cosine distance 0.90, above the 0.85-
0.90 threshold that unifies them) but because *other* candidates in the same large pool provide an
indirect chaining path between them. Confirmed by isolating just the two groups (no other
candidates present): they correctly stay separate at the same eps. Two candidate fixes discussed,
neither built: (a) prune the candidate pool before piling (reconnects to the parked Phase-1
prefilter work below), (b) try HDBSCAN (stability-based cluster extraction across an eps range,
rather than one fixed global eps) as a more robust alternative to plain DBSCAN.

**Also found, real but not yet incorporated**: within-candidate contamination can hide *inside* a
dominant pile, not just show up as an obviously-separate secondary cluster -- Hayward's "main"
106-work pile (eps=0.85) was only 59% (63/106) from the ORCID-confirmed correct candidate; the
other 37 works trace to a confirmed *different* real person (externally verified: "Andrew Hayward,"
UCL Institute of Epidemiology and Health Care director, not "Alice Hayward" at all -- shares only
the family name + first-initial "A" that Splink blocks on). Piling's own clustering doesn't
guarantee purity within a pile; the pile-to-ACIF channeling step (ORCID-first, then HEP-overlap,
then FOR-grounded field/subfield match -- see the piling design notes, none built as production
code yet) is meant to catch this by checking each pile as a whole, but a 59%-correct pile would
likely still pass a whole-pile institution check. Not resolved.

**Design decision, validated against a real case**: pile-to-ACIF assignment should allow *multiple*
independently-corroborated piles per ACIF (fragment-splitting, e.g. one real person's career split
by OAX across several `author_idx`s), not force everything into one "keep pile" that might blend
different real people -- the latter would reintroduce exactly the over-merge failure mode this
project has repeatedly found and fixed (Wei Wang, Gilbert, Young). Not yet implemented as
production code (design only, in the now-removed plan file).

**Cross-section spot-check findings, useful precedent for future case-by-case validation**:
Meyer's "secondary" 3-work Waterloo/Canada pile initially looked like a correctly-caught
contamination case, but checking the *whole* candidate's institution history (67 of ~76 works are
also Waterloo/Canada) showed the "main" pile was equally Canada-based -- likely one real,
internationally-mobile person (Canada-based with a genuine Australian grant connection), not two
different people. Retracted after checking, rather than left standing on an unverified assumption
-- a reminder that "the secondary pile looks foreign, the main pile must be the true one" isn't a
safe inference without checking the main pile's own composition too.

**Files**: `src/utils/work_piling.py` (all of the above), `PROCESSED_DATA/work_tf_*.parquet` (five
tables), `PROCESSED_DATA/piling_cross_section.csv` (the 13-ACIF test set, frozen for reuse).
`requirements.txt` gained `scikit-learn>=1.4` (DBSCAN/AgglomerativeClustering/cosine_distances).

## Scope and FOR-code fixes (2026-08-17)

Three related fixes, found while investigating the piling work above and validated against real
cases before being applied population-wide -- not chased purely from aggregate statistics.

**FOR-code handling: every code is a real, ARC-declared discipline signal, not just the primary
one.** `is_primary` marks emphasis, not exclusivity -- restricting to primary-only throughout the
codebase (the prior convention) was discarding real evidence for no reason specific to any one use
case. `cluster_checks.py` gained `for2020_all_fields()`/`for2020_all_subfields()` (same shape as
the existing `for2020_primary_fields()`/`for2020_primary_subfields()`, but over every code, not
just the primary one) -- these are now the production functions used by `oeuvre_build.py`'s Stage 3
field filter, `division_mismatch_for2020()`/`division_mismatch_for2020_pairwise()`, and
`01_prepare_arc.py`'s report labelling. The old primary-only functions are kept, unchanged, for
whichever caller genuinely wants strictly-primary semantics and for the existing tests asserting
that behavior -- no production call site uses them anymore. Measured population effect: 73.8% of
ACIFs show exactly one FOR2020 division under primary-only vs. 34.2% under all-codes -- most
people's grants carry several FOR codes, and once every code counts, a person doesn't need many
grants before their union spans multiple divisions almost automatically.

**Discovery Indigenous (DI) scheme dropped from `KEEP_SCHEMES`** (`config/scope.py`). DI
("Discovery Indigenous Researchers Development") exists specifically to fund Indigenous-focused
research and develop Indigenous researchers -- every grant under it is exactly the kind of research
this project already deliberately keeps out of its bibliometric methods (see
`set_aside_indigenous_research()`, CLAUDE.md history above), so relying on the downstream
FOR2020-division-45 check to catch DI grants was the wrong mechanism: a grant's own declared
primary FOR code doesn't always resolve to "Indigenous Studies" even when the scheme and subject
matter unambiguously are (confirmed on a real case, `DI0347845_DonnaOxenham` -- primary FOR
"Historical Studies," subject "the Malgana Aboriginal people" -- also flagged separately as an
`inv_source='announcement'` name that may not match ARC's current register, an unrelated data-
quality note). `01a_diagnose.py`'s separate, duplicate `SCHEMES_OF_INTEREST` literal was updated to
match, to avoid the two lists silently drifting apart again.

**`set_aside_indigenous_research()` widened to catch non-primary division-45 codes too**
(`src/utils/awards_cif.py`). The exclusion trigger itself is unchanged (a division-45 code as
*primary* -- still 104 AwardsCIF excluded, same as before). New second step: for AwardsCIF that
are NOT excluded, any non-primary division-45 entries get stripped out of their own aggregated
`for2020_codes` (1,124 AwardsCIF affected at last run) -- confirmed via keyword-sweeping every FOR
code actually appearing on any in-scope ARC grant (raw pre-upgrade text across all three vintages,
RFCD98/FOR2008/FOR2020) for "indigenous," "aboriginal," "torres strait," "māori" (note: real ARC
data uses proper macron diacritics), "pacific peoples" -- every genuine hit resolves into division
45, confirming the fix is complete and sufficient; two apparent hits (FOR codes 3904, 4705) are
false positives, both explicitly *excluding* Indigenous content from their own scope via
"(excl. ...)" naming, the opposite of what a keyword sweep should flag. Root cause of the leakage:
FOR2008/RFCD98-vintage grants routinely tagged a substantive discipline as primary and Indigenous-
relatedness as a secondary code; the upgrade to FOR2020 correctly preserves those original
primary/non-primary flags, but FOR2020's own convention would have made Indigenous-focus primary
for that research -- a genuine artifact of two schemes disagreeing about what "primary" means for
this category of work, not a bug in the upgrade step itself (87.2% of the 1,124 affected AwardsCIF
have their earliest grant pre-2020, consistent with this mechanism, though not exclusively --
12.8% are 2020 or later). Considered but NOT adopted: excluding every AwardsCIF with *any*
division-45 code regardless of primary status (1,228 total) -- rejected because 1,216 of those 1,228
(99%) mix division 45 with substantial other, non-Indigenous divisions; wholesale exclusion would
throw away a large amount of legitimate, ordinarily-analyzable research just because of some
secondary Indigenous-focused work mixed in. Stripping the codes from classification while keeping
the person in the population was judged the better fix.

**`ACCEPTABLE_DIVISION_PAIRS` re-derivation attempted, NOT adopted -- reverted to the original
41-pair list.** Full account in `src/utils/cluster_checks.py`'s module docstring. Short version:
switching `division_mismatch_for2020()` to all-codes divisions (above) meant the 41-pair whitelist
was now being tested against a broader population than it was calibrated for, so a re-derivation
was attempted using the same core z-score methodology as the original. Three iterations, each
surfacing a new problem rather than converging on the original count: (1) first pass landed at
84-120 pairs, 2-3x the original, unexplained; (2) adding a lift (observed/expected >= 2.0)
correction barely moved it, ruling out propensity/large-n significance inflation as the main cause;
(3) running the resulting list through the test suite caught a real regression -- it whitelisted
`(40, 46)`, the exact division pair from the confirmed `DE230100180_WeiWang` false-merge case this
whole mechanism was built to catch, because the "confirmed cluster" ground-truth population used
`orcid_status=='HAS_ORCID'` (only requires no *conflicting* ORCID among records that have one) where
the original methodology requires ORCID present on 100% of a cluster's own records -- Wei Wang's
own cluster (9 merged grant records, only 1 carries an ORCID, never actually split, see the
`is_suspicious_for2020()` history above) satisfied the loose definition and was directly inflating
the observed count for the pair it falsely merges. Rebuilding the population with a strict 100%-
coverage check correctly excludes Wei Wang and correctly drops (40,46) back out -- but the overall
count is *still* 86-92 pairs, not 41, meaning some other, still-unidentified methodological
difference remains. The original derivation script was never persisted (only its resulting 41-pair
list was committed), so there's no way to diff against it directly. Decision: stop iterating rather
than risk shipping a worse whitelist under time pressure -- the original 41 pairs remain in force
(now applied against all-codes divisions per the fix above, a known, accepted, documented mismatch
that over-flags for manual review -- the safe failure direction, not a silent one). `resolution_
status` UNRESOLVED = 733 / 22,920 non-excluded AwardsCIF at last run (up from the historical
178-231 range documented earlier in this file, entirely due to the primary-to-all-codes division
switch, not a new problem). Revisiting this needs real, named-case validation from the start
(the way the original 3 example pairs and Wei Wang as a negative control were used), not aggregate
statistics alone, which is what let the Wei Wang regression through undetected until the test
suite caught it.

**Separately found, real, NOT fixed**: a gap in `division_mismatch_for2020()`'s own gating logic,
independent of whitelist contents entirely -- found tracing `DP230101204_MohammadIslam` (externally
ORCID-confirmed via ORCID.org as correctly resolved by this pipeline's institution/HEP evidence,
see the group-level gate section above). His two FOR codes ("Materials engineering," primary;
"Numerical modelling and mechanical characterisation") both sit in ANZSRC division 40 (Engineering)
-- only one division, ever -- but resolve to two different OAX fields ("Materials Science" and
"Engineering"). The function's field-count gate sees 2 fields and doesn't return early, then falls
through to the division-pair whitelist check, which -- with only one division present -- can never
form a pair to test, so it unconditionally returns True (mismatch). Confirmed directly: substituting
an all-inclusive whitelist (every possible division pair) still returns True for Islam's real codes
-- whitelist contents are provably irrelevant to this failure mode. A legitimate single-division
researcher can be wrongly flagged whenever OAX_FIELD splits their one ANZSRC division into 2+
fields. Correct fix is in the function's own gating logic (recognize "only one division present" as
"nothing to check," return False before the pairwise branch), not in the whitelist -- left for a
future session.

**Population after all fixes** (full `06_build_oeuvre.py` re-run): 22,920 AwardsCIF built, 104 set
aside as Indigenous-focused (primary division 45, unchanged), 1,124 had non-primary division-45
codes stripped from classification, resolution_status 22,187 RESOLVED / 733 UNRESOLVED.

## `oeuvre_piling_results.parquet` persisted; Dossier() wired to piling; Stage 3 field-filter gates (2026-08-18)

Three connected pieces of work, in the order they actually happened: persisting piling as a real
pipeline checkpoint, a bug fix in channeling's own field-match logic, wiring `Dossier()` to read
piling output directly (with a "show the whole person" correction along the way), and -- triggered
by manually reviewing real Dossiers -- two real, complementary bugs found in Stage 3's field filter
and fixed.

### Piling persisted as its own checkpoint, not merged into `AwardsCIF`

`src/utils/work_piling.py::persist_piling_results()` runs piling (feature vectors -> cosine
distance -> DBSCAN) + channeling (ORCID -> HEP -> field) across every non-excluded ACIF with 2+
Stage-3 survivor works, and persists one row per `(cluster_id, work_idx)`:
`pile_id, orcid_match, hep_match, field_match, subfield_match, confirmed` to
`PROCESSED_DATA/oeuvre_piling_results.parquet`. Architecture decision, reached by direct
back-and-forth rather than a single clean derivation: piling output is a **separate, persisted
work-level table**, matching the existing Stage1/Stage3/subfield_hep_signals precedent -- never
merged into `awards_cif.parquet` itself. `Dossier()`/any other reporting tool reads it the same way
it reads `arc_persons.parquet`/`oeuvres.parquet` -- it never triggers piling computation itself.
When piling surfaces something that contradicts an upstream ACIF fact, the right mechanism is the
project's existing manual-override pattern (`manual_splits.csv` etc.) -- a human-confirmed
correction applied upstream, then a forward re-run -- not a live feedback loop between piling and
the ACIF it reads from.

Batched (500 ACIFs/batch, DuckDB `COPY ... UNION ALL BY NAME` append pattern) to bound peak memory
regardless of population size, same rationale as the original Stage1/3 OOM fix. First full run:
22,816 non-excluded ACIFs, 2,549,619 rows, ~3 minutes wall-clock (batch_size=500; an earlier
attempt at batch_size=50, plus piping through `tail -40` which swallows all output until exit,
looked hung for over an hour before being killed and relaunched correctly -- lesson: never pipe a
long background run through `tail` without `-f`, and prefer the tool's own background-task output
file over ad hoc piping).

### `channel_piles()` bug: `field_match` was actually computing a subfield-level check

Found while manually tracing why `DP0346211_KasperKowalski` (a real ECR case, small oeuvre) wasn't
corroborated despite an apparently on-topic pile. Both `pile_fields` and `pile_subfields` were
already being computed inside `channel_piles()`'s loop, but only `pile_subfields` was ever used --
the variable *named* `field_match` was computing `bool(acif_subfields & pile_subfields)`, i.e. a
subfield-level check, while `pile_fields` sat unused. Confirmed concretely on Kowalski: his
declared FOR2020 codes (3101 "Biochemistry and cell biology", 5105 "Biological physics") resolve to
OAX **field** `Biochemistry, Genetics and Molecular Biology` (exact match with his pile's own
field) but OAX **subfield** `Biochemistry` -- his pile's subfield is the adjacent, un-mapped
`Molecular Biology` (the FOR2020 group's crosswalk picks one subfield even though "cell biology" is
in the group's own name) -- so the old code found no subfield overlap and marked it unconfirmed,
even though the field-level match was clean.

**Fix**: `field_match` now correctly computes `bool(acif_fields & pile_fields)`; the old
subfield-level check is kept as a new, separate `subfield_match` field (not dropped -- persisted
alongside, for a possible future waterfall, e.g. subfield-first for precision with field as a
fallback) but no longer gates `confirmed`. `confirmed = orcid_match or hep_match or field_match`.
`persist_piling_results()` and its output schema updated to carry `subfield_match` as its own
column. Population-wide effect on the ECR cohort (DECRA/APD/APDI): "clean uncorroborated" (1 pile,
0 confirmed) dropped from 31 to 0, "fully ambiguous" (2+ piles, 0 confirmed) dropped from 5 (ECR
subset) to 0, but "multi-pile, 2+ confirmed" rose from 1,133 to 1,413 -- flagged, not yet checked
either way, as a possible reintroduction of the over-permissive-field-matching failure mode
subfield-level checking was originally adopted to prevent (see "Next Priority" above).

### `Dossier()`/`dossier_build.py` hardwired to piling, ACIF fields/subfields/institutions, and the whole person's award history

Triggered by direct user feedback: ad hoc scratch scripts building one-off HTML/markdown reports
were "frustratingly brief" -- the user's own field selection was rarely what was actually needed,
and rebuilding a bespoke report generator each time wasted the cheap part (Dossier already computes
almost everything) while never fixing the real problem (curation). Direction: make `to_markdown()`
itself fuller by default, hardwired as real project code, not scratch scripts re-derived per
question. Also established this session: **plain markdown published via the Artifact tool is now
the default reporting medium** for anything beyond a couple of cases -- Artifact renders `.md`
files natively with no HTML/CSS authoring step, so `Dossier.to_markdown()`'s output can be
concatenated and published directly at near-zero marginal cost over what already prints to the
terminal; the custom-styled HTML treatment (sparklines, status chips, TOC) is reserved for a
finished, repeatedly-browsed, or shared deliverable, not the default.

Concrete additions to `analysis/utils/dossier.py`:
- New `PileDiagnostic` frozen dataclass: `pile_id, n_works, orcid_match, hep_match, field_match,
  subfield_match, confirmed, pile_fields, pile_subfields, coauthor_names` (coauthors exclude the
  ACIF's own candidate `author_idx`(s)). `bool | None` match fields -- `None` means not evaluated
  (e.g. no ARC-recorded ORCID to check), not "evaluated, no match" -- same three-valued discipline
  used throughout this project.
- `AwardContext.other_investigators: list[str]` -- every other ARC investigator on that specific
  grant ("First Last (role_code)"), and `AwardContext.role_code` (this person's own role on that
  grant) -- added earlier this session, see below.
- `Dossier.acif_fields` / `.acif_subfields` -- OAX fields/subfields implied by this ACIF's own
  declared FOR2020 codes (`cluster_checks.for2020_all_fields()`/`for2020_all_subfields()`), i.e.
  exactly what piling's `field_match`/`subfield_match` are checked against.
- `Dossier.acif_institutions` -- this ACIF's own HEP institutions (`awards_cif.parquet`'s
  `hep_codes`, the union across every grant's eligible orgs, not just the administering one),
  resolved to full names via `admin_orgs.csv` ("Name (CODE)"), falling back to the bare code if
  unmapped. What piling's `hep_match` is checked against.
- `Dossier.piles: list[PileDiagnostic]` -- this ACIF's own piling result, empty if not yet piled or
  fewer than 2 candidate works (not evaluated, not "evaluated and found nothing" -- the same
  distinction almost got collapsed in an early draft of the `to_markdown()` render before being
  caught and fixed).
- `to_markdown()` renders all of the above: `other_investigators` under each award, a new
  "## OpenAlex candidate piling" section per person.

`dossier_build.py` gained matching fetch functions (`_fetch_acif_fields_subfields()`,
`_fetch_acif_institutions()`, `_fetch_piling_diagnostics()`), all reading already-persisted
checkpoints (`awards_cif.parquet`, `oeuvre_piling_results.parquet`, `oeuvre_stage3_survivors.parquet`,
raw OpenAlex `authorships`/`authors`), never triggering computation. A module-level
`HEP_CODE_TO_NAME` dict is built once from `admin_orgs.csv` at import time (42 real mappings).

**`_fetch_award_contexts()` no longer scoped to `ECR_ROLES`** (2026-08-18, direct user correction:
"the ACIF is about a person, not an award... I would not expect this interpretation"). Found via
Andrew Burrow (`DP0665744_AndrewBurrow`): his real grant `DP0985878` (RMIT, "Ethics and aesthetics
as criteria for innovation") was silently missing from his Dossier's Awards section because his
role on it is `CI`, not an ECR role -- the function used to filter `role_code IN ECR_ROLES` outright.
Fixed: it now returns *every* grant the cluster holds any role on; an ECR-specific view should
filter on `award_year`/`career_age_at_award` instead of hiding non-ECR-role grants. `arc_grant_
cluster_map` confirmed the grant was always correctly linked -- this was a display/filtering bug,
not a linkage one.

### `analysis/07_analyse_ecr_fellowships.py::build_cohort()` stale `for_codes` gap

Found via Amanda Macdonald (`DP0346551_AmandaMacdonald`): her Dossier's "FOR codes" line showed
only `4702`, but `awards_cif.parquet`'s own `for2020_codes` has two entries, `4702` (Cultural
studies, primary) and `4703` (French language, secondary) -- confirmed correct by hand-resolving
her raw `field-of-research` block (`raw_json.csv`: `2002/FOR08` "Cultural Studies" primary,
`420106/RFCD98` "French", `420302/RFCD98` "Cultural Theory", `420306/RFCD98` "Postcolonial and
Global Cultural Studies") through `for_resolve.resolve_arc_for_entry()` by hand and getting exactly
`{4702, 4703}` back. Root cause: `arc_persons.parquet` carries **two** different FOR-code columns --
`for_codes` (legacy, primary-only) and `for2020_codes` (full, correctly-resolved list, same as
`awards_cif.parquet`) -- and `build_cohort()` was still reading the legacy `for_codes` column,
a gap the 2026-08-17 "use all FOR codes, not just primary" migration never reached. Fixed:
`build_cohort()` now selects `p.for2020_codes` and derives the plain code list from it
(`for2020_codes` is already ordered primary-first, so no re-sorting needed); `add_for_division_
panel()`'s downstream logic (`upgrade_for_code()` on each code, FOR2020 passthrough) needed no
change.

### Adam Hulme (`DE240100095`): an ORCID-confirmed candidate with OpenAlex-side-only contamination

Investigated after the user pasted Hulme's real ORCID profile (PhD 2014-2017, Federation
University) alongside his Dossier's implausible `first_pub_year: 1960` / `career_age_at_award: 64`.
Sequence of findings, each checked directly rather than assumed:

- **ORCID confirms the right person, not a linking error.** `awards_cif.parquet`'s ARC-recorded
  ORCID for this cluster (`0000-0002-3305-8538`) exactly matches OpenAlex candidate `A5077201705`'s
  own `orcid` field (confirmed both via our local snapshot and a live OpenAlex API pull the user
  provided). This is genuinely Adam Hulme, not a wrong-candidate link.
- **The contamination is inside OpenAlex's own author record.** Fetched his real ORCID publication
  list directly (`mcp__alex-mcp__get_orcid_publications`, 203 works) -- every single one dated
  2015-2026, none before. But a direct join of our local `OPENALEX_COMPACT_DIR/authorships` +
  `works` tables for `author_idx=5077201705` shows works dated 1960, 1971, 1985, 1989, 2001, 2002 --
  OpenAlex's own per-work authorship disambiguation has misattributed someone else's older work
  onto this ORCID-verified author bucket. Not an ARC<->OpenAlex linkage problem on our side.
- **Raw-row-count gotcha, resolved**: the raw authorships join returned 153 rows for this
  author_idx against `authors.parquet`'s own `works_count=125` -- traced to 22 duplicate
  `(author_idx, work_idx)` rows in the authorships extract; `COUNT(DISTINCT work_idx)` gives 125,
  matching the dimension table exactly. Not evidence of additional contamination beyond the
  confirmed pre-2015 works -- a data-extraction duplication artifact, noted for awareness, not
  investigated further this session.
- **Why piling never caught it**: his Stage-3 candidate pool had collapsed to exactly **1** survivor
  work (of 58 Stage-1-quality-passing works) -- DBSCAN needs 2+ works to form a pile at all, so this
  wasn't "evaluated and missed," it was structurally unable to attempt evaluation. Traced the
  funnel precisely: 58 Stage-1 survivors -> Stage 3's field filter dropped 57 of them. His declared
  FOR2020 codes (3505 "Human resources and industrial relations", 3507 "Strategy, management and
  organisational behaviour", 4203 "Health services and systems") resolve to OAX fields `Business,
  Management and Accounting` + `Medicine` only. Verified `4203`'s own resolution precisely:
  `4203 -> OAX subfield "Public Health, Environmental and Occupational Health" -> OAX field
  "Medicine"` (confirmed correct against real `work_topics` data -- that subfield genuinely sits
  under Medicine in OpenAlex's own taxonomy, not a crosswalk bug). The real gap is that his actual
  papers mostly land on OAX field `Health Professions` (31 of 108 works) -- a field none of his
  three FOR codes reach at all -- a genuine breadth gap, not a resolver error.
- **The conceptual bug this exposed**: Hulme has 4 OpenAlex candidates in his pool, not 1 -- but
  ORCID already resolves which one is correct before Stage 3 ever runs. Stage 3's field filter
  builds one target-field set per ACIF and applies it uniformly to *every* pooled work from *every*
  candidate, with no awareness that a specific candidate's identity is already settled. Its
  original justification -- "disambiguate a pool with several possibly-wrong candidates" -- doesn't
  apply once ORCID has already answered that question for one of them; applying it anyway can only
  ever remove that person's real work, never help.

### Stage 3 fixes: stale `for2020_primary_fields()` import, ORCID gate, small-pool gate

`src/utils/oeuvre_build.py::apply_field_filter_stage3()` was still importing and calling
`for2020_primary_fields()`, contradicting CLAUDE.md's own documentation that this call site had
been migrated to the all-codes version -- a real stale gap between docs and code (didn't change
Hulme's outcome, his primary and secondary codes resolve to the same two fields either way, but the
mismatch was real). Fixed: now calls `for2020_all_fields()`. The old, superseded
`apply_subfield_filter()` function still legitimately imports `for2020_primary_fields()` too -- both
stay imported, only the Stage 3 call site changed.

Two new, explicitly interim gates added to the same function (both parameterized, both flagged in
their own docstrings as needing revisiting once piling/channeling's ORCID-first logic is
positioned to take over this role properly -- not a durable design):

- **ORCID gate** (`orcid_gate_max_candidates=10`): for an ACIF with a recorded ORCID and fewer than
  10 total candidates, works whose own `source_author_idxs` include the specific ORCID-matched
  candidate bypass the field filter entirely -- protects only that one already-confirmed
  candidate's works, not the whole pool. Resolves the OpenAlex `orcid` field's known ARC-orcid
  bare-string-vs-URL mismatch via `contains()`, same fix already used in `channel_piles()`.
- **Small-pool gate** (`small_pool_max_candidates=10`, user-directed, broader): for *any* ACIF with
  fewer than 10 total candidates, skip the field filter entirely for every candidate's works, ORCID
  or not. Reasoning: this filter predates piling and was originally the *only* tool available for
  pruning a small candidate pool -- but Stage 3 now sits upstream of piling/channeling, which does
  real per-work clustering and ORCID/HEP/field corroboration on whatever survives here. For a small
  pool it's safer to let more Stage-1-quality-passing work through and let piling do the actual
  discrimination downstream. Kept as a genuinely separate mechanism from the ORCID gate (not
  collapsed into one) even though both currently share the same cutoff -- if the small-pool
  threshold is ever raised independently, the narrower ORCID-anchored exemption still stands on its
  own for pools it no longer covers.

Smoke-tested in isolation on Hulme's cluster only (scratch output paths, never pointed at the real
`STAGE3_SURVIVORS` path) before the full rerun -- confirmed his own survivor count jumped from 1 to
58 (the full Stage-1 set), as expected from either gate firing.

**Full population rerun** (`06_build_oeuvre.py` -> IDF tables -> `persist_piling_results()`, ~20
minutes total): 22,918 AwardsCIF built; Stage 1: 7,285,909 survivors (unchanged logic, minor count
drift from upstream data); **Stage 3: 4,499,176 survivors** (up from 2,570,763 pre-session baseline
and ~2.55M just before these gates -- 2,786,733 excluded for `field_mismatch`, down from ~4.75M);
piling: **4,485,777 rows** (up from 2,549,619). ECR cohort (DECRA/APD/APDI) piling-bucket shift,
before -> after the Stage 3 gates:

| bucket | before | after |
|---|---|---|
| n_piles==0 (nothing to pile) | 96 | 16 |
| clean, confirmed (1 pile, 1 confirmed) | 2,543 | 1,668 |
| clean, uncorroborated (1 pile, 0 confirmed) | 0 | 6 |
| fully ambiguous (2+ piles, 0 confirmed) | 0 | 2 |
| multi-pile, 1 confirmed | 10 | 343 |
| multi-pile, 2+ confirmed | 1,413 | 2,094 |
| missing piling row | 100 | 33 |

## ORCID Public API integration; name-parsing bugs found via ORCID-vs-OAX comparison (2026-08-19/20)

**`src/utils/orcid_client.py`** — a registered ORCID Public API client (OAuth client-credentials
flow, `ORCID_CLIENT_ID`/`ORCID_CLIENT_SECRET` in `.env`, ~20-year token validity) replacing
anonymous requests, clearing the daily quota wall the 2026-08-08 `00b_enrich_orcid.py` run hit (see
above). `00b_enrich_orcid.py` patched to send `Authorization` headers when a client is registered,
falling back to anonymous behavior otherwise. **diskcache gotcha**: the default `size_limit` is
1GiB with silent eviction on overflow (no error, no log) — fixed via `cache.reset("size_limit",
value)` on an *existing* cache (passing `size_limit=` to the constructor alone doesn't resize one
that already exists on disk). With this, the deferred search-phase run from 2026-08-08 was
completed in full: remaining 7,010/7,010 searches, zero errors — `orcid_enrichment.parquet`,
11,960 rows total (4,988 high, 1,613 au_match, 62+18 wildcard, 1,743 low, 1,712 too_common, 1,824
not_found). Promoted into production in the 2026-08-20 rebuild above.

**Name-comparison investigation**: joined ORCID's own API-returned name for a known ORCID against
OpenAlex's `display_name` for the same ORCID, as a sanity check on name-matching quality — and used
it to find and fix four real, separate production bugs in `src/utils/names.py`, all reached at
every one of its 4 `HumanName()` call sites (`names.py::parse_given`, `02_prepare_oax.py`,
`01_prepare_arc.py`, `awards_cif.py`):

1. **`credit-name` wrongly prioritized over `given-names`+`family-name`** in an early draft of
   `orcid_names()` — ORCID's `credit-name` is a self-chosen short/nickname display form (e.g. "Rob"
   for "Robert Norman"), not more authoritative than the structured given+family fields. Caught via
   a user-provided real ORCID page (Rob/Robert Norman) showing all three fields distinctly. Fixed:
   given+family is now the primary comparison; credit-name/other-names are checked separately as
   alternates, never substituted in as the primary form.
2. **Curly apostrophe (U+2019) vs straight (U+0027) silently dropped, not just mismatched** —
   `strip_diacriticals()`'s NFD-normalize-then-ascii-encode approach discards U+2019 entirely (same
   failure class as the already-known ø/ł/œ drop-to-nothing bug, since it has no NFD canonical
   decomposition), so an apostrophe-bearing surname could carry two different normalized forms
   purely depending on which apostrophe character the source system used (OpenAlex's own
   `display_name` uses curly; ARC/ORCID data typically uses straight). New
   `canonicalize_name_punctuation()` preprocessor collapses every apostrophe/hyphen variant to a
   canonical ASCII form *before* `HumanName()` parses the string (not just after, on already-split
   parts — `HumanName`'s own splitting decisions depend on the punctuation already being uniform).
3. **Bare, unpunctuated initials treated as real given names** (e.g. "PG" for P.G., "MJ" for M.J.)
   — new `_split_bare_initials()`, gated on vowel presence: a 2-4 letter alpha token with no vowel
   is split into individual letters, one with a vowel is left whole. Empirically validated against
   616 real OAX 2-character first names before shipping (genuine names like "yu"/"li"/"mo" all
   contain a vowel; the vowel-less combinations actually present — mj/dj/aj/jm/pj/rj/sj/cj — are
   absent from genuine usage). **"y" had to be added to the vowel set after directly checking real
   ARC first-name data** (user-directed, not assumed): without it, 171 real people with genuine
   Welsh-pattern names (Lyn 54, Rhys 42, Lynn 38, Kym 11, Gwyn 11, Glyn 4, Bryn 2) would have been
   wrongly shredded into individual letters.
4. **Polish ł (U+0142) silently dropped**, same no-NFD-decomposition failure as ø/ß — added to
   `_DIACRITIC_VARIANTS` (`"Włodkowic"` was becoming `"wodkowic"`, missing the L entirely, not just
   losing the diacritic mark).
5. **Postnominal letters beyond AC/AO/AM/OAM leaking into the parsed family name** —
   `strip_postnominals()` already existed (with a broader letter list: FAA/FAHMS/FTSE/FASSA/FAHA/
   FRS/CBE/OBE/MBE/KBE/DBE) but was never actually chained before `HumanName()` parsing at any call
   site; only the narrower `CONSTANTS.suffix_acronyms` (AC/AO/AM/OAM only) was registered. Fixed by
   calling `strip_postnominals()` in sequence with `canonicalize_name_punctuation()` before every
   `HumanName()` call; `FAHMS` also added to `strip_postnominals()`'s own list (found missing when
   "Aleksandra Filipovska FAA FAHMS" still parsed to family="fahms" after the first fix pass).

**Result**: the diagnostic comparison itself needed a fix too, once these landed — comparing only
the single "longest" family-name variant (`max_by_len`) discarded genuine matches whenever a bare
ASCII source spelling matched a *non-longest* member of an umlaut-expanded set (e.g. `"Grün"` →
`{"gruen","grun"}`, and ORCID's own bare `"Grun"` only matches the shorter one) — fixed to compare
the full variant set instead of one representative. Final count: **87 → 48 → 31** genuinely
unexplained ORCID/OAX name mismatches (of 6,680 compared pairs), 99.3% matching on primary name by
the end. The remaining 31 split into given/family order swaps, a bogus OAX `display_name`
("R. A. F."), several likely-wrong-ORCID red flags (Hagan/Hebblewhite, Jakeman/Grun, Hopper/
Kientz), and small one-off typos — none safe to fix with a shared rule.

**This investigation is what surfaced the systemic `family_name_main` "longest string wins" bug**
documented under "Next Priority" above (`max_by_len()` over the *combined* display+alternatives
list, used ~13 times project-wide) — found via two independently-confirmed real cases where a
correct OpenAlex identity exists in the AU-context pool but never became a Splink candidate because
its own blocking key was corrupted by a contaminating name pulled from `display_name_alternatives`
(`DE220100680_SarahMonazamErfani` → `"montague"`; `DE130100970_TraceyClarke` → `"campoy-quiles"`,
7 genuine "Clarke" variants against exactly 1 contaminant). See "Next Priority" for the fix options
and population-wide scale (318,858 of 2,779,559 AU-context records, 11.5%, are self-inconsistent
this way; 4,636 ACIFs, ≈20% of the population, carry ≥1 affected candidate) — not yet built.

The `n_piles==0` bucket dropping from 96 to 16 is the gates working as intended (most were
filter-starved, not genuinely unpileable). The growth in multi-pile buckets is a real, honest
side effect flagged above under "Next Priority", not yet checked either way.
