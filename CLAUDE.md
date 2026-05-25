# ARC Grants → OpenAlex Linkage Pipeline

## Project Goal
Link ARC Chief Investigators/Fellows (CIFs) to their OpenAlex author records for bibliometric analysis.

## Pipeline Architecture (Splink-based)
```
00_extract_arc.py       → grants_flat.parquet, investigators_raw.parquet
01_prepare_arc.py       → arc_investigators_prep.parquet, arc_persons.parquet
                           (ARC name/inst/FOR prep + Splink dedupe_only: 62k rows → 22,819 persons)
02_prepare_oax.py       → openalex_authors_prep.parquet, oax_tf_*.parquet
03_link_arc_oax.py      → arc_oax_links.parquet (link_only: ARC persons → OAX authors)
04_resolve_links.py     → arc_oax_resolved.parquet, arc_ambiguous_deferred.parquet
```
The Splink pipeline replaces the entire old multi-layer pipeline in `src_archive_20260520/`.
`02_run_splink.py` is the old wrong approach — superseded, can be deleted.

## Key Paths
- Config: `config/settings.py`
- Data root: `/home/lc/m/working/WORKING_ARC_PROJECT/`
- Processed data: `/home/lc/m/working/WORKING_ARC_PROJECT/processed/`
- OAX authors parquet: set via `OPENALEX_DIR` env var in `.env` → `/home/lc/m/openalex_feb26/parquet/`
  - **Note**: `/media/d-drive/openalex_feb26/` is a different copy — always use `.env` path

## Data Scale
- ARC CIF rows (after role/scheme filter): 62,712
- ARC person clusters (output of 01): 22,819  (avg 2.75 grants/person)
- OAX Australian authors: 1,149,339
- ORCID coverage in ARC CIFs: 44.5%

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
- Two ANZSRC series (2008 and 2020) use different numeric codes and slightly different names
- **Do NOT compare numeric codes** across series — they don't harmonise
- `for_name_tokens()` in `src/utils/names.py`: tokenises FOR names, strips stopwords
- `make_expanded_for_tokens()`: loads `config/for_concordance.csv` (49 J≥0.5 pairs) and
  unions each name's tokens with its canonical form's tokens — bridges near-synonym names

## 02_dedupe_arc.py Design
1. **Block on `first_initial` + `family_name_main`** (compound surname, not split) + ORCID
2. **Post-process ORCID conflict split**: any cluster with 2+ distinct non-null ORCIDs
   is split — rows get sub-clusters keyed on ORCID; no-ORCID rows become singletons
3. **Diagnostic**: categorises multi-row clusters by institution count and FOR consistency

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
2b. **Field score filter**: restrict to candidates with maximum field score (unigram+bigram overlap
    of ARC FOR tokens vs OAX topic_names/subfield_names). Only fires if `max_fs >= 2` AND
    `min_fs == 0` (at least one candidate has zero overlap — avoids within-field false positives).
3. **Unique highest probability**: among remaining → resolve.
4. **works_count dominance**: `max / sum > TOP_CUT` → OAX split record, take dominant.
5. **Defer**: genuine common-name collisions.
Manual overrides: `config/manual_resolutions.csv` (resolve/unlink actions applied after all steps).

Output columns: `arc_id, oax_id, match_probability, resolved_by, secondary_oax_ids`
`secondary_oax_ids`: all other HC candidates not chosen (split-record duplicates + alternatives).

## Current Linkage Results (2026-05-25)
- Resolved: 22,048 / 22,819 (96.6%)
  - unique_hc: 8,828 | oax_orcid_dedup: 955 | oax_topic_dedup: 2,619 | orcid: 5,003
  - inst_overlap: 3,164 | field: 861 | probability: 64 | works_count: 546 | manual: 8
- Ambiguous deferred: 418
- Manual unlinked: 1
- Unlinked (no HC match): 352

## Fellowship Cohort Status (2026-05-25)
- **FF** (Federation Fellows): 141/141 resolved
- **FL** (Australian Laureate Fellows): 277/277 resolved
- **FT** (Future Fellows): 265/277 resolved — 6 deferred (common names), 6 unlinked (overseas-based)

## Important: cluster_id vs scheme membership
Most FF/FL fellows also hold DP grants; their `cluster_id` starts with "DP" not "FF"/"FL".
To find all clusters for a scheme, search `grant_ids` in arc_persons, or use
`arc_grant_cluster_map.parquet` (output of 01) which maps every grantID_personName → cluster_id.

## Next Priority (start of next session)
FF and FL cohorts are complete. Consider FT deferred/unlinked cases next.

## 03_link_arc_oax.py Design (updated 2026-05-25)
- Given-name comparison uses HumanName cascade: compound (f+m) → first → cross (f=other's m) → initials
- Blocking: `(family_name_main, first_initial)` + two middle-initial cross-blocking rules + ORCID
- Cross-blocking catches cases like ARC "Z Smith" vs OAX "Herb Z Smith" (middle initial match)
- HumanName on `full_names` (ARC longest) and `full_name` (OAX display_name) for consistent parsing
- Compound match "shi xue" = "shi xue" fixes Chinese compound given names (e.g. Shi Xue Dou)
- TF adjustment on `first_name` (hn.first) exact level only

## Next Priority (start of next session)
**Identify and verify all FF (Federation Fellows) and FL (Australian Laureate Fellows)** —
small cohorts (141 and 277 persons), high-profile, should be near-100% matchable.
These are role_code FF/FL in investigators_raw.parquet; is_fellowship=True.

## Splink Design Decisions
- **`arrays_to_explode` is NOT supported** in EM training sessions
- **Prior inflation risk**: second EM session on family-name block causes false merges.
  Fix: one ORCID-based EM session + `estimate_probability_two_random_records_match(recall=0.8)`
- **Cluster threshold**: 0.9 (high precision, prefer splitting over merging)
- **Seeded m_probabilities** for orcid comparison (can't train from ORCID-blocked EM)

## Known Issues in 02 Output
- **Raymond Gilbert / Robert Gilbert** (n=34, no ORCID): different first names + 3 different
  fields — suspected mis-merge of 2–3 people
- **Paul Young** (n=28, no ORCID): AI + Biochemistry + Chemical Eng at 4 institutions —
  likely multiple people
