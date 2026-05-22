# ARC Grants → OpenAlex Linkage Pipeline

## Project Goal
Link ARC Chief Investigators/Fellows (CIFs) to their OpenAlex author records for bibliometric analysis.

## Pipeline Architecture (Splink-based)
```
00_extract_arc.py       → grants_flat.parquet, investigators_raw.parquet
01_prepare_splink.py    → arc_investigators_prep.parquet, openalex_authors_prep.parquet
02_dedupe_arc.py        → arc_persons.parquet   (dedupe_only: 62k grant rows → 22,819 persons)
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
- ARC person clusters (output of 02): 22,819  (avg 2.75 grants/person)
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
1. **OAX same-ORCID pre-dedup** (NOT YET IMPLEMENTED): if two OAX candidates share an
   ORCID with each other, they are the same person split across two OAX IDs. Keep the one
   with the most works as representative; add others to `secondary_oax_ids`.
2. **ARC–OAX ORCID match**: if exactly 1 HC candidate matches the ARC person's ORCID → resolve.
3. **Institution overlap**: restrict to candidates with maximum overlap count (if any > 0).
4. **Unique highest probability**: among remaining → resolve.
5. **works_count dominance**: `max(works_count) / sum(works_count) > 0.90` → OAX split record,
   take the dominant one. Distinguishes split records (116 vs 2) from different people (52 vs 29).
6. **Defer**: genuine common-name collisions.

Output columns: `arc_id, oax_id, match_probability, resolved_by, secondary_oax_ids`
`secondary_oax_ids`: list of all other HC candidates not chosen (split-record duplicates + alternatives).

## Current Linkage Results (2026-05-22)
- Resolved: 20,517 / 22,819 (89.9%)
- Ambiguous deferred: 1,411 (common names, evenly split works — cannot resolve without extra signal)
- Unlinked: 891 (no HC match — not in OAX AU, or pre-Australia overseas researchers)

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
