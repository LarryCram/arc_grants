# ARC Grants → OpenAlex Linkage Pipeline

## Project Goal
Link ARC Chief Investigators/Fellows (CIFs) to their OpenAlex author records for bibliometric analysis.

## Pipeline Architecture (Splink-based)
```
00_extract_arc.py       → grants_flat.parquet, investigators_raw.parquet
01_prepare_splink.py    → arc_investigators_prep.parquet, openalex_authors_prep.parquet
02_dedupe_arc.py        → arc_persons.parquet   (dedupe_only: 62k grant rows → 22,785 persons)
03_link_arc_oax.py      → arc_oax_links.parquet (link_only: ARC persons → OAX authors) ← NOT YET WRITTEN
```
The Splink pipeline replaces the entire old multi-layer pipeline in `src_archive_20260520/`.
`02_run_splink.py` is the old wrong approach — superseded, can be deleted.

## Key Paths
- Config: `config/settings.py`
- Data root: `/home/lc/m/working/WORKING_ARC_PROJECT/`
- Processed data: `/home/lc/m/working/WORKING_ARC_PROJECT/processed/`
- OAX parquets: `/media/d-drive/openalex_feb26/parquet/`

## Data Scale
- ARC CIF rows (after role/scheme filter): 62,712
- ARC person clusters (output of 02): 22,785  (avg 2.75 grants/person)
- OAX Australian authors: ~1,128,792
- ORCID coverage in ARC CIFs: 44.5% (32,646 rows)

## Python Environment
**Always use `.venv/bin/python`** — never bare `python`.

## Name Tokenisation (src/utils/names.py)
- `name_part_tokens()`: strips apostrophes before tokenising → `O'Brien → ["obrien"]`
- Hyphens/spaces still split: `Watson-Parker → ["watson","parker"]`
- `strip_diacriticals()`: NFD normalise + ASCII-only
- Applied identically on both ARC and OAX sides in `01_prepare_splink.py`

## FOR Code Handling
- Two ANZSRC series (2008 and 2020) use different numeric codes and slightly different names
- **Do NOT compare numeric codes** across series — they don't harmonise
- `for_name_tokens()` in `src/utils/names.py`: tokenises FOR names, strips stopwords
- `make_expanded_for_tokens()`: loads `config/for_concordance.csv` (49 J≥0.5 pairs) and
  unions each name's tokens with its canonical form's tokens — bridges near-synonym names
- Comparison in Splink: `ArrayIntersectAtSizes("for_name_tokens", [2, 1])` — 3 levels:
  - ≥2 tokens: strong same-field (positive)
  - ≥1 token: weak/concordance-bridged match (partial positive)
  - else (0 tokens): different discipline → **anti-match** (negative weight)

## 02_dedupe_arc.py Design (Three Rules)
1. **Block on `first_initial` + `family_name_main`** (longest surname token) + ORCID
2. **Post-process ORCID conflict split**: any cluster with 2+ distinct non-null ORCIDs
   is split — rows get sub-clusters keyed on ORCID; no-ORCID rows become singletons
3. **Diagnostic**: categorises multi-row clusters by institution count and FOR consistency

## Splink Design Decisions
- **`arrays_to_explode` is NOT supported** in EM training sessions
- **Prior inflation risk**: second EM session on family-name block causes false merges.
  Fix: one ORCID-based EM session + `estimate_probability_two_random_records_match(recall=0.8)`
- **Cluster threshold**: 0.9 (high precision, prefer splitting over merging)
- **Seeded m_probabilities** for orcid comparison (can't train from ORCID-blocked EM)
- `family_name_main` = longest token in family_names array (avoids spurious "o", "van" blocks)

## Known Issues in 02 Output (full run)
- **Raymond Gilbert / Robert Gilbert** (n=34, no ORCID): different first names + 3 different
  fields (Civil Eng, Food Sciences, Materials Chemistry) — suspected mis-merge of 2–3 people
- **Paul Young** (n=28, no ORCID): AI + Biochemistry + Chemical Eng at 4 institutions —
  likely multiple people
- These are the hardest no-ORCID cases; Splink correctly flags them via FOR anti-match but
  can't split without more signal

## Current State
- Scripts 00, 01, 02 complete and run on full dataset ✓
- `arc_persons.parquet`: 22,785 person records ready for OAX linkage
- `03_link_arc_oax.py`: **not yet written** — next step
- `02_run_splink.py`: old wrong approach, can be deleted
