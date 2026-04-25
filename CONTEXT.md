# Project Context for Claude Sessions

Paste this at the start of any Claude session (here or VS Code) to restore context.

## Project
Longitudinal study of ARC grant productivity effects, 2001-2025.
Three outputs: colleague commission report, policy report, scholarly paper.
ERC extension planned once ARC pipeline is established.

## My Stack
Python, SQL, DuckDB, Parquet. Linux (home/lc/m SSD mount). VS Code. Moderate disambiguation experience.
Prefer .py files over notebooks. LaTeX in codebase as living methodology document.

## Data
- ARC grants CSV, ~34,000 records, sourced from ARC public API
- OpenAlex Feb 2026 snapshot as parquets
- Working data: `/home/lc/m/working/ARC/` (DATA_ROOT in .env)
- OpenAlex authors: `/home/lc/m/openalex_feb26/parquet/authors/*.parquet` (OPENALEX_DIR in .env)
  - 99 partitions, ~106M authors globally
  - 579,975 AU authors (last_known_institutions); 1,139,293 ever-AU (affiliations field)

## Key Decisions Made
- Precision over recall in disambiguation (false merge worse than missing record)
- FoR codes used at 2-digit level only due to 2018 ANZSRC revision
- ORCID treated as hint requiring validation, not authoritative
- Chronological strategy: anchor on post-2018 grants, extend backwards
- No embeddings/vectors until deterministic methods are exhausted

## Scope Decisions (config/scope.py)
**KEEP_ROLES (14):** CI, CI-DORA, DECRA, FT, FL, FF, APD, APF, ARF, QEII, APDI, ARFI, DAATSIA, IRF
**KEEP_SCHEMES (7):** DP, LP, DE, FT, FL, FF, DI

## Phase Status
**Phase 0 — COMPLETE.** `src/00_profile_arc.py`
**Phase 1 — COMPLETE.** `src/01_wrangle.py`
**Phase 2 — IN PROGRESS.** Disambiguation scripts 02–06 written and producing output.

## Completed Scripts
| Script | Purpose | Status |
|---|---|---|
| `src/00_profile_arc.py` | Parse raw_json.csv, extract grants/investigators/FOR | Complete |
| `src/01_wrangle.py` | Join summaries, filter to in-scope schemes/roles | Complete |
| `src/02_profile_openalex_authors.py` | Profile OAX authors table | Complete |
| `src/03_match_layer1_orcid.py` | ORCID matching, two passes (AU then global) | Complete |
| `src/04_build_institution_concordance.py` | ARC admin_org → OAX institution_id | Complete |
| `src/05_match_layer2_names.py` | Family-name candidates + first_score signal | Complete |
| `src/06_assign_layer2_matches.py` | Tiered assignment of Layer 2 matches | Complete |

## Current Processed Outputs (`/home/lc/m/working/ARC/processed/`)
| File | Rows | Notes |
|---|---|---|
| grants.parquet | 30,551 | Phase 1 |
| investigators.parquet | 62,747 | Phase 1 |
| layer1_orcid_matches.parquet | 10,971 | Layer 1 — one row per matched ORCID |
| layer1_residual.parquet | 31,054 | Layer 1 — no_orcid / not_in_oax |
| institution_concordance.parquet | 114 | ARC org → OAX institution_id (many-many) |
| layer2_name_candidates.parquet | 2,657,451 | All family-name candidate pairs |
| layer2_matches.parquet | 6,835 | Layer 2 resolved |
| layer3_residual.parquet | 13,710 | Input for Layer 3 |

## Disambiguation Summary (~23,041 unique investigators)
| | Persons | % |
|---|---|---|
| Layer 1 ORCID matched | 10,865 | 47% |
| Layer 2 name matched | 6,835 | 30% |
| **Total with OAX link** | **17,700** | **77%** |
| Layer 3 residual | 5,385 | 23% |

## Layer 2 Design
- Family-name token index on OAX AU authors
- `first_score`: 1.0 full token match / 0.8 initial match (bidirectional) / 0.0 none
- Bidirectional initial: ARC initial→OAX full AND ARC full→OAX initial-only both get 0.8
- Positional-agnostic tokens: "C. Fred Bloggy" yields [c, fred, bloggy] — "Fred" matches
- Assignment tiers: unique full match / unique initial match / inst-confirmed shortlist

## Next Step — Immediate
Switch script 05 OAX AU pool from `last_known_institutions` to `affiliations`.
Covers researchers who held ARC grants at AU institutions but have since moved abroad.
Examples: Adina Roskies (Sydney → UCSB), Aina Puce (UQ/UniMelb → Indiana).
Pool grows 580k → 1.1M (+96%). Change both WHERE filter and au_inst_ids extraction.

## Known Data Quality Issues
- All 51,784 ARC ORCIDs had trailing whitespace (stripped on extraction)
- 40 Layer 1 name-fail cases: 4 diacriticals (accept), ~3 married name changes, 3 possible wrong OAX ORCIDs
- OAX AU pool misses researchers who have left Australia (fix: use affiliations field)

## Deferred Tasks
1. Revert `00_profile_arc.py` to retain both admin_org fields (announcement vs current)
2. Revisit UNSW Canberra/ADFA split in institution concordance

## What Claude Should NOT Do
- Build architecture before data is profiled
- Add dependencies without discussion
- Solve edge cases before the common case is working
- Change scope lists (KEEP_ROLES, KEEP_SCHEMES) without discussion
- Use Dropbox paths — working data is always on /home/lc/m/
