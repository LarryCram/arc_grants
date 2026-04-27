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
- No embeddings/vectors until deterministic methods are exhausted
- HEP codes (2–5 char strings) used for institution signal — not raw admin_org or OAX IDs
- Prefer persisted checkpoints over runtime construction
- Frozensets not required — use serialisable forms (lists, dicts)
- `first_names_compatible` is a "cannot rule out" criterion, not a positive match
- Post-nominals (AO, AM, OAM, AC, FAA…) stripped from family_name before any processing

## Scope Decisions (config/scope.py)
**KEEP_ROLES (14):** CI, CI-DORA, DECRA, FT, FL, FF, APD, APF, ARF, QEII, APDI, ARFI, DAATSIA, IRF
**KEEP_SCHEMES (7):** DP, LP, DE, FT, FL, FF, DI

## Phase Status
**Phase 0 — COMPLETE.** `src/00_profile_arc.py`
**Phase 1 — COMPLETE.** `src/01_wrangle.py`
**Phase 2 — COMPLETE.** Scripts 02–08 written and producing output. 85% of investigators linked to OAX.
**Phase 3 — IN DESIGN.** Cluster-based pipeline. Design doc: `docs/cluster_pipeline_design.md`.

## Phase 2 Scripts (complete, do not modify without discussion)
| Script | Purpose |
|---|---|
| `src/00_profile_arc.py` | Parse raw JSON, extract grants/investigators/FOR |
| `src/01_wrangle.py` | Join summaries, filter to in-scope schemes/roles |
| `src/02_profile_openalex_authors.py` | Profile OAX authors table |
| `src/03_match_layer1_orcid.py` | ORCID matching, two passes (AU then global) |
| `src/04_build_institution_concordance.py` | ARC admin_org → OAX institution_id + hep_code |
| `src/05_match_layer2_names.py` | Family-name candidates + first_score signal |
| `src/06_assign_layer2_matches.py` | Tiered assignment of Layer 2 matches |
| `src/07_filter_layer3_for.py` | FoR→OAX topic pre-pass, tiers 4/5/6 |
| `src/08_match_layer3_oax_api.py` | OAX API search for no-match residual, tier 7 |

## Analysis/Probe Scripts
| Script | Purpose |
|---|---|
| `src/00b_profile_name_clusters.py` | Name cluster profiling — keep as-is, not a pipeline step |
| `src/00c_probe_nameparser_family.py` | Validates nameparser on ARC family names |

## Phase 3 Scripts (planned, scripts 10–15)
| Script | Layer | Output |
|---|---|---|
| `src/10_build_clusters.py` | Layer 0 | `clusters.jsonl` |
| `src/11_oax_name_index.py` | Pre-processing | `oax_name_index.parquet` |
| `src/12_layer1_orcid.py` | Layer 1 | `clusters.jsonl` (updated) |
| `src/13_layer1_5_orcid_api.py` | Layer 1.5 | `clusters.jsonl` (updated) |
| `src/14_layer2_names.py` | Layer 2 | `clusters.jsonl` (updated) |
| `src/15_layer3_for.py` | Layer 3 | `clusters.jsonl` (updated) |

## Current Processed Outputs (`/home/lc/m/working/ARC/processed/`)
| File | Rows | Notes |
|---|---|---|
| grants.parquet | 30,551 | Phase 1 |
| investigators.parquet | 62,747 | Phase 1 |
| institution_concordance.parquet | 115 | hep_code column added; UNSW/UNE/Macquarie/Torrens/Newcastle fixed |
| layer1_orcid_matches.parquet | 10,971 | Layer 1 |
| layer2_matches.parquet | 7,881 | Layer 2 |
| layer3_for_matches.parquet | 749 | Layer 3 FoR tiers 4/5/6 |
| layer3_api_matches.parquet | 4 | Layer 3 API tier 7 |
| layer3_residual.parquet | 11,490 | Input for cluster pipeline |
| name_clusters_summary.csv | 22,524 | Profiling |
| oax_name_index.parquet | TBD | Planned: reverse OAX name index |

## Disambiguation Summary (~23,041 unique investigators)
| | Persons | % |
|---|---|---|
| Layer 1 ORCID matched | 10,865 | 47% |
| Layer 2 name matched | 7,881 | 34% |
| Layer 3 FoR pre-pass | 749 | 3% |
| Layer 3 API | 4 | <1% |
| **Total with OAX link** | **19,499** | **85%** |
| Residual ambiguous | 3,554 | 15% |
| Residual no-match | 32 | <1% |

## Institution Signal
- AU HEPs only (~42 institutions for all time); identified by HEP=y in `DATA_ROOT/admin_orgs.csv`
- 2–5 char HEP codes: ANU, UNSW, UM, MON, UQ, UWA, UTAS, UNISA, RMIT, QUT, UTS…
- Non-HEP orgs (research institutes, government) are treated as no institution signal
- `src/utils/names.py` — shared helpers: strip_postnominals, strip_diacriticals, strip_parens, norm_alpha, tokens

## Known Data Quality Issues
- All 51,784 ARC ORCIDs had trailing whitespace (stripped on extraction)
- 10 family_name entries had post-nominal awards (AO, AM, OAM, AC, FAA) — stripped by strip_postnominals()
- "Rachel Ong ViforJ" — company name appended to family name (4 records, real family = "Ong")
- 231 family names where nameparser disagrees with ARC: mostly compound names and post-nominals

## Deferred Tasks
1. Revert `00_profile_arc.py` to retain both admin_org fields (announcement vs current)
2. UNSW Canberra/ADFA split in institution concordance
3. Fix 5 confirmed compromised matches from Phase 2 (Timothy Dwyer, Claire Roberts, Jun Ma, David Morrison, Paul Burke) — defer until cluster pipeline validates

## What Claude Should NOT Do
- Build architecture before data is profiled
- Add dependencies without discussion
- Solve edge cases before the common case is working
- Change scope lists (KEEP_ROLES, KEEP_SCHEMES) without discussion
- Use Dropbox paths — working data is always on /home/lc/m/
- Use frozensets where serialisable forms (lists, dicts) will do
- Apply institution filter as a gate — it is always a tiebreaker
