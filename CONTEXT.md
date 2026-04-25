# Project Context for Claude Sessions

Paste this at the start of any Claude session (here or VS Code) to restore context.

## Project
Longitudinal study of ARC grant productivity effects, 2001-2025.
Three outputs: colleague commission report, policy report, scholarly paper.
ERC extension planned once ARC pipeline is established.

## My Stack
Python, SQL, DuckDB, Parquet. Windows. VS Code. Moderate disambiguation experience.
Prefer .py files over notebooks. LaTeX in codebase as living methodology document.

## Data
- ARC grants CSV, ~34,000 records, sourced from ARC public API
- OpenAlex Feb 2026 snapshot as parquets
- All data on portable SSD — DATA_ROOT set in .env pointing to SSD mount
- SSD mount: D:\DATA_TRANSFER\ARC\

## Key Decisions Made
- Precision over recall in disambiguation (false merge worse than missing record)
- FoR codes used at 2-digit level only due to 2018 ANZSRC revision
- ORCID treated as hint requiring validation, not authoritative
- Chronological strategy: anchor on post-2018 grants, extend backwards
- No embeddings/vectors until deterministic methods are exhausted
- No GPU infrastructure — CPU only if embeddings needed at all

## Scope Decisions (config/scope.py)
**KEEP_ROLES (14):** CI, CI-DORA, DECRA, FT, FL, FF, APD, APF, ARF, QEII, APDI, ARFI, DAATSIA, IRF
- Excluded: PI, NP, OI (non-investigator); ECIF/MCIF/LXF*/ILF/DIA/LIF/RC-ATSI (travel grants, not fellowship holders)
- Note: DAATSIA has zero survivors after scheme filter — all in out-of-scope schemes

**KEEP_SCHEMES (7):** DP, LP, DE, FT, FL, FF, DI
- Excluded: LE/IE (equipment), LX/IN/IL (mobility), IH (industry hubs), SR, CE, and small schemes

## Phase Status
**Phase 0 — COMPLETE.** ARC CSV profiled. Outputs written.
**Phase 1 — COMPLETE.** Wrangled and filtered. Outputs written.
**Phase 2 — BLOCKED.** Disambiguation cannot start until OpenAlex author entity parquet is available.

## Completed Scripts
- `src/00_profile_arc.py` — parses raw_json.csv, extracts grants/investigators/FOR codes, writes parquets + profile txt
- `src/01_wrangle.py` — joins grant_summaries enrichment, filters to in-scope schemes/roles, writes wrangled parquets

## Current Outputs (D:\DATA_TRANSFER\ARC\processed\)
| File | Rows | Notes |
|---|---|---|
| grants_flat.parquet | 33,650 | Phase 0 — all grants |
| investigators_raw.parquet | 116,238 | Phase 0 — all roles |
| for_codes.parquet | 115,880 | Phase 0 |
| grants.parquet | 30,551 | Phase 1 — enriched + filtered |
| investigators.parquet | 62,747 | Phase 1 — in-scope roles only |
| for_codes_wrangled.parquet | 104,530 | Phase 1 — filtered |

## OpenAlex Data (D:\DATA_TRANSFER\ARC\OPENALEX\)
- `authors_AU.parquet` — 10.4M rows — authorships format (work_id, author_id, author_name, institution_id, institution_name, ror, country_code). NOT the author entity table.
- `authorships_AU.parquet` — 11.5M rows — same schema, slightly different filter (reason TBD)
- **Still needed:** Author entity parquet (one row per author, with orcid, display_name, display_name_alternatives, last_known_institution). This is required for Layer 1 ORCID matching.

## Disambiguation Plan (agreed, not yet coded)
Three layers:

**Layer 1 — ORCID anchor**
Match ARC ORCID ↔ OpenAlex ORCID. Four sub-cases:
- Both have ORCID → direct match, validate with name similarity
- ARC has ORCID, OpenAlex doesn't → search by name, confirm with ORCID post-hoc
- ARC has no ORCID, OpenAlex has ORCID → name match → verify
- Neither has ORCID → Layer 2/3
ARC ORCID coverage: 44.5%. OpenAlex AU coverage higher than global 30% due to AU ~2015 requirement.
OpenAlex author entity table has: orcid, display_name, display_name_alternatives, last_known_institution.

**Layer 2 — Name + institution matching**
Match remaining investigators by name against OAX AU authors, confirmed by institution.
Do NOT attempt name matching for investigators already matched in Layer 1.

Matching rules (designed for precision over recall):
- Search OAX display_name AND all display_name_alternatives
- If ARC has a full first name: require a full first-name token match — do NOT give credit for an OAX initial matching an ARC full name (too many false positives)
- If ARC has an initial-only first name: initial match against OAX full names is acceptable
- Diacriticals: strip in first pass (ARC rarely uses them; OAX retains them). After seeing the misses, add a diacritical-normalisation pass
- Multiple OAX candidates above threshold → flag as ambiguous → Layer 3
- Institution (ARC admin_org vs OAX last_known_institutions) used as confirmation signal, not primary filter

Rationale for name over co-author network: Australia's multicultural population means most names are effectively unique. Name + institution matching is direct evidence; co-author network is indirect, computationally heavier, and less informative about misses and duplicates.

**Layer 3 — AI for residual**
Estimated ~10–15% of investigators after Layers 1–2. Bounded, well-defined problem — genuinely hard cases only.

## Known Data Quality Issues
- All 51,784 ARC ORCIDs had trailing whitespace (stripped on extraction)
- 451 investigators with initial-only first names — mostly CI role, genuine disambiguation challenge
- authors_AU.parquet and authorships_AU.parquet have slightly different row counts for same schema — filter difference TBD

## What Claude Should NOT Do
- Build architecture before data is profiled
- Add dependencies without discussion
- Solve edge cases before the common case is working
- Change scope lists (KEEP_ROLES, KEEP_SCHEMES) without discussion

## Next Step
Locate or create the OpenAlex author entity parquet. Then write `src/02_profile_openalex_authors.py`.
Always peek at schema (columns, dtypes, 2 sample rows) before writing any profile script.
