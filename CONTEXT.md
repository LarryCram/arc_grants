# Project Context for Claude Sessions

Paste this at the start of any Claude session (here or VS Code) to restore context.

## Project
Longitudinal study of ARC grant productivity effects, 2001-2025.
Three outputs: colleague commission report, policy report, scholarly paper.
ERC extension planned once ARC pipeline is established.

## My Stack
Python, SQL, DuckDB, Parquet. Ubuntu. VS Code. Moderate disambiguation experience.
Prefer .py files over notebooks. LaTeX in codebase as living methodology document.

## Data
- ARC grants CSV, ~60,000 records, sourced from ARC public API
- Stored on portable SSD, not in repo
- DATA_ROOT set in .env pointing to SSD mount

## Key Decisions Made
- Precision over recall in disambiguation (false merge worse than missing record)
- FoR codes used at 2-digit level only due to 2018 ANZSRC revision
- ORCID treated as hint requiring validation, not authoritative
- Chronological strategy: anchor on post-2018 grants, extend backwards
- No embeddings/vectors until deterministic methods are exhausted
- No GPU infrastructure — CPU only if embeddings needed at all

## Current Phase
Phase 0 — Data profiling. ARC CSV not yet examined.
Next step: run 00_profile_arc.py once data path is configured.

## Repo
https://github.com/yourusername/arc_grants

## What Claude Should NOT Do
- Build architecture before data is profiled
- Add dependencies without discussion
- Solve edge cases before the common case is working