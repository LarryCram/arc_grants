# Project Context for Claude Sessions

Paste this at the start of any Claude session (here or VS Code) to restore context.
**For full detail on pipeline architecture, current data scale, design decisions, and
recent session history, read `CLAUDE.md` — that's the actively maintained source of truth.**
This file is a condensed orientation only; don't let it drift out of sync with `CLAUDE.md`
again (see git history for what happened last time — it described an entirely superseded
pre-Splink pipeline for months before being rewritten 2026-08-09).

## Project
Longitudinal study of ARC grant productivity effects, 2001-2025.
Three outputs: colleague commission report, policy report, scholarly paper.
Early-career-researcher (DECRA/APD/APDI) focused "prompt paper" in progress.

## My Stack
Python, SQL, DuckDB, Parquet. Linux (home/lc/m SSD mount). VS Code. Moderate disambiguation
experience. Prefer .py files over notebooks. LaTeX in codebase as living methodology document.

## Pipeline status: COMPLETE, in extended analysis phase
The Splink-based `00_extract_arc.py` → `01_prepare_arc.py` → `02_prepare_oax.py` →
`03_link_arc_oax.py` → `04_resolve_links.py` pipeline (see `CLAUDE.md`'s "Pipeline Architecture")
is complete and has been for some time — this is not a design-phase project anymore.
Current work is downstream: bibliometric analysis (`analysis/00`-`07`), OpenAlex snapshot
migration (Feb26 → Jul26, done 2026-08-08), and the early-career fellowship cohort study.
`ZARCHIVE/src_archive_20260520/` holds a fully superseded earlier pipeline attempt — never
reference it for current architecture.

## Data
- ARC grants CSV, ~34,000 records, sourced from ARC public API
- OpenAlex data: Jul26 native-parquet snapshot (migrated from an older Feb26 gz-derived
  extract 2026-08-08 — see `CLAUDE.md`'s "OpenAlex Snapshot Migration"). Path via `OPENALEX_DIR`
  in `.env`; exact current scale numbers live in `CLAUDE.md`'s "Data Scale", not duplicated here.
- Working data: `DATA_ROOT` in `.env`

## Key Decisions Made
- Precision over recall in disambiguation (false merge worse than missing record)
- FOR-code cross-series harmonisation now delegated to the external `research_classification`
  package (reaches full FOR2020 group/4-digit precision, not just 2-digit division) — see
  `CLAUDE.md`'s "FOR Code Handling". The old "2-digit level only" limitation no longer applies.
- ORCID treated as hint requiring validation, not authoritative
- No embeddings/vectors until deterministic methods are exhausted
- Institution signal (HEP codes) is always a disambiguation tiebreaker, never a hard gate
- Prefer persisted checkpoints over runtime construction
- Post-nominals (AO, AM, OAM, AC, FAA…) stripped from family_name before any processing
- `data_persisted/` (repo root) holds every hand-curated/hard-to-reacquire file — see
  `CLAUDE.md`'s "Key Paths". Never let precious manual work live only in a gitignored path again.

## Scope Decisions
See `config/scope.py` directly (`KEEP_ROLES`, `KEEP_SCHEMES`, `ECR_ROLES`) — role-code labels
there were corrected 2026-08-08 against ARC's own `role_name` field (APF, ARFI, IRF were
mislabeled). Don't duplicate the list here; it drifts.

## Institution Signal
- AU HEPs only (~42 institutions for all time); identified by HEP=y in `DATA_ROOT/admin_orgs.csv`
- Non-HEP orgs (research institutes, government) treated as no institution signal
- Institution overlap is always a disambiguation tiebreaker in `04_resolve_links.py`, never a
  hard gate

## What Claude Should NOT Do
- Build architecture before data is profiled
- Add dependencies without discussion
- Solve edge cases before the common case is working
- Change scope lists (`KEEP_ROLES`, `KEEP_SCHEMES`, `ECR_ROLES`) without discussion
- Use Dropbox paths — working data is always on `/home/lc/k/` (moved from `/home/lc/m/` 2026-08-14
  drive consolidation — see `CLAUDE.md`'s "Key Paths")
- Apply institution filter as a gate — it is always a tiebreaker
- Let this file or any other doc duplicate numbers/architecture detail that `CLAUDE.md` already
  owns — link to it instead, so there's one source of truth to keep current
