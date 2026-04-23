# arc_grants# ARC Grants Investigator Study

Longitudinal study of research productivity effects associated with ARC funding,
2001–2025. Includes investigator disambiguation, publication linkage via OpenAlex,
and matched comparison set construction.

## Structure

- `src/` — Analysis pipeline scripts, numbered sequentially
- `docs/` — LaTeX methodology and decisions documents
- `config/` — Settings and path configuration
- `data/` — **Not in repo.** Stored on portable SSD. See `config/settings.py`
- `output/` — **Not in repo.** Machine-local generated outputs

## Setup

1. Copy `.env.example` to `.env` and set `DATA_ROOT` to your local data path
2. `pip install -r requirements.txt`
3. Run scripts in `src/` in numbered order

## Data

Raw data is ARC grants CSV (sourced from ARC public API), stored externally.
Not committed to this repository.
Study of ARC grants
