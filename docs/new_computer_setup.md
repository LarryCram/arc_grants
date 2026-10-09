# Setting up arc_grants on a new computer (2026-10-10)

GitHub (`git@github.com:LarryCram/arc_grants.git`) holds all code, tests, docs and every
hand-curated file (`data_persisted/`, `data/*.xlsx`). Everything else lives outside the repo and must
be copied by hand. Sizes are as measured on the old machine on 2026-10-10.

## 1. What to copy (not in git)

| What | Old path | Size | Needed for |
|---|---|---|---|
| `.env` (secrets + machine paths) | `~/Projects/arc_grants/.env` | tiny | everything. Copy privately (USB / password manager), **never through git** |
| Working data root | `/home/lc/k/WORKING_ARC_PROJECT/` | 14 GB | everything (see below) |
| OpenAlex Jul26 snapshot | `/home/lc/m/openalex_jul26/parquet_converted/` | 443 GB | 00b, linker (02), oeuvres (03); skip if the new machine already has it |
| ORCID bulk table | `/home/lc/s/orcid/orcid_bulk.parquet` | 1.8 GB | 00e (ORCID bulk pass) |
| ORCID bulk dump (source of the table) | `/home/lc/s/orcid/records.jsonl.gz` | 0.9 GB | only to rebuild `orcid_bulk.parquet` |
| Scopus (pybliometrics) cache | `/home/lc/k/era/.scopus/` | 4.1 GB | 00d, 00f without spending Scopus quota (shared with SmallProjects) |
| Claude Code memory | `~/.claude/projects/-home-lc-Projects-arc-grants/memory/` | 120 KB | Claude's project memory |
| Claude Code plans | `~/.claude/plans/` | 0.5 MB | plan files CLAUDE.md refers to (e.g. `sunny-sauteeing-peach.md`) |
| Repo scratch (optional) | `~/Projects/arc_grants/scratch/` | 28 MB | throwaway checks; gitignored |

Inside the working data root:

| Folder | Size | Note |
|---|---|---|
| `processed/` | 7.4 GB | all pipeline outputs, incl. `oeuvre/gemini_verdicts.jsonl` (paid Gemini answers -- **irreplaceable**), `scopus_extract/`, `orcid_bulk_extract/`, `oax_link/`, `authorships_hep.parquet` / `works_hep.parquet` (built by hand via `SQL.md`) |
| `diskcache/` | 4.7 GB | ORCID record cache (`orcid_records_authenticated` etc.) -- hard to reacquire |
| `raw/` | 243 MB | ARC source (`raw_json.csv`, `grant_summaries.csv`) |
| `admin_orgs.csv`, `for_oax_concordance.csv`, `FOR_2008_2020.xlsx` | small | inputs read via `config/settings.py` |
| `output/` | 306 MB | optional |
| `ZARCHIVE/` | 1.1 GB | optional (old outputs) |
| `scopus_config.ini` | tiny | regenerated automatically by `src/utils/scopus.py::init_scopus()` |

Not needed: `/home/lc/s/orcid/ORCID_2023_10_activities` (146 GB) and `_summaries` (raw ORCID dump),
`orcid_bulk_pre_all_full_name_keys_20260906.parquet.bak`.

Copying with rsync keeps timestamps (some scripts compare file times):

```bash
rsync -aP /home/lc/k/WORKING_ARC_PROJECT/  NEW:/path/WORKING_ARC_PROJECT/
```

## 2. On the new computer

```bash
# 1. GitHub access (SSH key added to the GitHub account), then
git clone git@github.com:LarryCram/arc_grants.git ~/Projects/arc_grants
cd ~/Projects/arc_grants

# 2. Python 3.12 virtual environment (old machine: Python 3.12.3)
python3.12 -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install -r requirements.txt     # includes research_classification from GitHub

# 3. Put .env in the repo root and edit the machine paths in it:
#    DATA_ROOT, OUTPUT_ROOT, OPENALEX_DIR, DUCKDB_TMP_DIR (a folder on a disk with real free
#    space), ORCID_DISKCACHE_DIR (= DATA_ROOT/diskcache), ORCID_BULK_PARQUET, SCOPUS_CACHE_DIR.
#    Keys stay as they are: OPENALEX_EMAIL/API_KEY, ORCID_CLIENT_ID/SECRET, sco_apikey,
#    sco_insttoken, GEMINI_API_KEY, GEMINI_FREE_API_KEY.

# 4. Check
.venv/bin/python -c "from config.settings import PROCESSED_DATA, OPENALEX_DIR; print(PROCESSED_DATA, OPENALEX_DIR)"
.venv/bin/python -m pytest -q tests analysis/tests
```

Keeping the same paths as the old machine (e.g. `/home/lc/k`, `/home/lc/m`, `/home/lc/s` as
symlinks to the data drives) means `.env` needs no edits. A few defaults in code still point at
`/home/lc/s/orcid/...` (`config/settings.py` ORCID_BULK_DUMP/PARQUET); set them in `.env` if the
paths differ.

Claude Code memory: the folder name under `~/.claude/projects/` is the repo path with `/` replaced
by `-`. Clone to `~/Projects/arc_grants` under the same user name (`lc`) and copy the memory folder
to `~/.claude/projects/-home-lc-Projects-arc-grants/memory/`; otherwise rename it to match.

Scopus: the project writes its own pybliometrics config from `.env` (`init_scopus()`); the global
`~/.config/pybliometrics.cfg` credentials can't use Author Retrieval.

## 3. Rerunning the pipeline (order)

```
00a_extract_arc.py -> 00b_extract_oax.py -> 00c_extract_propensities.py -> 00d_extract_scopus.py
-> 00e_extract_orcid_bulk.py -> (00f_extract_scopus_documents.py, Scopus quota) ->
01_build_arc_acifs.py -> 02_link_arc_oax.py -> 03_build_oeuvres.py -> analysis/23_dossier.py
```

With `processed/` copied, nothing earlier than `01` needs rerunning; `01 -> 02 -> 03` takes about
8 minutes. Before switching, check on the old machine that `git status` is clean and
`git log origin/main..` is empty.
