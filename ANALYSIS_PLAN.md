# ARC Oeuvres & Bibliometrics Plan

## Context

We have 22,490 resolved ARC chief-investigator/fellow → OpenAlex author mappings
(`arc_oax_resolved.parquet`). The goal is to:

1. Fetch all OAX works for each resolved ARC person and validate the mapping and oeuvre accuracy
2. Compute year-by-year bibliometric performance (2000–2025) relative to Australian and
   world baselines, broken down by ARC FoR code (new version)
3. Persist results as parquet for interactive querying in DuckDB UI or Python notebooks

All heavy OAX scans are done once and persisted; subsequent analysis hits only the
small derived parquets.

---

## Data Landscape

| Source | Format | Key columns |
|--------|--------|-------------|
| `processed/arc_oax_resolved.parquet` | flat | arc_id, oax_id (URL), secondary_oax_ids[] |
| `$OPENALEX_DIR/authorships/` | ~1980 partitioned parquets | work_idx, author_idx (BIGINT), institution_name, ror, country_code |
| `$OPENALEX_DIR/works/` | ~1981 partitioned parquets | work_idx, title, publication_year, cited_by_count, type, doi |
| `$OPENALEX_DIR/topics/` | ~1978 partitioned parquets | work_idx, topic_idx, score, subfield_name, field_name, domain_name |
| `processed/openalex_authors_prep.parquet` | flat | unique_id (URL), works_count, topic_names |
| `$OPENALEX_DIR/references/` | ~1981 partitioned parquets | work_idx, cited_work_idx — needed for year-by-year citation accumulation (future step, very large) |

**ID mapping**: `oax_id = 'https://openalex.org/A5027429235'` →
`author_idx = CAST(regexp_replace(oax_id, 'https://openalex.org/A', '') AS BIGINT) = 5027429235`

Secondary IDs: `secondary_oax_ids` is `VARCHAR[]` in resolved; same extraction applies.

**Known limitation — secondary OAX ID completeness**: `secondary_oax_ids` in the resolved
file captures only the split-record duplicates already detected by `04_resolve_links.py`.
Additional OAX records for the same real person (name variants, institution changes,
early-career records) will exist but are not yet found. Extending secondary ID coverage is
a linkage-pipeline problem (`03_link_arc_oax` / `04_resolve_links`) to be revisited once
oeuvre quality diagnostics reveal the gaps. The oeuvres pipeline uses what it is given
and flags this as a known source of missing works.

---

## Output Location

All data outputs go to `PROCESSED_DATA.parent / "analysis"` →
`/home/lc/m/working/WORKING_ARC_PROJECT/analysis/` (fast SSD, not in the git repo).
Scripts live under `analysis/` in the project tree.

```
/home/lc/m/working/WORKING_ARC_PROJECT/analysis/
  samples.parquet              # arc_id sample flags: in_sample_10, in_sample_1000
  oeuvres.parquet              # arc_id, work_idx, publication_year, cited_by_count,
                               # type, doi, title, topic_idx, subfield_name,
                               # field_name, domain_name, is_primary_author_id
  annual_metrics.parquet       # arc_id, year, first_pub_year, n_pubs, n_citations,
                               # h_index, n_highly_cited, top_field
  collab_metrics.parquet       # arc_id, year, collab_country, collab_institution,
                               # n_shared_works
  au_works.parquet             # all OAX work_idxs with ≥1 AU author (baseline)
  au_annual.parquet            # year, n_au_pubs, n_au_citations, au_h_index
  world_annual.parquet         # year, total_pubs, total_citations (from works)
  citation_quantiles.parquet   # field_name, publication_year, p90, p99 cited_by_count
```

---

## Scripts (all under `analysis/`)

### `00_samples.py`
- Load `arc_oax_resolved.parquet`, take `USING SAMPLE 10 REPEATABLE(42)` and
  `SAMPLE 1000 REPEATABLE(42)`
- Persist `samples.parquet` with columns `arc_id, in_sample_10, in_sample_1000`
- Print sample arc_ids with names from `arc_persons.parquet` for inspection

### `01_fetch_oeuvres.py`
The expensive step — scans all OAX authorships (no year filter; all years fetched).

Steps:
1. Build `arc_author_map`: a flat table `(author_idx BIGINT, arc_id VARCHAR, is_primary BOOL)`.
   - For each row in `arc_oax_resolved`: extract numeric `author_idx` from `oax_id` URL
     (strip `https://openalex.org/A`), mark `is_primary = TRUE`.
   - For each element of `secondary_oax_ids[]`: extract `author_idx`, mark `is_primary = FALSE`.
   - Result: one row per (arc_id, author_idx) pair. Multiple author_idxs per arc_id is normal
     (primary + secondary). One author_idx appearing for multiple arc_ids would be an error —
     flagged in accuracy check.
2. DuckDB glob scan over `authorships/**/*.parquet`, filter:
   ```sql
   WHERE author_idx IN (SELECT author_idx FROM arc_author_map)
   ```
3. Join filtered authorships → `works/**/*.parquet` on `work_idx` (get title, year,
   cited_by_count, type, doi).
4. Left join → `topics/**/*.parquet` on `work_idx` (some works have no topic; keep one
   row per work — take the highest-score topic if multiple).
5. Map back to `arc_id` and `is_primary` via `arc_author_map`.
6. Deduplicate: if a work appears under both primary and secondary author_idx for the same
   arc_id, keep one row with `is_primary_author_id = TRUE`.
7. **No year filter** — persist all years. Year filtering is applied at the metrics stage.
8. Persist `oeuvres.parquet`.

**Block strategy**: DuckDB reads all partition files in parallel via glob. Run first
against the sample-10 `arc_author_map` subset to measure time and verify output before
running on all 22,490 persons.

### `02_accuracy_check.py`
Diagnostics printed to stdout. Takes `--sample 10` or `--sample 1000` flag.

Checks:
1. **Shared OAX IDs across ARC persons**: flatten `arc_author_map` (primary + secondary),
   flag any `author_idx` appearing for 2+ distinct `arc_id`s — these are linkage errors.
2. **Extra works from merged split-records**: for persons with secondary IDs, count works
   attributable only to secondary author_idxs vs. primary — shows dedup value and flags
   cases where secondary IDs add suspiciously many or few works.
3. **works_count reconciliation** (sample of 10): compare oeuvres row count per arc_id
   vs. `works_count` from `openalex_authors_prep`. Divergence expected (year filter, split
   records); flag large unexplained gaps as possible missing secondary IDs.
4. **Author name check** (sample of 10): for each arc_id, show distinct `author_name`
   values from authorships — verify consistent with ARC name variants.
5. **Work type distribution**: count by `type` across the sample.
6. **Year continuity diagnostic** (key quality check):
   - Per arc_id: first publication year, last publication year, full year range
   - Gaps ≥ 3 consecutive years in the middle of a career (suspicious — may indicate
     missing OAX records or a name change period)
   - Distribution of first-publication years across the sample (feeds "academic age")
   - `academic_age = 2026 − first_pub_year`; career stage at first ARC grant =
     `first_grant_year − first_pub_year` (join from `arc_persons.grant_ids` → `grants_flat`)

### `03_annual_metrics.py`
Queries `oeuvres.parquet` only (no OAX scan).

For each `arc_id`:
- Derive `first_pub_year = MIN(publication_year)` across all works (all years, not just 2000+)

For each `arc_id × year` (2000–2025):
- `n_pubs`: count distinct `work_idx` published in that year
- `n_citations`: sum `cited_by_count` of works published in that year (snapshot total —
  limitation flagged in output)
- `h_index`: cumulative, using all works published up to and including that year,
  ranked by `cited_by_count` DESC; h = MAX(rank) WHERE cited_by_count ≥ rank
- `n_highly_cited`: field×year-normalized count (populated after `04b_citation_quantiles.py`)
- `top_field`: modal `field_name` by work count that year
- Persist `annual_metrics.parquet` with `first_pub_year` as a per-person constant column.

For `collab_metrics.parquet`: join `oeuvres` back to `authorships` filtered to co-authors
(other `author_idx` on same `work_idx`), group by `arc_id, year, country_code,
institution_name`.

### `04_au_baseline.py`
One-time scan of authorships for `country_code = 'AU'` → collect all `work_idx` values.
Join to works for `publication_year, cited_by_count`.
Persist `au_works.parquet` (work_idx, year, cited_by_count only).
Compute `au_annual.parquet`.

For world baseline: aggregate `works/**/*.parquet` by year.
Persist `world_annual.parquet`.

### `04b_citation_quantiles.py`
One-time scan of `works/**/*.parquet` joined to `topics/**/*.parquet`.
For each `(field_name, publication_year)`: compute P90 and P99 of `cited_by_count`.
Persist `citation_quantiles.parquet`.
Used by `03_annual_metrics.py` to populate `n_highly_cited` (works above P90 or P99
in their field × year).

### `05_explore.py` (skeleton, grows over time)
- Load persisted parquets into DuckDB in-memory
- Summary tables: top 20 ARC persons by citations, H-index distribution, first-pub-year
  distribution, academic age at first grant
- Time series plots: ARC vs AU vs World publication counts, citation counts
- This file is exploratory — not a fixed pipeline stage

---

## Execution Order & Sample Strategy

```
python analysis/00_samples.py
python analysis/01_fetch_oeuvres.py --sample 10     # test + verify
python analysis/01_fetch_oeuvres.py --sample 1000   # timing
python analysis/01_fetch_oeuvres.py                 # full (22,490 persons)
python analysis/02_accuracy_check.py --sample 10
python analysis/03_annual_metrics.py
python analysis/04_au_baseline.py
python analysis/04b_citation_quantiles.py
python analysis/05_explore.py
```

`--sample N` restricts the `arc_author_map` to persons flagged in `samples.parquet`.

---

## Confirmed Design Decisions

1. **"Highly cited" threshold**: field × year-normalized (P90 / P99 of `cited_by_count`
   within field × publication year). Requires `citation_quantiles.parquet` from
   `04b_citation_quantiles.py`.

2. **Citation year problem**: `cited_by_count` is a Feb-2026 snapshot total, not
   citations received by year Y. Year-by-year citation flows require the `references`
   parquet (very large join — future step). Current workaround: snapshot citations used
   for H-index and totals, clearly flagged.

3. **Work types**: all included (article, book-chapter, book, preprint). Type column
   retained in `oeuvres.parquet`; filter at query time.

4. **Year range**: `oeuvres.parquet` has no year filter — all years fetched. This is
   essential for correct H-index baselines at year 2000 and for computing first_pub_year
   (academic age). Annual metrics report 2000–2025 but use the full history as input.

5. **Academic age baseline**: `first_pub_year` = earliest publication year in oeuvres.
   `academic_age = 2026 − first_pub_year`. Career stage at first ARC grant derived from
   `grants_flat`. Publication year continuity is a key oeuvre quality diagnostic.

---

## Future Steps (not in initial build)

- **Year-by-year citation accumulation**: use `references` parquet to count how many
  papers citing work W were published in each year Y — enables true longitudinal citation
  curves. Deferred due to size.
- **Extended secondary OAX IDs**: return to `03_link_arc_oax` / `04_resolve_links` once
  oeuvre quality diagnostics (year gaps, works_count reconciliation) reveal systematic
  missing-record patterns.

---

## Verification

After `01_fetch_oeuvres.py --sample 10`:
- Query `oeuvres.parquet` in DuckDB UI: check row counts, year range, work types per person
- For a known Laureate Fellow, verify work titles match known publications

After `03_annual_metrics.py`:
- Spot-check H-index for 2–3 known researchers against Google Scholar
- Check `first_pub_year` looks plausible (career length sanity)
