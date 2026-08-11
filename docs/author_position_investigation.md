# Author position: what we know, for whoever re-extracts the raw OpenAlex snapshot

## Purpose of this note

Author position (first/middle/last, corresponding) is not available anywhere in this project's
local OpenAlex extract. This was investigated once (2026-08-10, during planning for an ECR
bibliometric analysis extension — see `analysis/07_analyse_ecr_fellowships.py` and the plan that
prompted this note) and deliberately parked as a side project, not built into any current
pipeline work. This document exists so a future session tasked with fixing this — most likely one
doing raw-snapshot re-extraction work, mirroring the Feb26→Jul26 migration documented in
`CLAUDE.md`'s "OpenAlex Snapshot Migration" — doesn't have to re-derive these findings from
scratch.

## What's confirmed absent locally

None of this project's three local OpenAlex extract variants carry any author-position or
ordinal field. Checked directly via `DESCRIBE` against all three:
- `$OPENALEX_COMPACT_DIR/authorships/*.parquet`
- `$OPENALEX_DIR/xpac/authorships/*.parquet`
- `$OPENALEX_DIR/xpac_raw/authorships/*.parquet`

All three have the identical schema: `work_idx, author_idx, author_name, institution_idx,
institution_name, ror, country_code`. No `author_position`, no `is_corresponding`, no ordinal/
sequence column of any kind. `$OPENALEX_COMPACT_DIR/works/*.parquet` was also checked and has
nothing relevant either (`work_idx, doi, title, authors_count, institutions_distinct_count,
publication_year, referenced_works_count, cited_by_count, type, is_retracted, is_paratext,
volume, issue, first_page, last_page, source_id, host`).

## What OpenAlex's own API confirms is real, upstream

Fetched `https://api.openalex.org/works/doi:10.33448/rsd-v9i7.3545` directly (a real 5-author
paper, chosen because it happened to be in the local ARC-linked data with a usable DOI) and got
the raw JSON `authorships` array back from the user directly (not just a summarized read — see
"Methodology note" below for why that distinction mattered). Each authorship object carries:
- `author_position`: `"first"` | `"middle"` | `"last"` — **only a 3-way categorical**. A paper
  with 8 authors has exactly one `"first"`, one `"last"`, and six undifferentiated `"middle"`
  entries — OpenAlex's own schema does not expose finer ordinal rank among middle authors. This
  caps what's ever recoverable even from a perfect re-extraction: "was this person first or last
  author" (a real, useful PI-vs-junior-collaborator signal), not "which numbered position."
- `is_corresponding`: boolean, present per authorship.
- The array's own element order in the JSON response, which for this work was: Kirby (first) →
  Jung (middle) → Neves (middle) → Gregório (middle) → Gouvêa (last) — i.e. the JSON array order
  and the `author_position` labels agree with each other for this example, as expected.

## What's confirmed: local row order does not match this true order

For the same work (local `work_idx = 3021264700`), a bare scan of the local `authorships` table
(`PRAGMA threads=1`, no `ORDER BY`) returned:

```
Jung, Ivi Evelin Ferraz  de Souza      (true position: middle, #2 of 5)
Gouvêa, Mônica Villela                 (true position: last,   #5 of 5)
Kirby, Endi Evelin Ferraz              (true position: first,  #1 of 5)
Neves, Luciene Miguel Lima             (true position: middle, #3 of 5)
Gregório, Ana Paula Alves              (true position: middle, #4 of 5)
```

Completely scrambled relative to the true order. **Do not use local row/file order as a proxy for
authorship order under any circumstances** — it was checked and does not work, at least for this
example (n=1 — worth re-checking against more examples if this is ever revisited, but this one
case is a clear existence proof that order is not preserved).

## Why the order is scrambled — two independent possible causes, not yet distinguished

1. **Write-side**: whatever ETL process built this project's local flat `authorships` fact table
   from OpenAlex's native nested per-work authorship list may not have preserved order while
   flattening/unnesting it — e.g. if that step used a parallel job, a hash-based groupby/join, or
   multiple writers/partitions interleaving without tracking original sequence.
2. **Read-side**: DuckDB's glob scan across many partition files, especially with more than one
   thread, does not guarantee returning rows in file order without an explicit `ORDER BY` on a
   stored ordinal. The test above used `threads=1` to reduce this specific risk, but that only
   constrains behavior within a single file — glob file-enumeration order across the many
   partition files in `authorships/*.parquet` is a separate, unaddressed variable.

Parquet as a file format is not the culprit either way — it preserves physical write order at
rest (row groups are written and read back sequentially, the format doesn't reshuffle rows on its
own). Whatever scrambling exists was introduced either in what got written (cause 1) or how it's
being read back (cause 2), not by parquet itself.

## What to actually do, if this becomes a real task

1. **Check whether OpenAlex's raw native parquet snapshot** — upstream of whatever this project's
   own `parquet_converted` ETL does — carries `author_position` (and/or an explicit sequence
   number) inline within its own authorships representation. Given the public API clearly returns
   it, this is very likely present in the raw snapshot too, but needs confirming against the
   actual raw snapshot files directly, not assumed from the API alone.
2. **If present, re-extract with it kept as an explicit column** — e.g. `author_position VARCHAR`
   and, if the raw snapshot has one, an explicit integer sequence/row-pointer column reflecting
   true position within each work's author list. This is the same pattern already used for the
   Jul26 migration's "Step 0" (re-extracting with 3 specific columns deliberately kept — see
   `CLAUDE.md`). **Never rely on implicit row/file order to reconstruct this later** — persist it
   explicitly, for exactly the write-side/read-side reasons above.
3. **Verify against real known works before trusting it**, at more than n=1 — repeat the check
   done here (compare the new column's values against the OpenAlex API's own record for the same
   DOI) across a handful of real papers, ideally including some with well-known/checkable author
   order, before treating the new column as reliable for anything downstream.

## Methodology note (why this took two passes to get right)

The first pass here used the `WebFetch` tool, which does not return raw content — it fetches the
page and has a separate, smaller model summarize/interpret it against a prompt. That summary
happened to be accurate when checked against the user-supplied raw JSON afterward, but it was a
mistake to treat it as decisive proof before that cross-check — an intermediate summarization
step is a real place for errors (e.g. mis-ordering) to be silently introduced, invisible in the
final text response. If this is re-verified in the future, prefer fetching and reading raw JSON
directly over relying on a summarized fetch, especially for something as order-sensitive as this.

## Status

Deferred / out of scope for the current ECR bibliometric analysis work (see the plan referenced
at the top of this note). Every co-author-related table in that plan uses country + track-record
signals only — no author position field, no proxy inferred from row order.
