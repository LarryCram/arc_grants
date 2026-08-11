# Oeuvre decontamination: detecting and correcting blended-identity OpenAlex records

## Terminology and problem statement

**Blended idx**: an OpenAlex `author_idx` that actually represents the merged publication
record of two or more distinct real people, produced by OpenAlex's own name-based author
disambiguation failing on a common-enough name. Confirmed via this session's investigation,
with escalating severity:

1. **A few historical stragglers** (Urwin, Fordyce, Urquhart, Dias, and the 8-case systematic
   scan): a small number (1-8) of works from a different real person, usually decades removed
   from the ARC person's real active career, usually thin/missing institution metadata.
2. **Two (or more) genuinely productive people blended** (Quinn, Moore): each contributing a
   large, internally coherent body of work in an unrelated field, overlapping in time, not
   separable by any chronological-gap heuristic at all -- Moore's case spans a single
   continuous 1989-2018 window containing at least 4 distinct real careers (classics scholar,
   drug-policy epidemiologist, labor economist, urban-planning researcher).
3. **A few contaminating strays inside an otherwise-clean oeuvre** (Hua): the subtlest case --
   4-5 topically unrelated one-off papers scattered *mid-career*, invisible to both a
   chronological-gap check and a raw work-count check, only visible via topic-coherence
   inspection.

**The symmetric failure mode** (explicitly flagged by the user, not yet investigated): everything
above is *over-merging* (one `author_idx`, too many real people). The same weak-disambiguation
root cause should, symmetrically, also produce *under-merging* -- a real ARC person's true
oeuvre split across two or more `author_idx` records, with only one captured as their `oax_id`
in `arc_oax_resolved.parquet` and the rest missing entirely (not even caught as a
`secondary_oax_id`, since `04_resolve_links.py`'s existing secondary-ID mechanism only captures
split-records that were already candidates in the same Splink blocking pass -- it has no
mechanism to go looking for a plausible match *after the fact*, informed by what the confirmed
cluster's real coauthors/topics/institutions turned out to be). Given how *frequently*
over-merging turned up in this session's small, non-exhaustive sample, under-merging is likely
underestimated by a similar margin. Both directions need addressing, and the same evidence
(topic coherence, coauthor-network coherence, institution/country trajectory) drives both.

## Detection signals

None of these should be used alone -- each has real false-positive/false-negative modes
demonstrated directly in this session's cases. Combine them.

1. **Growth-curve check**: `N(t) ~ t^γ`, with `γ≈2.05` for highly prospective researchers and
   `γ≈1.55` for ordinary ones (user-supplied bibliometric rule of thumb). For a DECRA-type ECR,
   `t≈7` years is the realistic ceiling on genuine output-producing time (PhD duration + early
   post-PhD years, excluding the first couple of PhD years before output typically starts), giving
   `N_max≈49-54`. Total work count (through `award_year+1`, using the *cleaned* — deduped,
   exclusion-filtered — oeuvre) exceeding this is a **prior-free, gap-independent** signal: it
   doesn't need to know *where* the excess sits chronologically, just that there's too much of it.
   Caught Moore (121) cleanly; did not catch Hua (38, within bounds) — expected, since Hua's
   contamination is a handful of strays, not enough volume to trip a count check.
2. **Chronological-gap check** (this session's first approach): `award_year - first_pub_year`
   implausibly large. Caught the historical-straggler cases (Urwin, Fordyce, Urquhart, Dias, the
   flagged-8) cleanly. Blind to contamination that isn't at the chronological edge (missed
   Quinn/Moore's interleaved pattern entirely, and Hua's mid-career strays entirely).
3. **Topic-cluster coherence** (not yet built as code, done by eye this session, directly
   responsible for correctly identifying Quinn/Moore/Hua): does a person's oeuvre form one
   coherent topic cluster, or does it decompose into 2+ mutually-disconnected coherent clusters?
   Richest available signal -- caught every case the other two missed.
4. **Coauthor-network coherence** (not yet used at all this session, proposed here): a genuine
   single researcher's collaborator network usually has continuity even across institution moves
   (shared PhD advisor, recurring lab members, a co-author who follows them from one institution
   to the next). A work whose coauthor set shares literally zero people with the rest of the
   oeuvre is a real (if individually weak) signal; a whole *sub-cluster* of works sharing
   coauthors only *within* that sub-cluster, never crossing into the rest of the oeuvre, is a
   much stronger signal — this is presumably why Quinn's two clusters read as obviously distinct
   even before checking coauthors explicitly: two different collaborator communities.
5. **Institution/country trajectory plausibility** (used ad hoc this session via the
   "back-in-time" printouts; not yet systematised): does institution/country over time form a
   sensible single-career trajectory (the Dias UQ→NII→OIST→UQ pattern, confirmed independently
   against her real LinkedIn), or does it jump between unrelated countries/institutions with no
   overlap and no plausible connecting move?
6. **Name commonness** (user's suggestion, TF/IDF-flavoured): reuse this project's existing
   Splink blocking key (`family_name_main`, `first_initial` — see `CLAUDE.md`'s "03_link_arc_oax.py
   Design") rather than inventing a new key. For each blocking key, count distinct OpenAlex
   `author_idx` records sharing it — a direct measure of the size of the real collision-risk
   pool, more directly interpretable than an abstract corpus-IDF score. Not a per-record
   correctness signal by itself (a common name doesn't prove any *specific* oeuvre is blended),
   but a legitimate **prior** for triage: a person with a common blocking key and a positive
   anomaly score is more likely a true positive than the same anomaly score on a rare name,
   simply because more real collision-candidates exist in the wild for common names.

## Proposed pipeline

### Phase 1 — build the work-work similarity graph, per person

For each ARC-linked person's cleaned oeuvre (`oeuvres.parquet` → `create_deduped_works()`,
reusing exactly what `dossier_build.py` already does — no new dedup/exclusion logic needed),
build a weighted graph over their own `work_idx`s:

- **Topic edge weight**: cosine similarity between each pair of works' full topic-score vectors
  (`work_topics_full.parquet`-style — the *full* per-work topic distribution, not best-topic-only,
  matching the existing plan's design principle for topic diversity work) — richer than a
  single-field categorical match, captures partial/adjacent-field similarity properly.
- **Coauthor edge weight**: binary or count-weighted — do the two works share ≥1 coauthor
  `author_idx` (excluding the ARC person's own)? Reuse the same early-filter discipline as
  everywhere else in this codebase: pull coauthor `author_idx` sets only for this person's own
  (already small) `work_idx` list, never scan `authorships` unfiltered.
- **Deliberately no strong temporal-proximity weighting**: Hua's case proves chronological
  closeness is not protective against contamination (her strays sit mid-career, not at an edge)
  — a temporal prior would suppress exactly the signal needed to catch that pattern. If a
  temporal term is included at all, keep it weak/tie-breaking only.

### Phase 2 — cluster, classify

Run community detection (e.g. Louvain or simple connected-components after thresholding weak
edges — start with the simpler method, escalate only if it doesn't separate the known cases
cleanly when back-tested against Quinn/Moore/Hua/Urquhart/Dias as a validation set) over the
graph. Classify the result:

- **One dominant cluster, no significant outliers** → clean, no action.
- **One dominant cluster + a few near-zero-degree singleton works** → Hua's pattern: flag the
  singletons individually as likely-contaminating strays, not a full identity split.
- **Two or more substantial, mutually-disconnected clusters** → Quinn/Moore's pattern: flag as
  blended-idx, multiple real people.

### Phase 3 — corroborate which cluster is the real ARC person

Critical, easy to get wrong: clustering alone tells you an `author_idx` is blended, not *which*
cluster is the ARC-linked person. Corroborate the leading candidate cluster against ARC-side
truth already in `arc_persons.parquet`:
- Does the cluster's dominant field/subfield match `arc_persons.for_names`/`for_codes`?
- Does the cluster's dominant institution match `arc_persons.inst_arr`?
- Does any work in the cluster carry an ORCID matching the ARC person's own (where
  `orcid_status = HAS_ORCID`)?

A cluster with no corroborating match against any of these should not be auto-assumed to be the
"other" (contaminating) cluster either — flag as low-confidence, route to manual review rather
than guessing, consistent with this project's existing precision-over-recall stance
(`CONTEXT.md`: "false merge worse than missing record").

### Phase 4 — the symmetric direction: recover missing secondary IDs

Once Phase 3 establishes a confirmed cluster (with its actual observed coauthors, institutions,
topics — richer evidence than the original Splink match had access to), use it as a **retrospective
search query** against the broader OpenAlex `authors`/`authorships` corpus: same-surname +
compatible first-initial candidates whose own oeuvre shares topic/coauthor/institution signal
with the confirmed cluster become candidate *new* `secondary_oax_id`s — extending
`arc_oax_resolved.parquet` coverage for people whose true oeuvre is currently split across
records the original linkage pass never even considered as candidates.

### Output: flag, don't auto-fix

Matching this project's existing `manual_resolutions.csv` workflow (flag → human review → apply),
not a silent filter:
- `oeuvre_contamination_flags.parquet`: `arc_id, work_idx, cluster_id, confidence,
  corroboration_score, recommended_action (keep/strip/review)`.
- `candidate_secondary_ids.parquet`: `arc_id, candidate_author_idx, evidence_summary,
  confidence` — the under-merge direction's output, feeding the same kind of manual-review queue
  `04_resolve_links.py`'s existing deferred/ambiguous cases already use.
- Name-commonness prior (`name_commonness.parquet`: blocking key → distinct-author_idx count)
  used to prioritise the manual-review queue, not to auto-decide anything.

## Open questions before implementation

1. **Validation set**: back-test against the cases already found this session
   (Urwin/Fordyce/Urquhart/Dias/flagged-8/Quinn/Moore/Hua) before trusting the clustering on the
   full cohort — do the known-correct answers fall out of Phase 1-2 without hand-tuning?
2. **Clustering method**: start with connected-components on a thresholded graph (simplest,
   most auditable) before reaching for a heavier community-detection algorithm — only escalate
   if the simple version doesn't separate the validation cases cleanly.
3. **Scale**: full cohort is ~23k ARC persons (not just the ~4.2k ECR subset — the user's
   concern about underestimating prevalence applies to the whole linked cohort, not only DECRAs,
   though ECRs are the current focus). Per-person graphs are small (tens to low-hundreds of
   works each) so this is likely cheap; the coauthor-lookup step needs the same
   filter-small-set-before-big-scan discipline already established for `authorships`.
4. **Threshold calibration**: what edge-weight threshold separates "same cluster" from "different
   cluster" — needs tuning against the validation set, not picked a priori.

## Critical files / precedent to reuse

- `analysis/utils/dedup.py`, `analysis/utils/exclusions.py` — cleaned-oeuvre input, already built.
- `CLAUDE.md`'s "03_link_arc_oax.py Design" — existing blocking key convention to reuse for
  name-commonness, not reinvent.
- `data_persisted/manual_resolutions.csv` — existing flag→review→apply workflow pattern to mirror.
- `src/04_resolve_links.py` — existing `secondary_oax_ids` mechanism this extends (Phase 4).
- `CONTEXT.md` — "precision over recall... false merge worse than missing record" — the standing
  design principle this whole flag-don't-auto-fix approach is built around.
