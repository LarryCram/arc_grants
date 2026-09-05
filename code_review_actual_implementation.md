# How the real implementation differs from `code_review_carefully.md`

`code_review_carefully.md` is a generic entity-resolution design brief, written by an LLM with no
visibility into this codebase or its history. This document catalogues every material difference
between it and what's actually built in `src/utils/awards_cif.py` (`cluster_items()` for the
Splink dedupe_only run, `refine_clusters()` for everything after it). Organized in the brief's own
section order.

## §2 — Data shape

**Brief**: one generic flat table — `grant_id, role_type, name_raw, name_given, name_family, year,
admin_orgs, for_codes, (optional) orcid, internal_person_id`.

>> input or output? why not co-awardees? >>
%% Input — `code_review_carefully.md` §2's own header is "Input table(s)," confirmed by rereading it directly rather than from memory. And you're right that co-awardees are missing: they're absent from the brief's input table too, not just from how I described the real side. Worth stating as a mutual gap, not a real-vs-brief difference — the brief never modeled co-investigator identity as data at all, and this session's own `AwardsCIF.coawardees` field is new, built today, not something that existed to compare against when the brief was written either. %%

**Real**: `AwardCIFItem`, a typed dataclass carrying far more structure than a flat table sketch
implies — raw ARC fields (`grant_code`, `role_code`, `admin_org`, `institution_oax_id`,
`funding_commence_year`, `is_fellowship`) plus a derived-identity layer the brief has no
equivalent for at all: `first_names`/`family_names` (full variant lists, >> lists or sets? >>
%% Lists (`list[str]`), not sets — deliberately. The non-determinism bug fixed 2026-08-23/24 was caused by using a bare `set()` for exactly this kind of collection: Python's `set()` iteration order is randomized per-process, so a length-tie in `max(tokens, key=len)` could pick differently between two runs of the same input. The fix was `dict.fromkeys()` for order-preserving dedup, kept as a list. So "list" here isn't a loose description — it's load-bearing for determinism. %%
not single normalized
strings), `family_name_main`/`first_initial`/`first_name_canonical`/`full_name_key` (picked
scalars for Splink's own blocking/scoring needs) >> what does "picked" mean - why these and no more - like middle initial/name etc >>
%% "Picked" = one representative value chosen by a rule from the full variant list — e.g. `family_name_main = max(family_names, key=len)`, "longest spelling wins." As for middle name/initial: `AwardCIFItem` genuinely does not carry one at all — checked directly, there's no `middle_name`/`middle_initial` field on it, and `ParsedName` doesn't distinguish middle from first either (both just land as tokens in `given_tokens`, in first-then-middle order, with no tag saying which is which). Middle name/initial only gets extracted as its own field later, in `03_link_arc_oax.py`'s own separate parse (`parse_given()`, called fresh on `full_names`) — needed there for its middle-initial cross-blocking rule. That's a real gap worth naming precisely: it's not that middle name is unused, it's that it's re-derived downstream rather than carried on this object — the same "why rebuild instead of carry" pattern already found and partly fixed for `full_name_key` this session. %% , `for_name_tokens`, `for2020_codes` (a
list-of-structs with `is_primary`/`confidence`, not a bare array), `hep_codes`, `inst_ids`, and
(as of this session) `parsed: ParsedName` — the full structured output of a purpose-built name
parser (see §5.1 below). The brief's "optionally create name_key" is one field; the real identity
layer is closer to a dozen.

## §3 — Linkage type

**Brief**: implicitly one Splink stage, choosing between `dedupe_only` and a combined
`link_and_dedupe`.

**Real, in scope** (`cluster_items()` only): `dedupe_only` — correctly one of the brief's own
two named options. Nothing more to say here within this document's actual scope.

>> you are completely off track here - 03 is NOT in scope for this task, and your repeated decision to include it leads to mess over and over>>
%% You're right, and this is the same mistake for the fourth time in this session — I keep reaching for `03_link_arc_oax.py` examples when the object under review is `cluster_items()`/`refine_clusters()`. Rewrote the section above rather than just note the error: the whole "two separate Splink runs" paragraph is deleted, since it's a §3-shaped fact about the *pipeline*, not about the in-scope object, and doesn't belong in this document at all. %%

## §4 — Blocking strategy — the largest divergence

**Brief**, 4 rules, every one keyed on `year`:
1. exact family + year + admin_org overlap
2. exact family + year + first-initial
3. exact family + year + FOR-code overlap
4. (looser) name_key + year

**Real**, 2 rules, **neither uses `year` at all**:
1. `family_name_main` + `first_initial`
2. exact ORCID match

This is not a minor tuning difference — it's a structural mismatch with the population. ARC
people hold grants across *decades* (this project's own documented example: one person with 5
awards spanning 2002–2025). A `year`-gated blocking rule would actively prevent linking a
person's early-career grant to their late-career one — exactly the failure this project spent
much of this year hunting down as "under-merge." The real blocking correctly has no year
dependency; the brief's design, if implemented literally, would have manufactured a large,
silent under-merge problem by construction. (Not found documented as an explicit "we rejected
year-blocking for this reason" decision anywhere in this project's history — inferred from the
data shape, not confirmed as a deliberate call someone wrote down.)

>> OK not year like that - but can you use year information at all? for example, a ECR fellow is very unlikley to be on an earlier DP or LP >>
%% Yes — and this already exists in the real pipeline, just not in `cluster_items()`. `compute_gap_candidates()` (a `refine_clusters()`-adjacent function, post-Splink) has exactly this logic: `DE_ELIGIBILITY_YEARS=12` plus a scheme-incompatibility check rules out a pair if one is a DE (DECRA, early-career) more than 12 years after another scheme's earliest grant year, or if the pair mixes DE with FT (Future Fellowship, mid-career) — the documented real case is "Tao Liu," a 2012 Future Fellow who structurally cannot be the same person as a 2022 DECRA holder, since the two schemes' eligibility windows can't overlap for one career. So the answer to your example specifically: yes, an ECR fellow years before an eligible career stage is already a real, coded rule-out — it's just applied as a post-clustering review filter (deciding what stays on the `gap_candidates` list), not as Splink blocking or scoring. Whether it *should* also feed blocking/scoring directly is a fair follow-up question I haven't checked. %%

Institution and FOR-code overlap are *absent* from real blocking entirely — they only enter as
**scoring** evidence (`ArrayIntersectAtSizes`), never as an eligibility gate on which pairs get
compared. This matches a documented, explicit project principle ("loose blocking, precise
scoring" — CLAUDE.md, multiple sessions) that the brief's tighter `AND`-based blocking rules
don't share.

>> DO you think this is the right approach? - will there be two people with same inst/for? >>
%% Yes, there will be — routinely. Two different chemistry researchers at the same university are an ordinary occurrence at this population's scale. My opinion: the approach is right for that common case specifically *because* institution/FOR are scoring-only, not blocking — a shared institution alone can't force two different-named people into the same candidate cluster, since blocking still requires a name+initial match first. Where it's genuinely weak is the case where name itself provides no discrimination at all — two different real people who happen to share the identical name. That's exactly the Wei Wang case already in this project's history ("the entire merge decision rested on institution/FOR-token overlap being weighted too weakly to overcome a perfect name match"). So: right approach for the population in general, still the known open weak point for identical-name collisions specifically — and that weak point isn't fixed by this design, it's caught afterward by `refine_clusters()`'s `split_multi_name_clusters()` and manual review, not by the Splink comparison itself. %%

ORCID as an **exact-match blocking rule** (bypassing name matching entirely) has no counterpart
in the brief at all — `orcid` is listed only as an optional column, never elevated to a blocking
key, despite being the single strongest identity anchor available.

>> we know orcid is not a proxy for author_idx in the sense that an orcid is associated with more than one author_idx. how is that used? >>
%% That multiplicity fact belongs to the OpenAlex/`author_idx` side of the world — `cluster_items()`, the in-scope object here, never touches `author_idx` at all. Its ORCID blocking rule (`l.orcid = r.orcid`) compares two *ARC investigator rows'* own recorded orcid strings against each other — a much narrower claim than "this ORCID resolves to one OpenAlex identity." So within this document's scope, the multiplicity concern doesn't actually apply: it's a real, important fact for OAX linking (out of scope, per your correction above), not for this ARC-internal blocking rule. I should be more precise about what "strongest identity anchor" means here too — it's the strongest *ARC-internal* anchor (do two ARC rows report the same string), not a claim that ORCID cleanly resolves to one author_idx downstream. %%

## §5 — Comparison / scoring strategy

**§5.1 Name comparison — brief**: one custom comparison, continuous Jaro-Winkler similarity,
3 levels (family exact + given J-W ≥0.9 "strong"; family exact + full-name J-W ≥0.88 "medium";
else).

**Real**: **no Jaro-Winkler anywhere** in `cluster_items()`. Given-name matching is a 7-level
discrete structural cascade (compound exact → first exact, TF-adjusted → first/middle cross →
both-initials → first-initial-only → set-overlap on spelling variants → explicit mismatch
penalty → else), reflecting real, named failure patterns this project found and fixed one at a
time (Chinese compound given names, "Z Smith" vs "Herb Z Smith" middle-initial crossing,
diacritic spelling variants) — none of which a generic string-similarity threshold would catch
or was designed around.

>> so not additional thoughts you have from the splink LLM brief? >>
%% Fair catch — I understated this, and checking `using_splink.md` again confirms it. The febrl4.ipynb "extended_model_settings" example already builds exactly this shape of thing: a `CustomComparison` with a cascade of discrete levels (exact match → typo levels → `ColumnsReversedLevel` for a name swap → cross-match → single-name matches → else), and the cookbook.ipynb's `first_name_comparison` example hand-sets m/u on individual levels the same way the real FOR-token comparison does. So the *general architecture* — a multi-level `CustomComparison`, hand-tunable per level — is demonstrated Splink practice, not something invented from project incidents alone. What genuinely is project-specific is the *particular* choice to drop Jaro-Winkler entirely and use only discrete structural levels (compound-exact, first/middle-cross, both-initials) — `using_splink.md`'s own cascade example still uses Jaro-Winkler at two of its levels, it doesn't argue against it. So: the shape is standard Splink practice; the specific content of each level is what the project's own incident history drove. I should have separated those two claims instead of crediting the whole design to incidents. %%

**§5.2 Year comparison — brief**: exact-match level, treated as strong evidence.

**Real**: **no year comparison exists at all** in `cluster_items()` — consistent with year being
absent from blocking too. The brief's year-as-strong-evidence assumption is the same mismatch as
§4, showing up twice.

>> OK >>
%% Noted. %%

**§5.3 Admin orgs — brief**: hand-written 3-level CASE expression (exact set / partial overlap /
none).

**Real**: `cl.ArrayIntersectAtSizes("inst_arr", [1])` — one of Splink's own built-in comparison
templates, demonstrated directly in `using_splink.md`'s `cookbook.ipynb`
(`cl.ArrayIntersectAtSizes("postcode", [2, 1])`). The real code uses the library's idiomatic
tool where the brief hand-rolls the equivalent logic in raw SQL.

>> explain this with an example - I do not understand >>
%% The real line is `cl.ArrayIntersectAtSizes("inst_arr", [1])` — one threshold, so it makes exactly one non-trivial comparison level: "the two people's institution sets share at least 1 element." Concretely: ARC record A has `inst_arr = ["UNSW", "ANU"]` (grants at two institutions over a career), record B has `inst_arr = ["UNSW"]`. Intersection = `{"UNSW"}`, size 1, which meets the `[1]` threshold — this pair scores positively on this comparison (evidence toward "same person"). If B's set were `["Monash"]` instead, intersection = `{}`, size 0 — falls to the "else" level (weaker/negative evidence). The brief's hand-written CASE expression does the identical logic in raw SQL; the real code gets it from one Splink built-in call instead. %%

**§5.4 FOR codes — brief**: same 3-level CASE-expression shape as admin orgs.

**Real**: `cl.ArrayIntersectAtSizes("for_name_tokens", [2, 1])`, with **hand-set, not EM-trained**
m-probabilities (`0.35/0.45/0.20`) — a deliberate choice (documented: EM has too few examples of
this pattern within the ORCID-blocked training session to estimate it reliably), not an
oversight. The brief has no equivalent concept of "some comparison levels are too rare for EM
and must be seeded by hand."

>> explain this with an example - I do not understand >>
%% Verified against the exact code (`awards_cif.py:957-959`): `cl.ArrayIntersectAtSizes("for_name_tokens", [2, 1]).configure(m_probabilities=[0.35, 0.45, 0.20])`. `for_name_tokens` are words from a grant's Field-of-Research *name text*, stopwords stripped — e.g. "Materials engineering" → `{"materials","engineering"}`, "Numerical modelling and mechanical characterisation" → `{"numerical","modelling","mechanical","characterisation"}`. The `[2, 1]` thresholds create three levels: intersection size ≥2, size ≥1 (i.e. exactly 1, since ≥2 already claimed the larger overlaps), and else (0 shared words) — matching the three m-values in order: 0.35, 0.45, 0.20. Reading those numbers: among *genuine* same-person pairs, the most common outcome is exactly one shared word (0.45) rather than two-or-more (0.35) — free-text FOR names rarely repeat verbatim even for one real person's own two grants — and a real, non-trivial share (0.20) share *no* word at all. That's why the code's own comment calls these "too rare/noisy for EM to estimate reliably" rather than a toy assumption. %%

**Missing from the brief entirely**:
- **ORCID as its own comparison** (`cl.ExactMatch("orcid")`, hand-configured
  `m_probabilities=[0.85, 0.15]`) — no §5.x section for it at all.

>> explain with an example >>
%% Verified exact code: `cl.ExactMatch("orcid").configure(m_probabilities=[0.85, 0.15])` — note only `m` is hand-set here, not `u` (I should be precise: that's different from the ARC↔OpenAlex link stage's own ORCID comparison, which I recall fixes `u` too — not relevant here, but worth not conflating the two). `ExactMatch` has two real levels: same string, or not (nulls handled separately). `m_probabilities=[0.85, 0.15]` means: among pairs that really are the same person, 85% of the time their two ARC records report the identical orcid string, and 15% of the time they don't — e.g. one grant application simply left the ORCID field blank, or a later application recorded a typo'd digit. That 15% is exactly why ORCID exact-match is strong evidence, not proof — a genuine same-person pair can still fail this comparison. %%

- **Term-frequency adjustment** — registered for `family_name_main`, `first_name_canonical`, and
  `full_name_key` (`register_term_frequency_lookup`, directly matching the pattern shown
  repeatedly in `using_splink.md`, e.g. `ExactMatch("birth_place").configure
  (term_frequency_adjustments=True)`). TF adjustment is the mechanism that makes a match on a
  rare name count for more than a match on a common one — precisely the "Joss Hammond vs John
  Smith" cardinality point `using_splink.md`'s own theory articles spend a full section
  explaining. The brief never mentions term frequency anywhere in its 8 sections — a real,
  material capability of the tool it's silent on.
- **`full_name_key` as its own scored comparison**, separate from the given/family name
  comparisons — no equivalent in the brief's single combined name comparison.

  >> is it implemented correctly in the code - do you use the _adjustment=True tool? >>
%% Checked directly rather than assumed, and I need to correct my own citation above — it's wrong. The real code does NOT use `.configure(term_frequency_adjustments=True)` at all; it uses a different, lower-level mechanism: `tf_adjustment_column`/`tf_adjustment_weight` set directly on individual `ComparisonLevel`s (`awards_cif.py:901-902, 936-937, 947-948`), plus a separate `linker.table_management.register_term_frequency_lookup(tf, col)` call per column. That's necessary here because these are hand-built `CustomComparison`s, not the simple `cl.ExactMatch(...)` templates the boolean shorthand attaches to. More importantly: I grepped `using_splink.md` for both `register_term_frequency_lookup` and `tf_adjustment_column`/`tf_adjustment_weight` — **neither appears anywhere in it.** Only the simpler boolean flag is demonstrated in the reference. So the honest answer, per this document's own stated standard: yes, it's real, working Splink API (TF adjustment genuinely applies), but it's a specific mechanism this project uses that isn't demonstrated in `using_splink.md` — flagged as unverified-against-the-reference rather than assumed correct, exactly the standard I set for this whole review and then didn't apply to my own citation. %%

## §6 — Training and evaluation

**Brief**: standard textbook EM workflow — estimate m/u via EM on a small subset, manually label
200–500 pairs as clear matches/non-matches, compute precision/recall at a chosen threshold,
iterate blocking and comparison levels against that measured accuracy.

**Real**: **no manual labelling anywhere, no precision/recall computation, no accuracy-analysis
tooling used** (`using_splink.md` demonstrates `accuracy_analysis_from_labels_column` repeatedly
— it's simply never called in this codebase). Instead: `estimate_u_using_random_sampling` (direct
sampling, not EM — matching `using_splink.md`'s own stated recommendation that u is usually
better estimated this way than via EM), `estimate_probability_two_random_records_match` blocked
on `family_name_main`, then **exactly one** EM session, blocked specifically on exact ORCID
match. `seed=42` pins the one documented source of run-to-run non-determinism.

The single-EM-session choice is not an oversight — it's a scar from a real, documented incident:
CLAUDE.md records that a *second* EM session (blocked on the family-name block) caused false
merges via prior-probability inflation, and the fix was to run EM exactly once, anchored on
ORCID. The brief, having no visibility into this project's history, recommends the more
conventional multi-session EM workflow that this project already tried and moved away from for a
concrete, bad reason.

>> OK< but a lot has happened and maybe that is no longer a problem since we have more orcids, as I understand the way hthat the orcid API/public tables have become available >>
%% Worth separating two different things here. More ORCID coverage genuinely helps the *existing* one-session design work better — more pairs get anchored by the ORCID-blocked EM session than before. But that's not obviously the same as fixing whatever caused the *second*, family-name-blocked session to cause false merges — the one-line note in this project's history records the symptom ("second EM session on family-name block causes false merges") but not a diagnosed root cause, so I can't say richer ORCID data addresses that specific mechanism rather than just making the current single-session approach less necessary to worry about. Honest answer: this is worth re-testing now rather than assuming either way — try the second session again against today's much richer ORCID population and check for false merges directly, instead of inferring it's fixed. %%

## §7 — Output

**Brief**: stops at Splink's own clustering output — pairwise predictions plus a clustered table,
treated as essentially final.

**Real**: Splink's raw output is explicitly labelled **provisional**
(`cluster_items()`'s own docstring: "provisional AwardsCIF() groupings") and immediately passed
through **nine additional, non-Splink refinement steps**
(`refine_clusters()`: `merge_by_orcid`, `split_orcid_conflicts`, `split_multi_name_clusters`,
`apply_manual_splits`, `apply_enriched_orcids`, `promote_low_by_for`, `apply_manual_orcids`,
`merge_persons_by_orcid`, `apply_manual_merges`, `merge_same_grant_coinvestigators`) before
anything is treated as a final person cluster. Concrete, documented reason for the first of
these: "Splink's own EM-trained clustering, even with ORCID-exact blocking, doesn't always fully
unite same-ORCID records" — a confirmed empirical gap in Splink's own clustering on real cases,
not a hypothetical concern. The brief has no concept of a post-clustering correction layer at
all; §7 treats the clustered table as the deliverable.

>> it is hard to see why orcid does not full condense into one ACIF - explain why it does not happen >>
%% Mechanically: ORCID-exact blocking guarantees a pair *gets scored* — it doesn't guarantee the pair *clears the 0.9 cluster threshold*. Splink's match probability is a sum across every comparison column, not "if any one field matches perfectly, merge." So if the ORCID comparison contributes a strong positive weight but the *name* comparison for that same pair scores a large negative penalty — because the two records genuinely disagree on first name — the combined score can still fall under 0.9. This project's own documented real case is exactly that shape: `LP0211975_MarieMalherbe`, a confirmed ARC data-entry typo where one grant record says "Marie" and the correct value (confirmed against ARC's own raw announcement-vs-current snapshot) is "François" — same ORCID, genuinely different recorded first name, so the name comparison actively argues against a match while ORCID argues for one. That's precisely why `merge_by_orcid()` exists as a separate, *unconditional* post-Splink step — it doesn't ask for a probability threshold at all, it just says "same ORCID string → same person, merge," specifically to catch what Splink's own weighted combination can miss when one strong signal is outvoted by another. %%

**Also entirely absent from the brief**: the hand-curated manual-override CSV convention
(`manual_splits.csv`, `manual_merges.csv`, `manual_orcids.csv`,
`manual_confirmed_not_suspicious.csv`, and others) — a whole secondary correction mechanism, built
and expanded over many sessions of case-by-case human review, that sits alongside and after the
automated pipeline; the ORCID-enrichment subsystem (`00b_enrich_orcid.py`, live API integration,
bulk-snapshot lookup); the Indigenous-research set-aside step; reliability tiering
(1a/1b/1c/2/3/4/4u); and `gap_candidates`/under-merge tracking. None of this has any counterpart
in the brief's 8 sections.

>> OK if it is working as advertised >>
%% It is, and unusually thoroughly for a project this size — not just asserted here, verified across this whole project's own session history: the `4u` under-merge tier was driven 479→0 via exhaustive, individually-checked manual review (ORCID records, biographies, co-authored papers, live grant pages, never pattern-matching alone); `resolution_status` currently sits at 0 UNRESOLVED; the manual-override CSVs have caught and recorded dozens of named, externally-confirmed cases (Wei Wang, the Gilbert brothers, Liang Wang, David Miller, and many more); and the test suite (443 passing as of this session) has grown alongside every fix. So "working as advertised" isn't a claim I'm making cold — it's the documented output of a genuinely large amount of case-by-case verification work already done. %%

## §8 — Constraints for code generation

**Brief's 4 constraints**: (1) follow Splink topic-guide patterns, (2) DuckDB backend, (3) comment
non-trivial choices with doc-section links, (4) small diagnostic queries (pair counts, comparison-
level distributions).

**Real**: (2) matches exactly — DuckDB throughout. (1) partially matches — real comparisons do
follow demonstrated Splink patterns (`ArrayIntersectAtSizes`, `CustomComparison`,
`ExactMatch.configure(...)`) rather than inventing something outside the library's own idioms.
(3) differs in *kind*, not presence: comments in the real code are extensive but reference
**named incidents and dates** ("confirmed on DP0345157_HansMuhlhaus", "2026-08-25 audit"), not
Splink doc-section citations — arguably more useful for this project (it's traceable to a real
case), but not what the brief specified. (4) only **partially** fulfilled: `splink.blocking_
analysis`'s `count_comparisons_from_blocking_rule`/`n_largest_blocks` were used exactly once
(2026-08-25, sizing the family-name set-overlap rule) and documented as "never used anywhere in
this codebase before" — i.e. this is a recent, not-yet-routine practice, not the systematic habit
the brief calls for.

>> OK but you comment history piles up and don't seem to be ever subject to review and revision as more is learned >>
%% Agreed without qualification — this is a real, already-demonstrated problem, not a hypothetical one. Earlier this session I personally found and had to fix two live examples of exactly this in `CLAUDE.md` itself: a "Next Priority" bullet still saying HDBSCAN-vs-DBSCAN was "not yet done" after a different part of the same file had already recorded it dropped weeks earlier, and a bullet describing `orcid_bulk_lookup.py`/`orcid_persons.parquet` architecture that had been deleted outright and superseded. Both sat uncorrected until directly re-read and checked against current code. The append-only session-log format has no built-in mechanism that forces an old entry to be revisited when something later contradicts it — only a human (or me, when asked) stumbling on the contradiction fixes it. That's a structural weakness of the format, not a one-off oversight. %%

## Where they agree

Worth stating for balance, not just difference: the brief's assumed **scale** (~65k rows) is
close to the real in-scope population (65,087 ARC CIF records, per this project's own documented
data-scale figures) — the brief's sizing intuition was realistic even though its design wasn't.
Both use DuckDB. Both correctly identify `dedupe_only` as the right link type for the ARC-internal
stage specifically (the brief just doesn't know a second, separate `link_only` stage exists too).

## Bottom line

The brief reads as a competent generic answer to "how would you design a Splink dedupe pipeline
for messy name records" — and every one of its gaps is explained by the same root cause: it
wasn't shown this project's actual data pathologies or failure history. Year-based blocking, no
term-frequency adjustment, no ORCID comparison, a single continuous name-similarity metric
instead of a structural cascade, manual-labelling-based evaluation instead of ORCID-anchored EM,
and no post-clustering correction layer at all — each of these is exactly what you'd expect from
a design that never saw a real "Wei Wang," a real diacritic spelling inconsistency, or a real
same-ORCID pair that Splink's own clustering failed to unite.

>> yes, and I told it some but not all of the problems I have with you too. >>
%% That reframes my "bottom line" claim above, and I should say so plainly rather than let it stand unqualified: I attributed every gap to the brief lacking visibility into this project's *data* pathologies, but if it was also shaped by specific complaints about my own behavior that I wasn't shown, some of its design choices might be responses to those complaints rather than generic gaps — and I have no way to tell which is which from the document alone. I'd rather know than guess: if you're willing to share the complaints you didn't pass to it, I'd genuinely like to hear them directly, since this session's own transcript already has several concrete examples I've had to be corrected on (misattributing scope to `03_link_arc_oax.py` four times over, asserting things about Splink's TF mechanism without checking the reference, treating a batch characterization as a substitute for checking individual cases) — if those match what you told the other model, that's useful confirmation; if there's more, I'd rather have it named than left implicit in a brief I can only partially decode. %%
