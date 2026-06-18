# ARC Grants → OpenAlex Linkage Pipeline

## Working Rules
- **Read before editing**: read every line of a function before proposing a fix — do not propose until you have read everything it touches
- **Do not guess or assume**: if something is unclear, ask the user rather than proceeding on an assumption
- **Stop after each step**: after completing one step, stop and wait for the user to review before moving to the next

## Project Goal
Link ARC Chief Investigators/Fellows (CIFs) to their OpenAlex author records for bibliometric analysis.

## Pipeline Architecture (Splink-based)
```
00_extract_arc.py       → grants_flat.parquet, investigators_raw.parquet
01_prepare_arc.py       → arc_investigators_prep.parquet, arc_persons.parquet
                           (ARC name/inst/FOR prep + Splink dedupe_only: 65k rows → 23,056 persons)
02_prepare_oax.py       → openalex_authors_prep.parquet, oax_tf_*.parquet
03_link_arc_oax.py      → arc_oax_links.parquet (link_only: ARC persons → OAX authors)
04_resolve_links.py     → arc_oax_resolved.parquet, arc_ambiguous_deferred.parquet
```
The Splink pipeline replaces the entire old multi-layer pipeline in `src_archive_20260520/`.
`02_run_splink.py` is the old wrong approach — superseded, can be deleted.

## Key Paths
- Config: `config/settings.py`
- Data root: `/home/lc/m/working/WORKING_ARC_PROJECT/`
- Processed data: `/home/lc/m/working/WORKING_ARC_PROJECT/processed/`
- OAX authors parquet: set via `OPENALEX_DIR` env var in `.env` → `/home/lc/m/openalex_feb26/parquet/`
  - **Note**: `/media/d-drive/openalex_feb26/` is a different copy — always use `.env` path

## Data Scale
- ARC CIF rows (after role/scheme filter): 65,087 (37 previously dropped by INNER JOIN on admin_org — now LEFT JOIN)
- ARC person clusters (output of 01): 23,056
- OAX HEP-context authors: 2,453,347 (all authors appearing in AU-context works, not AU-last-institution filter)
- ORCID coverage in ARC CIFs: 44.5%
- ORCID coverage in arc_persons (after enrichment): 73.1%

## Python Environment
**Always use `.venv/bin/python`** — never bare `python`.

## Name Tokenisation (src/utils/names.py)
- `name_part_tokens()`: strips apostrophes before tokenising → `O'Brien → ["obrien"]`
- Hyphens/spaces still split: `Watson-Parker → ["watson","parker"]`
- `strip_diacriticals()`: NFD normalise + ASCII-only, also maps Unicode hyphens to ASCII
- Applied identically on both ARC and OAX sides in `01_prepare_splink.py`

## Name Parsing Design (01_prepare_splink.py)
- **Family names**: use `strip_diacriticals(hn.last).lower()` as a single compound string —
  do NOT split with `name_part_tokens`. HumanName correctly handles compound surnames
  (van der Ent, de Almeida, etc.) and splitting destroys them.
- **OAX name source**: `display_name` is primary (always "First Last", curated by OAX).
  `display_name_alternatives` used only as fallback when display_name yields no family name.
  Alternatives are contaminated with co-author names from OAX entity disambiguation errors.
- **first_initial**: always derived from `first_name_canonical[0]`, never from the
  unordered `first_names` list (set-derived, arbitrary order).

## FOR Code Handling
- Two ANZSRC series (2008 and 2020) use different numeric codes and slightly different names
- **Do NOT compare numeric codes** across series — they don't harmonise
- `for_name_tokens()` in `src/utils/names.py`: tokenises FOR names, strips stopwords
- `make_expanded_for_tokens()`: loads `config/for_concordance.csv` (49 J≥0.5 pairs) and
  unions each name's tokens with its canonical form's tokens — bridges near-synonym names

## 03_link_arc_oax.py Design
- Splink `link_only`: arc_persons → openalex_authors_prep
- Blocking: `(family_name_main, first_initial)` + ORCID exact
- Comparisons: first_name_canonical (exact/initial/mismatch), family_name_main, full_name_key,
  orcid (seeded m/u), inst_arr intersection
- TF adjustment on first_name_canonical, family_name_main, full_name_key
- ORCID force-add: pairs missed by predict (score < 0.5) but sharing exact ORCID → added at p=1.0
- Output: all pairs ≥ 0.5, `high_confidence` flag for ≥ 0.9

## 04_resolve_links.py Design
Disambiguation of ARC persons with 2+ HC OAX matches. Steps in order:
0. **OAX same-ORCID pre-dedup**: two OAX candidates sharing an ORCID → split records;
   keep dominant (`works_count / group_total > TOP_CUT=0.7`).
0b. **OAX same-topic pre-dedup**: two OAX candidates sharing ≥1 specific topic name → split
    records; keep the one with more works. ORCID-matched records are protected from removal.
1. **ARC–OAX ORCID match**: if exactly 1 HC candidate matches the ARC person's ORCID → resolve.
2. **Institution overlap**: restrict to candidates with maximum overlap count (if any > 0).
2b. **Field score filter**: restrict to candidates with maximum field score (unigram+bigram overlap
    of ARC FOR tokens vs OAX topic_names/subfield_names). Only fires if `max_fs >= 2` AND
    `min_fs == 0` (at least one candidate has zero overlap — avoids within-field false positives).
3. **Unique highest probability**: among remaining → resolve.
4. **works_count dominance**: `max / sum > TOP_CUT` → OAX split record, take dominant.
5. **Defer**: genuine common-name collisions.
Manual overrides: `config/manual_resolutions.csv` (resolve/unlink actions applied after all steps).

Output columns: `arc_id, oax_id, match_probability, resolved_by, secondary_oax_ids`
`secondary_oax_ids`: all other HC candidates not chosen (split-record duplicates + alternatives).

## Current Linkage Results (2026-06-17, arc_persons 23,056)
- Resolved: 22,599 / 23,056 (98.0%)
  - unique_hc: 9,385 | oax_orcid_dedup: 965 | oax_topic_dedup: 2,337 | orcid: 6,594
  - inst_overlap: 1,846 | field: 679 | probability: 34 | works_count: 299 | name_filter: 36 | manual: 424
- Ambiguous deferred: 179  ← up from 102; larger OAX pool (2.45M) generating more multi-HC candidates
- Manual unlinked: 16
- Unlinked (no HC match): 262
- manual_resolutions.csv: 424 resolve + 16 unlink = 440 rows
- Key change vs previous: orcid +1,616; inst_overlap −1,310 (direct payoff from ORCID enrichment)

## Fellowship Cohort Status (2026-06-13)
- **FF** (Federation Fellows): 141/141 resolved ✓
- **FL** (Australian Laureate Fellows): 277/277 resolved ✓
- **FT** (Future Fellows): 277/277 resolved ✓ — 23 manual entries added; blocking failure categories catalogued: Unicode apostrophe (O'Neil→oneill), space-vs-hyphen surnames (gonzalez tokman), ø stripping (krabbenhft), ue-transliteration (rueger), given-name alias (Jenny/Yingzi), first_name_canonical token-length (xu-feng→feng), name-order reversal (Swaminathan/Vishwanathan), patronymic-vs-publication-name (Lakshminarasimha/Gubbi)
- **APDI** (ARC Postdoctoral Industry): 80/80 resolved ✓ — 3 manual entries added
- **CI-DORA**: 80/80 resolved ✓ — 1 manual entry (Jennifer Hocking, ORCID not in OAX)
- **APF**: 239/240 — 1 unresolvable (art theorist turned drama director, no OAX presence)
- **APD**: ~480/495 — 31 unlinked investigated: 18 resolved, 13 genuine unlinks; 15 manual unlink entries

## Important: cluster_id vs scheme membership
Most FF/FL fellows also hold DP grants; their `cluster_id` starts with "DP" not "FF"/"FL".
To find all clusters for a scheme, search `grant_ids` in arc_persons, or use
`arc_grant_cluster_map.parquet` (output of 01) which maps every grantID_personName → cluster_id.

## 03_link_arc_oax.py Design (updated 2026-05-25)
- Given-name comparison uses HumanName cascade: compound (f+m) → first → cross (f=other's m) → initials
- Blocking: `(family_name_main, first_initial)` + two middle-initial cross-blocking rules + ORCID
- Cross-blocking catches cases like ARC "Z Smith" vs OAX "Herb Z Smith" (middle initial match)
- HumanName on `full_names` (ARC longest) and `full_name` (OAX display_name) for consistent parsing
- Compound match "shi xue" = "shi xue" fixes Chinese compound given names (e.g. Shi Xue Dou)
- TF adjustment on `first_name` (hn.first) exact level only

## 01_prepare_arc.py Design (current, 2026-06-17)
Post-Splink steps in order:
1. `_merge_by_orcid`: collapse clusters sharing same ORCID → canonical = `min(cluster_ids)`
2. `_split_orcid_conflicts`: split any cluster with 2+ distinct ORCIDs
3. `_split_multi_name_clusters`: split on disjoint coinvestigator+FOR signals; skips single-ORCID clusters
4. `_apply_manual_splits`: apply `config/manual_splits.csv` confirmed splits
5. `_apply_enriched_orcids`: promote high/au_match ORCIDs from `orcid_enrichment.parquet`; checks `enrichment_blocklist.csv`
6. `_promote_low_by_for`: promote low-confidence enrichment candidates via ERA FOR token overlap
7. `_apply_manual_orcids`: inject verified ORCIDs from `config/manual_orcids.csv`
8. `_merge_persons_by_orcid`: post-enrichment dedup — merge clusters sharing same ORCID; skips first-initial mismatch
9. `_apply_manual_merges`: apply `config/manual_merges.csv` confirmed same-person pairs
10. `_merge_same_grant_coinvestigators`: **NEW** — same blocking key on same single-org grant → auto-merge;
    uses `grants_flat.n_eligible_orgs`; ORCID-conflict guard; 61 absorbed in current run

Output columns in `arc_persons.parquet`:
- `orcid_status`: `HAS_ORCID` | `NO_ORCID` | `MULTI_ORCID`
- `resolution_status`: `RESOLVED` | `UNRESOLVED` (driven by `_is_suspicious`)
- `orcid_for_codes`: ERA FOR codes derived from ORCID works via for_cache
- `gap_candidates`: compatible same-blocking-key clusters not yet merged (for review)
- `reliability_tier`: `1a`/`1b`/`1c`/`2`/`3`/`4`/`4u`
- `cluster_history`: JSON list of events — `splink_cluster`, `orcid_merge`, `orcid_conflict_split`,
  `name_split`, `manual_split`, `enriched_orcid`, `manual_orcid`, `same_grant_merge`, `manual_merge`

**config/manual_orcids.csv** — 12 entries (verified ORCIDs for clusters missing one):
- DP0451043_SusanOConnor, DP0343994_TerenceONeill, DP0452137_JefferyMalpas,
  DP110100881_TheodorusSloots, DP170102529_PhilipBland, DP0209045_FrankPate,
  DP130101651_LanfengDong, DP0774201_StephenBell, DP110100091_MichaelAdams,
  DP0556160_MichaelHooker, DE220100417_JathanSadowski, DE200100121_BenScheele

**config/manual_merges.csv** — 11 entries (same-grant nickname pairs + cross-grant confirmed same-person)
**config/enrichment_blocklist.csv** — 2 entries: LP0220171_JNichols, DP0452211_RobertMarks

## Analysis Pipeline Status (2026-06-18)
- `analysis/01_fetch_oeuvres.py` ✓ — 4,236,839 rows, 22,599 persons
- `analysis/02_accuracy_check.py --full` ✓ — 0 over/under-coverage flags; 105 shared author_idx (B3 cross-grant, not errors); 11,604 year flags + 41,835 domain outliers written to work_flags_full.csv
- `analysis/03_annual_metrics.py` ✓ — annual_metrics 544,627 rows/22,586 persons; collab_metrics 4,915,067 rows/22,250 persons
- `analysis/06_analyse_fellowships.py` — plot 2 updated: median of active publishers (not mean+zeros); DECRA bug fixed (role_code `DECRA` not `DE`); award_year >= 2015 filter added to trajectory plot

## Next Priority (start of next session)
Analysis pipeline complete as of 2026-06-18. Pipeline improvement TODOs below.

**Pending code TODOs:**
- 03 Splink inst comparison: when all ARC grants are single-org (`all_single_org` bool in arc_persons), give strong negative weight to inst_arr mismatch (requires conditioning Splink comparison level weights on this flag)
- 6-digit FOR → OAX topic field score: (1) obtain ABS ANZSRC 2008→2020 6-digit concordance table; (2) build 2020 6-digit → OAX topic lexical map (token overlap, Jaccard threshold — longer strings make false positives unlikely); (3) wire into `_field_score` in 04 and Splink comparison in 03. Replaces current 4-digit→subfield path with finer-grained signal. Prereq: `for_2008_to_2020_fields.csv`
- Refactor cluster to dataclass with stable opaque id and explicit provenance fields
- Refactor 00b to target arc_persons (resolution_status==RESOLVED, orcid_status==NO_ORCID)
- Cross-grant B3 rule: same blocking key + shared co-i + same admin_org → auto-merge (catches Jun Li)
- Complete 00b run: 7,084 ARC-ORCID records still need fetching (running 2026-06-18; was 11,566)
- Strengthen reliability_tier: add ARC for_names vs orcid_for_codes agreement signal for HAS_ORCID clusters

## Manual Resolution Techniques (Not Yet Automated in Pipeline)

### Nickname / informal-name variants
Many Australian researchers publish under informal given names not recorded in ARC data.
The pipeline has no lookup table for these. Common patterns seen:
- Bill = William, Tony = Anthony, Beth = Elizabeth
- Geoff = Geoffrey, Greg = Gregory, Cris = Christiaan
- Tim = Timothy, Chris = Christiaan, Rob = Robert
**Potential pipeline addition**: a nickname expansion table applied to `first_name_canonical`
during blocking (add both canonical and common nicknames as candidate first_initials).

### Full-OAX surname search for no-affiliation records
Researchers absent from `openalex_authors_prep.parquet` (AU-filtered) because their
`last_known_institutions` is empty in the Feb26 snapshot. Manual path: scan raw OAX authors
parquet by family name + field topic to find the record, then add to manual_resolutions.csv.
Cases this session: BrienNorton (A5111895832), FrederickRavenhill (A5002864862).
The pipeline cannot rescue these automatically — they are simply not Splink candidates.

### Chinese compound given-name parsing failure
OAX `display_name` "Wing Kong Chiu" → HumanName parses `first='Wing', middle='Kong'` but the
prep code extracts `first_name='kong'` (middle used as first) → `first_initial='k'` not `'w'` →
wrong blocking key → no Splink candidates generated.
**Pattern**: OAX display_names of form "GivenA GivenB Surname" where GivenA+GivenB is a Chinese
compound given name. HumanName may vary in which part it treats as first.
**Potential fix**: add a cross-blocking rule on middle_initial in 03_link_arc_oax.py (partially
done already) or detect compound-given-name patterns and index both initials.

### ORCID mismatch / OAX entity-disambiguation errors
In rare cases OAX has merged two real people into one record (or reassigned an ORCID to a wrong
record). Symptom: OAX `display_name` doesn't match the ORCID holder's actual name.
Example: A5091677854 had display_name="Randal Douc" but ORCID 0000-0003-3910-9495 belongs to
Arnaud Doucet. Cross-check via ORCID.org or raw `raw_author_names` list in OAX.

### Sub-HC rescue (now automated)
The name-filter step in `04_resolve_links.py` rescues arc_ids with only sub-HC candidates
(0.7–0.9) when character-mismatch filtering leaves exactly one compatible candidate.
`resolved_by='name_filter'` (24 cases in current run). Already in pipeline.

## Splink Design Decisions
- **`arrays_to_explode` is NOT supported** in EM training sessions
- **Prior inflation risk**: second EM session on family-name block causes false merges.
  Fix: one ORCID-based EM session + `estimate_probability_two_random_records_match(recall=0.8)`
- **Cluster threshold**: 0.9 (high precision, prefer splitting over merging)
- **Seeded m_probabilities** for orcid comparison (can't train from ORCID-blocked EM)

## Known Issues in 02 Output
- **Raymond Gilbert / Robert Gilbert** (n=34, no ORCID): different first names + 3 different
  fields — suspected mis-merge of 2–3 people
- **Paul Young** (n=28, no ORCID): added to `config/manual_splits.csv` (confirmed_different_people=True);
  splits by institution into UQ virologist / USyd pharmacologist / Monash engineer / UNSW (crop) groups.
  Re-run 01_prepare_arc.py to materialise sub-clusters, then add per-sub-cluster manual resolutions.
