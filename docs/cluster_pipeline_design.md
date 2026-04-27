# Cluster-Based Disambiguation Pipeline — Design

**Status: draft — working document**

---

## Conceptual model

A **Cluster** is a partition element of the ARC investigator space.

- Every investigator record (one person × one grant) belongs to exactly one cluster.
- Every real person belongs to exactly one cluster.
- A cluster contains one or more real people — the pipeline determines how many.
- No real person appears in more than one cluster.

The pipeline is a sequence of **splitting and resolving** operations, not a
matching pipeline. Each layer takes the current set of clusters and either:

- **Resolves** a cluster as a confirmed individual (with or without an OAX match), or
- **Splits** it into two or more child clusters when the evidence makes separation
  certain (e.g. two distinct ORCIDs → two distinct people), or
- **Leaves** it unchanged when evidence is insufficient.

The endpoint is a **complete partition**: every cluster is in exactly one of:

| Status | Meaning |
|---|---|
| `MATCHED` | One person, OAX author id known |
| `ABSENT` | One person, confirmed not in OAX |
| `UNDECIDABLE` | Cannot resolve or split further with this data |

Nothing falls through. Every ARC investigator record ends up in a cluster with
a known status.

---

## Source data note

ORCIDs are recorded on **investigator records** (one person × one grant), not
on grants. The same person may have an ORCID on some grant appearances and null
on others, and the same person may have **different ORCIDs recorded on different
grants** — this has been observed in this dataset.

**Post-nominal noise.** Ten `family_name` entries carry trailing Australian or
British post-nominal awards (AO, AM, OAM, AC, FAA, …), e.g. `"Finch AO"`,
`"Raston AO FAA"`. These are stripped as the first step of any name processing
via `strip_postnominals()` in `src/utils/names.py`. One additional case
(`"Ong ViforJ"`) has a company name appended by data entry error; the real
family name `"Ong"` is recovered the same way if `"ViforJ"` is added to the
strip list, or handled as a manual correction in the concordance.

OAX treats ORCID as a canonical identifier: one ORCID maps to at most one OAX
author, and one OAX author has at most one ORCID. This gives a clean splitting
rule: if two distinct ORCIDs in a cluster **both resolve to OAX**, they
necessarily map to different authors — the cluster certainly contains two people
and must be split. The ambiguous case is when one or both ORCIDs are absent from
OAX: the absent ORCID may be an invalid or duplicate entry for the same person
whose other ORCID is in OAX, or a valid ORCID for a second person who happens
not to be in OAX.

---

## Data model

### `InvestigatorRecord`

The atomic unit — one row from `investigators.parquet` (joined with
`grants.parquet` for `admin_org` and `for_codes_wrangled.parquet` for FoR).

```python
@dataclass(frozen=True)
class InvestigatorRecord:
    grant_code:  str
    arc_first:   str        # exact first name as stored in ARC
    arc_family:  str        # exact family name as stored in ARC
    orcid:       str | None # ORCID on this specific investigator record; may be null
    grant_heps:  frozenset[str]  # HEP codes (2–5 char) for AU HEPs on this grant
                                 # (union of organisations-current and
                                 #  organisations-at-announcement, filtered to AU HEPs
                                 #  and mapped through institution_concordance.
                                 #  e.g. {"UNSW", "MON"}. OAX institution IDs are
                                 #  looked up from the concordance only when needed.)
    for_2d:      frozenset[str]  # 2-digit FoR codes for this grant
    role_code:   str
```

### `ParsedName`

A structured name, modelled on nameparser's `HumanName` fields. `middle` is a
tuple (not a list) so the whole object is hashable.

```python
from typing import NamedTuple

class ParsedName(NamedTuple):
    title:    str             # honorific prefix ("Dr", "Prof")
    first:    str             # primary given name
    middle:   tuple[str, ...] # additional given names, in order; often ()
    last:     str             # family name
    suffix:   str             # generational suffix ("Jr", "III")
    nickname: str             # parenthetical nickname
```

### `NameForm`

Two `ParsedName` representations of one unique (arc_first, arc_family) pair.

```python
@dataclass(frozen=True)
class NameForm:
    arc:     ParsedName  # ARC fields taken verbatim (post-nominals already stripped)
    norm_np: ParsedName  # normalise string first, then parse through HumanName
```

**Construction rules:**

- `arc`: parse `arc_first` alone through `HumanName` to get title/first/middle/
  suffix/nickname; set `last` directly from the ARC `family_name` column.
  Nameparser misassigns single tokens to `first` not `last`, so the ARC column
  is the only reliable source for `last` in the `arc` form.

- `norm_np`: apply `strip_parens()` and `strip_diacriticals()` to both ARC
  fields (preserving case so nameparser still recognises titles), concatenate,
  pass through `HumanName`, then lowercase all output fields and remove
  non-alpha-space characters. This combines normalisation and nameparser
  parsing in a single step. For compound family names (e.g. "Sen Gupta")
  nameparser promotes the prefix to `middle` and keeps only the final token
  as `last` — improving recall against OAX display names that store only the
  final token.

Each unique (arc_first, arc_family) pair (after post-nominal stripping) produces
exactly one NameForm. Multiple investigator records may share the same NameForm.

### `ClusterStatus`

```python
class ClusterStatus(Enum):
    UNRESOLVED  = auto()  # not yet processed
    SPLIT       = auto()  # replaced by child clusters; no longer active (audit only)
    MATCHED     = auto()  # individual, oax_id known
    ABSENT      = auto()  # individual, confirmed not in OAX
    UNDECIDABLE = auto()  # exhausted all layers
```

The pipeline only processes clusters where `status == UNRESOLVED`. `SPLIT`
clusters are retained in the store for audit but never processed further.

### `SplitReason`

```python
class SplitReason(Enum):
    ORCID_BOTH_IN_OAX       = auto()  # two ORCIDs resolve to distinct OAX authors
    OVERLAPPING_FELLOWSHIPS = auto()  # two fellowship records with overlapping periods
    MANUAL                  = auto()  # manually certified
```

Recorded on every child cluster at split time so splits can be reviewed and
checked. The parent cluster (status `SPLIT`) retains its original records;
each child carries the reason it was separated.

### `Cluster`

```python
@dataclass
class Cluster:
    cluster_id:   int
    name_forms:   frozenset[NameForm]

    # Full grant-level detail — needed for splitting
    records:      frozenset[InvestigatorRecord]

    # Aggregated evidence (derived from records; kept for fast lookup)
    orcids:       frozenset[str]      # all non-null ORCIDs across all records
    institutions: frozenset[str]      # union of grant_heps across all records (HEP codes)
    for_2d:       frozenset[str]      # union of all 2-digit FoR codes
    co_awardee_cluster_ids: frozenset[int]  # other clusters sharing grants (set after build)

    # Lineage (set when this cluster is created by a split)
    parent_id:           int | None        = None
    sibling_cluster_ids: frozenset[int]    = field(default_factory=frozenset)
    split_reason:        SplitReason | None = None

    # Resolution
    status:  ClusterStatus = ClusterStatus.UNRESOLVED
    oax_id:  str | None    = None
    tier:    int | None    = None
```

The `records` field makes the Cluster self-contained: it carries everything
needed to split itself without external lookups at split time.

The aggregated frozensets (`orcids`, `institutions`, `for_2d`) are redundant
with `records` but allow the matching layers to operate without iterating over
records every time.

`sibling_cluster_ids` is set at split time on all children of the same parent.
It supports the no-ORCID remainder case (Open Question 1): if a sibling is
already `MATCHED`, its OAX author can be used as a candidate for the remainder
cluster before falling through to Layers 2–3.

---

## Institution signal

The institution signal is restricted to **Australian Higher Education Providers
(HEPs)**. There are approximately 43 AU HEPs across the 2001–2025 period
(including institutions that changed name or eligibility status). International
universities and industry partners appear in the ARC organisations fields but
are excluded — they do not appear in OAX AU author affiliations and add noise.

Institution names are mapped to OAX institution IDs through
`institution_concordance.parquet`. This concordance covers the main AU HEP name
strings drawn from `admin_org`. It needs a small extension to cover name
variants that appear in the `organisations-current` and
`organisations-at-announcement` fields (e.g. `"University of Technology, Sydney"`
vs `"University of Technology Sydney"`). All other non-HEP strings are dropped.

A CI recorded at a non-HEP institution is treated as having no institution
signal (very rare; accepted loss).

### Fellowship preprocessing

Fellowship grants (7,735 of 7,755 have exactly one investigator) are a
natural special case: the single investigator's institution is unambiguously
the administering HEP. These can be extracted as a preprocessing step before
cluster building, reducing cluster complexity for the common case. The 20
fellowship grants with more than one investigator are treated as standard
multi-investigator grants.

---

## Layer 0 — Build clusters

**Input:** `investigators.parquet`, `grants.parquet`, `for_codes_wrangled.parquet`,
`institution_concordance.parquet`

**Steps:**

0. **Strip post-nominals** from `family_name` for every investigator record
   (`strip_postnominals()`) before any name processing. Applied once at load
   time; the cleaned value is what all downstream steps see.

1. For every unique (arc_first, arc_family) pair, compute all three NameForm
   representations.

2. Build a name-compatibility graph:
   - Nodes: unique (arc_first, arc_family) pairs
   - Edge between A and B if:
     - `norm_family(A) == norm_family(B)` (same normalised family name), AND
     - first names are mutually non-contradicting: every multi-character token
       in either name either matches exactly or is covered by an initial in the
       other (the `first_names_compatible` criterion already implemented in
       `00b_profile_name_clusters.py`)

3. Connected components of this graph → initial clusters.

4. **Flag multi-ORCID clusters:** any cluster whose investigator records carry
   two or more distinct ORCIDs is annotated with `n_distinct_orcids > 1` and
   prioritised for Layer 1. The split is not applied here — it is deferred to
   Layer 1, where OAX resolution determines whether the ORCIDs map to different
   authors (split certified) or the same author (one person, data quality issue,
   no split).

5. Load co-awardee links: for each cluster, find all other clusters that share
   at least one grant. Store as `co_awardee_cluster_ids`.

**Output:** A complete set of `Cluster` objects in status `UNRESOLVED`.

**Invariant after Layer 0:** every investigator record belongs to exactly one
cluster. Clusters with multiple ORCIDs are flagged but not yet split.

---

## Layer 1 — ORCID → OAX

**Operates on:** clusters with `len(orcids) == 1`

For each such cluster:
- Search OAX for the ORCID (local snapshot first, API fallback).
- If found and name validation passes: status → `MATCHED`, record `oax_id` and
  `tier=1`.
- If not found: status → `ABSENT`.

No propagation rule is needed. A person whose ORCID appears on some grants
but not others has compatible name forms across all those grants — by Layer 0
construction they are already in the same cluster. All their records are
resolved together when the cluster is MATCHED.

---

## Layer 1.5 — ORCID API name search

**Operates on:** clusters still `UNRESOLVED` after Layer 1 (no ORCID in ARC data)

For each such cluster, query the ORCID public API by name alone first:

```
GET https://pub.orcid.org/v3.0/search/
    ?q=family-name:{norm_family}+AND+given-names:{norm_first}
```

If the name search returns multiple results **and** the cluster has institution
evidence, re-query adding `affiliation-org-name:` to narrow:

```
GET https://pub.orcid.org/v3.0/search/
    ?q=family-name:{norm_family}+AND+given-names:{norm_first}
      +AND+affiliation-org-name:{hep_name}
```

Institution is a tiebreaker, not a gate — the name search always runs
regardless of whether institution evidence exists. Each candidate ORCID iD is
then passed through the standard Layer 1 OAX lookup (local snapshot → API
fallback).

**Classification:**
- Exactly one ORCID after name (or name+institution) search, and OAX lookup
  succeeds with name validation: status → `MATCHED`, `tier=1.5`
- Zero or multiple ORCIDs after both passes, or OAX lookup fails: cluster
  passes through to Layer 2 unchanged.

**Rationale:** converts a name-matching problem into an ORCID-anchored match.
The ORCID API search is effective even on name alone for distinctive names
(confirmed: `family-name:Cram AND given-names:Lawrence` → one result,
unambiguous). For common names, institution filtering makes it viable.

**Rate limit:** the ORCID public API is unauthenticated; registered API access
raises limits. This layer is run once per cluster (not per record), so volume
is bounded by the number of unresolved clusters after Layer 1 (~10k at most).

---

## Layer 2 — Name + institution

**Operates on:** clusters still `UNRESOLVED` after Layer 1

**OAX name index (built once before Layer 2 runs):** for every AU OAX author,
apply the `norm_np` pipeline to `display_name` and every entry in
`display_name_alternatives`. For each resulting name, also generate
initial-expanded forms: "David Bloggs" yields both `"david bloggs"` and
`"d bloggs"`. Index all forms under their normalised family token:
`norm_family_token → {oax_id, ...}`. This mirrors Layer 0's cluster
construction, which used the same initial-compatibility logic for ARC names.

**Retrieval:** for each cluster, query the reverse index with the normalised
family tokens from both `arc` and `norm_np` NameForms. The union of all
matching OAX IDs is the candidate pool.

**Candidate filtering:** the test applied to each candidate is
`first_names_compatible` — a "cannot rule out" criterion, not a positive
match. A candidate is *eliminated* only if its name forms are certifiably
incompatible with all ARC name forms in the cluster (different full first
names with no initial relationship). Candidates that cannot be ruled out
remain in the pool for the assignment tiers to decide.

**Assignment tiers** mirror the current pipeline (unique full match, unique
initial match, institution-confirmed shortlist).

**Layer 2 API supplement:** after the local parquet match, clusters that are
still `UNRESOLVED` because the local family-name index returned zero candidates
(not ambiguous — genuinely absent from the local index) are passed to a pyalex
`Authors().search_filter(display_name=...)` query. This catches names stored
differently in OAX than in ARC — transliteration variants, compound surnames
split differently, diacritical forms not normalised in the local index. The
returned candidates are scored and assigned using the same tiers as the local
pass. This replaces the ad-hoc `08_match_layer3_oax_api.py` no-match pass and
positions it where it belongs: immediately after the local name search fails,
before FoR and co-awardee filtering.

---

## Layer 3 — FoR topic filter + co-awardee

**Operates on:** clusters still `UNRESOLVED` after Layer 2

- FoR→OAX topic overlap filter (already implemented in `07_filter_layer3_for.py`)
  applied to the cluster's full `for_2d` set.
- Co-awardee signal: if any `co_awardee_cluster_ids` are already `MATCHED`,
  the identity of those OAX authors is available as a filtering hint. In the
  first implementation this narrows the candidate pool by institution and FoR
  overlap with the matched co-awardees' OAX profiles (no works data required).
  A future pass can extend this using OAX works data: the matched co-awardee's
  publication co-authors are a strong signal for early-career researchers whose
  own OAX profile is thin or fragmentary. `co_awardee_cluster_ids` is retained
  in the data model specifically to support this.

---

## Open questions

1. **Unanchored remainder after ORCID split.** A two-ORCID cluster almost
   always represents two different people whose names are compatible (e.g. both
   stored as "D Smith") — each brought their own ORCID. One person having two
   ORCIDs in ARC is rare and is a data quality issue rather than the norm.
   The genuinely ambiguous case is when only one ORCID resolves in OAX: the
   other may be a real second person absent from OAX, or a bad ORCID entry for
   the same person. When both ORCIDs resolve to distinct OAX authors the split
   is certified.

   When a cluster splits on two ORCIDs the no-ORCID investigator records form
   a third child cluster. Should this child immediately inherit the most likely
   ORCID-anchored sibling's match (based on institution/FoR overlap), or should
   it proceed through Layers 2–3 independently? The conservative answer is
   independent processing; inheriting requires a similarity threshold decision.

2. **Common-name ambiguity in Layer 2.** A cluster with a common name ("D
   Smith") and weak institution/FoR signal will produce a large OAX candidate
   pool that cannot be narrowed to a single match. There is no propagation
   risk — by Layer 0 construction no other cluster can share the same name
   form — but the cluster may be genuinely `UNDECIDABLE` with the available
   evidence. The question is whether any additional signal (co-awardee, ORCID
   API, time-period) can rescue these cases before they are declared
   undecidable.

3. **Splitting beyond ORCIDs.** After the ORCID split, some clusters will still
   contain multiple people (no ORCIDs, common name, different institutions and
   FoR fields). Time period overlap is a useful signal but its strength depends
   on role: a CI can hold multiple concurrent grants, so CI record overlap
   proves nothing. A fellow cannot hold two concurrent fellowships, so two
   fellowship records (FL, FT, FF, DE, DECRA, …) with overlapping grant periods
   in the same cluster certifies a split without any threshold decision — it is
   impossible for one person to be in that state. Institution × FoR
   non-overlap remains a probabilistic signal and still needs a precision
   threshold.

4. **Co-awardee loop.** Using co-awardee clusters as evidence creates an
   iterative dependency: resolve what you can, propagate to neighbours, resolve
   more. How many passes, and what is the stopping criterion?

5. **Serialisation.** Use serialisable forms throughout to enable
   checkpointing and avoid repeated reprocessing. Frozensets are not required
   — they were chosen for hashability in frozen dataclasses, but that
   constraint can be relaxed. Sets and lists serialise cleanly to JSON lines
   or parquet arrays. The cluster store should be writable and resumable at
   any layer boundary.

6. **Triaging genuine absences from matching failures.** The most likely
   real cases of an ARC investigator genuinely absent from OAX are in the
   humanities, creative arts, and indigenous research fields — OAX coverage
   is weaker in these areas and many practitioners do not have a publication
   record indexed there. Clusters in STEM and medical fields that end up
   `ABSENT` or `UNDECIDABLE` are more likely to be matching failures than
   true absences — those investigators almost certainly exist in OAX.
   Once the pipeline produces a clean residual list, the priority for
   re-examination should be non-humanities, non-creative, non-indigenous
   `ABSENT`/`UNDECIDABLE` clusters (using the cluster's `for_2d` set as the
   classifier). These are the cases where further effort is most likely to
   recover a correct match.

---

## Implementation plan

Prefer persisted datasets over runtime construction — expensive preprocessing
is done once, saved with a clear name, and loaded by downstream steps.
Serialisable forms (lists, dicts) are used throughout; frozensets are not
required.

### Scripts and persisted outputs

| Script | Reads | Writes |
|---|---|---|
| `00b_profile_name_clusters.py` | investigators, grants, for_codes | *(profiling only — keep as-is)* |
| `10_build_clusters.py` | investigators, grants, for_codes, institution_concordance | `clusters.jsonl` |
| `11_oax_name_index.py` | OAX AU authors parquet (ever-AU) | `oax_name_index.parquet` |
| `12_layer1_orcid.py` | `clusters.jsonl`, OAX authors | `clusters.jsonl` *(updated in place)* |
| `13_layer1_5_orcid_api.py` | `clusters.jsonl`, ORCID API | `clusters.jsonl` *(updated)* |
| `14_layer2_names.py` | `clusters.jsonl`, `oax_name_index.parquet` | `clusters.jsonl` *(updated)* |
| `15_layer3_for.py` | `clusters.jsonl`, OAX topics | `clusters.jsonl` *(updated)* |

`clusters.jsonl` is the single cluster store — one JSON object per line,
one line per cluster. Each script loads it, processes its target layer, writes
it back. Earlier layer outputs are preserved as checkpoints:
`clusters_after_layer0.jsonl`, `clusters_after_layer1.jsonl`, etc.

`oax_name_index.parquet` is built once from the full AU author snapshot.
Schema: `norm_family_token`, `oax_id`, `name_form`, `is_initial_form`.
Building it is the most expensive preprocessing step (~1.1M authors ×
multiple name forms each).

### Reuse of existing scripts

Scripts `00`–`04` are unchanged. `00b` remains a profiling tool.
Scripts `05`–`08` are superseded by `10`–`15` and can be retired once
the new pipeline is validated.
