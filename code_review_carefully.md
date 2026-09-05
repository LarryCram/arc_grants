# Splink Design: ARC Grants → AwardCIF (CI/Fellow) Disambiguation

## 1. Goal
Disambiguate ordinary human names across ~65k ARC grant records and merge them into a person-level “AwardCIF” object, using:
- Name variants (e.g. “Li Han”, “Li (Joan) Han”)
- Grant properties: year, adminOrg (as a set), fields of research (FoR, as a set)

Output: a clustered set of grant-role records, each cluster = one person (AwardCIF).

## 2. Data shape

### Input table(s)
One table (or two, if separating ARC vs internal grants):

Columns (example):
- grant_id
- role_type (CI, Fellow, etc.)
- name_raw (string)
- name_given (string, if available)
- name_family (string, if available)
- year (int)
- admin_orgs (array<string> or delimited string)
- for_codes (array<string> or delimited string)
- (optional) orcid, internal_person_id

Record count: ~65k rows.

### Preprocessing (done before Splink)
- Normalize name_raw (lowercase, strip punctuation, expand common variants if known).
- Parse admin_orgs and for_codes into arrays (or consistent delimited strings).
- Optionally create:
  - name_given_norm, name_family_norm
  - name_initial_family (e.g. “L Han”)
  - name_key (e.g. family + first initial)

## 3. Linkage type
- link_type: "dedupe_only" if single table of grant roles.
- Or "link_and_dedupe" if linking ARC grants to another grants table and also deduplicating within each.

## 4. Blocking strategy

Purpose: generate candidate pairs likely to be the same person, without exploding comparisons.

Use multiple rules combined with OR; each rule uses AND internally.

Example rules (to be refined):

1. Exact family name + same year + at least one overlapping admin_org
   - SQL-ish: 
     l.name_family = r.name_family
     AND l.year = r.year
     AND arrays_overlap(l.admin_orgs, r.admin_orgs)

2. Exact family name + same year + same first initial of given name
   - l.name_family = r.name_family
     AND l.year = r.year
     AND substr(l.name_given_norm,1,1) = substr(r.name_given_norm,1,1)

3. Exact family name + overlapping FoR codes + same year
   - l.name_family = r.name_family
     AND l.year = r.year
     AND arrays_overlap(l.for_codes, r.for_codes)

4. (Optional, looser) Same name_key + same year
   - l.name_key = r.name_key
     AND l.year = r.year

Implementation in Splink:
- Use `blocking_rules_to_generate_predictions` as a list of SQL strings or `block_on` + custom SQL where needed.
- Ensure at least one rule is broad enough to catch name variants (e.g. same family + year + org overlap).

## 5. Comparison strategy

Each comparison defines how similarity is measured for a logical attribute.

### 5.1 Name comparison (custom, 1 left vs 2 right if needed)

Goal: treat “Li Han” and “Li (Joan) Han” as strong matches.

Options:
- Use normalized given/family columns and apply Jaro–Winkler on both.
- Or define a custom comparison that uses:
  - name_raw_l vs name_raw_r
  - plus name_given_norm_r or a variant column.

Example custom comparison (conceptual):

- Columns used: ["name_raw", "name_given_norm"]
- Levels:
  - Level 2 (strong): 
    - name_family exact match AND
    - Jaro–Winkler(name_given_norm_l, name_given_norm_r) ≥ 0.9
  - Level 1 (medium):
    - name_family exact match AND
    - Jaro–Winkler(name_raw_l, name_raw_r) ≥ 0.88
  - Level 0 (weak/no signal): else

SQL CASE sketch:

```sql
CASE
  WHEN name_family_l IS NULL OR name_family_r IS NULL THEN -1
  WHEN name_family_l = name_family_r
       AND jaro_winkler_sim(name_given_norm_l, name_given_norm_r) >= 0.9 THEN 2
  WHEN name_family_l = name_family_r
       AND jaro_winkler_sim(name_raw_l, name_raw_r) >= 0.88 THEN 1
  ELSE 0
END
```

If you need 2 right columns (e.g. name_raw_r and name_variant_r), extend the CASE accordingly.

### 5.2 Year comparison

- Exact match on year is strong evidence.
- Levels:
  - Level 1: year_l = year_r
  - Level 0: else (or allow ±1 year as a weaker level if appropriate).

### 5.3 Admin orgs (set comparison)

Treat as sets; use overlap measures.

Possible levels:
- Level 2: exact same set (sorted strings equal).
- Level 1: non-empty intersection but not exact.
- Level 0: no intersection.

SQL sketch (assuming a function `arrays_overlap` or using string contains):

```sql
CASE
  WHEN admin_orgs_l IS NULL OR admin_orgs_r IS NULL THEN -1
  WHEN admin_orgs_l = admin_orgs_r THEN 2
  WHEN arrays_overlap(admin_orgs_l, admin_orgs_r) THEN 1
  ELSE 0
END
```

### 5.4 Fields of Research (FoR)

Similar to admin orgs:

- Level 2: exact same set of FoR codes.
- Level 1: partial overlap.
- Level 0: no overlap.

## 6. Training and evaluation

- Use EM to estimate m/u probabilities:
  - Start with a small subset (e.g. 5–10k rows) for speed.
  - Use blocking rules for training similar to prediction rules.
- Manually label a small sample (200–500 pairs) of:
  - Clear matches (same person)
  - Clear non-matches
- Compute precision/recall at chosen probability threshold.
- Adjust:
  - Blocking rules (to improve recall)
  - Comparison levels/thresholds (to improve precision)

## 7. Output

- Pairwise predictions table with match probability.
- Clustered table: each cluster = one AwardCIF (person).
- Retain:
  - grant_id, role_type, name_raw, year, admin_orgs, for_codes
  - cluster_id (person_id)

## 8. Constraints for code generation

Any Splink code produced must:
1. Follow patterns in the Splink topic guides on:
   - Blocking rules
   - Custom comparisons / case_expression
   - Training vs prediction blocking
2. Use DuckDB backend (unless specified otherwise).
3. Include comments linking non-trivial choices to specific sections of the Splink docs.
4. Provide small diagnostic queries (e.g. number of pairs generated, distribution of comparison levels).




