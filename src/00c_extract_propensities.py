"""
00c_extract_propensities.py

PURPOSE:
    Precompute population-wide propensity/rarity tables over ARC's own award data, for the
    cyclic ACIF build (src/acif/). Same shape as this project's existing precomputed-frequency
    tables (oax_tf_*.parquet, work_tf_*.parquet): build once here, read many times elsewhere
    (src/acif/features.py wires these onto AwardCIFItem; nothing there re-scans the population).

INPUT:
    PROCESSED_DATA/grants_flat.parquet   -- one row per grant (eligible_orgs, primary_for_name)
    config.settings.GRANT_SUMMARIES_CSV  -- primary_field_of_research (for the FOR2008 code)
    config.settings.ARC_GRANTS_CSV       -- raw_json.csv, via load_grant_for2020_codes()

OUTPUT (all under PROCESSED_DATA):
    for_name_rarity.parquet        -- {signature_key, count, frequency}
    for_name_pair_freq.parquet     -- {name_a, name_b, count, frequency}
    institution_rarity.parquet     -- {institution_name, count, frequency}
    institution_pair_freq.parquet  -- {institution_a, institution_b, count, frequency}

DECISIONS ENCODED HERE (all direct 2026-09-19 corrections to an earlier draft of this design --
see /home/lc/.claude/plans/plan-that-in-tiny-immutable-heron.md's "Feature-availability gaps"
section for the full back-and-forth):
    - Every table is a GRANT-level population statistic, not an item/investigator-level one.
      Counting per investigator-item would inflate a value's frequency by however many
      investigators happen to share that grant -- a real bug in cluster_items()'s current
      for_name sig_counts, which this table corrects, not just relocates.
    - No z-score/null-model test anywhere ("this is not amenable to z-score being one-sided") --
      every table is a direct empirical frequency distribution: count / n_grants_in_scope.
    - No ORCID/identity-clustering ground truth anywhere ("what has orcid got to do with this.
      It is a property of the for/divs over the arc awards") -- co-occurrence is read directly
      off each grant's own multi-entry FOR list / eligible_orgs list, never off a resolved
      person's clustered items.
    - Institution rarity/pairs are NOT gated on n_eligible_orgs==1. That gate is a different,
      per-item ATTRIBUTION concern (AwardCIFItem.single_institution_grant -- "can we say THIS
      investigator is at THIS institution"), unrelated to institution POPULATION FREQUENCY.
      Gating the frequency table on it would bias the distribution: a smaller institution is
      more likely to appear as a co-eligible partner org than as sole admin org, so an n=1 gate
      would systematically undercount it (or, for one that's never sole admin org, exclude it
      entirely).
    - Institution names ARE filtered to genuine HEPs (2026-09-19, direct user correction --
      "I can see non-HEP in the inst which will dilute a lot"): eligible_orgs carries real
      research-PARTNER organisations too (CSIRO divisions, museums, botanic gardens, industry/
      aged-care partners -- 17 of the raw 103 names had no HEP flag at all), which aren't places
      a CI is actually employed and shouldn't dilute an institution-identity propensity table.
      HEP status resolved via the canonical organisationName GROUP (same defensive resolution as
      _load_hep_crosswalk()), not each alias row individually.
    - Institution names ARE canonicalized through admin_orgs.csv's organisationName_alias ->
      organisationName crosswalk before counting (2026-09-19 fix -- grants_flat.parquet's raw
      eligible_orgs strings were never rewritten to the canonical form themselves, e.g.
      "University of Technology, Sydney" / "University of Technology" both alias to "University
      of Technology Sydney"; reading eligible_orgs directly without this crosswalk split one real
      institution into several rows).
    - Pair ordering is by name, alphabetically (a < b) -- ARC's own primary/secondary flag isn't
      a reliable ordering signal here since FOR codes are themselves "projected" (resolved
      through the FOR2008/RFCD98->FOR2020 upgrade), so a canonical name-sort is the only
      deterministic ordering available (same convention as cluster_id's own tie-break and
      cluster_checks.ACCEPTABLE_DIVISION_PAIRS's frozenset pairs).
    - for_name_rarity (single-value) keeps the EXISTING for_name_tokens definition (primary
      for_name only, upgraded to FOR2020 naming via upgrade_for_name(), then synonym-expanded
      via expanded_for_tokens()/for_concordance.csv) -- a straight port of cluster_items()'s
      current sig_counts/for_name_freq mechanism, just fixed to count once per grant instead of
      once per item. A grant has exactly one primary_for_name, so this can only ever be a
      single-value signature -- it cannot itself produce an internal pair.
    - for_name_pair_freq therefore draws from a DIFFERENT, necessarily multi-entry source:
      load_grant_for2020_codes() (already KEEP_SCHEMES-scoped, already resolves every FOR entry
      ARC recorded for a grant -- 2 to 16 entries -- to a FOR2020 group name). This is coarser
      (FOR2020 group precision, no synonym expansion) than for_name_tokens, but it's the only
      source with more than one FOR value per grant to pair against another.
"""

import sys
from collections import Counter
from itertools import combinations
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA, GRANT_SUMMARIES_CSV, ADMIN_ORGS_CSV as _ADMIN_ORGS_CSV_PATH
from config.scope import KEEP_SCHEMES
from src.utils.names import make_expanded_for_tokens
from src.utils.for_resolve import upgrade_for_name
from src.utils.awards_cif import load_grant_for2020_codes, _FOR_CONCORDANCE_CSV


def _scope_filter(grant_code: str) -> bool:
    return grant_code[:2] in KEEP_SCHEMES


def _load_institution_name_crosswalk() -> tuple[dict[str, str], set[str]]:
    """admin_orgs.csv organisationName_alias -> organisationName (canonical display name), plus
    the set of canonical names that are genuinely HEP institutions.

    Canonicalization: raw eligible_orgs strings in grants_flat.parquet were never rewritten to
    the canonical form, the fix lives only in this alias table (e.g. "University of Technology,
    Sydney" and "University of Technology" both alias to "University of Technology Sydney") --
    confirmed 2026-09-19 by directly checking grants_flat.parquet still carries the
    un-canonicalized variants. Institution names with no alias row pass through unchanged, not
    dropped.

    HEP filter (added 2026-09-19, direct user correction): eligible_orgs is NOT restricted to
    Higher Education Providers -- CSIRO divisions, museums, botanic gardens, aged-care/industry
    partner orgs etc. all appear (confirmed directly: 17 of 103 raw institution names before this
    fix had no HEP flag at all, e.g. "Botanic Gardens and Parks Authority", "CSIRO - Petroleum
    Resources", "Aged Care and Housing Group"). These are real research-partner organisations,
    not places a CI is actually employed, and diluting institution_rarity/institution_pair_freq
    with them doesn't serve this table's purpose (matches load_award_cif_items()'s own established
    scope: only a recognised Australian HEP admin_org is in scope for identity work). HEP status
    resolved via the canonical organisationName GROUP, not each alias row individually -- same
    defensive resolution as _load_hep_crosswalk(), for the same reason (a correctly-HEP-flagged
    alias can still have its own hep_code cell blank if a sibling alias carries the real data)."""
    import csv as _csv
    rows = list(_csv.DictReader(open(_ADMIN_ORGS_CSV_PATH, newline="")))
    canonical_is_hep: dict[str, bool] = {}
    for row in rows:
        name = row["organisationName"].strip()
        if row["HEP"].strip().lower() == "y":
            canonical_is_hep[name] = True
    crosswalk: dict[str, str] = {}
    for row in rows:
        alias = row["organisationName_alias"].strip()
        name = row["organisationName"].strip()
        if alias and name:
            crosswalk[alias] = name
    hep_names = set(canonical_is_hep)
    return crosswalk, hep_names


def build_for_name_rarity() -> pd.DataFrame:
    """One row per grant's own for_name_tokens signature (primary for_name only, upgraded +
    synonym-expanded) -- count/frequency across the KEEP_SCHEMES grant population. Ports
    cluster_items()'s current per-item sig_counts to the correct, grant-level population."""
    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT
                g.grant_code,
                g.primary_for_name,
                regexp_extract(s.primary_field_of_research, '^\\d{{4}}') AS for2008_code
            FROM read_parquet('{PROCESSED_DATA}/grants_flat.parquet') g
            LEFT JOIN read_csv_auto('{GRANT_SUMMARIES_CSV}') s
                ON g.grant_code = s.grant_id
        """).fetchall()
        col_names = [d[0] for d in con.description]
    finally:
        con.close()

    expanded_for_tokens = make_expanded_for_tokens(str(_FOR_CONCORDANCE_CSV))

    sig_counts: Counter[str] = Counter()
    n_grants = 0
    for row in rows:
        r = dict(zip(col_names, row))
        if not _scope_filter(r["grant_code"]):
            continue
        for_name = upgrade_for_name(r["for2008_code"], r["primary_for_name"])
        if not for_name:
            continue
        tokens = expanded_for_tokens(for_name)
        if not tokens:
            continue
        n_grants += 1
        sig_counts["|".join(sorted(tokens))] += 1

    return pd.DataFrame(
        {"signature_key": k, "count": c, "frequency": c / n_grants}
        for k, c in sig_counts.items()
    ).sort_values("signature_key").reset_index(drop=True)


def build_for_name_pair_freq() -> pd.DataFrame:
    """One row per (name_a, name_b) pair of distinct FOR2020 group names co-occurring on the
    SAME grant -- alphabetically ordered, counted once per grant, no identity/ORCID involved."""
    grant_codes = load_grant_for2020_codes()  # already KEEP_SCHEMES-scoped

    pair_counts: Counter[tuple[str, str]] = Counter()
    n_pairs = 0
    for grant_code, entries in grant_codes.items():
        names = sorted({e["name"] for e in entries if e["name"]})
        if len(names) < 2:
            continue
        for a, b in combinations(names, 2):  # names already sorted -> a < b
            pair_counts[(a, b)] += 1
            n_pairs += 1

    return pd.DataFrame(
        {"name_a": a, "name_b": b, "count": c, "frequency": c / n_pairs}
        for (a, b), c in pair_counts.items()
    ).sort_values(["name_a", "name_b"]).reset_index(drop=True)


def build_institution_rarity() -> pd.DataFrame:
    """One row per institution name -- how many KEEP_SCHEMES grants list it anywhere in
    eligible_orgs (admin org or partner org alike), count/frequency over n_grants_in_scope.
    Deliberately NOT gated on n_eligible_orgs==1 -- see module docstring."""
    df = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet", columns=["grant_code", "eligible_orgs"])
    df = df[df["grant_code"].map(_scope_filter)]
    n_grants = len(df)
    crosswalk, hep_names = _load_institution_name_crosswalk()

    inst_counts: Counter[str] = Counter()
    for orgs in df["eligible_orgs"]:
        # orgs is a numpy array (parquet list column via pyarrow) -- `orgs or []` is invalid,
        # ambiguous truth value for an array with 2+ elements.
        if orgs is None or len(orgs) == 0:
            continue
        for name in {crosswalk.get(o, o) for o in orgs}:
            if name in hep_names:
                inst_counts[name] += 1

    return pd.DataFrame(
        {"institution_name": k, "count": c, "frequency": c / n_grants}
        for k, c in inst_counts.items()
    ).sort_values("institution_name").reset_index(drop=True)


def build_institution_pair_freq() -> pd.DataFrame:
    """One row per (institution_a, institution_b) pair of distinct institutions co-listed in the
    SAME grant's own eligible_orgs -- alphabetically ordered, counted once per grant."""
    df = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet", columns=["grant_code", "eligible_orgs"])
    df = df[df["grant_code"].map(_scope_filter)]
    crosswalk, hep_names = _load_institution_name_crosswalk()

    pair_counts: Counter[tuple[str, str]] = Counter()
    n_pairs = 0
    for orgs in df["eligible_orgs"]:
        if orgs is None or len(orgs) == 0:
            continue
        names = sorted({crosswalk.get(o, o) for o in orgs} & hep_names)
        if len(names) < 2:
            continue
        for a, b in combinations(names, 2):
            pair_counts[(a, b)] += 1
            n_pairs += 1

    return pd.DataFrame(
        {"institution_a": a, "institution_b": b, "count": c, "frequency": c / n_pairs}
        for (a, b), c in pair_counts.items()
    ).sort_values(["institution_a", "institution_b"]).reset_index(drop=True)


def main() -> None:
    for_name_rarity = build_for_name_rarity()
    for_name_rarity.to_parquet(PROCESSED_DATA / "for_name_rarity.parquet", index=False)
    print(f"for_name_rarity: {len(for_name_rarity)} signatures")

    for_name_pair_freq = build_for_name_pair_freq()
    for_name_pair_freq.to_parquet(PROCESSED_DATA / "for_name_pair_freq.parquet", index=False)
    print(f"for_name_pair_freq: {len(for_name_pair_freq)} pairs")

    institution_rarity = build_institution_rarity()
    institution_rarity.to_parquet(PROCESSED_DATA / "institution_rarity.parquet", index=False)
    print(f"institution_rarity: {len(institution_rarity)} institutions")

    institution_pair_freq = build_institution_pair_freq()
    institution_pair_freq.to_parquet(PROCESSED_DATA / "institution_pair_freq.parquet", index=False)
    print(f"institution_pair_freq: {len(institution_pair_freq)} pairs")


if __name__ == "__main__":
    main()
