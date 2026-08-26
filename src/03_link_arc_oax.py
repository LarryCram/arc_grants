"""
src/03_link_arc_oax.py

Link ARC person clusters to OpenAlex (OAX) Australian authors.
Splink link_only: awards_cif_arc_only.parquet → openalex_authors_prep.parquet.

Reads awards_cif_arc_only.parquet specifically, NOT awards_cif.parquet -- the former is
01_prepare_arc.py's ARC/ORCID-only output (build_arc_only_population(), no OAX dependency);
the latter is 04_resolve_links.py's OAX-enriched rebuild of it (2026-08-25: absorbed from a
former separate stage, 03b_enrich_awards_cif.py, now archived). Reading the enriched file here
would be circular (this script IS what produces the OAX linkage that file depends on) -- see
CLAUDE.md's 2026-08-21 "ARC processing must produce checkable output before connecting with
OAX" session notes for the incident this avoids.

Output: arc_oax_links.parquet
    arc_id, oax_id, match_probability, high_confidence
    All candidate pairs ≥ PREDICT_THRESHOLD; high_confidence flags pairs ≥ LINK_THRESHOLD.

## Blocking design (2026-08-25 audit against a user-supplied Splink blocking-rules reference)

`blocking_rules_to_generate_predictions` is a disjunctive UNION of simple, tight rules -- never
one rule with `OR` inside a single SQL string (that forces a full cross-join before filtering;
a Python list of rules lets Splink execute each as its own equi-join and take the set union).
Current rules, in order, and what each is for:

1. `block_on("family_name_main", "first_initial")` -- the primary tight intersection.
2-3. Middle-initial cross rules -- one side's middle initial = the other's first initial
     (catches "Z Smith" vs "Herb Z Smith").
4. `l.orcid = r.orcid AND l.orcid IS NOT NULL` -- exact identity anchor, bypasses every
   name-based condition entirely.
5. `list_has_any(l.family_names, r.family_names) AND l.first_initial = r.first_initial` --
   surname SET-overlap (2026-08-25a). `family_name_main` is a single scalar picked via
   `max_by_len()` ("longest variant wins"), which can disagree between the two sides even when a
   real shared spelling exists in both full `family_names` sets -- confirmed on
   `DP0345157_HansMuhlhaus` (ARC `family_names=['muhlhaus']`; OpenAlex's own records have BOTH
   'muehlhaus' and 'muhlhaus', but `max_by_len` always picks the longer 'muehlhaus', so the
   scalar comparison never matched). MEASURED cost (`splink.blocking_analysis`, run against the
   real 22,910×2,779,882-record population): 3.69 BILLION pre-filter comparisons -- DuckDB can't
   push `list_has_any` into an efficient equi-join, so the only pushable condition is
   `first_initial` alone (weak anchor at this population size), and the array-overlap check runs
   as a post-join filter. Confirmed NOT a real performance problem despite the alarming raw
   count: full-pipeline blocking time barely moved (27.3s -> 28.8s) after adding this and rules
   6-7 below -- DuckDB's vectorized execution handles it. Left as-is; flagged here as a real,
   measured cost characteristic in case future population growth changes that conclusion.
6. `l.family_name_main = r.family_name_main AND list_has_any(l.first_names_multichar, r.first_names_multichar)`
   -- given-name SET-overlap (2026-08-25b), the given-name-side sibling of rule 5, added after
   direct user pushback that the given-name side had been left on the old single-scalar
   `first_initial` design while the surname side was fixed. Catches a genuine spelling/diacritic
   given-name variant -- NOT a nickname/alias like Jenny/Yingzi, which has no shared string for
   any set-overlap rule to find (see CLAUDE.md's "Manual Resolution Techniques"). Anchored on
   EXACT `family_name_main` (not a second set-overlap) specifically to keep this rule tight --
   `first_names_multichar` filters out bare single-character initial tokens first (confirmed via
   direct inspection: ALL 22,910 ARC clusters' `first_names` arrays include one, since
   `_name_forms()` always self-adds the initial for the primary blocking key -- an unfiltered
   `list_has_any` here would match on that initial alone in effectively every pair, pure noise,
   and redundant with rule 1). MEASURED (`n_largest_blocks`): worst single block
   (`family_name_main='li'`) is 2,282,655 pre-filter pairs -- proportionally worse skew than rule
   1's worst block (44,208), because this rule has no `first_initial` anchor at all. Real
   tension: the rule is most needed for exactly the population (common East Asian surnames)
   where it's also most skew-prone. Kept as-is after the same full-pipeline timing check as rule
   5 showed no real cost -- "loose blocking, precise scoring": a big block is a performance
   concern, not an accuracy one, as long as Splink's own comparison scoring (institution, ORCID,
   TF-adjusted name agreement) still has to agree before a pair becomes high-confidence, and
   performance is confirmed fine.
7. `l.first_name = r.family_name_main AND l.family_name_main = r.first_name` -- given/family
   name-ORDER INVERSION (2026-08-25c). Catches a real cultural naming-convention feature (e.g.
   Kotagiri Ramamohanarao / Ramamohanarao Kotagiri -- Telugu; Pathegama Ranjith / Ranjith
   Pathegama -- Sri Lankan), not a data error -- CLAUDE.md had this documented as "a genuinely
   distinct, unfixed issue... not yet designed in detail or scheduled" despite the exact rule
   needed having already been supplied in a user-provided Splink blocking-rules reference and
   never implemented. Cheap: 4,036 pre-filter pairs, both sides scalar equality, no skew risk.
   Scored by its own standalone `name_order_swap` comparison (see `comparisons=[...]` below) --
   NOT folded into the `given_name`/`family_name_main` comparisons, since a swapped pair scores
   as a MISMATCH on both of those (each compares l.X to r.X on the same field only) -- the exact
   same "blocking rule generates the pair, comparison scores it as a mismatch, fix defeated"
   failure the set-overlap comparison levels below exist to avoid, here crossing a field boundary
   instead of within one field.

Every set-overlap/cross-field blocking rule above has a matching comparison level (or, for the
inversion rule, a whole standalone comparison) with an EXPLICITLY FIXED, not EM-trained,
m_probability/u_probability -- the ORCID-blocked EM training session never visits these rare
patterns in enough volume to estimate them reliably. Without this, a blocking rule can generate
a genuinely-matching candidate pair that the comparison step then scores as a near-zero-weight
mismatch anyway, silently defeating the blocking fix -- confirmed as the actual failure mode on
the very first version of the rule-5 fix, before its matching comparison level was added.

Diagnostic tools used to produce every measured number above:
`splink.blocking_analysis.count_comparisons_from_blocking_rule` /
`.n_largest_blocks` -- never used anywhere in this codebase before 2026-08-25, despite being the
standard Splink-recommended method for verifying blocking recall/skew (measure, don't just reason
about named cases). Re-run these against the real prep tables before trusting any future blocking
rule change; do not assume a rule's cost or skew from its SQL shape alone -- rule 5 and rule 6
above both turned out non-obvious (5's real anchor is far weaker than its SQL suggests; 6's skew
is real and specifically concentrated on the population the rule is meant to help).
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.utils.names import max_by_len, parse_given
from src.utils.pipeline_freshness import (
    assert_fresh, AWARDS_CIF_SOURCE, NAME_LOGIC_SOURCES,
)

PREDICT_THRESHOLD = 0.5
LINK_THRESHOLD    = 0.9

_DATA_PERSISTED = Path(__file__).resolve().parents[1] / "data_persisted"
# Every file build_arc_only_population() reads that could change awards_cif_arc_only.parquet's
# content -- raw ARC data, every manual override CSV, and for_concordance.csv (feeds
# for_name_tokens via make_expanded_for_tokens() -- free-text FOR-name fuzzy bridging for
# Splink's own comparison scoring, NOT FOR-code resolution, which goes exclusively through
# for_resolve.py/Resolver() and isn't file-based here). admin_orgs.csv/grant_summaries.csv
# omitted -- genuinely static reference data this project's own source, not user-edited.
_AWARDS_CIF_INPUTS = [
    PROCESSED_DATA / "investigators_raw.parquet",
    PROCESSED_DATA / "grants_flat.parquet",
    _DATA_PERSISTED / "manual_name_corrections.csv",
    _DATA_PERSISTED / "manual_orcid_corrections.csv",
    _DATA_PERSISTED / "manual_orcids.csv",
    _DATA_PERSISTED / "manual_merges.csv",
    _DATA_PERSISTED / "manual_splits.csv",
    _DATA_PERSISTED / "manual_splits_by_grant.csv",
    _DATA_PERSISTED / "manual_confirmed_not_suspicious.csv",
    _DATA_PERSISTED / "manual_confirmed_distinct.csv",
    _DATA_PERSISTED / "enrichment_blocklist.csv",
    _DATA_PERSISTED / "for_concordance.csv",
    AWARDS_CIF_SOURCE, *NAME_LOGIC_SOURCES,
]


def _make_full_name_key(df):
    return (
        (df["first_compound"] + "_" + df["family_name_main"])
        .where(df["first_compound"].notna() & df["family_name_main"].notna())
    )


def _prep_arc(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    print(f"  ARC persons: {len(df)}")

    df["family_name_main"] = df["family_names"].apply(max_by_len)

    parsed = df["full_names"].apply(max_by_len).apply(parse_given)
    df[["first_name", "middle_name", "first_compound", "first_initial", "middle_initial"]] = (
        pd.DataFrame(parsed.tolist(), index=df.index)
    )
    df["full_name_key"] = _make_full_name_key(df)
    df["orcid"]    = df["orcids"].apply(lambda lst: lst[0] if lst is not None and len(lst) > 0 else None)
    df["inst_arr"] = df["inst_arr"].apply(lambda x: list(x) if x is not None else [])
    # family_names itself (not just the max_by_len-collapsed family_name_main) is passed through
    # for the set-overlap blocking rule/comparison below -- see the 2026-08-25 fix note near
    # blocking_rules_to_generate_predictions.
    df["family_names"] = df["family_names"].apply(lambda x: list(x) if x is not None else [])
    # first_names_multichar: the given-name-side sibling of family_names, same set-overlap
    # purpose (added 2026-08-25 per direct user pushback -- this project's own given-name
    # blocking had never been widened past the single-scalar first_initial, the exact same
    # "longest/single-value wins" fragility already fixed for surnames). Filtered to length>1
    # tokens ONLY -- awards_cif.py's own _name_forms() always self-adds a bare first-initial
    # token to first_names (needed for Splink's family+first_initial blocking key), so an
    # unfiltered list_has_any() here would match on that initial alone in effectively every
    # pair (confirmed: all 22,910 ARC clusters carry a single-char token in first_names) --
    # pure noise, and redundant with the existing first_initial blocking rule.
    df["first_names_multichar"] = df["first_names"].apply(
        lambda x: [t for t in x if len(t) > 1] if x is not None else []
    )

    return df[[
        "cluster_id", "family_name_main", "family_names",
        "first_name", "middle_name", "first_compound", "first_initial", "middle_initial",
        "first_names_multichar",
        "full_name_key", "orcid", "inst_arr",
    ]].rename(columns={"cluster_id": "unique_id"})


def _prep_oax(con: duckdb.DuckDBPyConnection, path: Path) -> pd.DataFrame:
    # HumanName-parsed columns are persisted by 00c_prepare_oax.py — no re-parsing needed.
    # Blocks on the FULL family_names (display_name + display_name_alternatives combined), not
    # just the display-only half -- reversed 2026-08-25 after direct pushback on an earlier,
    # overcautious version that excluded alternatives entirely over contamination risk (the
    # documented Clarke/Erfani cases, where display_name_alternatives sometimes carries an
    # unrelated co-author's name). That risk is real for max_by_len()'s SCALAR collapse -- one
    # wrong pick corrupting the blocking key even for a genuinely correct match -- but is a
    # different mechanism from SET-OVERLAP blocking: a spurious pair generated by a rare
    # contaminant still has to survive Splink's own comparison scoring (first name, ORCID,
    # institution) before it becomes a match, exactly the "loose blocking, precise scoring"
    # pattern blocking rules are meant to follow. Excluding alternatives also throws away real,
    # legitimate evidence -- the case where ARC's own spelling only ever shows up in an author's
    # alternatives, not their current curated display_name.
    df = con.execute(f"""
        SELECT unique_id, orcid, family_name_main, family_names,
               first_name, middle_name, first_compound, first_initial, middle_initial,
               first_names,
               inst_ids
        FROM read_parquet('{path}')
    """).fetchdf()
    print(f"  OAX authors:  {len(df)}")

    df["full_name_key"] = _make_full_name_key(df)
    df["inst_arr"] = df["inst_ids"].apply(lambda x: list(x) if x is not None else [])
    df["family_names"] = df["family_names"].apply(lambda x: list(x) if x is not None else [])
    # See _prep_arc()'s matching comment -- same length>1 filter, same reason (OAX's own
    # first_names also mixes full given-name forms with bare initial tokens).
    df["first_names_multichar"] = df["first_names"].apply(
        lambda x: [t for t in x if len(t) > 1] if x is not None else []
    )

    return df[[
        "unique_id", "family_name_main", "family_names",
        "first_name", "middle_name", "first_compound", "first_initial", "middle_initial",
        "first_names_multichar",
        "full_name_key", "orcid", "inst_arr",
    ]]


def main():
    arc_path = PROCESSED_DATA / "awards_cif_arc_only.parquet"
    oax_path = PROCESSED_DATA / "openalex_authors_prep.parquet"
    out_path = PROCESSED_DATA / "arc_oax_links.parquet"

    # Refuse to link against a stale ARC-side population -- e.g. a manual_*.csv edited, or
    # investigators_raw.parquet refreshed, since awards_cif_arc_only.parquet was last built.
    # This is exactly the class of bug this project hit twice in one session (2026-08-21): a
    # wired-in correction that never actually reached production, and a diagnostic run against
    # a population that predated the fix it was meant to verify.
    assert_fresh(
        "03_link_arc_oax (awards_cif_arc_only.parquet)",
        outputs=[arc_path], inputs=_AWARDS_CIF_INPUTS,
    )
    # openalex_authors_prep.parquet's own freshness policy (2026-08-25, user-directed): it's
    # expensive to rebuild (minutes, 2.78M authors) and only needs updating a few times a year --
    # when the OpenAlex snapshot changes, not on every unrelated source-code edit. So this checks
    # it against authorships_hep.parquet/works_hep.parquet (the real snapshot-derived sources),
    # matching 00c_prepare_oax.py's own ensure_fresh() policy exactly -- NOT against
    # 00c_prepare_oax.py's own source file, which would (and did) fire on every cosmetic edit to
    # that script, contradicting ensure_fresh()'s own deliberate choice to ignore exactly that.
    assert_fresh(
        "03_link_arc_oax (openalex_authors_prep.parquet)",
        outputs=[oax_path],
        inputs=[
            PROCESSED_DATA / "authorships_hep.parquet", PROCESSED_DATA / "works_hep.parquet",
        ],
    )

    con = duckdb.connect()

    print("[1/4] Preparing input tables...")
    df_arc = _prep_arc(con, arc_path)
    df_oax = _prep_oax(con, oax_path)

    settings = SettingsCreator(
        unique_id_column_name="unique_id",
        link_type="link_only",
        blocking_rules_to_generate_predictions=[
            block_on("family_name_main", "first_initial"),
            # Cross-blocking: one side's middle initial matches the other's first initial,
            # catching cases like ARC "Z Smith" vs OAX "Herb Z Smith".
            "l.family_name_main = r.family_name_main AND l.middle_initial IS NOT NULL AND l.middle_initial = r.first_initial",
            "l.family_name_main = r.family_name_main AND r.middle_initial IS NOT NULL AND r.middle_initial = l.first_initial",
            "l.orcid = r.orcid AND l.orcid IS NOT NULL",
            # Set-overlap blocking (2026-08-25): family_name_main is a single scalar picked via
            # max_by_len() ("longest variant wins"), which can disagree between the two sides even
            # when a real shared spelling exists in both full family_names sets -- confirmed on
            # DP0345157_HansMuhlhaus: ARC has family_names=['muhlhaus']; OpenAlex's own records
            # include BOTH 'muehlhaus' and 'muhlhaus', but max_by_len always picks the longer
            # 'muehlhaus', so the scalar comparison never matched despite 'muhlhaus' sitting in
            # both sets. list_has_any() blocks on real set membership instead. Additive, not a
            # replacement -- only adds candidate pairs the scalar rule misses.
            "list_has_any(l.family_names, r.family_names) AND l.first_initial = r.first_initial",
            # Given-name set-overlap (2026-08-25): the same fix, given-name side -- catches a
            # genuine spelling/diacritic-variant given name (the same class family_names solves,
            # NOT a nickname/alias like Jenny/Yingzi, which has no shared string for any
            # set-overlap rule to find -- see CLAUDE.md's "Manual Resolution Techniques"). Exact
            # surname required (not family_names set-overlap) to keep this rule tight, per the
            # Splink blocking-rule design note above: pair a loose signal with a strong anchor.
            "l.family_name_main = r.family_name_main AND list_has_any(l.first_names_multichar, r.first_names_multichar)",
            # Given/family name-order inversion (2026-08-25): catches the reversed-name-order
            # failure mode documented in CLAUDE.md (Kotagiri Ramamohanarao / Ramamohanarao
            # Kotagiri -- a real cultural naming-convention feature, not a data error) -- one
            # side's first_name matches the other's family_name_main and vice versa. Previously
            # documented as "not yet designed," even though this exact rule had already been
            # supplied and was never implemented.
            "l.first_name = r.family_name_main AND l.family_name_main = r.first_name",
        ],
        comparisons=[
            cl.CustomComparison(
                output_column_name="given_name",
                comparison_description="Given name: compound / first / cross / initial cascade",
                comparison_levels=[
                    # Null level fires first; all subsequent levels are guaranteed non-null on first_name.
                    {"sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
                     "label_for_charts": "null", "is_null_level": True},
                    # "shi xue" = "shi xue"
                    {"sql_condition": "first_compound_l = first_compound_r",
                     "label_for_charts": "Compound exact"},
                    # "shi" = "shi" — plain equality so Splink recognises the exact match level for TF.
                    {"sql_condition": "first_name_l = first_name_r",
                     "label_for_charts": "First exact",
                     "tf_adjustment_column": "first_name",
                     "tf_adjustment_weight": 1.0},
                    # first of one = middle of other (e.g. ARC "z" vs OAX middle "z" in "herb z")
                    {"sql_condition": (
                        "(first_name_l = middle_name_r AND middle_name_r IS NOT NULL)"
                        " OR (middle_name_l = first_name_r AND middle_name_l IS NOT NULL)"),
                     "label_for_charts": "First/middle cross"},
                    # S.X. = S.X.
                    {"sql_condition": (
                        "first_initial_l = first_initial_r"
                        " AND middle_initial_l IS NOT NULL AND middle_initial_r IS NOT NULL"
                        " AND middle_initial_l = middle_initial_r"),
                     "label_for_charts": "Both initials"},
                    # S. = S.
                    {"sql_condition": "first_initial_l = first_initial_r",
                     "label_for_charts": "First initial"},
                    # Set overlap (2026-08-25): pairs reaching Splink only via the
                    # first_names_multichar blocking rule (see blocking_rules_to_generate_predictions)
                    # need to score as real evidence here, same reasoning as family_name_main's own
                    # set-overlap level -- otherwise the blocking rule generates the pair but this
                    # comparison scores it as a mismatch, defeating the fix. Placed after the exact
                    # first_initial level, before the mismatch levels below.
                    {"sql_condition": "list_has_any(first_names_multichar_l, first_names_multichar_r)",
                     "label_for_charts": "Set overlap (shared given-name spelling variant)",
                     "m_probability": 0.5, "u_probability": 0.02},
                    # two full names present but they disagree
                    {"sql_condition": (
                        "length(first_name_l) > 1 AND length(first_name_r) > 1"
                        " AND first_name_l != first_name_r"),
                     "label_for_charts": "Full name mismatch",
                     "m_probability": 0.02},
                    {"sql_condition": "ELSE", "label_for_charts": "All other"},
                ],
            ),
            cl.CustomComparison(
                output_column_name="family_name_main",
                comparison_levels=[
                    cll.NullLevel("family_name_main"),
                    cll.ExactMatchLevel("family_name_main").configure(
                        tf_adjustment_column="family_name_main",
                        tf_adjustment_weight=1.0,
                    ),
                    # A pair reaching Splink only via the new set-overlap blocking rule above
                    # (family_name_main itself disagrees, e.g. 'muhlhaus' vs 'muehlhaus') still
                    # needs to score as real evidence, not fall to ElseLevel's near-zero default
                    # -- otherwise the new blocking rule generates the candidate pair but the
                    # comparison step kills it anyway, defeating the fix. m_probability set
                    # explicitly (not EM-trained -- the ORCID-blocked training session has too few
                    # examples of this specific pattern to estimate it reliably): genuine
                    # corroborating evidence given first_initial already matched via blocking, but
                    # weaker than an exact match on the trusted primary spelling.
                    cll.CustomLevel(
                        "list_has_any(family_names_l, family_names_r)",
                        label_for_charts="Set overlap (shared spelling variant)",
                    ).configure(m_probability=0.5, u_probability=0.02),
                    cll.ElseLevel(),
                ],
            ),
            # Given/family name-order swap (2026-08-25): a standalone comparison, not folded into
            # given_name/family_name_main above, because a swapped pair scores as a MISMATCH on
            # both of those (each compares l.X to r.X on the same field; a swap means l.first_name
            # matches r.family_name_main instead) -- exactly the same "blocking rule generates the
            # pair, comparison scores it as a mismatch, fix defeated" failure the set-overlap levels
            # above exist to avoid, just crossing a field boundary this time. Contributes its own
            # independent evidence per Splink's Fellegi-Sunter design rather than trying to make one
            # of the existing per-field comparisons recognise a cross-field match, which would
            # conflate two different kinds of agreement within one field's TF-adjusted scoring.
            # m/u fixed explicitly, not EM-trained -- too rare a pattern for the ORCID-blocked EM
            # session to estimate reliably (same rationale as the set-overlap levels above); u set
            # very low since two unrelated people's names coincidentally swapping this way by pure
            # chance should be rare.
            cl.CustomComparison(
                output_column_name="name_order_swap",
                comparison_description="Given/family name order inverted (e.g. Kotagiri Ramamohanarao vs Ramamohanarao Kotagiri)",
                comparison_levels=[
                    {"sql_condition": (
                        "first_name_l IS NOT NULL AND family_name_main_l IS NOT NULL"
                        " AND first_name_r IS NOT NULL AND family_name_main_r IS NOT NULL"
                        " AND first_name_l = family_name_main_r AND family_name_main_l = first_name_r"),
                     "label_for_charts": "Given/family swapped",
                     "m_probability": 0.9, "u_probability": 0.0005},
                    {"sql_condition": "ELSE", "label_for_charts": "No swap"},
                ],
            ),
            cl.CustomComparison(
                output_column_name="full_name_key",
                comparison_levels=[
                    {"sql_condition": "full_name_key_l IS NULL OR full_name_key_r IS NULL",
                     "label_for_charts": "null", "is_null_level": True},
                    {"sql_condition": "full_name_key_l = full_name_key_r",
                     "label_for_charts": "Exact match",
                     "tf_adjustment_column": "full_name_key",
                     "tf_adjustment_weight": 1.0,
                     "u_probability": 4.25e-06},
                    {"sql_condition": "ELSE", "label_for_charts": "All other"},
                ],
            ),
            cl.ExactMatch("orcid").configure(
                m_probabilities=[0.85, 0.15],
                u_probabilities=[0.0001, 0.9999],
            ),
            cl.ArrayIntersectAtSizes("inst_arr", [2, 1]),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker([df_arc, df_oax], settings, db_api=db_api)

    print("  Registering name frequency tables...")
    for fname, col, old_col in [
        ("oax_tf_family_name.parquet", "family_name_main", None),
        ("oax_tf_first_name.parquet",  "first_name",       "first_name_canonical"),
        ("oax_tf_full_name.parquet",   "full_name_key",    None),
    ]:
        tf = pd.read_parquet(PROCESSED_DATA / fname)
        if old_col:
            tf = tf.rename(columns={old_col: col, f"tf_{old_col}": f"tf_{col}"})
        linker.table_management.register_term_frequency_lookup(tf, col)

    print("[2/4] Training...")
    # seed=42: pins the one real source of run-to-run nondeterminism in this pipeline -- see
    # the matching comment in awards_cif.py's cluster_items() for the incident that motivated it.
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000, seed=42)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("family_name_main")], recall=0.8,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.orcid = r.orcid AND l.orcid IS NOT NULL",
        fix_u_probabilities=True,
    )

    print("[3/4] Predicting...")
    df_pred = linker.inference.predict(threshold_match_probability=PREDICT_THRESHOLD)
    links = (
        df_pred.as_pandas_dataframe()
        [["unique_id_l", "unique_id_r", "match_probability"]]
        .rename(columns={"unique_id_l": "arc_id", "unique_id_r": "oax_id"})
        .sort_values(["arc_id", "match_probability"], ascending=[True, False])
    )
    links["high_confidence"] = links["match_probability"] >= LINK_THRESHOLD

    # Force-add ORCID-exact-match pairs missed by predict (corrupted OAX name fields
    # can push score below threshold despite identical ORCID).
    orcid_pairs = (
        df_arc[df_arc["orcid"].notna()][["unique_id", "orcid"]]
        .merge(
            df_oax[df_oax["orcid"].notna()][["unique_id", "orcid"]],
            on="orcid",
            suffixes=("_arc", "_oax"),
        )
        .rename(columns={"unique_id_arc": "arc_id", "unique_id_oax": "oax_id"})
        [["arc_id", "oax_id"]]
    )
    existing = set(zip(links["arc_id"], links["oax_id"]))
    forced = orcid_pairs[
        ~orcid_pairs.apply(lambda r: (r["arc_id"], r["oax_id"]) in existing, axis=1)
    ].copy()
    if len(forced):
        forced["match_probability"] = 1.0
        forced["high_confidence"] = True
        print(f"  Forced {len(forced)} ORCID-exact pairs missed by predict.")
        links = pd.concat([links, forced], ignore_index=True)

    links.to_parquet(out_path, index=False)

    print("[4/4] Summary...")
    hc = links[links["high_confidence"]]
    per_arc = hc.groupby("arc_id").size()
    n_arc = len(df_arc)
    print(f"  ARC persons with ≥1 high-confidence OAX link: {hc['arc_id'].nunique()} / {n_arc} "
          f"({100*hc['arc_id'].nunique()/n_arc:.1f}%)")
    print(f"  Total candidate pairs (≥{PREDICT_THRESHOLD}): {len(links)}")
    print(f"  High-confidence pairs (≥{LINK_THRESHOLD}): {len(hc)}")
    print(f"  ARC persons with exactly 1 HC match: {(per_arc == 1).sum()}")
    print(f"  ARC persons with 2+ HC matches:      {(per_arc > 1).sum()}")
    print(f"  Saved → {out_path}")


if __name__ == "__main__":
    main()
