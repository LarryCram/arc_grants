"""
src/00c_prepare_oax.py

Loader: prepares OpenAlex HEP-context authors for Splink linkage. Rebuild only when stale
relative to authorships_hep.parquet/works_hep.parquet -- a few times a year, not every run.

Phase 1 – author_hep: group authorships_hep by author, aggregating institution
    IDs (from HEP authorships), field distribution with fractions (from HEP
    works), and name/ORCID/topic data (from authors entity).
    → author_hep.parquet

Phase 2 – Splink prep: parse names via oax_name_arrays UDF.
    → openalex_authors_prep.parquet

Phase 3 – TF tables for Splink term-frequency adjustment.
    → oax_tf_family_name.parquet
    → oax_tf_first_name.parquet
    → oax_tf_full_name.parquet
"""

import sys
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROCESSED_DATA
from src.utils.names import max_by_len, name_part_tokens, parse_given, strip_postnominals
from src.utils.name_diacritic_variants import (
    DIACRITIC_CHARS, canonicalize_name_punctuation, expand_diacritic_variants,
    strip_diacriticals,
)

PROC = PROCESSED_DATA
_DATA_PERSISTED = Path(__file__).resolve().parents[1] / "data_persisted"


def _oax_authors_newest_mtime() -> float:
    """Newest mtime among OAX_AUTHORS' own parquet files -- the freshness signal for the raw
    snapshot itself (changes only on an OpenAlex snapshot migration, a few times a year), distinct
    from author_hep.parquet/openalex_authors_prep.parquet's own derived-output mtimes."""
    return max(p.stat().st_mtime for p in Path(OAX_AUTHORS).glob("*.parquet"))


_DIACRITIC_SCAN_MARKER = _DATA_PERSISTED / "diacritic_scan_marker.txt"


def scan_for_new_diacritics(force: bool = False) -> set[str]:
    """Scan OAX_AUTHORS' full raw dimension table (119M rows, not the HEP-filtered subset --
    a new script could show up anywhere, not just in the HEP-context population) for any LATIN-
    script letter that would be SILENTLY DROPPED by diacritic_variants()'s generic fallback --
    i.e. one with no NFD canonical decomposition (unicodedata.decomposition(ch) == "") and not
    already in DIACRITIC_CHARS. Three false-positive classes were found and excluded on real
    runs, not assumed: (1) the overwhelming majority of accented letters (é, ñ, ā, ç, ...) already
    decompose cleanly to a bare ASCII letter via diacritic_variants()'s own NFD-normalise step
    and need no special-casing at all -- an earlier, broader version of this function flagged
    15,575 of these as "new" on one real run; (2) a logographic/syllabic character (Hangul, CJK
    ideographs, ...) also has no NFD decomposition, for an entirely unrelated reason (it isn't a
    base letter + accent mark at all) -- found flooding a real run too (thousands of Hangul
    syllables and rare CJK variants), excluded via unicodedata.name(ch, "").startswith("LATIN");
    (3) IPA/phonetic-transcription symbols (U+0250-02AF, U+1D00-1DBF) are named "LATIN ..." by
    Unicode (built on Latin letter shapes) but represent phonetic sounds, never used in an actual
    person's name -- 83 of an initial 188 real candidates, excluded by codepoint range. This
    module only ever folds Latin-alphabet name systems (European languages, Turkish, Vietnamese,
    etc); a genuinely new script needing that kind of folding would need its own, separate
    mechanism, not an extension of this one. A monitor for OpenAlex growing into a Latin-script
    diacritic this module doesn't yet know how to fold, not a name-matching mechanism.

    Explicitly NOT what this function checks (2026-08-26, raised directly and checked, not
    assumed -- an earlier draft of this docstring wrongly asserted non-Latin-original names
    always arrive already transliterated to ASCII; checked directly against OAX_AUTHORS and
    found that's false): OpenAlex's display_name field carries millions of names in their
    original, non-Latin script -- 3.78M containing a literal Cyrillic character, 1.2M Arabic,
    202K Greek, 2,160 Hebrew. None of those are Latin-script diacritics, so this scan correctly
    has nothing to say about them (there's no "bare ASCII fallback" convention for a Cyrillic
    letter the way ü->u is one for German) -- but whether this pipeline's name-parsing/matching
    machinery (built entirely around Latin-alphabet processing: name_part_tokens()'s `[a-z]+`
    regex, diacritic_variants()'s NFD-ascii-encode step, which would simply DELETE a Cyrillic
    character rather than fold it to anything) does anything sane at all for these records, or
    silently mangles them, is a real, separate, unverified question this scan does not answer
    and was never designed to.

    Gated on OAX_AUTHORS' own snapshot mtime via a small persisted marker file -- cheap no-op
    unless the snapshot has actually changed (an OpenAlex migration, a few times a year), same
    freshness pattern as ensure_fresh(). Returns the set of newly-found characters (empty if
    none, or if skipped as still fresh) and prints a warning for a human to review -- deliberately
    does NOT auto-extend any table itself: which bare/digraph convention (if any) a newly-found
    script's diacritic actually uses is a judgment call each time (this is exactly how Turkish
    ı/İ, Scandinavian å/ø, and German ü/ö/ä/ß were each found and added, one script at a time).

    The actual character extraction/dedup runs inside DuckDB (regexp_extract_all + unnest +
    DISTINCT), not by fetching every matching display_name into Python and iterating character by
    character -- the result set this pulls back is bounded by the size of the Unicode alphabet
    (at most a few thousand rows), not by corpus size, regardless of how many millions of authors
    happen to have a non-ASCII name.

    2026-08-26: replaces the former build_diacritic_table()/ensure_diacritic_table_fresh(), which
    additionally built a cross-name bare<->digraph equivalence table from whatever it found --
    removed as a real, confirmed bug (see name_diacritic_variants.py's module docstring). This
    keeps only the genuinely useful half: discovering what diacritic characters exist in the
    corpus, not linking names across people because they happen to share one."""
    if not force and _DIACRITIC_SCAN_MARKER.exists() and (
        _DIACRITIC_SCAN_MARKER.stat().st_mtime >= _oax_authors_newest_mtime()
    ):
        return set()
    con = duckdb.connect()
    rows = con.execute(
        f"""
        SELECT DISTINCT unnest(regexp_extract_all(display_name, '[^\\x00-\\x7F]', 0)) AS ch
        FROM read_parquet('{OAX_AUTHORS}/*.parquet')
        WHERE display_name IS NOT NULL
          AND regexp_matches(display_name, '[^\\x00-\\x7F]')
        """
    ).fetchall()
    con.close()
    found = {
        ch for (ch,) in rows
        if ch.isalpha()
        and ch not in DIACRITIC_CHARS
        and not unicodedata.decomposition(ch)
        # Restrict to Latin script -- this module only ever folds Latin-alphabet name systems
        # (European languages, Turkish, Vietnamese, etc). A logographic/syllabic character
        # (Hangul, CJK ideographs, ...) also has no NFD decomposition, for an entirely unrelated
        # reason (it isn't a base letter + accent mark at all), and is not a name-folding gap --
        # confirmed a real, live false-positive class on this project's own OAX_AUTHORS table
        # (thousands of Hangul syllables and rare CJK ideograph variants, zero of which are
        # anything this module should ever try to fold).
        and unicodedata.name(ch, "").startswith("LATIN")
        # IPA Extensions (U+0250-02AF) and Phonetic Extensions (U+1D00-1DBF) are named "LATIN
        # ..." by Unicode (built on Latin letter shapes) but are phonetic-transcription symbols,
        # never used in an actual person's name -- confirmed a second real false-positive class
        # on this project's own data (83 of an initial 188 candidates).
        and not (0x0250 <= ord(ch) <= 0x02AF or 0x1D00 <= ord(ch) <= 0x1DBF)
    }
    _DIACRITIC_SCAN_MARKER.write_text(f"scanned, {len(found)} new chars found\n")
    if found:
        print(
            f"  WARNING: {len(found)} letter(s) found in OAX_AUTHORS with no NFD decomposition "
            f"and not covered by DIACRITIC_CHARS: {sorted(found)!r} -- these would be silently "
            "dropped by diacritic_variants()'s generic fallback; review and extend "
            "name_diacritic_variants.py's _NO_DECOMP_FALLBACK/_DIACRITIC_VARIANTS if needed."
        )
    else:
        print("  scan_for_new_diacritics: no new diacritic characters found.")
    return found


def oax_name_arrays(display_name: str, alts: list[str]) -> dict:
    # Use dicts (insertion-ordered) instead of sets so first_names preserves
    # left-to-right token order (first before middle, display_name before alts).
    #
    # family_names_display / family_names_alt (2026-08-18, provenance-flagged, not merged
    # unlabelled): display_name is OpenAlex's own curated canonical spelling -- higher trust.
    # alternatives are every other spelling OpenAlex has seen attributed to this author,
    # genuinely useful (this is how the real Frank Grutzner/Grützner match becomes reachable at
    # all -- see expand_diacritic_variants()'s docstring) but also the field CLAUDE.md already
    # documents as sometimes contaminated with co-author names from OAX disambiguation errors.
    # Keeping the two sources in separate fields (not collapsed into one family_names list, a
    # boolean "flag" column would do the same job less legibly) lets a consumer validate
    # display_name-derived candidates first (e.g. against ORCID) before trusting
    # alternatives-derived ones -- the two are different kinds of evidence, not one.
    first_toks: dict[str, None] = {}
    family_from_display: dict[str, None] = {}
    family_from_alts: dict[str, None] = {}

    def _parse_name(n: str, family_target: dict[str, None]) -> None:
        if not n:
            return
        hn = HumanName(strip_postnominals(canonicalize_name_punctuation(n)))
        if not hn.last and hn.first:
            hn.last = hn.first
        for ft in name_part_tokens(hn.first) + name_part_tokens(hn.middle):
            first_toks[ft] = None
            first_toks[ft[0]] = None
        if hn.last:
            # Both bare (ü→u) and digraph (ü→ue) forms -- real conventions, not one "correct"
            # one; see expand_diacritic_variants()'s docstring for why both are kept.
            for variant in expand_diacritic_variants(hn.last):
                family_target[variant] = None

    _parse_name(display_name, family_from_display)
    # Always parsed now, not fallback-only (2026-08-18, user-directed) -- alternatives are a
    # real, distinct source of evidence, not just an emergency backstop for when display_name
    # fails to parse at all.
    for alt in (alts or []):
        _parse_name(alt, family_from_alts)

    family_toks: dict[str, None] = {**family_from_display, **family_from_alts}
    if not first_toks:
        for fam in family_toks:
            if fam:
                first_toks[fam[0]] = None

    return {
        "first_names": list(first_toks),
        "family_names": list(family_toks),
        "family_names_display": list(family_from_display),
        "family_names_alt": list(family_from_alts),
    }


def ensure_fresh(force: bool = False) -> bool:
    """Rebuild only if openalex_authors_prep.parquet is missing or older than
    authorships_hep.parquet/works_hep.parquet. Returns True if it rebuilt."""
    auth_hep  = PROC / "authorships_hep.parquet"
    works_hep = PROC / "works_hep.parquet"
    out_oax   = PROC / "openalex_authors_prep.parquet"
    if not force and out_oax.exists() and auth_hep.exists() and works_hep.exists():
        if out_oax.stat().st_mtime >= max(auth_hep.stat().st_mtime, works_hep.stat().st_mtime):
            print("  openalex_authors_prep.parquet fresh -- skipping rebuild.")
            return False
    main()
    return True


def main():
    con = duckdb.connect()

    auth_hep  = PROC / "authorships_hep.parquet"
    works_hep = PROC / "works_hep.parquet"
    out_hep   = PROC / "author_hep.parquet"
    out_oax   = PROC / "openalex_authors_prep.parquet"

    # ── Phase 1: Build author_hep ─────────────────────────────────────────────

    print(f"[1/3] Building author_hep ({out_hep})...")
    con.execute(f"""
        COPY (
            WITH
              -- one row per (work, field) — deduplicated so a work with
              -- multiple topics in the same field counts once per field
              work_fields AS (
                SELECT DISTINCT work_idx, field_name
                FROM '{works_hep}'
              ),
              -- field distribution per author across their HEP-context works
              field_counts AS (
                SELECT
                  a.author_idx,
                  count(DISTINCT a.work_idx)               AS works_count,
                  unnest(map_entries(histogram(wf.field_name))) AS entry
                FROM '{auth_hep}' a
                JOIN work_fields wf USING (work_idx)
                GROUP BY a.author_idx
              ),
              field_fracs AS (
                SELECT
                  author_idx,
                  works_count,
                  entry.key                                                             AS field,
                  entry.value                                                           AS field_count,
                  round(entry.value::DOUBLE / SUM(entry.value) OVER
                        (PARTITION BY author_idx), 4)                                  AS field_fraction
                FROM field_counts
              ),
              sorted_fields AS (
                SELECT
                  author_idx,
                  works_count,
                  list({{'field': field, 'count': field_count, 'fraction': field_fraction}}
                       ORDER BY field_fraction DESC) AS sorted_fields
                FROM field_fracs
                GROUP BY author_idx, works_count
              ),
              -- institution IDs per author from HEP authorships → full OAX URLs
              inst_agg AS (
                SELECT
                  author_idx,
                  list_distinct(list_transform(
                    list(institution_idx),
                    x -> 'https://openalex.org/I' || x::VARCHAR
                  )) AS inst_ids
                FROM '{auth_hep}'
                WHERE institution_idx IS NOT NULL
                GROUP BY author_idx
              )
            SELECT
              'https://openalex.org/A' || sf.author_idx::VARCHAR  AS unique_id,
              au.display_name                                       AS full_name,
              au.display_name_alternatives,
              replace(au.orcid, 'https://orcid.org/', '')          AS orcid,
              COALESCE(ia.inst_ids, [])                            AS inst_ids,
              list_transform(au.topics, x -> x.display_name)       AS topic_names,
              list_transform(au.topics, x -> x.subfield.display_name) AS subfield_names,
              sf.works_count,
              sf.sorted_fields
            FROM sorted_fields sf
            LEFT JOIN inst_agg ia USING (author_idx)
            JOIN read_parquet('{OAX_AUTHORS}/*.parquet') au
              ON au.author_idx = sf.author_idx
        ) TO '{out_hep}' (FORMAT PARQUET)
    """)
    n_hep = con.execute(f"SELECT count(*) FROM '{out_hep}'").fetchone()[0]
    print(f"  {n_hep:,} authors → {out_hep}")

    # ── Phase 2: Splink prep (name parsing) ───────────────────────────────────

    print(f"[2/3] Parsing names → {out_oax}...")
    con.create_function("oax_names", oax_name_arrays,
                        ['VARCHAR', 'VARCHAR[]'],
                        'STRUCT(first_names VARCHAR[], family_names VARCHAR[], '
                        'family_names_display VARCHAR[], family_names_alt VARCHAR[])')

    con.execute(f"""
        COPY (
            WITH base AS (
                SELECT
                    unique_id,
                    full_name,
                    oax_names(full_name, display_name_alternatives) AS parsed,
                    orcid,
                    inst_ids,
                    topic_names,
                    subfield_names,
                    works_count,
                    sorted_fields
                FROM '{out_hep}'
            )
            SELECT
                unique_id,
                full_name,
                parsed.first_names                                          AS first_names,
                list_filter(parsed.first_names, x -> len(x) = 1)           AS first_initials,
                list_filter(parsed.first_names, x -> len(x) > 1)           AS first_name_full,
                parsed.family_names                                         AS family_names,
                parsed.family_names_display                                 AS family_names_display,
                parsed.family_names_alt                                     AS family_names_alt,
                orcid,
                inst_ids,
                topic_names,
                subfield_names,
                works_count,
                sorted_fields
            FROM base
        ) TO '{out_oax}' (FORMAT PARQUET)
    """)

    # ── Phase 3: TF tables ────────────────────────────────────────────────────

    print("[3/3] Building term-frequency tables...")
    df = pd.read_parquet(out_oax)
    n = len(df)

    def _canonical(row):
        full = row["first_name_full"]
        inits = row["first_initials"]
        if full is not None and len(full) > 0:
            return max(full, key=len)
        if inits is not None and len(inits) > 0:
            return inits[0]
        return None

    df["family_name_main"]     = df["family_names"].apply(max_by_len)
    df["first_name_canonical"] = df.apply(_canonical, axis=1)
    df["full_name_key"] = (
        (df["first_name_canonical"] + "_" + df["family_name_main"])
        .where(df["first_name_canonical"].notna() & df["family_name_main"].notna())
    )

    # Persist HumanName-parsed given-name components so 03_link_arc_oax.py
    # can read them directly instead of re-parsing 2.5M display_names each run.
    df[["first_name", "middle_name", "first_compound", "first_initial", "middle_initial"]] = (
        pd.DataFrame(df["full_name"].apply(parse_given).tolist(), index=df.index)
    )
    df.to_parquet(out_oax, index=False)

    for col, fname in [
        ("family_name_main",     "oax_tf_family_name.parquet"),
        ("first_name_canonical", "oax_tf_first_name.parquet"),
        ("full_name_key",        "oax_tf_full_name.parquet"),
    ]:
        counts = df[col].dropna().value_counts()
        tf = counts.reset_index()
        tf.columns = [col, f"tf_{col}"]
        tf[f"tf_{col}"] = tf[f"tf_{col}"] / n
        tf.to_parquet(PROCESSED_DATA / fname, index=False)
        print(f"  {fname}: {len(tf):,} unique values")

    # Per-variant TF, not per-collapsed-scalar TF: each author's own family_names list
    # (family_names_display ∪ family_names_alt, deduped) is exploded so every spelling
    # variant gets its own population frequency -- feeds the name-SET Splink comparison's
    # tf_adjustment_column (via a per-record "rarest variant" lookup computed downstream in
    # 01_prepare_arc.py/03_link_arc_oax.py), replacing the old approach of computing rarity
    # only for whichever single variant max_by_len happened to pick.
    exploded = df[["family_names"]].explode("family_names").dropna(subset=["family_names"])
    variant_counts = exploded["family_names"].value_counts()
    tf_variant = variant_counts.reset_index()
    tf_variant.columns = ["family_name_variant", "tf_family_name_variant"]
    tf_variant["tf_family_name_variant"] = tf_variant["tf_family_name_variant"] / n
    tf_variant.to_parquet(PROCESSED_DATA / "oax_tf_family_name_variant.parquet", index=False)
    print(f"  oax_tf_family_name_variant.parquet: {len(tf_variant):,} unique values")

    print("OAX prep complete.")


if __name__ == "__main__":
    main()
