"""
Diacritic/special-character handling and corpus-grounded bare<->digraph equivalence matching.

Split out of names.py (2026-08-25) once this subsystem grew beyond simple string normalisation --
a plain "names.py" no longer described what lives here: punctuation canonicalisation, the
no-NFD-decomposition fallback, the bare/digraph cartesian-product generator, and the
corpus-grounded equivalence table (build/persist/load) used to widen ARC's always-ASCII data
with real spelling variants confirmed elsewhere in the OpenAlex corpus.

See diacritic_variants()'s own docstring for the core mechanism, and CLAUDE.md's 2026-08-25
sections for the incident history that motivated this module's design: a real production bug
(strip_diacriticals() silently dropping ß/ø/ł/etc, never fixed at its own source) and a
structurally unreachable Splink blocking pair (DP0345157_HansMuhlhaus) found via it.
"""

import itertools
import re
import unicodedata

# Every apostrophe/quote-mark variant seen in real name data -> canonical ASCII apostrophe.
# U+2019 (curly right single quote) is the one that actually matters most in practice: OpenAlex's
# own display_name uses it for O'Neill-style surnames (confirmed directly, 2026-08-19, comparing
# ORCID's own straight-apostrophe family names against OpenAlex's -- ~40 of ~90 real ORCID<->OAX
# name "mismatches" traced to exactly this, not a genuine spelling difference), while ARC/ORCID
# data typically uses the plain ASCII one.
_QUOTE_VARIANTS = re.compile(r"[‘’ʼ`´ʹ′]")

# Exotic Unicode hyphens → ASCII hyphen
_EXOTIC_HYPHENS = re.compile(r"[­‐‑‒–—―−－]")


def canonicalize_name_punctuation(s: str) -> str:
    """Preprocessor: collapse every apostrophe/quote-mark variant to a canonical ASCII
    apostrophe and every hyphen/dash variant to a canonical ASCII hyphen. Must run BEFORE
    HumanName() parses the string, not just afterward on already-extracted parts -- HumanName's
    own splitting decisions (what counts as one compound token, where a name breaks) depend on
    the punctuation actually being uniform, so canonicalizing only the output is too late to
    help. Call at every HumanName(...) call site on the raw input string; diacritic_variants()
    also applies it internally so anything processed downstream gets it too, even from a caller
    that forgot to canonicalize up front."""
    if not s:
        return s
    s = _QUOTE_VARIANTS.sub("'", s)
    s = _EXOTIC_HYPHENS.sub("-", s)
    return s


# Every Latin letter confirmed (via unicodedata.decomposition(ch) == "") to have NO NFD canonical
# decomposition -- plain NFD-based stripping silently DROPS these rather than folding them to a
# sensible bare letter ("Włodkowic" -> "Wodkowic", not "Wlodkowic"). ı/İ handled separately below
# (İ does decompose correctly on its own via NFD, but is folded explicitly anyway, before ANY
# lowering, because Python's str.lower() turns İ into "i" + a combining dot U+0307, not plain
# ASCII "i" -- confirmed directly; the explicit fold must happen first). Lowercase-only keys --
# diacritic_variants() lowercases before this table is consulted (see its own docstring).
# Deliberately EXCLUDES ß/ø despite them also having no NFD decomposition -- they belong
# exclusively to _DIACRITIC_VARIANTS below (which generates their bare fallback AS PART OF its
# bare/digraph pair); folding ß->s here first would remove the literal ß before the cartesian-
# product step ever saw it, silently losing the digraph "ss" alternative -- a real ordering bug
# caught by testing ("Straße" produced only "strase", never "strasse") before it shipped.
_NO_DECOMP_FALLBACK = {
    "ł": "l",
    "œ": "oe",
    "æ": "ae",
    "ð": "d",
    "þ": "th",
}

# German umlauts + eszett, Scandinavian stroke-o and overring-a -- each has two real,
# conventional ASCII spellings, not one, used interchangeably across ARC/OpenAlex data depending
# on which system captured the name -- confirmed on a real case (DP0343064_FrankGruetzner /
# OpenAlex A5035572752 "Frank Grützner"): ARC's own record uses the digraph "Gruetzner",
# OpenAlex's display_name uses the umlaut; a bare-only fold matches neither, and Splink's
# blocking never paired them (2026-08-18). Lowercase-only keys -- this table is only ever
# consulted after diacritic_variants() has already lowered the whole string (this codebase's
# 2026-08-25 convention: all string-matching code operates on lowercase; .title() is used only
# to restore case for human-readable display output, never mixed into matching logic).
_DIACRITIC_VARIANTS = {
    "ü": ("u", "ue"), "ö": ("o", "oe"), "ä": ("a", "ae"),
    "ß": ("s", "ss"),
    "ø": ("o", "oe"), "å": ("a", "aa"),
}

# Every character diacritic_variants() treats specially, both cases -- exported so callers can
# cheaply pre-filter a large corpus (e.g. a SQL WHERE clause over millions of rows) down to only
# the names that could possibly need this module's attention, before doing anything per-row in
# Python. ı/İ included even though they're handled inline in diacritic_variants() rather than via
# either dict above. str.upper() is filtered to single characters only -- "ß".upper() == "SS"
# (Python's default, not the Unicode capital eszett ẞ), which would otherwise leak a 2-character
# string into what must stay a set of single characters (this is used to build a SQL regex
# character class, where a multi-character entry is not "one more character to match", it's
# either a silent no-op or a syntax error depending on the regex engine -- confirmed worth
# guarding against directly rather than assuming).
DIACRITIC_CHARS = frozenset(
    c for c in (
        set(_NO_DECOMP_FALLBACK) | {k.upper() for k in _NO_DECOMP_FALLBACK}
        | set(_DIACRITIC_VARIANTS) | {k.upper() for k in _DIACRITIC_VARIANTS}
        | {"ı", "İ", "ẞ"}
    )
    if len(c) == 1
)


def diacritic_variants(s: str, table: dict[str, list[str]] | None = None) -> list[str]:
    """The single entry point for all diacritic/special-character handling in this codebase --
    consolidated 2026-08-25 (was split across strip_diacriticals()/expand_diacritic_variants(),
    which let a real bug -- strip_diacriticals() itself silently dropping ß/ø/ł/etc, never fixed
    at its own source, only in the separate expand_diacritic_variants() -- go undetected in
    production for weeks). Normalises punctuation, lowercases (this codebase's convention: all
    string-matching/normalisation code operates on lowercase; .title() is used only to restore
    case for human-readable display output, never mixed into matching-key logic -- this also
    means input casing never matters, whether Title Case, ALL CAPS, or already lowercase, which
    real ARC and OAX name data both use inconsistently), folds Turkish ı/İ and every no-NFD-
    decomposition letter (_NO_DECOMP_FALLBACK) to a bare fallback instead of silently dropping
    it, generates the bare/digraph cartesian product for characters with a real conventional
    digraph spelling (_DIACRITIC_VARIANTS), and -- if a corpus-grounded `table` is supplied
    (see build_diacritic_variant_table()) -- widens each resulting variant with its confirmed
    real-world counterpart(s).

    Returns an ORDERED list, shortest-first: [0] is always the most compact form, later entries
    progressively more expanded. Callers needing a single scalar should take [0] -- NOT the old
    "longest wins" convention (max_by_len), which was itself the root cause of a real production
    bug (a genuinely matching pair silently unreachable by Splink blocking because the two sides'
    "longest" picks disagreed; see CLAUDE.md 2026-08-25). Callers needing every plausible
    spelling (e.g. Splink blocking/comparison) should use the full list. A string with no special
    characters and no table hit returns a single-element list (itself, normalised) -- always at
    least as much as the old strip_diacriticals() alone would give, never less.
    """
    if not s:
        return []
    s = canonicalize_name_punctuation(s)
    s = s.replace("ı", "i").replace("İ", "I")  # before lowering -- İ.lower() != "i", see docstring
    s = s.lower()
    for ch, fallback in _NO_DECOMP_FALLBACK.items():
        s = s.replace(ch, fallback)
    choices = [_DIACRITIC_VARIANTS.get(ch, (ch,)) for ch in s]
    raw_variants = {"".join(combo) for combo in itertools.product(*choices)}
    variants = {
        unicodedata.normalize("NFD", v).encode("ascii", "ignore").decode("ascii").strip()
        for v in raw_variants
    } - {""}
    if table:
        for v in list(variants):
            variants.update(table.get(v, []))
    return sorted(variants, key=lambda v: (len(v), v))


def strip_diacriticals(s: str) -> str:
    """Thin wrapper over diacritic_variants() for callers that only need one compact string --
    every existing call site (name_part_tokens, parse_given, norm_alpha, tokens, and direct
    callers in awards_cif.py/00a_analyse_arc.py/00b_enrich_orcid.py) gets the no-more-silent-drop
    fix automatically with no changes needed at the call site itself."""
    variants = diacritic_variants(s)
    return variants[0] if variants else ""


def expand_diacritic_variants(s: str, table: dict[str, list[str]] | None = None) -> list[str]:
    """Thin wrapper over diacritic_variants() for existing callers of the full-list form."""
    return diacritic_variants(s, table)


# Persisted filename for the corpus-grounded diacritic equivalence table (see
# build_diacritic_variant_table() below). Lives in data_persisted/, not PROCESSED_DATA -- this
# is a precursor input other pipeline stages read from, the same category as
# for_concordance.csv/admin_orgs.csv, not a derived pipeline output like oax_tf_*.parquet
# (2026-08-25 direction). One row per (variant, counterpart) pair, not one row per variant with
# a list column -- matches this project's existing data_persisted/*.csv convention (git-diffable,
# human-reviewable), the same shape as manual_merges.csv etc.
DIACRITIC_VARIANT_TABLE_FILENAME = "name_diacritic_variants.csv"

_NAME_TOKEN_SPLIT = re.compile(r"[\s\-]+")


def build_diacritic_variant_table(name_strings) -> dict[str, list[str]]:
    """Scan a corpus of raw (pre-normalisation) name strings -- e.g. every OAX display_name AND
    every display_name_alternatives entry, family and given names alike, not pre-split by role --
    for tokens containing a literal umlaut/eszett/stroke/overring character, and build a
    bare<->digraph equivalence table for those CONFIRMED roots only.

    Returns variant -> sorted list of every OTHER confirmed variant for that root (a full
    equivalence class, not just one counterpart -- a token with 2+ diacritic characters, or
    multiple independently-observed real spellings of the same root, can have more than 2
    members). A token with no diacritic-bearing form anywhere in the corpus (e.g. "Fuentes")
    never enters the table at all, so its "ue" substring is never folded -- this is the safe,
    data-grounded alternative to a blind substring-fold, which was tried and rejected (confirmed
    to mangle ordinary non-German names -- "Fuentes" -> "funtes", "Guerrero" -> "gurrero").

    Tokenises on whitespace/hyphens only (not full HumanName parsing -- this only needs to
    isolate the diacritic-bearing token itself, not classify it as first/middle/last, since the
    resulting table is applied identically to any name role -- given or family -- via
    diacritic_variants()'s own `table` parameter).
    """
    groups: dict[str, set[str]] = {}
    for name in name_strings:
        if not name:
            continue
        for raw_token in _NAME_TOKEN_SPLIT.split(name):
            token = raw_token.strip(".,'\"")
            if not token or not any(ch in _DIACRITIC_VARIANTS for ch in token.lower()):
                continue
            variants = diacritic_variants(token)
            if len(variants) < 2:
                continue
            root = variants[0]
            groups.setdefault(root, set()).update(variants)

    table: dict[str, list[str]] = {}
    for members in groups.values():
        for v in members:
            others = sorted(members - {v})
            table[v] = sorted(set(table.get(v, [])) | set(others))
    return table


def persist_diacritic_variant_table(table: dict[str, list[str]], path) -> None:
    import csv as _csv
    rows = sorted(
        (variant, counterpart)
        for variant, counterparts in table.items()
        for counterpart in counterparts
    )
    with open(path, "w", newline="") as f:
        writer = _csv.writer(f)
        writer.writerow(["variant", "counterpart"])
        writer.writerows(rows)


def load_diacritic_variant_table(path) -> dict[str, list[str]]:
    import csv as _csv
    from pathlib import Path as _Path
    table: dict[str, list[str]] = {}
    if not _Path(path).exists():
        return table
    with open(path, newline="") as f:
        for row in _csv.DictReader(f):
            table.setdefault(row["variant"], []).append(row["counterpart"])
    return table
