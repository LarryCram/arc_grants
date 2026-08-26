"""
Diacritic/special-character handling for ARC and OpenAlex name strings.

Split out of names.py (2026-08-25) once this subsystem grew beyond simple string normalisation --
a plain "names.py" no longer described what lives here: punctuation canonicalisation, the
no-NFD-decomposition fallback, and the bare/digraph cartesian-product generator.

2026-08-26 removal: this module used to also build/persist/load a corpus-wide bare<->digraph
equivalence table (scanning all 119M OpenAlex authors for any name with a literal diacritic,
then linking every OTHER name sharing that name's bare-folded root). Found to be a real,
confirmed bug: since the lookup key was the bare-folded string, a single unrelated person
anywhere in OpenAlex whose real diacritic name happened to bare-fold to a common surname (e.g.
"Christian Bäker" -> "baker") silently injected a spurious "baeker" spelling into EVERY ARC/OAX
person named "Baker" project-wide -- confirmed concretely for Baker/Wang/Walker/Zhu/Wu/Xu/Xue,
traced to 8 real OpenAlex source names (one independently confirmed real via an external
publication; two others -- Wei Xü, Zhü Weifang -- turned out to already self-resolve via their
own OpenAlex display_name_alternatives, needing no table at all). Investigating why the table
was ever needed found it added nothing beyond what diacritic_variants() already computes
per-name: the table's equivalence classes are themselves built by running the same per-character
substitution on each corpus token, so its only actual effect beyond that was merging different
people's independently-computed roots together -- the bug, not a feature. The one motivating
case ever cited for it (DP0345157_HansMuhlhaus / OpenAlex's "Hans Mühlhaus") was re-verified to
already work via the per-character substitution alone: OpenAlex's own literal "ü" already
generates both "muhlhaus" and "muehlhaus" for that one record, which already overlaps ARC's bare
"muhlhaus" directly -- no cross-corpus table was ever actually load-bearing for it.

What's kept: the per-character substitution (diacritic_variants(), safe and local -- it only
ever expands a name's OWN literal characters into that same name's own plausible spellings, so
it cannot link two different people). ARC data is not "always ASCII" as an earlier version of
this docstring claimed -- confirmed 189 real ARC investigator records carry a genuine diacritic
character -- but that's exactly the case this substitution already handles correctly on its own,
for whichever side (ARC or OpenAlex) happens to carry the real character.

What replaced the table: 00c_prepare_oax.py's scan_for_new_diacritics() -- a much narrower,
freshness-gated monitor that watches for OpenAlex growing into scripts/diacritic characters this
module doesn't yet know how to fold (Turkish, Scandinavian, Finnish, etc. were all discovered
this way originally), without building any cross-name equivalence table from what it finds.

See diacritic_variants()'s own docstring for the core mechanism, and CLAUDE.md's 2026-08-25/26
sections for the full incident history: a real production bug (strip_diacriticals() silently
dropping ß/ø/ł/etc, never fixed at its own source) and a structurally unreachable Splink blocking
pair (DP0345157_HansMuhlhaus) that motivated building diacritic_variants() itself -- both real,
both still fixed by what remains in this module.
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


def diacritic_variants(s: str) -> list[str]:
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
    it, and generates the bare/digraph cartesian product for characters with a real conventional
    digraph spelling (_DIACRITIC_VARIANTS).

    Deliberately local and self-contained: every variant returned is derived purely from `s`'s
    own characters, never from any other name -- a plain ASCII string with no diacritic character
    at all (the overwhelming majority of names, on both the ARC and OpenAlex sides) always
    returns exactly `[s]` unchanged. This is a hard invariant, not an optimisation -- a former
    version of this function also consulted a cross-name corpus-wide equivalence table, which let
    one unrelated person's real diacritic name silently inject a spurious spelling into every
    OTHER person sharing that name's bare-folded root project-wide (found and removed 2026-08-26,
    see this module's own docstring for the full incident -- confirmed to have added nothing that
    per-name expansion didn't already give, once traced through the one case that was ever cited
    to justify it).

    Returns an ORDERED list, shortest-first: [0] is always the most compact form, later entries
    progressively more expanded. Callers needing a single scalar should take [0] -- NOT the old
    "longest wins" convention (max_by_len), which was itself the root cause of a real production
    bug (a genuinely matching pair silently unreachable by Splink blocking because the two sides'
    "longest" picks disagreed; see CLAUDE.md 2026-08-25). Callers needing every plausible
    spelling (e.g. Splink blocking/comparison) should use the full list.
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
    return sorted(variants, key=lambda v: (len(v), v))


def strip_diacriticals(s: str) -> str:
    """Thin wrapper over diacritic_variants() for callers that only need one compact string --
    every existing call site (name_part_tokens, parse_given, norm_alpha, tokens, and direct
    callers in awards_cif.py/00a_analyse_arc.py/00b_enrich_orcid.py) gets the no-more-silent-drop
    fix automatically with no changes needed at the call site itself."""
    variants = diacritic_variants(s)
    return variants[0] if variants else ""


def expand_diacritic_variants(s: str) -> list[str]:
    """Thin wrapper over diacritic_variants() for existing callers of the full-list form."""
    return diacritic_variants(s)
