"""
Shared name-normalisation helpers used across all pipeline layers.
"""

import itertools
import re
import unicodedata

from nameparser import HumanName
from nameparser.config import CONSTANTS

# Register Australian post-nominals so HumanName parses them as suffixes,
# not as family name tokens (e.g. "Summerhayes OAM" → last="Summerhayes").
for _pn in ("AC", "AO", "AM", "OAM"):
    CONSTANTS.suffix_acronyms.add(_pn)

# Exotic Unicode hyphens → ASCII hyphen
_EXOTIC_HYPHENS = re.compile(r"[­‐‑‒–—―−－]")

# Every apostrophe/quote-mark variant seen in real name data -> canonical ASCII apostrophe.
# U+2019 (curly right single quote) is the one that actually matters most in practice: OpenAlex's
# own display_name uses it for O'Neill-style surnames (confirmed directly, 2026-08-19, comparing
# ORCID's own straight-apostrophe family names against OpenAlex's -- ~40 of ~90 real ORCID<->OAX
# name "mismatches" traced to exactly this, not a genuine spelling difference), while ARC/ORCID
# data typically uses the plain ASCII one. strip_diacriticals() used to just drop it silently
# (non-ASCII, no NFD diacritic decomposition -- same failure class as ø/ł/œ), which meant an
# apostrophe-bearing surname could carry TWO different normalized forms depending purely on which
# apostrophe character the source system happened to use.
_QUOTE_VARIANTS = re.compile(r"[‘’ʼ`´ʹ′]")


def canonicalize_name_punctuation(s: str) -> str:
    """Preprocessor: collapse every apostrophe/quote-mark variant to a canonical ASCII
    apostrophe and every hyphen/dash variant to a canonical ASCII hyphen. Must run BEFORE
    HumanName() parses the string, not just afterward on already-extracted parts -- HumanName's
    own splitting decisions (what counts as one compound token, where a name breaks) depend on
    the punctuation actually being uniform, so canonicalizing only the output (as
    strip_diacriticals() alone used to do) is too late to help. Call at every HumanName(...)
    call site on the raw input string; strip_diacriticals() also applies it internally so
    anything processed downstream gets it too, even from a caller that forgot to canonicalize
    up front."""
    if not s:
        return s
    s = _QUOTE_VARIANTS.sub("'", s)
    s = _EXOTIC_HYPHENS.sub("-", s)
    return s

# Trailing Australian / British post-nominal awards (handles stacking: "AO FAA" or "FRS, FREng").
# OL (Officer of the Order of Logohu, PNG) and FREng (Fellow of the Royal Academy of Engineering)
# added 2026-08-20 -- found via a same-grant/same-ORCID duplicate-name screen: "Glenn Summerhayes
# OL OAM" was leaking "ol" into the parsed family name (Summerhayes is a PNG-focused archaeologist,
# explaining the honour), and "Anthony Kinloch FRS, FREng" wasn't stripped at all because (a)
# FREng wasn't in the list and (b) the comma before it didn't match the old whitespace-only
# separator. Separator widened from `\s+` to `[\s,]+` to handle comma-separated stacking too.
_POSTNOMINALS = re.compile(
    r"(?:[\s,]+(?:AO|AM|OAM|AC|AK|OL|FAA|FAHMS|FTSE|FASSA|FAHA|FRS|FREng|CBE|OBE|MBE|KBE|DBE))+\s*$",
    re.IGNORECASE,
)


def strip_postnominals(name: str) -> str:
    """Remove trailing post-nominal awards from a name string.

    Handles stacked awards: "Raston AO FAA" → "Raston", "Kinloch FRS, FREng" → "Kinloch".

    Must run in sequence with canonicalize_name_punctuation(), BEFORE HumanName() parses the
    string -- CONSTANTS.suffix_acronyms (registered above) only covers AC/AO/AM/OAM, so anything
    else (FAA, FAHMS, FTSE, FRS, ...) left in place gets parsed as part of the family name itself
    (confirmed 2026-08-19: "Anthony Thomas AC FAA" -> family="faa", 660-work ORCID<->OAX mismatch).
    Stripping first avoids depending on HumanName's own suffix detection at all, regardless of
    what's registered in suffix_acronyms.
    """
    return _POSTNOMINALS.sub("", name).strip()


def strip_diacriticals(s: str) -> str:
    """Normalise exotic hyphens/apostrophes to ASCII, Turkish dotless-ı → i, strip diacriticals."""
    s = canonicalize_name_punctuation(s)
    s = s.replace("ı", "i").replace("İ", "I")
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


# German umlauts + eszett, Scandinavian stroke-o and overring-a -- each has two real,
# conventional ASCII spellings, not one. strip_diacriticals() only ever produces the bare form
# (ü→u, ä→a) via NFD decomposition, and silently DROPS ø/ß entirely (confirmed directly: ø/Ø/ß
# have no NFD canonical decomposition, so unicodedata.normalize("NFD", ...).encode("ascii",
# "ignore") just discards them rather than substituting -- "Sørensen" -> "Srensen", a real,
# separate bug this table also happens to fix as a side effect). The digraph form (ü→ue, ø→oe,
# å→aa) is the other real convention (German/Scandinavian romanization), used interchangeably
# with the bare form across ARC/OpenAlex data depending on which system captured the name --
# confirmed on a real case (DP0343064_FrankGruetzner / OpenAlex A5035572752 "Frank Grützner"):
# ARC's own record uses the digraph "Gruetzner", OpenAlex's display_name uses the umlaut, and
# strip_diacriticals()'s bare-only output ("Grutzner") matched neither -- Splink's blocking never
# even paired them (2026-08-18).
_DIACRITIC_VARIANTS = {
    "ü": ("u", "ue"), "ö": ("o", "oe"), "ä": ("a", "ae"),
    "ß": ("s", "ss"),
    "ø": ("o", "oe"), "å": ("a", "aa"),
    # Polish ł (U+0142) has no NFD canonical decomposition either -- same drop-to-nothing failure
    # as ø/ß (strip_diacriticals() alone turns "Włodkowic" into "wodkowic", missing the L
    # entirely). No real digraph convention exists for it, unlike ü/ö/ä/ø/å -- just the bare "l".
    "ł": ("l",),
}


def expand_diacritic_variants(s: str) -> list[str]:
    """Every combination of bare-vs-digraph substitution for each umlaut/eszett/stroke/overring
    character in s, each then passed through strip_diacriticals().lower() for any other,
    ordinary diacritics. A name with no such characters returns a single-element list (itself,
    normalised) -- always at least as much as strip_diacriticals() alone would give, never less.
    Multiple special characters in one name produce the full cartesian product (e.g. a
    hypothetical "Müller-Søren" -> 4 variants), though real surnames rarely carry more than one.
    """
    if not s:
        return []
    lowered = s.lower()
    choices = [_DIACRITIC_VARIANTS.get(ch, (ch,)) for ch in lowered]
    raw_variants = {"".join(combo) for combo in itertools.product(*choices)}
    return sorted({strip_diacriticals(v).lower().strip() for v in raw_variants} - {""})


def strip_parens(s: str) -> str:
    """Remove parenthetical expressions: 'Murphy (née Paton-Walsh)' → 'Murphy'."""
    return re.sub(r"\s*\(.*?\)", "", s).strip()


def norm_alpha(s: str) -> str:
    """All-alpha lowercase key: strips diacriticals, parens, non-alpha."""
    return re.sub(r"[^a-z]", "", strip_diacriticals(strip_parens(s)).lower())


def tokens(s: str) -> list[str]:
    """Lowercase alpha tokens after diacritic stripping."""
    return re.findall(r"[a-z]+", strip_diacriticals(s).lower())


# Apostrophe characters that should be collapsed rather than treated as
# word-splitting punctuation. The standard ASCII apostrophe (U+0027), the
# Unicode modifier-letter apostrophe (U+02BC), and the grave accent (U+0060)
# all appear in name data and would otherwise split "O'Brien" into ["o", "brien"].
# Curly quote variants included too (2026-08-19) -- defense in depth for a caller that reaches
# name_part_tokens() without going through canonicalize_name_punctuation()/strip_diacriticals()
# first; name_part_tokens() already runs strip_diacriticals() internally below, so in practice
# this second list rarely needs to catch anything on its own, but keeping it in sync avoids a
# silent gap if that internal call is ever refactored away.
_APOSTROPHES = re.compile(r"['ʼ`‘’´ʹ′]")


_FOR_STOPWORDS = frozenset({
    "a", "an", "and", "at", "excl", "for", "in", "incl", "of", "other",
    "the", "to",
})


def for_name_tokens(name: str) -> list[str]:
    """Tokenise a FOR field-of-research name into content words."""
    if not name:
        return []
    return [
        t for t in re.findall(r"[a-z]+", name.lower())
        if t not in _FOR_STOPWORDS and len(t) > 1
    ]


def make_expanded_for_tokens(concordance_csv: str):
    """
    Return a closure that expands FOR tokens using a concordance.

    Each name's token set is unioned with its canonical form's tokens so that
    J>=0.5 near-synonym pairs share tokens (enabling Level 1 match) while
    completely different fields share nothing (enabling anti-match at Level 0).
    The concordance is keyed in both directions so aliases and canonicals
    both gain the union token set.
    """
    import csv as _csv
    # Build alias → canonical and canonical → [aliases] maps
    alias_to_canonical: dict[str, str] = {}
    canonical_to_aliases: dict[str, list[str]] = {}
    with open(concordance_csv, newline="") as f:
        for row in _csv.DictReader(f):
            c, a = row["canonical"], row["alias"]
            alias_to_canonical[a] = c
            canonical_to_aliases.setdefault(c, []).append(a)

    def _expanded(name: str) -> list[str]:
        if not name:
            return []
        own = set(for_name_tokens(name))
        # If this name is an alias, add canonical tokens
        if name in alias_to_canonical:
            own |= set(for_name_tokens(alias_to_canonical[name]))
        # If this name is a canonical, add all alias tokens
        for alias in canonical_to_aliases.get(name, []):
            own |= set(for_name_tokens(alias))
        return sorted(own)

    return _expanded


def max_by_len(lst) -> str | None:
    """Return the longest string in a list, or None if empty/null."""
    if lst is None or len(lst) == 0:
        return None
    return max(lst, key=len)


def parse_given(name_str: str) -> tuple:
    """HumanName → (first, middle, compound, f_init, m_init), all lowercased.

    compound = first + " " + middle when both present, else first.
    Used to build Splink comparison columns for given-name matching.
    """
    if not name_str:
        return None, None, None, None, None
    hn = HumanName(strip_postnominals(canonicalize_name_punctuation(name_str)))
    f  = strip_diacriticals(hn.first).lower()  or None
    m  = strip_diacriticals(hn.middle).lower() or None
    fc = (f + " " + m) if (f and m) else f
    return f, m, fc, f[0] if f else None, m[0] if m else None


_VOWELS = frozenset("aeiouy")  # "y" included deliberately -- checked directly against ARC's own
# raw first_name data (2026-08-19) before shipping this: without "y", real, moderately-common
# Western names using y-as-vowel (Lyn 54 occurrences, Rhys 42, Lynn 38, Kym 11, Gwyn 11, Glyn 4,
# Bryn 2 -- 171 real people total) would have been wrongly shredded into individual letters
# ("lyn" -> ["l","y","n"]). The genuine bare-initials cluster in that same ARC data is cleanly
# separate: all-consonant (even counting y), low-frequency (JC, BJ, MJ, RT, KD, LM, LK, SM, HS,
# KC, WMC, JT -- each 1-5 occurrences).


def _split_bare_initials(token: str) -> list[str]:
    """A bare, unpunctuated 2-4 letter token with no vowel is almost certainly concatenated
    initials (e.g. "pg" for "P. G.", "aj" for "A.J."), not a real given name -- verified
    empirically (2026-08-19) against 616 real 2-character OAX first names: every high-frequency
    genuine name contains a vowel (yu, li, yi, bo, qi, xu, lu, ...), while the vowel-less
    combinations present (mj, dj, aj, jm, pj, rj, sj, cj) are absent from genuine usage and
    cluster suspiciously around a "*j" pattern -- real, phonology-grounded reasoning (a spoken
    syllable needs a vowel nucleus), not an invented rule. A token WITH a vowel (e.g. "mo",
    itself a real, 138-occurrence given name -- Mo, short for Mohammed/Maurice) is left whole:
    true initials essentially never happen to form a pronounceable vowel-bearing syllable, so
    this correctly leaves the "mo"-style genuine ambiguity alone rather than forcing a guess,
    at the accepted cost of never catching a vowel-containing initials pair (rare) as a false
    negative, rather than risk corrupting a real short name as a false positive.

    Only ever called from name_part_tokens(), which every call site in this codebase applies
    exclusively to given-name material (hn.first/hn.middle) -- never to a family name, where a
    genuine short surname (Mo, Wu, Li, Ng) must never be split.
    """
    if len(token) < 2 or len(token) > 4 or not token.isalpha():
        return [token]
    if any(c in _VOWELS for c in token):
        return [token]
    return list(token)


def name_part_tokens(s: str) -> list[str]:
    """
    Normalise a nameparser-parsed name part into alpha tokens.

    Intended for the output of HumanName fields (first, middle, last) where
    nameparser has already done the structural splitting. Within each part:
      - diacritics are stripped (NFD normalise → ASCII, exotic hyphens → -)
      - apostrophes are removed without splitting, so "O'Brien" → ["obrien"]
        rather than ["o", "brien"]
      - hyphens and spaces still delimit tokens, so "Watson-Parker" → ["watson",
        "parker"] and "van den Berg" → ["van", "den", "berg"]
      - a bare, vowel-less 2-4 letter token is split into individual initials (see
        _split_bare_initials()) -- "pg" → ["p", "g"], but "mo" stays whole

    Apply to both ARC and OAX name fields so the token sets are comparable.
    """
    if not s:
        return []
    s = strip_diacriticals(s)
    s = _APOSTROPHES.sub("", s)
    toks = re.findall(r"[a-z]+", s.lower())
    out: list[str] = []
    for t in toks:
        out.extend(_split_bare_initials(t))
    return out
