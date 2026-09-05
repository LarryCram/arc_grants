"""
Shared name tokenisation/parsing helpers used across all pipeline layers. Diacritic-specific
handling (bare/digraph matching, the corpus-grounded equivalence table) split out to
name_diacritic_variants.py (2026-08-25) -- imported from here where still needed.

2026-09-02: HumanNameParser added -- the sole, real implementation of the full name-parsing
chain (canonicalize -> strip postnominals -> HumanName parse -> single-token-name fallback ->
diacritic-widened tokens), as a class with a proper ParsedName data interface, per direct
request following a detailed Unicode-normalization review (NFC/NFKC ingestion hygiene, zero-width
character stripping, a soft-hyphen bug, and -- separately -- the finding that ASCII-reduction
alone risks silently dropping non-Latin or not-yet-catalogued-diacritic names entirely, motivating
ParsedName's second, non-ASCII "raw" key). Every existing free function that used to implement
part of this chain itself (parse_given() here, awards_cif.py's _name_forms()) is now a pure,
behavior-preserving delegation to one shared instance -- there is only one implementation.
"""

import re
from dataclasses import dataclass

from nameparser import HumanName
from nameparser.config import CONSTANTS

from src.utils.name_diacritic_variants import canonicalize_name_punctuation, strip_diacriticals, diacritic_variants

# Register Australian post-nominals so HumanName parses them as suffixes,
# not as family name tokens (e.g. "Summerhayes OAM" → last="Summerhayes").
for _pn in ("AC", "AO", "AM", "OAM"):
    CONSTANTS.suffix_acronyms.add(_pn)

# Trailing Australian / British post-nominal awards (handles stacking: "AO FAA" or "FRS, FREng").
# OL (Officer of the Order of Logohu, PNG) and FREng (Fellow of the Royal Academy of Engineering)
# added 2026-08-20 -- found via a same-grant/same-ORCID duplicate-name screen: "Glenn Summerhayes
# OL OAM" was leaking "ol" into the parsed family name (Summerhayes is a PNG-focused archaeologist,
# explaining the honour), and "Anthony Kinloch FRS, FREng" wasn't stripped at all because (a)
# FREng wasn't in the list and (b) the comma before it didn't match the old whitespace-only
# separator. Separator widened from `\s+` to `[\s,]+` to handle comma-separated stacking too.
_POSTNOMINALS = re.compile(
    r"(?:[\s,]+(?:AO|AM|OAM|AC|AK|OL|FAA|FAHMS|FTSE|FASSA|FAHA|FRS|FREng|CBE|OBE|MBE|KBE|DBE|Pharmacist))+\s*$",
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


@dataclass(frozen=True)
class ParsedName:
    """Two parallel representations of one parsed name, per the 2026-09-02 Unicode-normalization
    review's conclusion: ASCII-reduction (diacritic_variants()'s bare/digraph expansion) does
    real, necessary work bridging genuine spelling-convention differences (ARC's "Gruetzner" vs
    OpenAlex's "Grützner" -- different LETTERS, not just different encodings of the same one),
    but it silently drops any name it can't reduce to ASCII at all -- guaranteed for a non-Latin-
    script name, and a real, worsening risk for a Latin name using a diacritic this module's
    hand-built fallback tables haven't been taught yet (already measured at 105 uncatalogued
    characters in one prior audit, against a smaller and less globally-diverse population than
    OrcidProcessor's new 17.15M-record source). Rather than have that one lossy representation be
    the ONLY one, every field comes in both forms:

      - given_tokens / family_names / family_name_main / first_name_canonical / full_name_key:
        the existing ASCII-reduced convention, unchanged in meaning from before this class
        existed -- real, necessary spelling-bridging value, but empty/lossy for non-Latin or
        uncatalogued-diacritic input.
      - given_tokens_raw / family_name_raw / full_name_key_raw: NFC-normalized + casefold()'d
        only -- no ASCII reduction, so nothing is dropped, but genuinely different spellings
        (Gruetzner vs Grützner) are correspondingly NOT unified here either. casefold() (not
        lower()) is used on this path specifically because this is where it has real, observable
        effect (Greek final sigma, Cherokee, etc.) -- unlike the ASCII-reduced path, where a
        casefold pass was built, tested, and found to be provably inert (see
        name_diacritic_variants.py::diacritic_variants()'s own docstring for why) and removed.

    Callers should try the ASCII-reduced key first (it's the one this project's whole pipeline
    already blocks/compares on) and fall back to the raw key when the ASCII one is empty/None --
    not the reverse -- since the raw key alone would fail to bridge known, real spelling
    differences the ASCII path exists specifically to catch.

    middle_tokens (2026-09-05): the middle-name-derived subset of given_tokens, kept as its own
    field so a caller can require a middle-name/middle-initial match as EXTRA selectivity on top
    of family+first (e.g. narrowing a common-surname/bare-first-initial ORCID search) without
    conflating "any given-name token matched" (given_tokens, deliberately loose -- first and
    middle are interchangeable alternatives there) with "the middle name specifically matched"
    (a stricter, AND-style signal). given_tokens/first_name_canonical are otherwise unaffected --
    middle_tokens is additive, not a replacement for anything existing.
    """
    given_tokens: tuple[str, ...]
    middle_tokens: tuple[str, ...]
    family_names: tuple[str, ...]
    family_name_main: str | None
    first_name_canonical: str | None
    full_name_key: str | None
    given_tokens_raw: tuple[str, ...]
    family_name_raw: str | None
    full_name_key_raw: str | None


_RAW_SPLIT = re.compile(r"[\s\-]+")


class HumanNameParser:
    """The sole, real implementation of this project's name-parsing chain -- every existing free
    function that used to implement part of it directly (parse_given() below, awards_cif.py's
    _name_forms()) is now a pure delegation to one shared instance of this class, so the logic
    lives in exactly one place. See ParsedName's own docstring for the ASCII-reduced-vs-raw
    design this class produces both halves of.
    """

    def canonicalize(self, raw_name: str) -> str:
        """The hardened Unicode/punctuation cleanup step alone (NFC, zero-width-character strip,
        NFKC, quote/hyphen substitution) -- exposed separately for callers that only need this
        much, without a full structural HumanName parse."""
        return canonicalize_name_punctuation(raw_name)

    def diacritic_variants(self, s: str) -> tuple[str, ...]:
        """The ASCII-reduced spelling-convention-bridging expansion alone, as an ordered tuple
        (shortest-first, matching diacritic_variants()'s own contract)."""
        return tuple(diacritic_variants(s))

    def _structural(self, raw_name: str) -> HumanName | None:
        """canonicalize -> strip postnominals -> HumanName parse -> single-token-name fallback.
        The one place this sequence is implemented; every method below builds on this."""
        if not raw_name:
            return None
        hn = HumanName(strip_postnominals(self.canonicalize(raw_name)))
        if not hn.last and hn.first:
            hn.last = hn.first
        return hn

    def parse(self, raw_name: str) -> ParsedName:
        """Full chain -> ParsedName, both the ASCII-reduced and non-ASCII raw representations."""
        hn = self._structural(raw_name)
        if hn is None:
            return ParsedName((), (), (), None, None, None, (), None, None)

        # ASCII-reduced (existing convention: diacritic-widen each of first/middle, tokenize,
        # add bare initials, order-preserving dedup -- dict.fromkeys(), never a raw set(), per
        # this project's own already-fixed non-determinism bug). first_tokens/middle_tokens are
        # kept separate (not immediately flattened) so first_name_canonical below can prefer the
        # actual first name -- confirmed a real bug otherwise: max(..., key=len) over the
        # flattened first+middle pool picks whichever STRING is longer with no regard for
        # position, so e.g. "George Stewart Walker" canonicalized to "stewart" and "Ben Martin
        # Tsamenyi" to "martin" -- 310 of 851 real ARC multi-token first_name values (36.4%,
        # checked directly 2026-09-05) hit this, each one a wrong first_initial fed straight into
        # Splink's primary blocking key (family_name_main + first_initial, per this file's own
        # documented convention below).
        first_tokens = [
            tok for variant in self.diacritic_variants(hn.first) for tok in name_part_tokens(variant)
        ] if hn.first else []
        middle_tokens = [
            tok for variant in self.diacritic_variants(hn.middle) for tok in name_part_tokens(variant)
        ] if hn.middle else []
        given_ascii = first_tokens + middle_tokens
        family_names = self.diacritic_variants(hn.last) if hn.last else ()
        family_name_main = max(family_names, key=len) if family_names else None
        # Fallback for a last-name-only input (HumanName found no first/middle at all): use the
        # family name's own first letter as a stand-in given-name token, so given_tokens isn't
        # left completely empty when something is known. Matches awards_cif.py::_name_forms()'s
        # existing, tested convention exactly.
        extra = [family_name_main[0]] if not given_ascii and family_name_main else []
        given_tokens = tuple(dict.fromkeys(given_ascii + [t[0] for t in given_ascii if t] + extra))
        # Prefer the first name's own longest widened form; only fall back to the middle name
        # when the first name is itself degenerate (a bare initial, or absent) -- e.g. "C. David
        # Thomas" correctly canonicalizes to "david", since "c" has no substantive (>1-char) form
        # of its own to prefer.
        first_full_toks = [t for t in first_tokens if len(t) > 1]
        middle_full_toks = [t for t in middle_tokens if len(t) > 1]
        if first_full_toks:
            first_name_canonical = max(first_full_toks, key=len)
        elif middle_full_toks:
            first_name_canonical = max(middle_full_toks, key=len)
        else:
            first_name_canonical = given_tokens[0] if given_tokens else None
        full_name_key = (f"{first_name_canonical}_{family_name_main}"
                          if first_name_canonical and family_name_main else None)
        # Same tokens+initials shape as given_tokens, restricted to the middle-name-derived
        # subset only -- see ParsedName's own docstring for why this is kept separate.
        middle_tokens_out = tuple(dict.fromkeys(middle_tokens + [t[0] for t in middle_tokens if t]))

        # Non-ASCII raw (NFC + casefold only -- no ASCII reduction, no bare-initial splitting,
        # since that heuristic is Latin-alpha-specific and doesn't generalize to other scripts)
        given_raw = [
            tok for raw in (hn.first, hn.middle) if raw
            for tok in _RAW_SPLIT.split(raw.casefold()) if tok
        ]
        given_tokens_raw = tuple(dict.fromkeys(given_raw))
        family_name_raw = hn.last.casefold() if hn.last else None
        full_name_key_raw = (f"{given_tokens_raw[0]}_{family_name_raw}"
                              if given_tokens_raw and family_name_raw else None)

        return ParsedName(
            given_tokens=given_tokens, middle_tokens=middle_tokens_out, family_names=family_names,
            family_name_main=family_name_main, first_name_canonical=first_name_canonical,
            full_name_key=full_name_key,
            given_tokens_raw=given_tokens_raw, family_name_raw=family_name_raw,
            full_name_key_raw=full_name_key_raw,
        )

    def parse_given_legacy(self, name_str: str) -> tuple:
        """Reproduces parse_given()'s exact historical 5-tuple shape (first, middle, compound,
        f_init, m_init) -- ASCII-stripped to ONE compact form (strip_diacriticals(), not the
        full expansion list), since that is what the Splink comparison-column callers need."""
        hn = self._structural(name_str)
        if hn is None:
            return None, None, None, None, None
        f = strip_diacriticals(hn.first).lower() or None
        m = strip_diacriticals(hn.middle).lower() or None
        fc = (f + " " + m) if (f and m) else f
        return f, m, fc, f[0] if f else None, m[0] if m else None


_default_parser = HumanNameParser()


def parse_given(name_str: str) -> tuple:
    """HumanName → (first, middle, compound, f_init, m_init), all lowercased.

    compound = first + " " + middle when both present, else first.
    Used to build Splink comparison columns for given-name matching.

    Pure delegation to HumanNameParser.parse_given_legacy() -- see that method and
    HumanNameParser's own docstring for where the actual logic lives.
    """
    return _default_parser.parse_given_legacy(name_str)


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
