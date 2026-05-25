"""
Shared name-normalisation helpers used across all pipeline layers.
"""

import re
import unicodedata

from nameparser import HumanName

# Exotic Unicode hyphens → ASCII hyphen
_EXOTIC_HYPHENS = re.compile(r"[­‐‑‒–—―−－]")

# Trailing Australian / British post-nominal awards (handles stacking: "AO FAA")
_POSTNOMINALS = re.compile(
    r"(?:\s+(?:AO|AM|OAM|AC|AK|FAA|FTSE|FASSA|FAHA|FRS|CBE|OBE|MBE|KBE|DBE))+\s*$",
    re.IGNORECASE,
)


def strip_postnominals(name: str) -> str:
    """Remove trailing post-nominal awards from a name string.

    Handles stacked awards: "Raston AO FAA" → "Raston".
    """
    return _POSTNOMINALS.sub("", name).strip()


def strip_diacriticals(s: str) -> str:
    """Normalise exotic hyphens to ASCII, Turkish dotless-ı → i, strip diacriticals."""
    s = _EXOTIC_HYPHENS.sub("-", s)
    s = s.replace("ı", "i").replace("İ", "I")
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


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
_APOSTROPHES = re.compile(r"['ʼ`]")


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
    hn = HumanName(name_str)
    f  = hn.first.lower()  or None
    m  = hn.middle.lower() or None
    fc = (f + " " + m) if (f and m) else f
    return f, m, fc, f[0] if f else None, m[0] if m else None


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

    Apply to both ARC and OAX name fields so the token sets are comparable.
    """
    if not s:
        return []
    s = strip_diacriticals(s)
    s = _APOSTROPHES.sub("", s)
    return re.findall(r"[a-z]+", s.lower())
