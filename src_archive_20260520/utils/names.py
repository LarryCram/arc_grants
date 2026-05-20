"""
Shared name-normalisation helpers used across all pipeline layers.
"""

import re
import unicodedata

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
