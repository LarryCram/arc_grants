"""
Describes how two parsed names differ -- for reporting only.

Used by 00a_extract_arc.py to fill the `kind` column of arc_name_changes.csv. Nothing here decides
whether two names are one person, builds an id, or produces a name form used anywhere: the inputs
are NameParser() output (ParsedName), and the output is a label such as "same|nickname_shaped",
"compound|same" or "swap".

Copied 2026-09-30 from analysis/utils/arc_name_survey.py (the ARC name survey, which stays as it
is); the survey's own copy is not imported here because src/ must not depend on analysis/.

Label = "<family relation>|<given relation>", or "swap" (given and family names exchanged).
  family: same | separator (differ only by spaces/hyphens/apostrophes/underscores/dots) |
          compound (one surname's words are a proper subset of the other's: Smith / Smith-Miles) |
          edit1 | edit2 (1 or 2 letters different) | different | missing
  given:  same | initials_overlap | initial_vs_full | contained | overlap | edit |
          nickname_shaped (one is a prefix of the other, or short and sharing two letters) |
          different | missing
"""

from __future__ import annotations

import re

from src.utils.names import ParsedName

_COMPACT = re.compile(r"[\s\-'’_.]")
_TOKEN_SPLIT = re.compile(r"[\s\-]+")


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _compact(s: str) -> str:
    return _COMPACT.sub("", s.lower())


def family_relation(pa: ParsedName, pb: ParsedName) -> str:
    A, B = set(pa.family_names), set(pb.family_names)
    if not A or not B:
        return "missing"
    if A & B:
        return "same"
    ca, cb = {_compact(x) for x in A}, {_compact(x) for x in B}
    if ca & cb:
        return "separator"
    ta = set(_TOKEN_SPLIT.split(pa.family_name_main or ""))
    tb = set(_TOKEN_SPLIT.split(pb.family_name_main or ""))
    if ta < tb or tb < ta:
        return "compound"
    d = min(levenshtein(x, y) for x in ca for y in cb)
    if d == 1:
        return "edit1"
    if d == 2:
        return "edit2"
    return "different"


def given_relation(pa: ParsedName, pb: ParsedName) -> str:
    if not pa.given_tokens or not pb.given_tokens:
        return "missing"
    ma = {t for t in pa.given_tokens if len(t) > 1}
    mb = {t for t in pb.given_tokens if len(t) > 1}
    ia = {t[0] for t in pa.given_tokens}
    ib = {t[0] for t in pb.given_tokens}
    if not ma or not mb:
        if not ma and not mb:
            return "same" if ia == ib else ("initials_overlap" if ia & ib else "different")
        return "initial_vs_full" if ia & ib else "different"
    ga, gb = pa.first_name_canonical or "", pb.first_name_canonical or ""
    if ga == gb or ma == mb:
        return "same"
    if ma < mb or mb < ma:
        return "contained"
    if ma & mb:
        return "overlap"
    fa, fb = pa.given_tokens[0], pb.given_tokens[0]  # first-name position ("M. Shumi" -> "m")
    if (len(fa) == 1) != (len(fb) == 1) and fa[0] == fb[0]:
        return "initial_vs_full"
    d = levenshtein(ga, gb)
    if d == 1:
        return "edit"
    if ga.startswith(gb) or gb.startswith(ga) or (min(len(ga), len(gb)) <= 5 and ga[:2] == gb[:2]):
        return "nickname_shaped"
    if d == 2:
        return "edit"
    return "different"


def is_swap(pa: ParsedName, pb: ParsedName) -> bool:
    return bool(pa.first_name_canonical and pb.first_name_canonical
                and pa.first_name_canonical in set(pb.family_names)
                and pb.first_name_canonical in set(pa.family_names))


def difference_label(pa: ParsedName, pb: ParsedName) -> str:
    fam = family_relation(pa, pb)
    if fam != "same" and is_swap(pa, pb):
        return "swap"
    return f"{fam}|{given_relation(pa, pb)}"


def keys_relation(pa: ParsedName, pb: ParsedName) -> str:
    """How the two names' full_name_keys sets relate: identical / one_contains_other / overlap /
    disjoint."""
    a, b = set(pa.full_name_keys), set(pb.full_name_keys)
    if a == b:
        return "identical"
    if a <= b or b <= a:
        return "one_contains_other"
    return "overlap" if a & b else "disjoint"
