"""
Shared name tokenisation/parsing helpers used across all pipeline layers. Diacritic-specific
handling (bare/digraph matching, the corpus-grounded equivalence table) split out to
name_diacritic_variants.py (2026-08-25) -- imported from here where still needed.

2026-09-02: HumanNameParser added -- the sole, real implementation of the full name-parsing
chain (canonicalize -> HumanName parse (postnominal suffixes handled natively, see
CONSTANTS.suffix_acronyms below) -> single-token-name fallback -> diacritic-widened tokens),
as a class with a proper ParsedName data interface, per direct
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

# Australian / British post-nominal award acronyms, registered directly into HumanName's own
# suffix vocabulary (CONSTANTS.suffix_acronyms) rather than pre-stripped by a hand-rolled regex
# (removed 2026-09-08 -- see git history for the retired _POSTNOMINALS/strip_postnominals()).
# That custom regex existed because an early test of this registration mechanism was run with
# only AC/AO/AM/OAM added and FAA left out, found "Anthony Thomas AC FAA" -> family="faa", and
# concluded the native mechanism couldn't handle stacked suffixes -- never re-tested with the
# missing acronym actually added. Checked directly (2026-09-08): once the full list below is
# registered, HumanName correctly strips stacked suffixes on its own, both comma- and
# space-separated ("Anthony Thomas AC FAA" -> last="Thomas", suffix="AC, FAA"; "Kinloch FRS,
# FREng" -> last="Kinloch"), case-insensitively, with no pre-processing needed -- maintaining a
# second, parallel list was solving a problem the tool already solved, once actually given the
# whole list. This also benefits OAX `display_name` parsing for free (oax_name_arrays() delegates
# to this same HumanNameParser), which the old ARC-specific regex never touched.
#
# One known, accepted residual gap, NOT fixed by this registration and not chased further here:
# a bare surname with no given name at all, followed by stacked suffixes ("Raston AO FAA"),
# still misparses (HumanName puts the surname in .first and a suffix acronym in .last) -- too few
# tokens for HumanName's own grammar to tell there's no given name. Real but rare; same category
# as the small number of two-part Spanish family names HumanName also gets wrong (unconnected
# parts, not a postnominal issue) -- both deferred rather than patched with new custom
# pre-processing.
_POSTNOMINAL_ACRONYMS = (
    "AC", "AO", "AM", "OAM", "AK", "OL", "FAA", "FAHMS", "FTSE", "FASSA", "FAHA",
    "FRS", "FREng", "CBE", "OBE", "MBE", "KBE", "DBE", "Pharmacist",
)
for _pn in _POSTNOMINAL_ACRONYMS:
    CONSTANTS.suffix_acronyms.add(_pn)

# nameparser's own built-in CONSTANTS.titles includes "wing" (from military ranks like "Wing
# Commander") -- a real, confirmed collision with "Wing" as a genuine Chinese/Cantonese given
# name (found 2026-09-16, OpenAlex author "Wing Kong Chiu": HumanName("Wing Kong Chiu") silently
# drops "Wing" into .title, leaving only "Kong" in .first -- .as_dict() shows
# {'title': 'Wing', 'first': 'Kong', ...} -- so "wing" never reaches given_tokens/full_name_keys
# at all, a real given-name loss, not a tokenization nuance). Discarded from the titles set
# entirely -- this project has no legitimate use for parsing a rank/title prefix off anyone's
# name (ARC/OAX author strings are never "Wing Commander Jane Smith"-shaped), so there's no
# competing case this could break.
#
# 2026-09-16 (same day, follow-on): "wing" was not a one-off. nameparser's titles list has 619
# words in it. Some of those words are ALSO real given names in cultures this project hadn't
# specifically checked against before -- when one of those words is the first word of someone's
# name, nameparser throws it away as a "title" instead of keeping it as their given name.
#
# Checking this against OpenAlex alone isn't enough to justify a fix, though -- the actual job is
# matching ARC investigators to OpenAlex authors, so a word only matters here if some real ARC
# investigator actually has it as their given name (otherwise there's no ARC person for the fixed
# OpenAlex record to ever match against, so fixing it changes nothing for this project). Checked
# every word from the OpenAlex side that looked like a real given name against ARC's own raw
# investigator names (investigators_raw.parquet) before deciding. Only kept the ones that are
# BOTH a real given name being thrown away on the OpenAlex side AND actually used by a real ARC
# investigator:
#   mahdi       4 ARC investigators (Mahdi Jalili, Disfani, Parsa, Fahmideh)
#   sultan      1 ARC investigator  (Sultan Mia)
#   do          1 ARC investigator  (Do Hau)
#   tirthankar  1 ARC investigator  (Tirthankar Bandyopadhyay)
#   king        4 ARC investigators (King Fung Wong, King Lai, King Yuk Chan, King Yang)
#   sheikh      2 ARC investigators (Sheikh Mohammad Fazle Rabbi, Sheikh Rahman)
#
# Several other words (prince, gen, imam, sultana, lama, guru, princess, baba, se) also turned out
# to be real given names being thrown away on the OpenAlex side, sometimes for many more people
# (e.g. "prince" hit 83 real OpenAlex authors) -- but zero ARC investigators use any of these as
# their given name, so fixing them wouldn't create a single new correct match here. Left alone,
# not because they're wrong, but because there's no payoff for this project right now.
#
# Also checked and deliberately left alone: "sa"/"ab" -- every real example on the OpenAlex side
# is just two letters used as initials ("S.A. McIlveen"), not an actual name. "shehu"/"pir" --
# real religious titles where the person's actual given name is usually still there right after
# the title, so removing these wouldn't recover much either way.
#
# Everything else in the 619-word list (Dr, Mr, Professor, Colonel, and ~150 more words that also
# showed up on real OpenAlex authors) is a genuine title being stripped correctly -- left alone.
# Not all 619 words were checked this closely, only the ones with enough real OpenAlex occurrences
# to be worth checking -- another one could still turn out to be a hidden case like this. Fix more
# as they're found and confirmed against a real ARC investigator the same way.
_TITLE_COLLISIONS_REMOVED = ("wing", "mahdi", "sultan", "do", "tirthankar", "king", "sheikh")
for _tc in _TITLE_COLLISIONS_REMOVED:
    CONSTANTS.titles.remove(_tc)


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

    nickname_tokens (2026-09-08): a quoted/parenthesized nickname written inline in the raw
    string (`HumanName('John "Johnny" Smith')` -> `hn.nickname='Johnny'`) -- confirmed real,
    0.34% of distinct ARC (first_name, family_name) pairs (e.g. 'Yingzi (Jenny) Wang'). Kept
    OUT of given_tokens deliberately, not folded in -- a nickname is a different KIND of
    relationship to the family name than a real first/middle name is (its own combinatorial
    axis, `nickname x family_name`, not one more interchangeable given-name candidate), and
    folding it into given_tokens would let it silently influence first_name_canonical/
    full_name_key selection, which nothing here has evidence to justify. Same tokenize-and-
    bare-initial shape as given_tokens/middle_tokens (a multi-word nickname like 'Seyed
    Ruhollah' splits into two independent tokens, matching how a compound given name already
    does). Guarded against `nameparser` treating unrelated OpenAlex `display_name` noise as a
    nickname purely because it shares the "(...)" shape -- author-disambiguation numeric
    suffixes, consortium/group-authorship strings, birth-year annotations -- by rejecting
    anything containing a digit or longer than 30 characters (see _guarded_nickname()).

    full_name_keys (2026-09-08): the actual combinatorial consumption of nickname_tokens --
    without this, nickname_tokens was a genuine dead end: built, but nothing crossed it against
    family_names, so it had zero production consumers (confirmed by grepping every call site).
    Every given x family AND nickname x family combination this one occurrence's own tokens
    produce, order-preserving deduped, plus full_name_key itself -- the SET of all possible full
    names for this occurrence, not one collapsed representative. Same logic as
    orcid_processor.py's all_full_name_keys(), now built directly into the canonical parser
    instead of a second, NameForms-only implementation (docs/pipeline_todo.md item #26 -- moving
    orcid_processor.py onto this field instead of its own reimplementation is still open, not
    done by this field's addition alone).
    """
    given_tokens: tuple[str, ...]
    middle_tokens: tuple[str, ...]
    nickname_tokens: tuple[str, ...]
    family_names: tuple[str, ...]
    family_name_main: str | None
    first_name_canonical: str | None
    full_name_key: str | None
    full_name_keys: tuple[str, ...]
    given_tokens_raw: tuple[str, ...]
    family_name_raw: str | None
    full_name_key_raw: str | None


_RAW_SPLIT = re.compile(r"[\s\-]+")

# Matches a leading "nee"/"née" maiden-name marker, capturing the former surname that follows --
# see HumanNameParser._maiden_name()'s own docstring for why this needs to be distinguished from
# a genuine given-name nickname before either is incorporated.
_NEE_RE = re.compile(r"^n[ée]e\.?\s+(.+)$", re.IGNORECASE)


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

    def _given_name_diacritic_variants(self, s: str) -> tuple[str, ...]:
        """Diacritic widening for a given-name token, guarded against a bare single-letter
        initial being cartesian-expanded into a multi-character digraph artifact that then
        looks like real given-name evidence downstream (a real, confirmed case: "Å" -- itself
        a single letter, strip_diacriticals("Å") == "a" -- widens via diacritic_variants() to
        {"a", "aa"}, since "aa" is the genuine Scandinavian digraph transliteration; that
        expansion does real, needed work for a genuine multi-character name like "Björn" ->
        {"bjorn", "bjoern"}, but "aa" derived from a bare initial carries exactly as little
        identifying information as the "a" it was derived from -- confirmed directly
        (2026-09-16 investigation, "Å. Ferrier" case): both survive into given_tokens/
        full_name_keys correctly, but downstream comparisons that treat any length>1 given
        token as real evidence (see sql/01_blocking_name_keys.sql's given_name_check) have no
        way to tell "aa" apart from a genuine short name once it's a bare string, wrongly
        counting it as substantial.

        The fix is structural, not a rarity/frequency threshold: was the token ALREADY just
        one character before any diacritic widening? That question was tested empirically
        against oax_tf_first_name.parquet's full given-name-token frequency distribution before
        settling on this approach -- rejected, because a real, protected short given name ("Mo",
        138 real ARC occurrences, tf=6.2e-5) sits statistically BETWEEN the suspected-initials
        cluster ("aw" 2.8e-5, "aa"/"am" ~6-12e-5) with no separating threshold, so any
        frequency-based cutoff that catches "aa" would also wrongly catch "Mo". This check has
        no such collision risk: "Mo" was never a single character at any point, so it never hits
        the guard below, while "Å" genuinely was.
        """
        stripped = strip_diacriticals(s) if s else ""
        if len(stripped) <= 1:
            return (stripped,) if stripped else ()
        return self.diacritic_variants(s)

    def _structural(self, raw_name: str) -> HumanName | None:
        """canonicalize -> HumanName parse (postnominal suffixes handled natively via the
        CONSTANTS.suffix_acronyms registration above, no pre-stripping) -> single-token-name
        fallback. The one place this sequence is implemented; every method below builds on
        this."""
        if not raw_name:
            return None
        hn = HumanName(self.canonicalize(raw_name))
        if not hn.last and hn.first:
            # A bare, title-less single-word name (e.g. "Smith") is nameparser's own default
            # given-name guess -- moved to .last since this project's matching is surname-
            # anchored (see this method's own history/CLAUDE.md for why). first must be cleared
            # too, not just copied from -- confirmed directly (2026-09-08) that leaving it set
            # let the same bare word leak into given_tokens as a spurious given-name candidate
            # downstream, alongside the family name it actually is. nameparser's own empty-field
            # sentinel is '' (confirmed directly), not None -- matched here rather than guessed.
            hn.last = hn.first
            hn.first = ""
        return hn

    @staticmethod
    def _guarded_nickname(hn: HumanName) -> str | None:
        """The nickname-plausibility guard alone -- see ParsedName.nickname_tokens' own
        docstring for why this exists: `nameparser` extracts a "(...)"/quoted nickname
        structurally, with no way to tell a genuine chosen name from OpenAlex `display_name`
        noise sharing the same shape (author-disambiguation numeric suffixes, consortium/
        group-authorship strings, birth-year annotations). Rejects empty, anything containing
        a digit (kills every numeric-ID and year case found in a direct 2026-09-08 check of
        real OAX display_name data), and anything longer than 30 characters (comfortably above
        real multi-word nicknames like 'Seyed Ruhollah' at 14)."""
        nick = hn.nickname
        if not nick or any(c.isdigit() for c in nick) or len(nick) > 30:
            return None
        return nick

    @staticmethod
    def _maiden_name(hn: HumanName) -> str | None:
        """A parenthetical/quoted maiden-name marker ('Judy Brown (nee Field)', 'Murphy (née
        Paton-Walsh)') lands in hn.nickname structurally identically to a genuine given-name
        nickname -- confirmed directly (2026-09-08): `HumanName('Judy Brown (nee Field)').nickname
        == 'nee Field'`, nothing distinguishes it from 'Johnny' except this marker word. But
        semantically it's a FAMILY-name alternative (a former surname), not a given-name one --
        confirmed as a real pattern in this project's own ARC data (Leesa Costello (nee
        Bonniface), see CLAUDE.md's 4u-review history). Caught here, before the generic nickname
        guard, so it feeds family_names instead of nickname_tokens."""
        nick = hn.nickname
        if not nick:
            return None
        m = _NEE_RE.match(nick.strip())
        return m.group(1) if m else None

    def parse(self, raw_name: str) -> ParsedName:
        """Full chain -> ParsedName, both the ASCII-reduced and non-ASCII raw representations."""
        hn = self._structural(raw_name)
        if hn is None:
            return ParsedName(
                given_tokens=(), middle_tokens=(), nickname_tokens=(), family_names=(),
                family_name_main=None, first_name_canonical=None, full_name_key=None,
                full_name_keys=(),
                given_tokens_raw=(), family_name_raw=None, full_name_key_raw=None,
            )

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
            tok for variant in self._given_name_diacritic_variants(hn.first) for tok in name_part_tokens(variant)
        ] if hn.first else []
        middle_tokens = [
            tok for variant in self._given_name_diacritic_variants(hn.middle) for tok in name_part_tokens(variant)
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

        # A "(nee X)"/"(née X)" maiden-name marker is checked FIRST and handled entirely
        # separately from a genuine nickname: X is a former FAMILY name, so it widens
        # family_names (computed above, family_name_main/full_name_key already picked from it
        # and unaffected by this later extension -- same "additive, doesn't override the primary
        # form" treatment as nickname_tokens below), never nickname_tokens. See _maiden_name()'s
        # own docstring for why nameparser can't tell these apart on its own.
        maiden = self._maiden_name(hn)
        if maiden:
            family_names = tuple(dict.fromkeys(family_names + self.diacritic_variants(maiden)))
            nick = None
        else:
            nick = self._guarded_nickname(hn)

        # Nickname, kept as its own field -- NOT folded into given_ascii/given_tokens above, and
        # so has no effect on first_name_canonical/full_name_key selection. See
        # ParsedName.nickname_tokens' own docstring for why this separation is deliberate.
        nick_words = [
            tok for variant in self.diacritic_variants(nick) for tok in name_part_tokens(variant)
        ] if nick else []
        nickname_tokens = tuple(dict.fromkeys(nick_words + [t[0] for t in nick_words if t]))

        # full_name_keys: the actual combinatorial consumption of nickname_tokens (and the
        # already-widened family_names, including any maiden-name form) -- every given/nickname x
        # family combination this occurrence's own tokens produce, same logic as
        # orcid_processor.py's all_full_name_keys(), built directly here instead of a second,
        # NameForms-only implementation. See ParsedName.full_name_keys' own docstring.
        given_and_nickname = tuple(dict.fromkeys(given_tokens + nickname_tokens))
        full_name_key_combos = [
            f"{g}_{f}" for g in given_and_nickname for f in family_names if g and f
        ]
        full_name_keys = tuple(dict.fromkeys(
            full_name_key_combos + ([full_name_key] if full_name_key else [])
        ))

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
            given_tokens=given_tokens, middle_tokens=middle_tokens_out,
            nickname_tokens=nickname_tokens, family_names=family_names,
            family_name_main=family_name_main, first_name_canonical=first_name_canonical,
            full_name_key=full_name_key, full_name_keys=full_name_keys,
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
      - a hyphenated part ALSO keeps the literal hyphenated whole as its own token
        (e.g. "Xu-Jia" → ["xu", "jia", "xu-jia"]) -- nameparser handed us this as ONE
        name-part field; splitting it is useful (a real single-syllable short form),
        but the split must not be the only representation, since that discards the
        real, literal spelling and leaves no way to match it as itself. This is the
        actual recorded string with only case/diacritics normalised, never a
        synthesized/concatenated form (2026-09-18: an earlier draft of this fix
        proposed "xujia", a string that never appears anywhere in the source data --
        corrected on direct challenge; only the literal hyphenated spelling is added).

    Apply to both ARC and OAX name fields so the token sets are comparable.
    """
    if not s:
        return []
    s = strip_diacriticals(s)
    s = _APOSTROPHES.sub("", s)
    s_lower = s.lower()
    toks = re.findall(r"[a-z]+", s_lower)
    out: list[str] = []
    for t in toks:
        out.extend(_split_bare_initials(t))
    # Preserve each hyphenated word's own literal spelling too -- matched independently per
    # space-separated word (never rejoined across a space), so "Mary-Jane Ann" correctly keeps
    # "mary-jane" as one unit without inventing a false "jane-ann" join.
    for word in re.findall(r"[a-z][a-z-]*[a-z]", s_lower):
        if "-" in word and word not in out:
            out.append(word)
    return out
