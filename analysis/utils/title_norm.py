"""
analysis/utils/title_norm.py

Strict title normalisation for dedup (analysis/utils/dedup.py). "Strict" means: normalise
only genuine equivalent-encodings of the same title -- capitalisation, whitespace/hyphen
placement, HTML-entity vs raw-HTML-tag markup, and LaTeX source-vs-name-vs-symbol
representations of the same word (e.g. "\\gamma" / "gamma") -- and otherwise leave the title
alone, so two genuinely different titles are never accidentally collapsed. This replaces an
earlier, much looser approach (strip every non-alphanumeric character indiscriminately) that
happened to work for the specific pairs checked by hand, but risked false merges at full
cohort scale since it discarded real content (all punctuation, all spacing) rather than only
known-equivalent variation.

Verified against real ARC-cohort titles before wiring in -- both a synthetic test set and,
more importantly, an actual full-cohort diff against the old (loose, strip-everything)
approach: found 8 real cases where titles that used to collapse now don't, all genuine
same-paper variants, not bugs -- 5 were fixed here (smart vs straight quotes/apostrophes,
trailing "?"/"." differences, stray whitespace left next to "/" after hyphen-to-space
conversion); 2 are accepted rare misses not worth the false-positive risk of a general fix
(a missing-space text-extraction typo; an Oxford-comma variant). Also correctly collapses a
real hyphen/space preprint-vs-article pair ("high-throughput" / "high throughput"), a synthetic
LaTeX-vs-plain pair ("\\gamma-ray" / "gamma-ray"), an HTML-entity-vs-raw-tag pair
("&lt;em&gt;...&lt;/em&gt;" / "<em>...</em>"), and correctly keeps a real, genuinely distinct
pair separate (a journal Letter, "...with a heralded linear amplifier", vs its differently-
worded conference-proceedings precursor, "...for arbitrary coherent states using heralded
linear amplifiers" -- same underlying research, different real title text, not a formatting
variant, so must not merge).

Reuses src/utils/names.py's strip_diacriticals() for the diacritic/exotic-hyphen-variant
step rather than reimplementing it.
"""

import re

from src.utils.names import strip_diacriticals

_HTML_ENTITIES = {
    "&lt;": "<", "&gt;": ">", "&amp;": "&", "&quot;": '"', "&apos;": "'", "&nbsp;": " ",
}
_HTML_TAG = re.compile(r"<[^>]+>")

# LaTeX macros that carry no content of their own -- typesetting instructions only.
# Strip the macro name + braces, keep the argument. Applied a few times for nesting.
_LATEX_FMT_MACRO = re.compile(
    r"\\(mathbb|mathrm|mathit|mathsf|mathcal|textit|textbf|textrm|emph|it|rm|bf|sf)\s*\{([^{}]*)\}"
)
# Backslash-escaped accent markers (\"o, \'e, \`e, \^e, \~n) -- drop the marker, keep the
# base letter (strip_diacriticals then removes the letter's own diacritic if any survives).
_LATEX_ACCENT = re.compile(r"\\[\"'`^~]")

_LATEX_META = re.compile(r"[${}^_]")
_HYPHENS = re.compile(r"[-\u2010-\u2015]")
# Straight and curly, single and double -- treated as decorative, not disambiguating content.
_QUOTES = re.compile(r"[\"'\u2018\u2019\u201c\u201d\u201a\u201e]")
_TRAILING_PUNCT = re.compile(r"[.?!:;]+\s*$")
_WHITESPACE = re.compile(r"\s+")
_SLASH_SPACE = re.compile(r"\s*/\s*")


def register(con) -> None:
    """Idempotent -- DuckDB doesn't allow re-registering a UDF name on the same
    connection, and callers may run this against the same connection more than once
    in one session (e.g. building several people's dossiers in a loop)."""
    import duckdb
    try:
        con.create_function("norm_title", normalize_title, ["VARCHAR"], "VARCHAR", null_handling="special")
    except duckdb.NotImplementedException:
        pass


def normalize_title(title: str | None) -> str | None:
    """Case/whitespace/hyphen/quote/HTML/LaTeX-symbol-insensitive normalisation. Preserves
    all other punctuation and content -- genuinely different titles must not collapse."""
    if not title:
        return None
    s = title
    for entity, char in _HTML_ENTITIES.items():
        s = s.replace(entity, char)
    s = _HTML_TAG.sub("", s)
    for _ in range(3):  # a few passes to unwrap nested macros, e.g. \textit{\mathbb{X}}
        s = _LATEX_FMT_MACRO.sub(r"\2", s)
    s = _LATEX_ACCENT.sub("", s)
    s = s.replace("\\", "")  # remaining backslash-commands (Greek letters etc.): keep the name
    s = strip_diacriticals(s)
    s = _LATEX_META.sub("", s)
    s = _QUOTES.sub("", s)
    s = _TRAILING_PUNCT.sub("", s)
    s = _HYPHENS.sub(" ", s)
    s = _SLASH_SPACE.sub("/", s)  # collapse whitespace introduced next to "/" by the above
    s = _WHITESPACE.sub(" ", s).strip()
    return s.lower()
