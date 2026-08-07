"""
src/utils/for_resolve.py

Thin, cached wrapper around research_classification.Resolver for ANZSRC FOR-code
harmonisation. Replaces the hand-built concordance in src/utils/lookup_for_topic.py
(itself fed by config/build_for_oax_field_map.py's ~370 hand-typed mapping rows) with
calls into an external, audited, versioned package.

Resolver().resolve() raises LookupError/ValueError on unmapped/malformed input and can
emit a UserWarning for a small set of documented permanent gaps (e.g. FOR1998 divisions
21/22) -- callers here need the tolerant Optional[str]-returning contract the old
ForTopicLookup had (SQL COALESCE / DuckDB UDF null_handling='special'), so every public
function below catches both and suppresses the warning explicitly.

resolve() measures ~0.6ms/call; ARC's FOR-code domain is small (under a few hundred
distinct codes across ~65k grant rows), so results are memoized with lru_cache -- without
it, a raw per-row DuckDB UDF pass would cost tens of seconds instead of a fraction of one.

DuckDB parallelises UDF execution across worker threads, and Resolver() holds a single
duckdb connection with no thread-safety of its own -- concurrent UDF calls hitting an
uncached code raced on that shared connection and corrupted memory (`malloc(): unsorted
double linked list corrupted` / `InvalidInputException: No open result set`) the first time
this was run against real data. lru_cache's own lock only protects its cache dict, not the
wrapped function body, so it does not prevent this. _RESOLVE_LOCK below serialises every
actual resolve() call across threads; cache hits (the overwhelming majority once warm) never
touch it.
"""

import functools
import threading
import warnings

from research_classification import Resolver

_resolver: Resolver | None = None
_RESOLVE_LOCK = threading.Lock()


def _get_resolver() -> Resolver:
    global _resolver
    if _resolver is None:
        _resolver = Resolver()
    return _resolver


def _for_scheme(code: str) -> str:
    """FOR2008 group codes are numbered 01xx-22xx, FOR2020 group codes 30xx-52xx -- the
    two ranges never overlap, so the vintage is recoverable from the code's own 2-digit
    prefix alone, same range check ForTopicLookup.upgrade_for_code used internally.
    Returns "" for anything outside both ranges.
    """
    prefix = code[:2]
    if "30" <= prefix <= "52":
        return "FOR2020"
    if "01" <= prefix <= "22":
        return "FOR2008"
    return ""


@functools.lru_cache(maxsize=None)
def _resolve_cached(code: str, from_scheme: str, to_scheme: str):
    with _RESOLVE_LOCK, warnings.catch_warnings():
        # resolve() warns on documented permanent gaps (e.g. FOR1998 divisions 21/22) --
        # expected here, not a signal worth surfacing on every one of tens of thousands
        # of per-row calls.
        warnings.simplefilter("ignore", UserWarning)
        try:
            return _get_resolver().resolve(code, from_scheme, to_scheme)
        except (LookupError, ValueError):
            return None


def upgrade_for_code(code: str | None) -> str | None:
    """Upgrade a 2008 FOR group code to its 2020 equivalent; 2020 codes pass through
    unvalidated (as ForTopicLookup.upgrade_for_code did). Returns None for unmappable,
    malformed, or non-4-digit codes (SQL COALESCE keeps the original in that case).
    """
    if not code or len(code) != 4 or not code.isdigit():
        return None
    scheme = _for_scheme(code)
    if scheme == "FOR2020":
        return code
    if scheme == "FOR2008":
        result = _resolve_cached(code, "FOR2008", "FOR2020")
        return result.code if result else None
    return None


def upgrade_for_name(code: str | None, name: str | None) -> str | None:
    """Return the official ANZSRC 2020 group name when a 2008 code is upgraded, or the
    original name when already 2020 or no mapping exists -- 2020-vintage codes never have
    their name rewritten, matching ForTopicLookup.upgrade_for_name's original asymmetry.
    """
    if not code:
        return name
    code20 = upgrade_for_code(code)
    if code20 is None or code20 == code:
        return name
    result = _resolve_cached(code20, "FOR2020", "FOR2020")
    return result.label if result else name


def oax_subfield_name(code: str | None) -> str | None:
    """FOR2008-or-FOR2020 group code -> OAX subfield label, in one resolve() call (replaces
    the old two-hop upgrade_for_code -> group_to_subfield chain in 04_resolve_links.py's
    _field_score).
    """
    if not code or len(code) != 4 or not code.isdigit():
        return None
    scheme = _for_scheme(code)
    if not scheme:
        return None
    result = _resolve_cached(code, scheme, "OAX_SUBFIELD")
    return result.label if result else None


def oax_to_for2020(oax_code: str | int | None) -> str | None:
    """OAX domain/field/subfield/topic code or label -> FOR2020 code, at whatever
    precision the OAX input supports (topic-level input can reach FOR2020 field/6-digit
    precision, finer than the ARC-FOR-to-OAX direction ever provides). Added for the
    OAX-field/subfield -> FOR-code direction needed for future ECR/DECRA analysis work --
    not called from any pipeline file yet.
    """
    if not oax_code:
        return None
    result = _resolve_cached(str(oax_code), "OAX", "FOR2020")
    return result.code if result else None
