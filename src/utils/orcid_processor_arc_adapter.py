"""
src/utils/orcid_processor_arc_adapter.py

Thin, project-specific glue connecting OrcidProcessor (a standalone, project-agnostic module,
src/utils/orcid_processor.py) to this project's own conventions.

2026-09-08: dropped its third responsibility, arc_name_normalizer() -- orcid_processor.py now
imports and uses names.py's HumanNameParser/ParsedName directly as its own default (item #26,
see that module's own docstring for the full incident: the "keep the core module free of
project-specific imports" premise this adapter step was built on didn't actually hold, since
names.py is itself genuinely standalone). No adapter conversion step is needed for name
normalization at all now -- this file's remaining two responsibilities are unrelated to naming.

Two responsibilities:
  institution_matched_candidates() / resolve_institution_overlap() -- the set-to-set institution
                                reduction OrcidProcessor's own discover() deliberately does NOT
                                perform itself (a candidate's career-long institution history
                                against this ACIF's own eligible HEP set is inherently
                                project-specific reasoning, not something a general-purpose ORCID
                                package should have an opinion on -- see docs/pipeline_todo.md
                                #19 and orcid_processor.py's own docstring for the full
                                reasoning). institution_matched_candidates() returns the raw
                                matched list (needed by 00b_enrich_orcid.py::_search_bulk_db() to
                                distinguish "0 matches" from "2+, a real ambiguity" -- both cases
                                where resolve_institution_overlap() alone returns None);
                                resolve_institution_overlap() is the collapsed single-winner form
                                for a caller that only needs the reduced answer.
  fetch_record()/get_record() -- wires OrcidProcessor.get_or_fetch()'s injected `cache`/`fetcher`
                                hooks to this project's own single ORCID /record store
                                (orcid_client.default_cache()/fetch_orcid_record()) -- reuses that
                                module's OAuth/HTTP/retry logic rather than a second copy of it,
                                and keeps every fetched record landing in the one existing cache
                                (DISKCACHE_DIR/orcid_records_authenticated), not a new one.
"""
from src.utils import orcid_client
from src.utils.orcid_processor import OrcidRecord, get_or_fetch


def institution_matched_candidates(candidates: list[dict], own_institution_names: list[str] | set[str]) -> list[dict]:
    """Every OrcidProcessor.discover() candidate whose career institution_names overlaps this
    ACIF's own eligible institution set (case-insensitive) -- the shared set-comparison both
    resolve_institution_overlap() (below) and 00b_enrich_orcid.py::_search_bulk_db() build on,
    so the "what counts as a match" definition lives in exactly one place. Returns the raw
    matched list, not a single winner -- a caller distinguishing "0 matches" from "2+ matches"
    (a real ambiguity, not a resolvable one) needs the list, not just resolve_institution_overlap()'s
    collapsed answer."""
    wanted = {i.lower() for i in own_institution_names}
    return [
        c for c in candidates
        if wanted & {n.lower() for n in c.get("institution_names", []) if n}
    ]


def resolve_institution_overlap(candidates: list[dict], own_institution_names: set[str]) -> str | None:
    """Given OrcidProcessor.discover()'s candidate list (each dict carries an
    "institution_names" list -- every employer/education/membership org name found across that
    candidate's whole career, per orcid_bulk.parquet's nested columns) and this ACIF's own set
    of eligible institution names (this project's HEP vocabulary, e.g. from admin_orgs.csv),
    narrow to the single ORCID-confirmed winner when exactly one candidate's career institution
    set overlaps -- never guesses on 0 or 2+ overlapping candidates."""
    matched = institution_matched_candidates(candidates, own_institution_names)
    if len(matched) == 1:
        return matched[0]["orcid"]
    return None


def get_record(orcid: str, cache=None, force: bool = False) -> OrcidRecord:
    """Fetch (cache-first) one ORCID's own live record, through this project's one existing
    ORCID /record store. `cache` defaults to orcid_client.default_cache() when not supplied --
    pass an already-open one to reuse a connection across many calls in a batch loop, matching
    the pattern 00b_enrich_orcid.py/04a_orcid_assist.py already use for that module directly."""
    cache = cache if cache is not None else orcid_client.default_cache()

    def _fetcher(o: str) -> dict:
        return orcid_client.fetch_orcid_record(o, cache, force=force)

    return get_or_fetch(orcid, cache=cache, fetcher=_fetcher, force=force)
