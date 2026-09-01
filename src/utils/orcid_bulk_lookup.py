"""
src/utils/orcid_bulk_lookup.py

Query the local Zenodo "Easy ORCID" bulk snapshot (orcid_persons.parquet +
orcid_affiliations.parquet, /home/lc/s/orcid/ -- see CLAUDE.md's "ORCID Public API
integration"/Zenodo dataset sections) for name+institution candidate matches. Built to
support the UNRESOLVED-cluster review: this local snapshot answers in milliseconds with
no rate limit, so it's the first thing to check before reaching for the live ORCID API
or asking for manual web verification -- not a replacement for either, since the
snapshot has real, known coverage gaps (see CLAUDE.md: per-record ROR grounding is
sparse; this is a frozen 2024 crawl, not live).

Institution matching is EXACT against org_name (case-insensitive), not substring
against every admin_orgs.csv alias -- an earlier loose-alias attempt this session
produced heavy false positives (a bare "University of Technology" alias for UTS
matching unrelated Chinese campuses). Callers should pass admin_orgs.csv's own
institution_name values (OpenAlex-style names like "UNSW Sydney"), which is what real
ORCID employment entries actually use -- not ARC's raw admin_org legal name.

CLI: .venv/bin/python -m src.utils.orcid_bulk_lookup "First" "Last" [institution ...]
"""
import sys

import duckdb

ORCID_PERSONS = "/home/lc/s/orcid/orcid_persons.parquet"
ORCID_AFFIL = "/home/lc/s/orcid/orcid_affiliations.parquet"


def find_candidates(first_name: str, family_name: str, institution_names: list[str] | None = None) -> list[dict]:
    """Return every local-snapshot person whose name or an alias matches
    "{first_name} {family_name}" exactly (case-insensitive), each with its full
    employment/education history and which entries (if any) match institution_names."""
    con = duckdb.connect()
    full = f"{first_name} {family_name}"
    rows = con.execute(
        """
        SELECT p.orcid, p.name, p.aliases, p.n_pubmed_works
        FROM read_parquet(?) p
        WHERE lower(p.name) = lower(?)
           OR list_contains(list_transform(p.aliases, x -> lower(x)), lower(?))
        """,
        [ORCID_PERSONS, full, full],
    ).fetchall()

    wanted = {i.lower() for i in institution_names} if institution_names else set()
    candidates = []
    for orcid, name, aliases, n_works in rows:
        affil = con.execute(
            """
            SELECT kind, org_name, start, "end", role
            FROM read_parquet(?)
            WHERE orcid = ?
            ORDER BY start
            """,
            [ORCID_AFFIL, orcid],
        ).fetchall()
        matched = [a for a in affil if a[1] and a[1].lower() in wanted]
        candidates.append({
            "orcid": orcid, "name": name, "aliases": list(aliases) if aliases is not None else [],
            "n_pubmed_works": n_works, "affiliations": affil, "matched_institutions": matched,
        })
    return candidates


def _name_matches(full_name: str, family_tokens: list[str], initial_lower: str) -> bool:
    """True if full_name's own trailing tokens equal family_tokens (handles simple compound
    family names, e.g. "van der Berg") and at least one of the remaining, leading tokens
    starts with initial_lower. Word-boundary-safe (checks whole tokens, not a raw string
    suffix) -- a naive `.endswith(family)` would wrongly match e.g. family="an" against
    "...tristan"."""
    tokens = full_name.lower().replace("-", " ").split()
    n = len(family_tokens)
    if len(tokens) <= n or tokens[-n:] != family_tokens:
        return False
    return any(t[:1] == initial_lower for t in tokens[:-n] if t)


def find_candidates_by_institution(family_name: str, first_initial: str,
                                    institution_names: list[str]) -> list[dict]:
    """Institution-first fallback for ARC records whose first_name is a bare initial (or
    otherwise doesn't match ORCID's own full given-name string) -- find_candidates()'s exact
    "{first} {family}" match structurally can't reach these, since ORCID accounts almost
    always carry a real given name, not a bare initial (2026-08-31, user-directed: "I am not
    sure about avoiding initial-only names... a matching name combined with an institution
    that is a member of the set of eligible orgs for the candidate's grants is a strong
    signal" -- most Australian academics active post-2015 have an institution-created ORCID).

    Queries by institution FIRST (narrows to a small candidate set before any name check),
    then filters to people whose own name/alias family-name tokens match family_name and
    whose given-name tokens include one starting with first_initial. Same conservatism as
    every other bulk-DB/institution-search path in this project: no ranking, no "closest
    match" -- callers should only trust this when it narrows to exactly one person."""
    if not institution_names:
        return []
    con = duckdb.connect()
    wanted = [i.lower() for i in institution_names]
    rows = con.execute(
        """
        SELECT DISTINCT p.orcid, p.name, p.aliases, p.n_pubmed_works
        FROM read_parquet(?) a
        JOIN read_parquet(?) p ON a.orcid = p.orcid
        WHERE lower(a.org_name) = ANY(?)
        """,
        [ORCID_AFFIL, ORCID_PERSONS, wanted],
    ).fetchall()

    fam_tokens = family_name.lower().replace("-", " ").split()
    init = first_initial.lower()
    candidates = []
    for orcid, name, aliases, n_works in rows:
        forms = [name] + (list(aliases) if aliases is not None else [])
        if not any(_name_matches(f, fam_tokens, init) for f in forms if f):
            continue
        affil = con.execute(
            """
            SELECT kind, org_name, start, "end", role
            FROM read_parquet(?)
            WHERE orcid = ?
            ORDER BY start
            """,
            [ORCID_AFFIL, orcid],
        ).fetchall()
        matched = [a for a in affil if a[1] and a[1].lower() in set(wanted)]
        candidates.append({
            "orcid": orcid, "name": name, "aliases": list(aliases) if aliases is not None else [],
            "n_pubmed_works": n_works, "affiliations": affil, "matched_institutions": matched,
        })
    return candidates


def fetch_by_orcid(orcids: list[str]):
    """Bulk KEYED lookup -- every given ORCID's own (name, aliases) from the local snapshot, in
    one query (~0.6s for this project's whole ~18K-ORCID population, confirmed by direct
    measurement). Not a search: this is for WIDENING the name-form evidence ARC already has for
    an ORCID it already claims (see awards_cif.py::widen_names_with_orcid_bulk_db()), not
    finding a candidate for one ARC is missing -- see find_candidates() for that direction.
    Returns a pandas DataFrame (orcid, name, aliases); an ORCID not present in the local
    snapshot simply doesn't appear in the result (a frozen 2024 crawl -- absence isn't evidence
    of anything, see this module's own docstring)."""
    con = duckdb.connect()
    return con.execute(
        "SELECT orcid, name, aliases FROM read_parquet(?) WHERE orcid = ANY(?)",
        [ORCID_PERSONS, orcids],
    ).df()


def main() -> None:
    if len(sys.argv) < 3:
        print('usage: python -m src.utils.orcid_bulk_lookup "First" "Last" [institution ...]')
        return
    first, family = sys.argv[1], sys.argv[2]
    insts = sys.argv[3:] or None
    candidates = find_candidates(first, family, insts)
    print(f"{len(candidates)} candidate(s) for '{first} {family}'" + (f" (checking against {insts})" if insts else ""))
    for c in candidates:
        tag = "  <-- INSTITUTION MATCH" if c["matched_institutions"] else ""
        print(f"\n{c['orcid']}  {c['name']}  aliases={c['aliases']}  pubmed_works={c['n_pubmed_works']}{tag}")
        for kind, org, start, end, role in c["affiliations"]:
            print(f"    {kind:10s} {org}  {start or '?'}-{end or 'present'}  {role or ''}")


if __name__ == "__main__":
    main()
