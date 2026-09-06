"""
src/utils/orcid_processor.py

OrcidProcessor -- consolidates ORCID processing that was scattered across
00b_enrich_orcid.py, orcid_bulk_lookup.py, and ad hoc scratch scripts during the 2026-09-01
NO_ORCID-population investigation (see docs/pipeline_todo.md #19 for the full incident this
replaces, and the "oeuvre QA: cleaning order" plan's item 12).

Two jobs, cleanly separated as methods on one class:

  discover()           -- PRE-LINK, local-data-only: find name-matched ORCID candidates for an
                           ARC cluster, from `orcid_bulk.parquet` (17.15M people, built from
                           the full Zenodo "Easy ORCID" records.jsonl.gz -- name, aliases,
                           dated employments/educations/memberships, all in one table; strictly
                           subsumes the now-retired `orcid.db`). Pure name matching -- returns
                           every candidate with its whole career's institution names attached as
                           plain data, but does NOT itself reduce that to a single winner via
                           institution overlap. That reduction is deliberately the CALLER's job
                           (see orcid_processor_arc_adapter.py's resolve_institution_overlap()
                           for this project's own version) -- a real match there is a set-to-set
                           comparison against a caller-specific institution vocabulary (this
                           project's HEP set), which a general-purpose ORCID package shouldn't
                           have an opinion on. 2026-09-02 design review, in full: an earlier
                           version of this method did the institution reduction itself against
                           `orcid.db`'s single current `ror` field -- both the "who reduces"
                           question and the single-`ror` incompleteness (confirmed against real
                           cases: Simon Kelly's real UQ role was invisible via `orcid.db` alone,
                           only found through fuller employment history) were fixed together.

  collapse_candidates() -- POST-LINK: an ACIF already resolved via ORCID (whether through
                           04_resolve_links.py's own `orcid` step or trivially because there
                           was only one high-confidence candidate to begin with) often still
                           carries its full, unpruned oax_candidates pool. Given the ACIF's
                           recorded orcid and a author_idx -> orcid lookup for its candidate
                           pool, collapse to the single ORCID-confirmed winner when there is
                           exactly one -- never silently resolves a genuine conflict (0 or 2+
                           matches leaves the pool untouched).

Discovery is conservative by construction, matching every other local-ORCID lookup in this
project: no ranking, no "closest match" -- a candidate set only resolves when it narrows to
exactly one person, otherwise it's left for review.

Fetching a *specific, already-identified* ORCID's own record (once discover() + the caller's
own institution reduction have picked a winner) is a different concern from discovery, and is
expected to use the live ORCID API as the normal path (neither orcid_bulk.parquet nor any local
source carries a real publication list) -- not yet built here, see docs/pipeline_todo.md #19's
own item list for what's still outstanding.

2026-09-01/02: `records.jsonl.gz` (Zenodo record 13333068, concept DOI
10.5281/zenodo.10137939) confirmed via a real full pass (17,152,673 records, 46s to stream)
to carry the same rich per-record schema (aliases, dated employments/educations/memberships,
works) at the FULL population scale that only the narrower ~4.8M "HQ" subset used to have.
`orcid_bulk.parquet` (this module's own `convert_bulk_dump()` output) is built from it, using
this project's own `HumanNameParser`-based normalizer (injected via
orcid_processor_arc_adapter.py, not imported directly here) for its matching-key columns, so
the ORCID-side keys are computed with the exact same NFC/NFKC/zero-width/postnominal-strip/
diacritic-widening hardening as the ARC and OAX sides, not the bare fallback default.
"""
import gzip
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from nameparser import HumanName

BULK_DUMP_DEFAULT = "/home/lc/s/orcid/records.jsonl.gz"
BULK_PARQUET_DEFAULT = "/home/lc/s/orcid/orcid_bulk.parquet"


# ---------------------------------------------------------------------------
# Name normalization -- pluggable. The default here is a bare, project-agnostic
# HumanName parse (genuinely reusable out of the box for a standalone ORCID library).
# A caller wanting exact parity with a specific project's own name-comparison convention
# (postnominal stripping, diacritic widening, etc.) injects its own normalizer instead --
# see src/utils/orcid_processor_arc_adapter.py for this project's own exact chain.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class NameForms:
    given_tokens: tuple[str, ...]
    family_name_main: str | None
    first_name_canonical: str | None
    full_name_key: str | None
    # The full family-name variant set (e.g. diacritic-widened forms: {"gruen","grun"}), not
    # just family_name_main's own single "longest wins" pick -- added 2026-09-06 because
    # collapsing this to one scalar at conversion time (the original design) threw away
    # information no downstream consumer could ever recover, the same "pick one from an
    # equally-plausible set" mistake this project has repeatedly found and fixed elsewhere
    # (family_name_main/max_by_len() contamination, first_name_canonical). Defaults to a
    # single-element tuple of family_name_main for normalizers that don't compute variants.
    family_names: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self):
        if not self.family_names and self.family_name_main:
            object.__setattr__(self, "family_names", (self.family_name_main,))


def all_full_name_keys(nf: NameForms) -> list[str]:
    """Every given x family combination this NameForms could produce, order-preserving
    deduped -- the SET of all possible full names, not one collapsed representative. Mirrors
    FetchOrcid._candidate_full_name_keys()'s ACIF-side logic exactly, so both sides of a match
    are built the same way."""
    combos = [f"{g}_{f}" for g in nf.given_tokens for f in nf.family_names if g and f]
    extra = [nf.full_name_key] if nf.full_name_key else []
    return list(dict.fromkeys(combos + extra))


def default_name_normalizer(raw_name: str) -> NameForms:
    """Bare nameparser.HumanName parse, no project-specific customization. Order-preserving
    dedup throughout (dict.fromkeys(), never a raw set()) -- this project already found and
    fixed a real non-determinism bug from set()-based dedup flipping results between runs."""
    if not raw_name:
        return NameForms((), None, None, None)
    hn = HumanName(raw_name)
    if not hn.last and hn.first:
        hn.last = hn.first
    given_raw = [t for t in (hn.first.lower().split() if hn.first else []) +
                 (hn.middle.lower().split() if hn.middle else [])]
    given_tokens = tuple(dict.fromkeys(given_raw + [t[0] for t in given_raw if t]))
    family_name_main = hn.last.lower() if hn.last else None
    full_toks = [t for t in given_tokens if len(t) > 1]
    first_name_canonical = (max(full_toks, key=len) if full_toks
                             else (given_tokens[0] if given_tokens else None))
    full_name_key = (f"{first_name_canonical}_{family_name_main}"
                      if first_name_canonical and family_name_main else None)
    return NameForms(given_tokens, family_name_main, first_name_canonical, full_name_key)


# ---------------------------------------------------------------------------
# Bulk dump conversion: records.jsonl.gz -> one nested-column parquet table
# ---------------------------------------------------------------------------

def _year(date_block: dict | None) -> int | None:
    return (date_block or {}).get("year")


def _affil_list(entries: list[dict] | None) -> list[dict]:
    """Shared shape for employments/educations/memberships -- each entry in the bulk dump
    carries name/start/end/role/xrefs (ror/grid/ringgold/lei/funderregistry), confirmed
    directly against real records.jsonl.gz rows (2026-09-01)."""
    out = []
    for e in entries or []:
        xrefs = e.get("xrefs") or {}
        out.append({
            "name": e.get("name"),
            "start_year": _year(e.get("start")),
            "end_year": _year(e.get("end")),
            "role": e.get("role"),
            "ror": xrefs.get("ror"),
            "grid": xrefs.get("grid"),
            "ringgold": xrefs.get("ringgold"),
        })
    return out


def parse_bulk_record(rec: dict, normalizer: Callable[[str], NameForms] = default_name_normalizer) -> dict:
    """One records.jsonl.gz row -> one flat dict ready for pyarrow, nested-column employments/
    educations/memberships/works, plus HumanName-derived matching columns for the primary name
    AND every alias (so any known name form is equally matchable, not just the primary one).

    all_full_name_keys is the SET of every given x family combination the primary name and every
    alias could produce (all_full_name_keys(), same combinatorial logic as
    FetchOrcid._candidate_full_name_keys() uses on the ACIF side) -- not full_name_key's single
    "longest wins" pick. Both are kept: full_name_key/family_name_main stay as convenient single
    scalars for display and TF-style weighting; all_full_name_keys is what matching should
    actually join against, precomputed once here rather than re-derived (or worse, silently
    collapsed to one value) at query time."""
    name = rec.get("name")
    aliases = list(rec.get("aliases") or [])
    primary = normalizer(name) if name else NameForms((), None, None, None)
    all_keys: list[str] = list(all_full_name_keys(primary))
    for a in aliases:
        if a:
            all_keys.extend(all_full_name_keys(normalizer(a)))
    all_keys = list(dict.fromkeys(all_keys))
    alias_keys = list(dict.fromkeys(
        nf.full_name_key for a in aliases if a and (nf := normalizer(a)).full_name_key
    ))
    xrefs = rec.get("xrefs") or {}
    return {
        "orcid": rec["orcid"],
        "name": name,
        "aliases": aliases,
        "locale": rec.get("locale"),
        "homepage": rec.get("homepage"),
        "emails": list(rec.get("emails") or []),
        "countries": list(rec.get("countries") or []),
        "keywords": list(rec.get("keywords") or []),
        "xref_keys": list(xrefs.keys()),
        "xref_values": [str(v) for v in xrefs.values()],
        "employments": _affil_list(rec.get("employments")),
        "educations": _affil_list(rec.get("educations")),
        "memberships": _affil_list(rec.get("memberships")),
        "works": [{"pubmed": w.get("pubmed")} for w in (rec.get("works") or [])],
        "given_tokens": list(primary.given_tokens),
        "family_name_main": primary.family_name_main,
        "first_name_canonical": primary.first_name_canonical,
        "full_name_key": primary.full_name_key,
        "alias_full_name_keys": alias_keys,
        "all_full_name_keys": all_keys,
    }


_AFFIL_STRUCT = pa.struct([
    ("name", pa.string()), ("start_year", pa.int32()), ("end_year", pa.int32()),
    ("role", pa.string()), ("ror", pa.string()), ("grid", pa.string()), ("ringgold", pa.string()),
])
BULK_SCHEMA = pa.schema([
    ("orcid", pa.string()),
    ("name", pa.string()),
    ("aliases", pa.list_(pa.string())),
    ("locale", pa.string()),
    ("homepage", pa.string()),
    ("emails", pa.list_(pa.string())),
    ("countries", pa.list_(pa.string())),
    ("keywords", pa.list_(pa.string())),
    ("xref_keys", pa.list_(pa.string())),
    ("xref_values", pa.list_(pa.string())),
    ("employments", pa.list_(_AFFIL_STRUCT)),
    ("educations", pa.list_(_AFFIL_STRUCT)),
    ("memberships", pa.list_(_AFFIL_STRUCT)),
    ("works", pa.list_(pa.struct([("pubmed", pa.string())]))),
    ("given_tokens", pa.list_(pa.string())),
    ("family_name_main", pa.string()),
    ("first_name_canonical", pa.string()),
    ("full_name_key", pa.string()),
    ("alias_full_name_keys", pa.list_(pa.string())),
    ("all_full_name_keys", pa.list_(pa.string())),
])


def convert_bulk_dump(src: str = BULK_DUMP_DEFAULT, out: str = BULK_PARQUET_DEFAULT,
                       normalizer: Callable[[str], NameForms] = default_name_normalizer,
                       batch_size: int = 200_000, limit: int | None = None,
                       progress: bool = True) -> int:
    """Stream records.jsonl.gz -> one nested-column parquet file (BULK_SCHEMA), batched to
    bound memory. `limit` truncates early for sampling/testing. Returns row count written."""
    writer = pq.ParquetWriter(out, BULK_SCHEMA, compression="zstd")
    batch: list[dict] = []
    n = 0
    t0 = time.time()
    try:
        with gzip.open(src, "rt", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                batch.append(parse_bulk_record(rec, normalizer))
                n += 1
                if len(batch) >= batch_size:
                    writer.write_table(pa.Table.from_pylist(batch, schema=BULK_SCHEMA))
                    batch = []
                    if progress:
                        print(f"  ...{n:,}  ({time.time()-t0:.0f}s)", flush=True)
                if limit and n >= limit:
                    break
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=BULK_SCHEMA))
    finally:
        writer.close()
    if progress:
        print(f"DONE: {n:,} rows -> {out}  ({time.time()-t0:.0f}s)")
    return n


class OrcidProcessor:
    def __init__(self, con: duckdb.DuckDBPyConnection | None = None,
                 name_normalizer: Callable[[str], NameForms] = default_name_normalizer,
                 bulk_parquet: str = BULK_PARQUET_DEFAULT):
        self._owns_con = con is None
        self.con = con or duckdb.connect()
        self.name_normalizer = name_normalizer
        self.bulk_parquet = bulk_parquet

    def close(self) -> None:
        if self._owns_con:
            self.con.close()

    def __enter__(self) -> "OrcidProcessor":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Pre-link discovery
    # ------------------------------------------------------------------

    def discover(self, first_name: str, family_name: str) -> list[dict]:
        """Pure name matching against orcid_bulk.parquet. Returns every name-matched candidate
        as {"orcid", "name", "institution_names"} -- institution_names is every employer/
        education/membership org name found across that candidate's whole recorded career, not
        reduced to a single winner here (see this module's own docstring for why that reduction
        is deliberately the caller's job). Empty list if nothing matches -- never guesses."""
        parsed = self.name_normalizer(f"{first_name} {family_name}".strip())
        candidates: list[dict] = []
        if parsed.full_name_key:
            candidates = self._match_by_full_name_key(parsed.full_name_key)
        if not candidates and parsed.family_name_main:
            initial = parsed.first_name_canonical[:1] if parsed.first_name_canonical else None
            candidates = self._match_by_family_and_initial(parsed.family_name_main, initial)
        return candidates

    def _match_by_full_name_key(self, key: str) -> list[dict]:
        rows = self.con.execute(
            """
            SELECT orcid, name, employments, educations, memberships
            FROM read_parquet(?)
            WHERE full_name_key = ? OR list_contains(alias_full_name_keys, ?)
            """,
            [self.bulk_parquet, key, key],
        ).fetchall()
        return [self._to_candidate(r) for r in rows]

    def _match_by_family_and_initial(self, family_name_main: str, initial: str | None) -> list[dict]:
        """Bare-initial-safe fallback (an ARC record with only "A Ng", not a full given name) --
        matches on orcid_bulk.parquet's own precomputed given_tokens column, which already
        includes every given-name token's own initial as a separate entry (see
        parse_bulk_record()), so no separate string-matching logic is needed here."""
        if not initial:
            return []
        rows = self.con.execute(
            """
            SELECT orcid, name, employments, educations, memberships
            FROM read_parquet(?)
            WHERE family_name_main = ? AND list_contains(given_tokens, ?)
            """,
            [self.bulk_parquet, family_name_main, initial],
        ).fetchall()
        return [self._to_candidate(r) for r in rows]

    @staticmethod
    def _to_candidate(row: tuple) -> dict:
        orcid, name, employments, educations, memberships = row
        institution_names = list(dict.fromkeys(
            e["name"] for group in (employments, educations, memberships)
            for e in (group or []) if e.get("name")
        ))
        return {"orcid": orcid, "name": name, "institution_names": institution_names}

    # ------------------------------------------------------------------
    # Keyed lookup -- widening name-form evidence for an ORCID already in hand
    # ------------------------------------------------------------------

    def lookup_by_orcid(self, orcids: list[str]) -> dict[str, dict]:
        """Bulk KEYED lookup against orcid_bulk.parquet -- every given ORCID's own (name,
        aliases), in one query (~0.6s for this project's whole ~18K-ORCID population, per the
        equivalent orcid_bulk_lookup.fetch_by_orcid() this supersedes). Not a search: this is for
        WIDENING name-form evidence already tied to a specific ORCID (a caller's own
        awards_cif.py::widen_names_with_orcid_bulk_db(), e.g.), not discovering a candidate for
        a name that's missing one -- see discover() for that direction. Returns
        {orcid: {"name": str|None, "aliases": list[str]}}; an ORCID absent from
        orcid_bulk.parquet simply doesn't appear in the result (absence isn't evidence of
        anything -- a frozen snapshot, not a live source)."""
        if not orcids:
            return {}
        rows = self.con.execute(
            "SELECT orcid, name, aliases FROM read_parquet(?) WHERE orcid = ANY(?)",
            [self.bulk_parquet, orcids],
        ).fetchall()
        return {
            orcid: {"name": name, "aliases": list(aliases) if aliases is not None else []}
            for orcid, name, aliases in rows
        }

    # ------------------------------------------------------------------
    # Post-link cleanup
    # ------------------------------------------------------------------

    @staticmethod
    def collapse_candidates(acif_orcid: str | None, oax_candidates: list[str],
                             oax_orcid_by_id: dict[str, str | None]) -> list[str]:
        """Collapse oax_candidates to the single ORCID-confirmed winner when the ACIF's own
        recorded orcid matches exactly one candidate's own OpenAlex orcid field. Returns
        oax_candidates unchanged if there's no ACIF orcid, no match, or 2+ matches (a real
        conflict to flag for review, never something to silently resolve here)."""
        if not acif_orcid:
            return oax_candidates
        matches = [c for c in oax_candidates if oax_orcid_by_id.get(c) == acif_orcid]
        if len(matches) == 1:
            return matches
        return oax_candidates


# ---------------------------------------------------------------------------
# OrcidRecord -- a specific, already-identified ORCID's own live /record data.
#
# Distinct from discover()/orcid_bulk.parquet above: discovery finds WHO a name might be from a
# frozen bulk snapshot; this is fetching the full, current record for an ORCID already settled on
# (by discover() + the caller's own institution reduction, or by any other means -- an ARC-
# recorded ORCID, a manual override, ...). Neither orcid_bulk.parquet nor any local source carries
# a real publication list, so the live API is the normal path here, not a fallback.
#
# Parsing logic (the section/summary-key walk, _year()) is ported from this project's own
# src/utils/orcid_client.py accessors -- same shape, same conventions (end_year=None means
# current/ongoing) -- but re-implemented standalone here rather than imported, since
# orcid_client.py pulls in project-specific config.settings (DISKCACHE_DIR/ORCID_CLIENT_ID/
# ORCID_CLIENT_SECRET) that this module must stay free of. The live-fetch call itself (OAuth,
# HTTP) is therefore NOT built here either -- get_or_fetch() takes `fetcher` as an injected
# callable instead, so orcid_processor_arc_adapter.py can wire in orcid_client.py's own
# get_access_token()/requests.get() logic (reusing it, not duplicating it a second time) without
# this module ever importing requests/diskcache/config.settings directly.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AffiliationEntry:
    organization: str | None
    country: str | None
    role_title: str | None
    start_year: int | None
    end_year: int | None  # None means current/ongoing


@dataclass(frozen=True)
class OrcidRecord:
    orcid: str
    given_names: str | None = None
    family_name: str | None = None
    credit_name: str | None = None
    other_names: tuple[str, ...] = field(default_factory=tuple)
    employments: tuple[AffiliationEntry, ...] = field(default_factory=tuple)
    educations: tuple[AffiliationEntry, ...] = field(default_factory=tuple)
    qualifications: tuple[AffiliationEntry, ...] = field(default_factory=tuple)
    distinctions: tuple[AffiliationEntry, ...] = field(default_factory=tuple)
    invited_positions: tuple[AffiliationEntry, ...] = field(default_factory=tuple)
    memberships: tuple[AffiliationEntry, ...] = field(default_factory=tuple)
    services: tuple[AffiliationEntry, ...] = field(default_factory=tuple)
    work_years: tuple[int, ...] = field(default_factory=tuple)
    error: str | None = None  # set (all other fields default) when the raw fetch itself failed

    @property
    def all_affiliations(self) -> tuple[AffiliationEntry, ...]:
        """Every affiliation-shaped section flattened into one tuple, order preserved
        (employments first, matching orcid_client.py's own orcid_all_affiliations() key order)."""
        return (
            self.employments + self.educations + self.qualifications + self.distinctions
            + self.invited_positions + self.memberships + self.services
        )

    @property
    def institution_names(self) -> tuple[str, ...]:
        """Every distinct organization name across every affiliation section, order-preserving
        (dict.fromkeys(), not set()) -- the same shape OrcidProcessor._to_candidate() attaches to
        a discover() candidate, so a caller can run resolve_institution_overlap()-style reasoning
        against a live-fetched record exactly the way it does against a bulk-snapshot one."""
        return tuple(dict.fromkeys(a.organization for a in self.all_affiliations if a.organization))

    @classmethod
    def from_raw(cls, orcid: str, raw: dict) -> "OrcidRecord":
        """Parse one ORCID /record response (the exact shape fetch_orcid_record() in
        orcid_client.py returns) into an OrcidRecord. `raw` carrying {"_error": ...} (that
        module's own failure convention) produces an OrcidRecord with only `orcid`/`error` set."""
        if not isinstance(raw, dict) or "_error" in raw:
            return cls(orcid=orcid, error=str(raw.get("_error")) if isinstance(raw, dict) else "invalid_response")

        def _year(date_block: dict | None) -> int | None:
            if not date_block:
                return None
            y = date_block.get("year") or {}
            val = y.get("value") if isinstance(y, dict) else None
            return int(val) if val else None

        def _affiliation_entries(section: str, summary_key: str) -> tuple[AffiliationEntry, ...]:
            out = []
            try:
                groups = raw["activities-summary"][section]["affiliation-group"]
            except (KeyError, TypeError):
                return ()
            for group in groups:
                for summary in group.get("summaries", []):
                    s = summary.get(summary_key, {})
                    org = s.get("organization", {}) or {}
                    out.append(AffiliationEntry(
                        organization=org.get("name"),
                        country=(org.get("address") or {}).get("country"),
                        role_title=s.get("role-title"),
                        start_year=_year(s.get("start-date")),
                        end_year=_year(s.get("end-date")),
                    ))
            return tuple(out)

        try:
            name = raw["person"]["name"] or {}
        except (KeyError, TypeError):
            name = {}
        given = (name.get("given-names") or {}).get("value")
        family = (name.get("family-name") or {}).get("value")
        credit = (name.get("credit-name") or {}).get("value")
        other = []
        try:
            for o in raw["person"]["other-names"]["other-name"]:
                v = (o or {}).get("content")
                if v:
                    other.append(v)
        except (KeyError, TypeError):
            pass

        work_years = set()
        try:
            groups = raw["activities-summary"]["works"]["group"]
        except (KeyError, TypeError):
            groups = []
        for group in groups:
            for summary in group.get("work-summary", []):
                y = _year(summary.get("publication-date"))
                if y:
                    work_years.add(y)

        return cls(
            orcid=orcid,
            given_names=given,
            family_name=family,
            credit_name=credit,
            other_names=tuple(dict.fromkeys(other)),
            employments=_affiliation_entries("employments", "employment-summary"),
            educations=_affiliation_entries("educations", "education-summary"),
            qualifications=_affiliation_entries("qualifications", "qualification-summary"),
            distinctions=_affiliation_entries("distinctions", "distinction-summary"),
            invited_positions=_affiliation_entries("invited-positions", "invited-position-summary"),
            memberships=_affiliation_entries("memberships", "membership-summary"),
            services=_affiliation_entries("services", "service-summary"),
            work_years=tuple(sorted(work_years)),
        )


def get_or_fetch(
    orcid: str,
    cache=None,
    fetcher: Callable[[str], dict] | None = None,
    force: bool = False,
) -> OrcidRecord:
    """Cache-first, fetch-on-miss retrieval of one ORCID's own live record.

    `cache` -- anything supporting `orcid in cache` / `cache[orcid]` / `cache[orcid] = raw`
    (a diskcache.Cache satisfies this directly; orcid_processor_arc_adapter.py passes
    orcid_client.default_cache()'s own cache, so this project's records stay in its one existing
    store -- no new/second cache is introduced by this module).
    `fetcher` -- callable(orcid) -> raw /record dict, called only on a cache miss (or when
    force=True). orcid_processor_arc_adapter.py wires this to a thin wrapper over
    orcid_client.fetch_orcid_record(), reusing its OAuth/HTTP/retry logic rather than
    reimplementing a second copy of it here.

    Raises ValueError if neither a cache hit nor a fetcher is available -- there is then no way
    to produce a record at all, which should fail loudly rather than return a silently-empty one.
    """
    raw = None
    if cache is not None and not force and orcid in cache:
        raw = cache[orcid]
    elif fetcher is not None:
        raw = fetcher(orcid)
        if cache is not None:
            cache[orcid] = raw
    else:
        raise ValueError(f"No cached record for {orcid} and no fetcher provided")
    return OrcidRecord.from_raw(orcid, raw)
