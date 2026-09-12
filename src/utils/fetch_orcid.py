"""
src/utils/fetch_orcid.py

FetchOrcid -- a standalone ORCID-lookup processor, composed on top of the pieces this session
(and the OrcidProcessor consolidation before it) already built and verified, not a reimplement:

  search_orcid()      -- normalised name structure (ParsedName, src/utils/names.py) -> candidate
                          ORCID(s), matched against orcid_bulk.parquet on EVERY normalized form
                          on both sides, not just one scalar key -- the multi-form improvement
                          over OrcidProcessor.discover() identified this session (Li Chen/L Chen,
                          Grun/Gruen: both given-token and family-name-diacritic variants are
                          tried, ASCII-reduced and raw forms unioned, never either/or). Local
                          data only, no network call.
  orcid_fetch_short()  -- one ORCID's own full row from the local orcid_bulk.parquet snapshot
                          (name, aliases, employments/educations/memberships, matching keys) --
                          no network call.
  orcid_fetch_long()   -- one ORCID's own live /record data (person, affiliations, work years),
                          cache-first then a live API call on miss -- reuses
                          orcid_processor_arc_adapter.get_record() (OrcidProcessor.get_or_fetch()
                          + orcid_client's own OAuth/HTTP/cache), not a second implementation.

Deliberately NOT wired into anything upstream of Splink: per this project's own established
principle (see docs/pipeline_todo.md and CLAUDE.md's "applying the information after ARC splink"
discussion), any ORCID this class finds is meant to be applied as a post-clustering promotion
(matching apply_enriched_orcids()'s existing pattern), never fed into primary blocking.
"""
import csv

import duckdb

from config.settings import ADMIN_ORGS_CSV, ORCID_BULK_PARQUET, PROCESSED_DATA
from src.utils import orcid_client
from src.utils.names import HumanNameParser, ParsedName
from src.utils.orcid_processor import OrcidRecord
from src.utils.orcid_processor_arc_adapter import get_record

ARC_ONLY_PARQUET_DEFAULT = PROCESSED_DATA / "awards_cif_arc_only.parquet"


class FetchOrcid:
    def __init__(self, con: duckdb.DuckDBPyConnection | None = None,
                 bulk_parquet: str = ORCID_BULK_PARQUET,
                 cache=None):
        self._owns_con = con is None
        self.con = con or duckdb.connect()
        self.bulk_parquet = bulk_parquet
        self._cache = cache  # None -> orcid_client.default_cache(), lazily, per get_record()
        self._au_institution_names: set[str] | None = None  # lazy, see _au_institution_name_set()
        self._table_ready = False  # lazy, see _ensure_table()
        self._parser = HumanNameParser()  # reused to re-derive a candidate's OWN middle_tokens

    def _ensure_table(self) -> None:
        """Materialize orcid_bulk.parquet into an in-memory DuckDB table once per instance,
        instead of every search_orcid()/orcid_fetch_short() call re-scanning the 17.15M-row,
        1.2GB parquet file from cold via read_parquet() -- measured directly: ~640ms/cluster
        (a full column scan + LIST<VARCHAR> decode of alias_full_name_keys, repeated per
        name-form, per fallback) against a real 4,891-cluster NO_ORCID batch, ~52 minutes total
        for one run. One in-memory load pays this cost exactly once per FetchOrcid instance.

        Also flattens all_full_name_keys (2026-09-06: orcid_bulk.parquet's own precomputed SET
        of every given x family combination the primary name AND every alias could produce --
        see orcid_processor.all_full_name_keys() -- not full_name_key's single "longest wins"
        pick) into its own one-row-per-key index table (orcid_key_index_tbl). Necessary for two
        reasons found building batch_search_orcids(): (1) `full_name_key = k.key OR
        list_contains(alias_full_name_keys, k.key)` can't be hash-joined -- the OR'd
        list_contains side has no equi-join DuckDB can plan against, so it falls back to a
        near-full-table scan per batch of keys (confirmed directly: 21s for a 5,030-key batch
        returning only 67K rows, versus 0.9s for a comparable equi-joinable query returning 1.2M
        rows); (2) matching should use the SAME full-set-vs-full-set principle on both sides, not
        a single scalar on the orcid side and a full set on the ACIF side -- flattening once here
        turns every key (primary or alias) into a plain equi-join target."""
        if not self._table_ready:
            self.con.execute(
                f"CREATE OR REPLACE TEMP TABLE orcid_bulk_tbl AS SELECT * FROM read_parquet('{self.bulk_parquet}')"
            )
            self.con.execute(
                "CREATE OR REPLACE TEMP TABLE orcid_key_index_tbl AS "
                "SELECT orcid, unnest(all_full_name_keys) AS key FROM orcid_bulk_tbl "
                "WHERE len(all_full_name_keys) > 0"
            )
            self._table_ready = True

    def close(self) -> None:
        if self._owns_con:
            self.con.close()

    def __enter__(self) -> "FetchOrcid":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # search_orcid: normalised name structure -> candidate ORCID(s)
    # ------------------------------------------------------------------

    def search_orcid(self, parsed: ParsedName, au_only: bool = False,
                      require_middle_match: bool = False) -> list[dict]:
        """Every orcid_bulk.parquet candidate matching ANY normalized form of `parsed` --
        ASCII-reduced given_tokens x family_names combinations, unioned (not either/or, per
        this project's own "lists are additive, never collapsed" rule) with the raw
        (non-ASCII-reducible) form. Checked against the ORCID side's own full_name_key AND
        alias_full_name_keys, so a known alias on either side can still connect. ALWAYS also
        runs the family_name_main + given-initial fallback and unions it in (deduped by orcid) --
        fixed 2026-09-05, this used to run only when the exact-key pass found nothing, which
        made it possible for one spurious exact match (e.g. someone's own canonical name really
        does reduce to a bare "d_craig") to silently hide every candidate the broader fallback
        would have found. The two passes are not one a superset of the other either direction --
        an alias match ("Bob" for "Robert") can hit where the initial-fallback (keyed off the
        PRIMARY name's own initials) misses, and vice versa -- so this is a real completeness
        fix, not just a safety margin. Returns [] if nothing matches -- never guesses.

        Every candidate carries an `au_signal` bool (see _au_signal()) -- ARC investigators are
        HEP-affiliated Australians by construction, so Australian-ness is real, available
        selectivity for exactly the common-initial/common-surname ambiguity this project has
        repeatedly hit (e.g. "L. Craig" -> 44 same-initial candidates worldwide, only 1 with any
        AU signal at all). `au_only=True` filters to that subset -- still returns [] rather than
        guessing if none carry the signal, since ORCID's own self-reported `countries` field is
        real but sparse (13% filled population-wide) and an employment-name match only fires when
        that specific candidate's career happens to include one of ARC's own 42 HEP institutions
        by its exact OpenAlex-convention name -- absence of the signal is not evidence of a
        non-Australian person, just of missing self-reported data.

        Every candidate also carries a `middle_match` bool | None (see _middle_match()) -- a
        SECOND, independent narrowing dimension from au_signal, for exactly the case au_signal
        can't help with: two same-surname, same-first-initial candidates who are BOTH Australian.
        `require_middle_match=True` excludes only a genuine CONTRADICTION (both sides have a
        middle name and they differ, middle_match=False) -- it does NOT require a positive match
        (middle_match=True). Requiring a positive match would be a materially different, stricter
        filter that silently discards every candidate whose own record simply never had a middle
        name entered -- absence of a middle name on their side is not evidence it differs from
        ours, the same "absence isn't evidence" principle as au_signal. Confirmed concretely:
        searching "David L Craig" against the 17-candidate "David Craig" pool correctly keeps the
        matching "David L. Craig" (True) and the 13 candidates with no middle name on file
        (None), and excludes only the ones with a DIFFERENT recorded middle initial like
        "David W Craig"/"David A. Craig" (False)."""
        self._ensure_table()
        keys = self._candidate_full_name_keys(parsed)
        by_orcid: dict[str, dict] = {}
        if keys:
            rows = self.con.execute(
                """
                SELECT o.orcid, o.name, o.countries, o.given_tokens,
                       o.employments, o.educations, o.memberships
                FROM (SELECT DISTINCT unnest(?) AS key) k
                JOIN orcid_key_index_tbl i ON i.key = k.key
                JOIN orcid_bulk_tbl o ON o.orcid = i.orcid
                """,
                [keys],
            ).fetchall()
            for r in rows:
                c = self._to_candidate(r, parsed.middle_tokens)
                by_orcid[c["orcid"]] = c
        for c in self._search_by_family_and_initials(parsed):
            by_orcid.setdefault(c["orcid"], c)
        candidates = list(by_orcid.values())
        if au_only:
            candidates = [c for c in candidates if c["au_signal"]]
        if require_middle_match and parsed.middle_tokens:
            candidates = [c for c in candidates if c["middle_match"] is not False]
        return candidates

    def batch_search_orcids(self, cluster_names: dict[str, list[str]],
                             au_only: bool = False) -> dict[str, list[dict]]:
        """The batched equivalent of calling search_orcid() once per cluster per name-form --
        joins against orcid_bulk_tbl on the DISTINCT key vocabulary only, then fans results out
        to clusters in Python, rather than one query per cluster. Per direct instruction
        (2026-09-04, this project's own session history): "make an sql to query all the names in
        [the whole population], and capture the joins against the orcid [table]... in one
        query" -- search_orcid() was built as a single-name convenience method and this
        project's own population-scale audit tools then called it in a Python loop across
        ~18,000 clusters, exactly the per-row pattern that instruction was given to avoid.

        Joining on (cluster_id, key) pairs directly -- the first attempt at this -- is NOT
        the fix: a common surname (`family_name_main='wang'` alone matches 239,315 rows in
        orcid_bulk.parquet, confirmed directly) shared across many ARC clusters would fan out
        combinatorially (matching rows x how many clusters share that key), producing a WORSE
        result than the per-cluster loop, not a better one -- confirmed by that exact version
        hanging on a 2,000-cluster sample. The fix: join on the distinct (family, initial) /
        exact-key vocabulary alone (bounded by how many distinct forms exist, independent of how
        many clusters share one), then look up each cluster's own keys in the resulting
        dict -- pure Python dict access, no further DB work.

        `middle_match` is not computed here (it needs a per-candidate reparse keyed to each
        specific ACIF's own middle_tokens, which doesn't batch the same way) -- callers needing
        it should use search_orcid() directly on the small number of candidates this returns,
        not re-derive it from this method's output.
        """
        self._ensure_table()
        parser = HumanNameParser()

        cluster_exact_keys: dict[str, set[str]] = {}
        cluster_fb_keys: dict[str, set[tuple[str, str]]] = {}
        all_exact_keys: set[str] = set()
        all_fb_keys: set[tuple[str, str]] = set()

        for cluster_id, full_names in cluster_names.items():
            ek: set[str] = set()
            fbk: set[tuple[str, str]] = set()
            for fn in full_names:
                p = parser.parse(fn)
                ek.update(self._candidate_full_name_keys(p))
                family_all = list(dict.fromkeys(
                    list(p.family_names) + ([p.family_name_raw] if p.family_name_raw else [])
                ))
                initials = list(dict.fromkeys(t[:1] for t in p.given_tokens + p.nickname_tokens if t))
                fbk.update((fam, init) for fam in family_all for init in initials)
            cluster_exact_keys[cluster_id] = ek
            cluster_fb_keys[cluster_id] = fbk
            all_exact_keys.update(ek)
            all_fb_keys.update(fbk)

        key_to_candidates: dict[str, dict[str, dict]] = {}
        if all_exact_keys:
            keys_list = list(all_exact_keys)
            # One equi-join against the flattened all_full_name_keys index -- not an OR'd
            # list_contains() (can't be hash-joined, see _ensure_table()'s docstring) and not
            # two separate queries against full_name_key/alias_full_name_keys either, now that
            # orcid_key_index_tbl already carries every key (primary AND alias) that matters.
            rows = self.con.execute(
                """
                SELECT k.key, o.orcid, o.name, o.countries, o.given_tokens,
                       o.employments, o.educations, o.memberships
                FROM (SELECT unnest(?) AS key) k
                JOIN orcid_key_index_tbl i ON i.key = k.key
                JOIN orcid_bulk_tbl o ON o.orcid = i.orcid
                """,
                [keys_list],
            ).fetchall()
            for key, *row in rows:
                c = self._to_candidate(tuple(row))
                key_to_candidates.setdefault(key, {})[c["orcid"]] = c

        fb_to_candidates: dict[tuple[str, str], dict[str, dict]] = {}
        if all_fb_keys:
            fams = [k[0] for k in all_fb_keys]
            inits = [k[1] for k in all_fb_keys]
            rows = self.con.execute(
                """
                SELECT f.family, f.initial, o.orcid, o.name, o.countries, o.given_tokens,
                       o.employments, o.educations, o.memberships
                FROM (SELECT unnest(?) AS family, unnest(?) AS initial) f
                JOIN orcid_bulk_tbl o
                  ON o.family_name_main = f.family
                 AND (list_contains(o.given_tokens, f.initial) OR list_contains(o.nickname_tokens, f.initial))
                """,
                [fams, inits],
            ).fetchall()
            for fam, init, *row in rows:
                c = self._to_candidate(tuple(row))
                fb_to_candidates.setdefault((fam, init), {})[c["orcid"]] = c

        out: dict[str, list[dict]] = {}
        for cluster_id in cluster_names:
            merged: dict[str, dict] = {}
            for k in cluster_exact_keys[cluster_id]:
                merged.update(key_to_candidates.get(k, {}))
            for fbk in cluster_fb_keys[cluster_id]:
                for orcid, c in fb_to_candidates.get(fbk, {}).items():
                    merged.setdefault(orcid, c)
            candidates = list(merged.values())
            if au_only:
                candidates = [c for c in candidates if c["au_signal"]]
            out[cluster_id] = candidates
        return out

    @staticmethod
    def _candidate_full_name_keys(parsed: ParsedName) -> list[str]:
        # nickname_tokens included alongside given_tokens (2026-09-08) -- a real nickname
        # ("Jenny" for "Yingzi") is exactly the kind of alternate form this combinatorial search
        # exists to catch; no raw-path counterpart exists for nicknames (ParsedName keeps
        # nickname_tokens ASCII-only), so it's unioned into the ASCII side only, same as
        # given_tokens itself.
        given_all = list(dict.fromkeys(
            list(parsed.given_tokens) + list(parsed.nickname_tokens) + list(parsed.given_tokens_raw)
        ))
        family_all = list(dict.fromkeys(
            list(parsed.family_names) + ([parsed.family_name_raw] if parsed.family_name_raw else [])
        ))
        combos = [f"{g}_{f}" for g in given_all for f in family_all if g and f]
        extra = [k for k in (parsed.full_name_key, parsed.full_name_key_raw) if k]
        return list(dict.fromkeys(combos + extra))

    def _search_by_family_and_initials(self, parsed: ParsedName) -> list[dict]:
        family_all = list(dict.fromkeys(
            list(parsed.family_names) + ([parsed.family_name_raw] if parsed.family_name_raw else [])
        ))
        initials = list(dict.fromkeys(t[:1] for t in parsed.given_tokens + parsed.nickname_tokens if t))
        if not family_all or not initials:
            return []
        self._ensure_table()
        rows = self.con.execute(
            """
            SELECT orcid, name, countries, given_tokens, employments, educations, memberships
            FROM orcid_bulk_tbl
            WHERE family_name_main = ANY(?)
              AND (list_has_any(given_tokens, ?) OR list_has_any(nickname_tokens, ?))
            """,
            [family_all, initials, initials],
        ).fetchall()
        return [self._to_candidate(r, parsed.middle_tokens) for r in rows]

    def _to_candidate(self, row: tuple, middle_tokens: tuple[str, ...] = ()) -> dict:
        orcid, name, countries, given_tokens, employments, educations, memberships = row
        institution_names = list(dict.fromkeys(
            e["name"] for group in (employments, educations, memberships)
            for e in (group or []) if e.get("name")
        ))
        countries = list(countries) if countries is not None else []
        return {
            "orcid": orcid,
            "name": name,
            "institution_names": institution_names,
            "countries": countries,
            "au_signal": self._au_signal(countries, institution_names),
            "middle_match": self._middle_match(middle_tokens, name),
        }

    def _middle_match(self, middle_tokens: tuple[str, ...], candidate_name: str | None) -> bool | None:
        """Three-valued, deliberately NOT "does my middle token appear anywhere in the
        candidate's flat given_tokens" (that conflates "confirmed different" with "this
        candidate's record simply never recorded a middle name" -- the same "absence isn't
        evidence" mistake au_signal already avoids). Re-parses the candidate's own `name` string
        with this project's own HumanNameParser to get ITS OWN middle_tokens -- the same
        first-vs-middle distinction just fixed for the search side -- so the three real cases can
        actually be told apart:
          - we have no middle_tokens of our own -> None, nothing to check
          - the candidate has no middle name of ITS OWN on file -> None, absence isn't evidence
          - both sides have one and they overlap -> True, real corroboration
          - both sides have one and they don't overlap -> False, a genuine contradiction
        `require_middle_match=True` in search_orcid() excludes only this last case."""
        if not middle_tokens:
            return None
        candidate_middle = self._parser.parse(candidate_name).middle_tokens if candidate_name else ()
        if not candidate_middle:
            return None
        return any(t in candidate_middle for t in middle_tokens)

    def _au_institution_name_set(self) -> set[str]:
        """Australia's own ARC-eligible HEP institution names (admin_orgs.csv, HEP=='y',
        institution_name column -- OpenAlex's own naming convention, e.g. "UNSW Sydney"), the
        exact vocabulary 00b_enrich_orcid.py's _search_by_institution() already matches ORCID
        employment-name strings against. Loaded once, lazily, cached on the instance."""
        if self._au_institution_names is None:
            names = set()
            with open(ADMIN_ORGS_CSV, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if (row.get("HEP") or "").strip() == "y":
                        iname = (row.get("institution_name") or "").strip()
                        if iname:
                            names.add(iname.lower())
            self._au_institution_names = names
        return self._au_institution_names

    def _au_signal(self, countries: list[str], institution_names: list[str]) -> bool:
        """True if this candidate's own self-reported `countries` includes AU, or any career
        institution name exactly matches (case-insensitive) a known ARC-eligible HEP name. Two
        independent, real signals -- neither one is dense enough alone (countries: 13% filled
        population-wide; institution-name: only an exact-string hit, no fuzzy/alias matching,
        per this project's own prior finding that loose institution-alias matching produces
        heavy false positives)."""
        if countries and "AU" in countries:
            return True
        au_names = self._au_institution_name_set()
        return any((n or "").lower() in au_names for n in institution_names)

    # ------------------------------------------------------------------
    # orcid_fetch_short: local bulk-table row, no network call
    # ------------------------------------------------------------------

    def orcid_fetch_short(self, orcid: str) -> dict | None:
        """One ORCID's full orcid_bulk.parquet row (every column), or None if this ORCID isn't
        in the local snapshot. Local data only -- for the live /record, see orcid_fetch_long()."""
        self._ensure_table()
        row = self.con.execute(
            "SELECT * FROM orcid_bulk_tbl WHERE orcid = ?",
            [orcid],
        ).fetchone()
        if row is None:
            return None
        cols = [d[0] for d in self.con.description]
        return dict(zip(cols, row))

    # ------------------------------------------------------------------
    # orcid_fetch_long: cache-first, live API on miss
    # ------------------------------------------------------------------

    def orcid_fetch_long(self, orcid: str, force: bool = False) -> OrcidRecord:
        """One ORCID's own live /record (person, affiliations, work years) -- cache-first, a
        live API call only on a cache miss. Reuses orcid_processor_arc_adapter.get_record()
        (OrcidProcessor.get_or_fetch() + orcid_client's OAuth/HTTP/retry + its one existing
        DISKCACHE_DIR/orcid_records_authenticated store), not a second cache or fetch path."""
        return get_record(orcid, cache=self._cache, force=force)


# ---------------------------------------------------------------------------
# Audit: run every ACIF's own name-form(s) through search_orcid() and compare against
# whatever this project has already recorded for it -- 2026-09-05, prompted by the Jocelyn
# Craig case (a fresh au_only=True search on ANY of that ACIF's own name-forms would have
# surfaced 0000-0001-9723-7255 as an "extra candidate" alongside the recorded
# 0000-0002-8288-3307, exactly the signal this reports).
#
# Works for NO_ORCID (recorded_orcids == []) and HAS_ORCID/MULTI_ORCID populations alike -- for
# a NO_ORCID cluster, `recorded_missing_from_search`/`extra_candidates` are trivially
# uninteresting (nothing recorded to check), so n_candidates (0/1/2+) is the whole story; for a
# HAS_ORCID cluster the recorded-vs-found comparison is the real point: a recorded orcid that
# never turns up under any of this ACIF's own name-forms, or other candidates turning up
# alongside it, are both real "other forms of error" worth a human look -- neither is proof of a
# mistake by itself (au_signal is real but partial; a sparse-`countries` real person exists),
# but both are exactly the shape of case this session's Craig investigation found by hand.
# ---------------------------------------------------------------------------

def load_cluster_rows(
    orcid_status: str | None = None,
    parquet_path=ARC_ONLY_PARQUET_DEFAULT,
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[tuple[str, list[str], list[str]]]:
    """(cluster_id, full_names, orcids) for every non-excluded ACIF in awards_cif_arc_only.parquet,
    optionally restricted to one orcid_status ('HAS_ORCID' | 'NO_ORCID' | 'MULTI_ORCID')."""
    own_con = con is None
    con = con or duckdb.connect()
    try:
        where = "WHERE excluded = False"
        params = [str(parquet_path)]
        if orcid_status:
            where += " AND orcid_status = ?"
            params.append(orcid_status)
        return con.execute(
            f"""
            SELECT cluster_id, full_names, orcids
            FROM read_parquet(?)
            {where}
            ORDER BY cluster_id
            """,
            params,
        ).fetchall()
    finally:
        if own_con:
            con.close()


def audit_clusters(fo: "FetchOrcid", rows: list[tuple[str, list[str], list[str]]],
                    au_only: bool = True) -> list[dict]:
    """For each (cluster_id, full_names, recorded_orcids), union search_orcid() candidates
    across every one of the ACIF's own recorded name-forms, then compare against
    `recorded_orcids`. Never picks a winner -- reports the raw comparison for a human/caller to
    act on, same discipline as search_orcid() itself."""
    parser = HumanNameParser()
    results = []
    for cluster_id, full_names, recorded_orcids in rows:
        found: set[str] = set()
        for fn in full_names:
            parsed = parser.parse(fn)
            for c in fo.search_orcid(parsed, au_only=au_only):
                found.add(c["orcid"])
        recorded = set(recorded_orcids or [])
        results.append({
            "cluster_id": cluster_id,
            "recorded_orcids": sorted(recorded),
            "candidates": sorted(found),
            "n_candidates": len(found),
            "recorded_missing_from_search": sorted(recorded - found),
            "extra_candidates": sorted(found - recorded),
        })
    return results


def reverse_lookup_orcid_name(fo: "FetchOrcid", orcid: str) -> dict:
    """Given an orcid already recorded on an ACIF, look it up the OTHER way -- by orcid, not by
    name -- and return its own name-object, so it can be compared against the ACIF's own name
    forms directly. Local bulk snapshot first (orcid_fetch_short(); already normalized the
    identical way as the ACIF side, via arc_name_normalizer, so full_name_key/
    alias_full_name_keys are directly comparable); a live /record fetch (cache-or-API) only on a
    local miss -- distinguishes "not in our 17.15M-record snapshot" from "genuinely unreachable"
    rather than conflating them, since a live-record hit there means the orcid is real and
    current, just newer than (or otherwise missing from) the local crawl."""
    short = fo.orcid_fetch_short(orcid)
    if short is not None:
        return {
            "source": "bulk",
            "name": short["name"],
            "full_name_key": short["full_name_key"],
            "alias_full_name_keys": list(short["alias_full_name_keys"] or []),
            "family_name_main": short["family_name_main"],
        }
    rec = fo.orcid_fetch_long(orcid)
    if rec.error:
        return {"source": "unreachable", "error": rec.error, "name": None,
                "full_name_key": None, "alias_full_name_keys": [], "family_name_main": None}
    forms = [n for n in (
        [f"{rec.given_names or ''} {rec.family_name or ''}".strip()]
        + ([rec.credit_name] if rec.credit_name else [])
        + list(rec.other_names)
    ) if n and n.strip()]
    keys: list[str] = []
    family_mains: set[str] = set()
    for f in forms:
        p = fo._parser.parse(f)
        keys.extend(k for k in (p.full_name_key, p.full_name_key_raw) if k)
        if p.family_name_main:
            family_mains.add(p.family_name_main)
    keys = list(dict.fromkeys(keys))
    return {
        "source": "live",
        "name": f"{rec.given_names or ''} {rec.family_name or ''}".strip() or None,
        "full_name_key": keys[0] if keys else None,
        "alias_full_name_keys": keys[1:],
        "family_name_main": sorted(family_mains) or None,
    }


def au_signal_for_recorded_orcid(fo: "FetchOrcid", orcid: str) -> tuple[str, bool | None]:
    """Checks AU/HEP signal for an already-recorded orcid DIRECTLY by ID (bulk row, then a live
    /record fetch) -- not via name search. Built 2026-09-06 after confirming, case by case
    (Yong Wang, Jie Wang, Rahul Sharma, Pengtang Wang), that ORCID's own search/crawl coverage
    has real gaps even for fully legitimate, richly-documented accounts -- "not found by name
    search" is not evidence against a recorded orcid, so triaging the 384-case
    recorded-orcid-not-found population needs a check that doesn't depend on name search at
    all. Reuses the exact same au_signal logic search_orcid() already applies to candidates,
    against the one specific known ID instead of a candidate pool.

    Returns (source, au_signal): source is "bulk" (in orcid_bulk.parquet), "live" (fetched via
    cache-or-API), or "unreachable" (neither -- au_signal is None in that case, not False, since
    there's nothing to check)."""
    short = fo.orcid_fetch_short(orcid)
    if short is not None:
        countries = list(short["countries"] or [])
        institution_names = list(dict.fromkeys(
            e["name"] for group in (short["employments"], short["educations"], short["memberships"])
            for e in (group or []) if e.get("name")
        ))
        return "bulk", fo._au_signal(countries, institution_names)
    rec = fo.orcid_fetch_long(orcid)
    if rec.error:
        return "unreachable", None
    countries = [a.country for a in rec.all_affiliations if a.country]
    return "live", fo._au_signal(countries, list(rec.institution_names))


def classify_name_compatibility(acif_own_keys: set[str], orcid_obj: dict) -> str:
    """Closes the loop a raw "not found" row can't answer on its own: is a recorded-but-
    unmatched orcid a real wrong-orcid signal, a matching-tool gap, or just a coverage/staleness
    gap in the local snapshot?
      - "orcid_unreachable": no bulk row, no live record either (deactivated, never existed, or
        a transient fetch error) -- can't classify further.
      - "name_compatible_key_match": the orcid's own full_name_key or an alias key exactly
        matches one of the ACIF's own keys -- the forward search SHOULD have found this; a hit
        here on the *bulk* source is a real search-logic bug worth chasing, while a hit on the
        *live* source just means the bulk snapshot itself doesn't carry this alias/form.
      - "family_compatible_given_mismatch": same surname, different given name -- plausibly a
        genuine name-form gap (a nickname/married-name our matching doesn't bridge), not
        necessarily a wrong orcid.
      - "name_incompatible": different surname entirely -- the strongest available red flag that
        the recorded orcid does not belong to this ACIF (the Jocelyn Craig signature).
    """
    if orcid_obj.get("source") == "unreachable":
        return "orcid_unreachable"
    orcid_keys = {k for k in ([orcid_obj.get("full_name_key")] + list(orcid_obj.get("alias_full_name_keys") or [])) if k}
    if orcid_keys & acif_own_keys:
        return "name_compatible_key_match"
    acif_families = {k.rsplit("_", 1)[-1] for k in acif_own_keys if "_" in k}
    fam = orcid_obj.get("family_name_main")
    orcid_families = set(fam) if isinstance(fam, (list, set)) else ({fam} if fam else set())
    if acif_families & orcid_families:
        return "family_compatible_given_mismatch"
    return "name_incompatible"


def audit_with_reverse_lookup(fo: "FetchOrcid", rows: list[tuple[str, list[str], list[str]]],
                               au_only: bool = False) -> list[dict]:
    """audit_clusters() plus, for every recorded orcid the forward search misses, the reverse
    lookup + classification above -- one pass, so the expensive forward search never has to run
    twice. Returns one row per (cluster_id, missing_orcid) pair -- clusters with nothing missing
    contribute no rows."""
    parser = HumanNameParser()
    out = []
    for cluster_id, full_names, recorded_orcids in rows:
        found: set[str] = set()
        own_keys: set[str] = set()
        for fn in full_names:
            parsed = parser.parse(fn)
            own_keys.update(k for k in (parsed.full_name_key, parsed.full_name_key_raw) if k)
            for c in fo.search_orcid(parsed, au_only=au_only):
                found.add(c["orcid"])
        for orcid in sorted(set(recorded_orcids or []) - found):
            obj = reverse_lookup_orcid_name(fo, orcid)
            out.append({
                "cluster_id": cluster_id,
                "full_names": list(full_names),
                "recorded_orcid": orcid,
                "orcid_source": obj["source"],
                "orcid_name": obj.get("name"),
                "orcid_full_name_key": obj.get("full_name_key"),
                "classification": classify_name_compatibility(own_keys, obj),
            })
    return out


def summarize_audit(results: list[dict], has_recorded: bool) -> dict:
    """n_candidates 0/1/2+ counts always; for a HAS_ORCID-style population also counts how many
    clusters have a recorded orcid the fresh search never found (`recorded_missing_from_search`)
    and how many have other plausible candidates besides the recorded one (`extra_candidates`)."""
    buckets: dict = {0: 0, 1: 0, "2+": 0}
    unconfirmed = 0
    has_extra = 0
    for r in results:
        buckets[r["n_candidates"] if r["n_candidates"] <= 1 else "2+"] += 1
        if has_recorded:
            if r["recorded_missing_from_search"]:
                unconfirmed += 1
            if r["extra_candidates"]:
                has_extra += 1
    out = {
        "total": len(results),
        "n_candidates_0": buckets[0],
        "n_candidates_1": buckets[1],
        "n_candidates_2plus": buckets["2+"],
    }
    if has_recorded:
        out["recorded_orcid_not_found_by_search"] = unconfirmed
        out["extra_candidates_besides_recorded"] = has_extra
    return out


if __name__ == "__main__":
    import sys
    import time

    status = sys.argv[1] if len(sys.argv) > 1 else "NO_ORCID"
    out_csv = sys.argv[2] if len(sys.argv) > 2 else f"/tmp/fetch_orcid_audit_{status}.csv"
    # au_only defaults True (candidate-narrowing use case) -- pass "0"/"false" as a 3rd arg for
    # the existence-check use case (does a RECORDED orcid turn up at all), where au_only=True
    # systematically inflates "not found" whenever the recorded orcid's own record simply lacks
    # AU signal (sparse `countries`, no exact HEP-name employment match) even though the
    # unfiltered search does find it -- confirmed 2026-09-05 on the NO_ORCID population (81.9% of
    # "0 candidates" cases actually had one once au_only was dropped).
    au_only = len(sys.argv) <= 3 or sys.argv[3].lower() not in ("0", "false", "no")

    rows = load_cluster_rows(orcid_status=None if status == "ALL" else status)
    print(f"{len(rows)} {status} non-excluded ACIFs to audit (au_only={au_only})", flush=True)

    t0 = time.time()
    with FetchOrcid() as fo:
        fo._ensure_table()
        results = audit_clusters(fo, rows, au_only=au_only)
    print(f"done in {time.time()-t0:.0f}s")

    print("SUMMARY:", summarize_audit(results, has_recorded=(status != "NO_ORCID")))

    fieldnames = ["cluster_id", "recorded_orcids", "candidates", "n_candidates",
                  "recorded_missing_from_search", "extra_candidates"]
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow({
                **r,
                "recorded_orcids": ";".join(r["recorded_orcids"]),
                "candidates": ";".join(r["candidates"]),
                "recorded_missing_from_search": ";".join(r["recorded_missing_from_search"]),
                "extra_candidates": ";".join(r["extra_candidates"]),
            })
    print(f"wrote -> {out_csv}")
