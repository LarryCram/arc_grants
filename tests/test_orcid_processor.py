"""
Tests for src/utils/orcid_processor.py -- OrcidProcessor (discover()/collapse_candidates()),
the records.jsonl.gz -> orcid_bulk.parquet conversion pipeline, and OrcidRecord/get_or_fetch()
(live-record retrieval, cache-first with an injected fetcher).

All fixtures are small, synthetic, and self-contained -- nothing here reads the real
orcid_bulk.parquet (17.15M rows) or hits the live ORCID API. orcid_processor.py's own
"standalone, project-agnostic" design is exercised directly: these tests use the bare
default_name_normalizer, never src/utils/names.py's HumanNameParser (that pairing is
orcid_processor_arc_adapter.py's own job, not this module's).
"""
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from src.utils.orcid_processor import (
    AffiliationEntry,
    NameForms,
    OrcidProcessor,
    OrcidRecord,
    convert_bulk_dump,
    default_name_normalizer,
    get_or_fetch,
    parse_bulk_record,
)


class TestDefaultNameNormalizer:
    def test_basic_given_family(self):
        nf = default_name_normalizer("David Smith")
        assert nf.given_tokens[0] == "david"
        assert nf.family_name_main == "smith"
        assert nf.first_name_canonical == "david"
        assert nf.full_name_key == "david_smith"

    def test_given_tokens_include_own_initial(self):
        # discover()'s bare-initial fallback depends on this: every given token's own initial
        # is itself present in given_tokens, so a family+initial match needs no separate logic.
        nf = default_name_normalizer("David Smith")
        assert "d" in nf.given_tokens

    def test_empty_string(self):
        nf = default_name_normalizer("")
        assert nf == NameForms((), None, None, None)

    def test_family_name_only_fallback(self):
        # No given name at all -- HumanName puts the bare word in .last already (not .first),
        # so no special-case fallback is even needed here (contrast with names.py's
        # HumanNameParser, which needs one for a title-prefixed "Dr. Smith").
        nf = default_name_normalizer("Smith")
        assert nf.family_name_main == "smith"

    def test_deterministic_tuples_not_sets(self):
        # This project's own already-fixed set()-iteration-order non-determinism bug --
        # confirm the bare default normalizer was built with the same discipline.
        results = {default_name_normalizer("Xiao Dong Chen").first_name_canonical for _ in range(20)}
        assert len(results) == 1
        assert isinstance(default_name_normalizer("Xiao Dong Chen").given_tokens, tuple)


class TestParseBulkRecord:
    def _rec(self, **overrides):
        base = {
            "orcid": "0000-0000-0000-0001",
            "name": "David Smith",
            "aliases": ["Dave Smith"],
            "locale": "en",
            "homepage": None,
            "emails": [],
            "countries": ["AU"],
            "keywords": [],
            "xrefs": {"scopus": "12345"},
            "employments": [
                {"name": "University of Queensland", "start": {"year": 2010}, "end": None,
                 "role": "Professor", "xrefs": {"ror": "https://ror.org/00rqy9422"}},
            ],
            "educations": [],
            "memberships": [],
            "works": [{"pubmed": "999"}],
        }
        base.update(overrides)
        return base

    def test_shape(self):
        row = parse_bulk_record(self._rec())
        assert row["orcid"] == "0000-0000-0000-0001"
        assert row["family_name_main"] == "smith"
        assert row["given_tokens"][0] == "david"
        assert row["full_name_key"] == "david_smith"
        assert row["employments"][0]["name"] == "University of Queensland"
        assert row["employments"][0]["end_year"] is None
        assert row["xref_keys"] == ["scopus"]
        assert row["xref_values"] == ["12345"]

    def test_alias_full_name_keys(self):
        row = parse_bulk_record(self._rec())
        assert row["alias_full_name_keys"] == ["dave_smith"]

    def test_no_name_at_all(self):
        row = parse_bulk_record(self._rec(name=None))
        assert row["family_name_main"] is None
        assert row["full_name_key"] is None

    def test_no_aliases(self):
        row = parse_bulk_record(self._rec(aliases=[]))
        assert row["alias_full_name_keys"] == []

    def test_missing_end_date_means_ongoing(self):
        row = parse_bulk_record(self._rec())
        assert row["employments"][0]["end_year"] is None


class TestConvertBulkDump:
    RECORDS = [
        {"orcid": "0000-0000-0000-0001", "name": "David Smith", "aliases": [],
         "employments": [{"name": "UQ", "start": {"year": 2010}, "end": None, "xrefs": {}}],
         "educations": [], "memberships": [], "works": [], "xrefs": {}},
        {"orcid": "0000-0000-0000-0002", "name": "Jane Doe", "aliases": ["J Doe"],
         "employments": [], "educations": [], "memberships": [], "works": [], "xrefs": {}},
    ]

    def _write_gz(self, tmp_path) -> str:
        src = tmp_path / "records.jsonl.gz"
        with gzip.open(src, "wt", encoding="utf-8") as f:
            for rec in self.RECORDS:
                f.write(json.dumps(rec) + "\n")
        return str(src)

    def test_round_trip(self, tmp_path):
        src = self._write_gz(tmp_path)
        out = str(tmp_path / "orcid_bulk.parquet")
        n = convert_bulk_dump(src=src, out=out, progress=False)
        assert n == 2

        import duckdb
        con = duckdb.connect()
        rows = con.execute(
            "SELECT orcid, family_name_main FROM read_parquet(?) ORDER BY orcid", [out]
        ).fetchall()
        assert rows == [
            ("0000-0000-0000-0001", "smith"),
            ("0000-0000-0000-0002", "doe"),
        ]

    def test_limit_truncates(self, tmp_path):
        src = self._write_gz(tmp_path)
        out = str(tmp_path / "orcid_bulk.parquet")
        n = convert_bulk_dump(src=src, out=out, limit=1, progress=False)
        assert n == 1

    def test_custom_normalizer_used(self, tmp_path):
        src = self._write_gz(tmp_path)
        out = str(tmp_path / "orcid_bulk.parquet")

        def upper_normalizer(raw_name):
            nf = default_name_normalizer(raw_name)
            return NameForms(nf.given_tokens, nf.family_name_main, nf.first_name_canonical,
                              (nf.full_name_key.upper() if nf.full_name_key else None))

        convert_bulk_dump(src=src, out=out, normalizer=upper_normalizer, progress=False)
        import duckdb
        con = duckdb.connect()
        keys = con.execute("SELECT full_name_key FROM read_parquet(?) ORDER BY orcid", [out]).fetchall()
        assert keys[0][0] == "DAVID_SMITH"


class TestOrcidProcessorDiscover:
    """Built on a small synthetic orcid_bulk.parquet -- never the real 17.15M-row table."""

    RECORDS = [
        {"orcid": "0000-0000-0000-0001", "name": "Simon Kelly", "aliases": [],
         "employments": [{"name": "University of Queensland", "start": {"year": 2005}, "end": None, "xrefs": {}}],
         "educations": [], "memberships": [], "works": [], "xrefs": {}},
        {"orcid": "0000-0000-0000-0002", "name": "Simon Kelly", "aliases": [],
         "employments": [{"name": "Macquarie University", "start": {"year": 2000}, "end": {"year": 2015}, "xrefs": {}}],
         "educations": [], "memberships": [], "works": [], "xrefs": {}},
        {"orcid": "0000-0000-0000-0003", "name": "William Cope", "aliases": [],
         "employments": [], "educations": [], "memberships": [], "works": [], "xrefs": {}},
        {"orcid": "0000-0000-0000-0004", "name": "Wendy Cope", "aliases": [],
         "employments": [], "educations": [], "memberships": [], "works": [], "xrefs": {}},
    ]

    @pytest.fixture
    def bulk_parquet(self, tmp_path):
        src = tmp_path / "records.jsonl.gz"
        with gzip.open(src, "wt", encoding="utf-8") as f:
            for rec in self.RECORDS:
                f.write(json.dumps(rec) + "\n")
        out = str(tmp_path / "orcid_bulk.parquet")
        convert_bulk_dump(src=str(src), out=out, progress=False)
        return out

    def test_full_name_key_match_multiple_candidates(self, bulk_parquet):
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            candidates = proc.discover("Simon", "Kelly")
        assert len(candidates) == 2
        assert {c["orcid"] for c in candidates} == {"0000-0000-0000-0001", "0000-0000-0000-0002"}

    def test_institution_names_attached(self, bulk_parquet):
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            candidates = proc.discover("Simon", "Kelly")
        by_orcid = {c["orcid"]: c for c in candidates}
        assert by_orcid["0000-0000-0000-0001"]["institution_names"] == ["University of Queensland"]
        assert by_orcid["0000-0000-0000-0002"]["institution_names"] == ["Macquarie University"]

    def test_bare_initial_fallback(self, bulk_parquet):
        # "W" alone doesn't form a full_name_key match against "William"/"Wendy" -- falls back
        # to family_name_main + initial, matching both.
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            candidates = proc.discover("W", "Cope")
        assert {c["orcid"] for c in candidates} == {"0000-0000-0000-0003", "0000-0000-0000-0004"}

    def test_no_match_returns_empty(self, bulk_parquet):
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            candidates = proc.discover("Nobody", "Nowhere")
        assert candidates == []

    def test_context_manager_closes_owned_connection(self, bulk_parquet):
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            pass
        with pytest.raises(Exception):
            proc.con.execute("SELECT 1")

    def test_injected_connection_not_closed(self, bulk_parquet):
        import duckdb
        con = duckdb.connect()
        with OrcidProcessor(con=con, bulk_parquet=bulk_parquet):
            pass
        # an injected connection is the caller's own responsibility -- still usable afterward
        assert con.execute("SELECT 1").fetchone() == (1,)
        con.close()


class TestLookupByOrcid:
    RECORDS = [
        {"orcid": "0000-0000-0000-0001", "name": "David Smith", "aliases": ["Dave Smith"],
         "employments": [], "educations": [], "memberships": [], "works": [], "xrefs": {}},
        {"orcid": "0000-0000-0000-0002", "name": "Jane Doe", "aliases": [],
         "employments": [], "educations": [], "memberships": [], "works": [], "xrefs": {}},
    ]

    @pytest.fixture
    def bulk_parquet(self, tmp_path):
        src = tmp_path / "records.jsonl.gz"
        with gzip.open(src, "wt", encoding="utf-8") as f:
            for rec in self.RECORDS:
                f.write(json.dumps(rec) + "\n")
        out = str(tmp_path / "orcid_bulk.parquet")
        convert_bulk_dump(src=str(src), out=out, progress=False)
        return out

    def test_returns_keyed_dict(self, bulk_parquet):
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            result = proc.lookup_by_orcid(["0000-0000-0000-0001", "0000-0000-0000-0002"])
        assert result["0000-0000-0000-0001"] == {"name": "David Smith", "aliases": ["Dave Smith"]}
        assert result["0000-0000-0000-0002"] == {"name": "Jane Doe", "aliases": []}

    def test_missing_orcid_absent_from_result(self, bulk_parquet):
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            result = proc.lookup_by_orcid(["0000-0000-0000-9999"])
        assert result == {}

    def test_empty_input_returns_empty_dict_no_query(self, bulk_parquet):
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            result = proc.lookup_by_orcid([])
        assert result == {}

    def test_partial_match(self, bulk_parquet):
        with OrcidProcessor(bulk_parquet=bulk_parquet) as proc:
            result = proc.lookup_by_orcid(["0000-0000-0000-0001", "0000-0000-0000-9999"])
        assert list(result.keys()) == ["0000-0000-0000-0001"]


class TestCollapseCandidates:
    def test_no_acif_orcid_leaves_unchanged(self):
        result = OrcidProcessor.collapse_candidates(None, ["a1", "a2"], {"a1": "X", "a2": "Y"})
        assert result == ["a1", "a2"]

    def test_exactly_one_match_collapses(self):
        result = OrcidProcessor.collapse_candidates(
            "0000-0001", ["a1", "a2", "a3"],
            {"a1": "0000-0001", "a2": "0000-9999", "a3": None},
        )
        assert result == ["a1"]

    def test_zero_matches_leaves_unchanged(self):
        result = OrcidProcessor.collapse_candidates(
            "0000-0001", ["a1", "a2"], {"a1": "0000-9998", "a2": "0000-9999"}
        )
        assert result == ["a1", "a2"]

    def test_two_plus_matches_leaves_unchanged_as_real_conflict(self):
        # two different oax candidates both claiming the same ACIF orcid -- a genuine conflict
        # to flag for review, never silently resolved here.
        result = OrcidProcessor.collapse_candidates(
            "0000-0001", ["a1", "a2"], {"a1": "0000-0001", "a2": "0000-0001"}
        )
        assert result == ["a1", "a2"]


class TestOrcidRecordFromRaw:
    RAW = {
        "person": {
            "name": {
                "given-names": {"value": "David"},
                "family-name": {"value": "Smith"},
                "credit-name": {"value": "Dave Smith"},
            },
            "other-names": {"other-name": [{"content": "D. J. Smith"}, {"content": None}]},
        },
        "activities-summary": {
            "employments": {"affiliation-group": [
                {"summaries": [{"employment-summary": {
                    "organization": {"name": "University of Queensland",
                                      "address": {"country": "AU"}},
                    "role-title": "Professor",
                    "start-date": {"year": {"value": "2010"}},
                    "end-date": None,
                }}]},
            ]},
            "educations": {"affiliation-group": []},
            "works": {"group": [
                {"work-summary": [{"publication-date": {"year": {"value": "2015"}}}]},
                {"work-summary": [{"publication-date": {"year": {"value": "2012"}}}]},
            ]},
        },
    }

    def test_parses_name(self):
        r = OrcidRecord.from_raw("0000-0001", self.RAW)
        assert r.given_names == "David"
        assert r.family_name == "Smith"
        assert r.credit_name == "Dave Smith"
        assert r.other_names == ("D. J. Smith",)
        assert r.error is None

    def test_parses_employments(self):
        r = OrcidRecord.from_raw("0000-0001", self.RAW)
        assert len(r.employments) == 1
        e = r.employments[0]
        assert isinstance(e, AffiliationEntry)
        assert e.organization == "University of Queensland"
        assert e.country == "AU"
        assert e.start_year == 2010
        assert e.end_year is None  # ongoing

    def test_empty_sections_are_empty_tuples_not_none(self):
        r = OrcidRecord.from_raw("0000-0001", self.RAW)
        assert r.educations == ()
        assert r.qualifications == ()

    def test_work_years_sorted_and_deduped(self):
        r = OrcidRecord.from_raw("0000-0001", self.RAW)
        assert r.work_years == (2012, 2015)

    def test_all_affiliations_flattens_every_section(self):
        r = OrcidRecord.from_raw("0000-0001", self.RAW)
        assert r.all_affiliations == r.employments  # only employments populated in this fixture

    def test_institution_names_property(self):
        r = OrcidRecord.from_raw("0000-0001", self.RAW)
        assert r.institution_names == ("University of Queensland",)

    def test_error_response_produces_error_record(self):
        r = OrcidRecord.from_raw("0000-0001", {"_error": 404})
        assert r.error == "404"
        assert r.given_names is None
        assert r.employments == ()

    def test_non_dict_response_produces_error_record(self):
        r = OrcidRecord.from_raw("0000-0001", None)
        assert r.error == "invalid_response"

    def test_missing_sections_do_not_raise(self):
        r = OrcidRecord.from_raw("0000-0001", {"person": {}, "activities-summary": {}})
        assert r.employments == ()
        assert r.work_years == ()
        assert r.given_names is None


class TestGetOrFetch:
    def test_cache_hit_no_fetcher_needed(self):
        cache = {"0000-0001": {"person": {"name": {"given-names": {"value": "David"},
                                                     "family-name": {"value": "Smith"}}},
                                "activities-summary": {}}}
        calls = []
        r = get_or_fetch("0000-0001", cache=cache, fetcher=lambda o: calls.append(o))
        assert r.given_names == "David"
        assert calls == []  # fetcher never invoked on a cache hit

    def test_cache_miss_calls_fetcher_and_populates_cache(self):
        cache: dict = {}
        raw = {"person": {"name": {"given-names": {"value": "Jane"},
                                    "family-name": {"value": "Doe"}}},
               "activities-summary": {}}
        r = get_or_fetch("0000-0002", cache=cache, fetcher=lambda o: raw)
        assert r.given_names == "Jane"
        assert cache["0000-0002"] == raw

    def test_force_refetches_even_on_cache_hit(self):
        cache = {"0000-0003": {"person": {"name": {"given-names": {"value": "Old"},
                                                     "family-name": {"value": "Name"}}},
                                "activities-summary": {}}}
        fresh = {"person": {"name": {"given-names": {"value": "New"},
                                      "family-name": {"value": "Name"}}},
                 "activities-summary": {}}
        r = get_or_fetch("0000-0003", cache=cache, fetcher=lambda o: fresh, force=True)
        assert r.given_names == "New"

    def test_no_cache_no_fetcher_raises(self):
        with pytest.raises(ValueError):
            get_or_fetch("0000-0004")

    def test_miss_no_fetcher_raises(self):
        cache: dict = {}
        with pytest.raises(ValueError):
            get_or_fetch("0000-0005", cache=cache)

    def test_fetcher_receives_the_orcid(self):
        received = []

        def fetcher(o):
            received.append(o)
            return {"person": {}, "activities-summary": {}}

        get_or_fetch("0000-0006", cache={}, fetcher=fetcher)
        assert received == ["0000-0006"]
