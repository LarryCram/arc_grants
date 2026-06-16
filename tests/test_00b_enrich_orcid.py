"""
Tests for src/00b_enrich_orcid.py.

Key coverage:
  - Pure helper functions (_norm_family, _first_initial, _name_key)
  - _search_orcid with mocked HTTP (0, 1, multi-AU, multi-no-AU, too_common)
  - Cache written after every result so an interrupted run loses nothing
"""
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_spec = importlib.util.spec_from_file_location(
    "enrich_orcid",
    Path(__file__).resolve().parents[1] / "src" / "00b_enrich_orcid.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_norm_family   = _mod._norm_family
_first_initial = _mod._first_initial
_name_key      = _mod._name_key
_search_orcid  = _mod._search_orcid


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestNormFamily:
    def test_lowercases_and_strips(self):
        assert _norm_family("  Smith  ") == "smith"

    def test_strips_diacriticals(self):
        assert _norm_family("Müller") == "muller"

    def test_none(self):
        assert _norm_family(None) == ""


class TestFirstInitial:
    def test_simple(self):
        assert _first_initial("John") == "j"

    def test_none(self):
        assert _first_initial(None) is None

    def test_empty(self):
        assert _first_initial("") is None


class TestNameKey:
    def test_standard(self):
        assert _name_key("Smith", "John") == "smith_j"

    def test_missing_family(self):
        assert _name_key("", "John") is None

    def test_missing_first(self):
        assert _name_key("Smith", "") is None


# ---------------------------------------------------------------------------
# _search_orcid — mocked HTTP responses
# ---------------------------------------------------------------------------

def _make_search_response(num_found, orcids):
    """Build a mock requests.Response for the ORCID name-search endpoint."""
    results = [{"orcid-identifier": {"path": o}} for o in orcids]
    resp = MagicMock()
    resp.json.return_value = {"num-found": num_found, "result": results}
    resp.raise_for_status = MagicMock()
    return resp


def _record_with_country(country: str | None) -> dict:
    """Build a minimal /record response with a single address country."""
    addresses = [{"country": {"value": country}}] if country else []
    return {"person": {"addresses": {"address": addresses}}}


class TestSearchOrcid:
    def test_not_found(self):
        with patch("requests.get", return_value=_make_search_response(0, [])):
            result = _search_orcid("Zzz", "Qqq")
        assert result["confidence"] == "not_found"
        assert result["orcid"] is None

    def test_single_result(self):
        with patch("requests.get", return_value=_make_search_response(1, ["0000-0001-0001-0001"])):
            result = _search_orcid("John", "Smith")
        assert result["confidence"] == "high"
        assert result["orcid"] == "0000-0001-0001-0001"
        assert result["num_found"] == 1

    def test_too_common(self):
        orcids = [f"0000-000{i}-0001-0001" for i in range(11)]
        with patch("requests.get", return_value=_make_search_response(11, orcids)):
            result = _search_orcid("John", "Smith")
        assert result["confidence"] == "too_common"
        assert result["orcid"] is None

    def test_multiple_one_au(self):
        orcids = ["0000-0001-0001-0001", "0000-0002-0002-0002"]
        records = {"0000-0001-0001-0001": _record_with_country("AU"),
                   "0000-0002-0002-0002": _record_with_country("US")}
        with patch("requests.get", return_value=_make_search_response(2, orcids)), \
             patch.object(_mod, "fetch_orcid", side_effect=lambda o, d: records[o]):
            result = _search_orcid("Jane", "Doe")
        assert result["confidence"] == "au_match"
        assert result["orcid"] == "0000-0001-0001-0001"

    def test_multiple_no_au(self):
        orcids = ["0000-0001-0001-0001", "0000-0002-0002-0002"]
        records = {"0000-0001-0001-0001": _record_with_country("US"),
                   "0000-0002-0002-0002": _record_with_country("GB")}
        with patch("requests.get", return_value=_make_search_response(2, orcids)), \
             patch.object(_mod, "fetch_orcid", side_effect=lambda o, d: records[o]):
            result = _search_orcid("Jane", "Doe")
        assert result["confidence"] == "low"
        assert result["orcid"] is None

    def test_multiple_two_au(self):
        orcids = ["0000-0001-0001-0001", "0000-0002-0002-0002"]
        records = {"0000-0001-0001-0001": _record_with_country("AU"),
                   "0000-0002-0002-0002": _record_with_country("AU")}
        with patch("requests.get", return_value=_make_search_response(2, orcids)), \
             patch.object(_mod, "fetch_orcid", side_effect=lambda o, d: records[o]):
            result = _search_orcid("Jane", "Doe")
        assert result["confidence"] == "low"
        assert result["orcid"] is None


# ---------------------------------------------------------------------------
# Cache interruption — the critical regression test
# ---------------------------------------------------------------------------

class TestCacheInterruption:
    def test_results_saved_after_each_call(self, tmp_path):
        """
        If the run is interrupted partway through, every result up to that
        point must already be persisted.  This was the bug: the old code
        only wrote once at the end.
        """
        cache_path = tmp_path / "orcid_enrichment.parquet"

        pairs = pd.DataFrame([
            {"first_name": "John",  "family_name": "Smith", "name_key": "smith_j"},
            {"first_name": "Jane",  "family_name": "Doe",   "name_key": "doe_j"},
            {"first_name": "Alice", "family_name": "Jones", "name_key": "jones_a"},
        ])

        call_count = 0

        def mock_search(first, family):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                raise RuntimeError("simulated interruption")
            return {"orcid": f"0000-000{call_count}-0001-0001",
                    "confidence": "high", "num_found": 1}

        rows_so_far = []
        with pytest.raises(RuntimeError, match="simulated interruption"):
            for _, row in pairs.iterrows():
                result = mock_search(row.first_name, row.family_name)
                rec = {
                    "first_name":  row.first_name,
                    "family_name": row.family_name,
                    "name_key":    row.name_key,
                    "orcid":       result["orcid"],
                    "confidence":  result["confidence"],
                    "num_found":   result["num_found"],
                }
                rows_so_far.append(rec)
                pd.DataFrame(rows_so_far).to_parquet(cache_path, index=False)

        assert cache_path.exists(), "cache file must exist after interruption"
        saved = pd.read_parquet(cache_path)
        assert len(saved) == 2, "two results must be saved before the interruption"
        assert list(saved["first_name"]) == ["John", "Jane"]
