"""Trove's 1900 placeholder ('19uu' issued as 1900) is dropped from a thesis's years; a real 1900 date is kept."""

import importlib

m = importlib.import_module("src.00i_extract_trove_theses")


def _version(issued, date):
    return {"issued": issued, "record": [{"metadata": {"dc": {"date": date}}}]}


def test_placeholder_1900_dropped():
    w = {"issued": "1900-2017", "version": [_version(["2017", "1991"], ["2017-05-11T02:31:41Z", "1991"]),
                                            _version(["1900"], [{"value": "19uu"}])]}
    assert min(m.work_years(w)) == 1991 and max(m.work_years(w)) == 2017


def test_real_1900_kept():
    w = {"issued": "1900", "version": [_version(["1900"], [{"value": "1900"}])]}
    assert m.work_years(w) == [1900]
