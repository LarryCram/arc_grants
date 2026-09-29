"""
Tests for src/00a_extract_arc.py::extract_investigators() -- the only place an ARC name is parsed.
Pure function over one grant's attributes dict, so tested with hand-built fixtures.

An announcement name and a current name are the same investigator when NameParser() gives them
the same id or overlapping full_name_keys (one-to-one only); anything else is an addition or
deletion. A "one dropped + one added = rename" rule that ignored the names merged different
people (e.g. "Jessica Hyles" / "Ben Trevaskis") and was removed 2026-09-29.
"""
import importlib.util
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_spec = importlib.util.spec_from_file_location(
    "extract_arc", Path(__file__).resolve().parents[1] / "src" / "00a_extract_arc.py"
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


def _inv(first, family, role="CI", orcid=None):
    return {"firstName": first, "familyName": family, "roleCode": role, "orcidIdentifier": orcid}


def _run(ann, curr):
    return _mod.extract_investigators(
        {"investigators-at-announcement": ann, "investigators-current": curr}, "G1"
    )


class TestExtractInvestigators:
    def test_same_name_in_both_lists_is_one_row(self):
        inv, names, renames = _run([_inv("Jan", "de Gier")], [_inv("Jan", "de Gier")])
        assert [r["unique_id"] for r in inv] == ["G1_jan_de gier"]
        assert names[0]["in_announcement"] and names[0]["in_current"]
        assert renames == []

    def test_overlapping_keys_merge_as_rename(self):
        inv, names, renames = _run([_inv("Mahmuda", "Akhtar")], [_inv("M. Shumi", "Akhtar")])
        assert [r["unique_id"] for r in inv] == ["G1_shumi_akhtar"]
        assert inv[0]["first_name"] == "M. Shumi"  # current form wins
        assert {"mahmuda_akhtar", "shumi_akhtar"} <= set(names[0]["full_name_keys"])
        assert names[0]["renamed_from"] == "G1_mahmuda_akhtar"
        assert len(renames) == 1 and renames[0]["shared_keys"] == ["m_akhtar"]

    def test_spelling_variant_merges(self):
        inv, _, renames = _run([_inv("David", "StJohn")], [_inv("David", "St John")])
        assert len(inv) == 1 and len(renames) == 1

    def test_different_people_not_merged(self):
        inv, _, renames = _run([_inv("Jessica", "Hyles")], [_inv("Ben", "Trevaskis")])
        assert len(inv) == 2 and renames == []

    def test_ambiguous_overlap_left_unmerged(self):
        # "Chris Clark" overlaps both current names (c_clark) -- not guessed
        inv, _, renames = _run(
            [_inv("Chris", "Clark")], [_inv("Christopher", "Clark"), _inv("Colin", "Clark")]
        )
        assert len(inv) == 3 and renames == []

    def test_current_role_wins_announcement_orcid_wins(self):
        inv, _, _ = _run(
            [_inv("Jan", "de Gier", role="CI", orcid="0000-0000-0000-0001")],
            [_inv("Jan", "de Gier", role="FT", orcid="0000-0000-0000-0002")],
        )
        assert inv[0]["role_code"] == "FT"
        assert inv[0]["orcid"] == "0000-0000-0000-0001"

    def test_compound_surname_kept_whole(self):
        inv, _, _ = _run([], [_inv("Beatriz", "Prieto Simon")])
        assert inv[0]["unique_id"] == "G1_beatriz_prieto simon"
