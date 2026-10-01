"""
Tests for src/00a_extract_arc.py::extract_investigators() -- the only place an ARC name is parsed.
Pure function over one grant's attributes dict, so tested with hand-built fixtures.

An announcement name and a current name are one investigator when NameParser() gives them the same
id, or (one-to-one) an 'add' override, the same ARC ORCID, overlapping full_name_keys, or the same
first or family name;
a 'no' override blocks a join. Also covers the overrides file and the ORCID key-sharing step.
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


def _run(ann, curr, overrides=None):
    gn = _mod.extract_investigators(
        {"investigators-at-announcement": ann, "investigators-current": curr}, "G1", overrides
    )
    return gn.investigators, gn.names, gn.renames


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
        assert renames[0]["rule"] == "automatic" and names[0]["rename_rule"] == "automatic"

    def test_spelling_variant_joined_by_first_name(self):
        # "StJohn"/"St John" parse to different keys (the parser adds no separator equivalence),
        # but share the first name, one-to-one -- joined by first_or_last.
        inv, _, renames = _run([_inv("David", "StJohn")], [_inv("David", "St John")])
        assert [r["unique_id"] for r in inv] == ["G1_david_st john"]
        assert renames[0]["rule"] == "first_or_last"

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

    def test_blank_given_name_joins_same_family_on_other_list(self):
        # DP0210314: blank given name at announcement, "P Yeadon" in current -- one person
        inv, _, renames = _run([_inv("", "Yeadon")], [_inv("P", "Yeadon")])
        assert [r["unique_id"] for r in inv] == ["G1_p_yeadon"]
        assert len(renames) == 1

    def test_blank_given_name_alone_gets_family_id(self):
        inv, _, _ = _run([_inv("", "Yeadon")], [])
        assert [r["unique_id"] for r in inv] == ["G1_yeadon"]

    def test_compound_surname_kept_whole(self):
        inv, _, _ = _run([], [_inv("Beatriz", "Prieto Simon")])
        assert inv[0]["unique_id"] == "G1_beatriz_prieto simon"


class TestSameOrcidRule:
    def test_same_orcid_resolves_what_names_cannot(self):
        # DP0772887: Shu Ng and Shu-Kay Angus Ng carry one ORCID; Alex S-W Ng (PI) does not
        gn = _mod.extract_investigators({
            "investigators-at-announcement": [_inv("Shu", "Ng", orcid="O1"), _inv("Alex S-W", "Ng", role="PI")],
            "investigators-current": [_inv("Shu-Kay Angus", "Ng", orcid="O1")]}, "G1")
        assert [(r["announcement_name"], r["current_name"], r["rule"]) for r in gn.renames] == [
            ("Shu Ng", "Shu-Kay Angus Ng", "same_orcid")]

    def test_same_orcid_joins_a_swapped_name(self):
        inv, _, renames = _run([_inv("Kotagiri", "Ramamohanarao", orcid="O1")],
                               [_inv("Ramamohanarao", "Kotagiri", orcid="O1")])
        assert len(inv) == 1 and renames[0]["rule"] == "same_orcid"

    def test_different_orcids_are_not_joined_by_orcid(self):
        inv, _, renames = _run([_inv("Jessica", "Hyles", orcid="O1")], [_inv("Ben", "Trevaskis", orcid="O2")])
        assert len(inv) == 2 and renames == []


class TestFirstOrLastRule:
    def test_married_name_joined(self):
        inv, names, renames = _run([_inv("Karen", "Ford")], [_inv("Karen", "Marsh")])
        assert [r["unique_id"] for r in inv] == ["G1_karen_marsh"]
        assert {"karen_ford", "karen_marsh"} <= set(names[0]["full_name_keys"])
        assert renames[0]["rule"] == "first_or_last"

    def test_not_one_to_one_left_alone(self):
        # DP0772887: two announcement Ngs, one current Ng -- no guess
        gn = _mod.extract_investigators({
            "investigators-at-announcement": [_inv("Shu", "Ng"), _inv("Alex S-W", "Ng")],
            "investigators-current": [_inv("Shu-Kay Angus", "Ng")]}, "G1")
        assert gn.renames == []
        assert {p["reason"] for p in gn.pairs} == {"not one-to-one"}
        assert all(p["apply"] == "no" for p in gn.pairs)

    def test_role_swap_between_two_people_is_not_a_join(self):
        # LP100100367: Taylor CI->PI, Gray PI->CI -- whole lists compared, each matches himself
        inv, _, renames = _run(
            [_inv("Matthew", "Taylor", "CI"), _inv("Charles", "Gray", "PI")],
            [_inv("Matthew", "Taylor", "PI"), _inv("Charles", "Gray", "CI")])
        assert renames == [] and len(inv) == 2


def _overrides(rows):
    ov = _mod.NameOverrides()
    for action, grant, orcid, a, b in rows:
        if action == "correct":
            ov.corrections[(grant, *a)] = b
        else:
            table = {"no": "no_grant" if grant else "no_orcid", "add": "add_grant"}[action]
            getattr(ov, table)[(grant or orcid, frozenset((a, b)))] = "note"
    return ov


class TestOverrides:
    def test_correct_rewrites_before_parsing(self):
        ov = _overrides([("correct", "G1", None, ("AW", "Snyder"), ("A W", "Snyder"))])
        inv, names, renames = _run([_inv("AW", "Snyder")], [_inv("Allan", "Snyder")], ov)
        assert [r["unique_id"] for r in inv] == ["G1_allan_snyder"]
        assert renames[0]["rule"] == "automatic"   # "A W" shares a_snyder with "Allan"
        assert names[0]["corrected_from"] == ["AW Snyder"]
        assert ov.unused() == []

    def test_no_blocks_a_join(self):
        ov = _overrides([("no", "G1", None, ("Jacqueline", "Croke"), ("Barry", "Croke"))])
        gn = _mod.extract_investigators({
            "investigators-at-announcement": [_inv("Jacqueline", "Croke")],
            "investigators-current": [_inv("Barry", "Croke")]}, "G1", ov)
        assert gn.renames == [] and len(gn.investigators) == 2
        assert gn.pairs[0]["apply"] == "no" and gn.pairs[0]["reason"] == "note"
        assert ov.unused() == []

    def test_add_joins_what_no_rule_finds(self):
        ov = _overrides([("add", "G1", None, ("Ren", "Yi"), ("Yi", "Ren"))])
        inv, _, renames = _run([_inv("Ren", "Yi")], [_inv("Yi", "Ren")], ov)
        assert len(inv) == 1 and renames[0]["rule"] == "hand_add"

    def test_unused_row_is_reported(self):
        ov = _overrides([("no", "G1", None, ("Ann", "Lee"), ("Bo", "Lee"))])
        _run([_inv("Jan", "de Gier")], [_inv("Jan", "de Gier")], ov)
        assert len(ov.unused()) == 1

    def test_load_rejects_bad_rows(self, tmp_path):
        import pytest
        f = tmp_path / "o.csv"
        head = "action,grant_code,orcid,first_name,family_name,first_name_2,family_name_2,notes\n"
        f.write_text(head + "no,G1,0000-0000-0000-0001,A,B,C,D,x\n")
        with pytest.raises(ValueError):
            _mod.load_name_overrides(f)
        f.write_text(head + "maybe,G1,,A,B,C,D,x\n")
        with pytest.raises(ValueError):
            _mod.load_name_overrides(f)
        f.write_text(head + "no,,0000-0000-0000-0001,Chien Ming,Wang,Wenhui,Duan,x\n")
        ov = _mod.load_name_overrides(f)
        assert ("0000-0000-0000-0001", frozenset({("Chien Ming", "Wang"), ("Wenhui", "Duan")})) in ov.no_orcid


class TestOrcidNameLinks:
    def _records(self, grants):
        """grants: [(grant_code, first, family, orcid)] -- one single-list grant each."""
        import pandas as pd
        inv_rows, entries = [], {}
        for g, f, l, o in grants:
            gn = _mod.extract_investigators({"investigators-current": [_inv(f, l, orcid=o)]}, g)
            inv_rows += gn.investigators
            entries.update(gn.entries)
        return pd.DataFrame(inv_rows), entries

    def test_forms_under_one_orcid_share_keys(self):
        inv, entries = self._records([("G1", "Karen", "Ford", "O1"), ("G2", "Karen", "Marsh", "O1")])
        rows, added = _mod.orcid_name_links(inv, entries, {"G1": 2008, "G2": 2015}, _mod.NameOverrides())
        assert "karen_marsh" in added["G1_karen_ford"] and "karen_ford" in added["G2_karen_marsh"]
        assert len(rows) == 1
        assert (rows[0]["source_family"], rows[0]["target_family"]) == ("Ford", "Marsh")  # later grant is target

    def test_no_row_blocks_sharing(self):
        inv, entries = self._records([("G1", "Chien Ming", "Wang", "O1"), ("G2", "Wenhui", "Duan", "O1")])
        ov = _overrides([("no", None, "O1", ("Chien Ming", "Wang"), ("Wenhui", "Duan"))])
        rows, added = _mod.orcid_name_links(inv, entries, {"G1": 2010, "G2": 2010}, ov)
        assert added == {} and rows[0]["apply"] == "no"
        assert ov.unused() == []

    def test_different_orcids_never_share(self):
        inv, entries = self._records([("G1", "Ben", "White", "O1"), ("G2", "Benedict", "White", "O2")])
        rows, added = _mod.orcid_name_links(inv, entries, {}, _mod.NameOverrides())
        assert rows == [] and added == {}


class TestRejectScopus:
    def test_loaded_and_marked_used_when_the_record_exists(self, tmp_path):
        f = tmp_path / "o.csv"
        f.write_text("action,grant_code,orcid,first_name,family_name,first_name_2,family_name_2,notes\n"
                     "reject_scopus,G1,0000-0000-0000-0009,Peter,Taylor,,,namesake profile\n")
        ov = _mod.load_name_overrides(f)
        assert ov.reject_scopus == {("G1", "Peter", "Taylor"): ("0000-0000-0000-0009", "namesake profile")}
        _run([], [_inv("Peter", "Taylor")], ov)
        assert ov.unused() == []

    def test_needs_an_orcid(self, tmp_path):
        import pytest
        f = tmp_path / "o.csv"
        f.write_text("action,grant_code,orcid,first_name,family_name,first_name_2,family_name_2,notes\n"
                     "reject_scopus,G1,,Peter,Taylor,,,x\n")
        with pytest.raises(ValueError):
            _mod.load_name_overrides(f)
