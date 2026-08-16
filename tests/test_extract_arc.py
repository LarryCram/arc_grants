"""
Tests for src/00_extract_arc.py's extract_grant_flat() -- pure function, no file I/O.

Covers the 2026-08-16 eligible_orgs addition: n_eligible_orgs keeps its original 2-role scope
(Administering + Other Eligible Organisation) since it's load-bearing downstream
(01_prepare_arc.py/awards_cif.py's _merge_same_grant_coinvestigators,
04_resolve_links.py's institution-overlap check all treat n_eligible_orgs==1 as "single-org
grant"), while the new eligible_orgs column is a deliberately wider 3-role set (adds
Collaborating Organisation) for HEP-code resolution.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from importlib import import_module

_extract = import_module("src.00_extract_arc")
extract_grant_flat = _extract.extract_grant_flat


def _org(name, role):
    return {"organisationName": name, "roleName": role}


def _attrs(orgs, **overrides):
    base = {
        "scheme-name": "Discovery Projects",
        "grant-status": "Active",
        "funding-commencement-year": 2020,
        "years-funded": 3,
        "administering-organisation": orgs[0]["organisationName"] if orgs else None,
        "organisations-at-announcement": orgs,
    }
    base.update(overrides)
    return base


class TestExtractGrantFlat:
    def test_admin_org_only(self):
        orgs = [_org("Uni A", "Administering Organisation")]
        out = extract_grant_flat(_attrs(orgs), "DP000001")
        assert out["n_eligible_orgs"] == 1
        assert out["eligible_orgs"] == ["Uni A"]

    def test_other_eligible_counted_in_both(self):
        orgs = [
            _org("Uni A", "Administering Organisation"),
            _org("Uni B", "Other Eligible Organisation"),
        ]
        out = extract_grant_flat(_attrs(orgs), "DP000001")
        assert out["n_eligible_orgs"] == 2
        assert out["eligible_orgs"] == ["Uni A", "Uni B"]

    def test_collaborating_org_in_eligible_orgs_but_not_n_eligible_orgs(self):
        # The regression this test guards against: n_eligible_orgs is load-bearing downstream
        # (n_eligible_orgs==1 means "single-org grant") -- a Collaborating Organisation must not
        # inflate it, even though it does belong in the wider eligible_orgs set.
        orgs = [
            _org("Uni A", "Administering Organisation"),
            _org("Uni B", "Collaborating Organisation"),
        ]
        out = extract_grant_flat(_attrs(orgs), "DP000001")
        assert out["n_eligible_orgs"] == 1
        assert out["eligible_orgs"] == ["Uni A", "Uni B"]

    def test_partner_organisation_excluded_from_both(self):
        orgs = [
            _org("Uni A", "Administering Organisation"),
            _org("Some Company", "Partner Organisation"),
        ]
        out = extract_grant_flat(_attrs(orgs), "DP000001")
        assert out["n_eligible_orgs"] == 1
        assert out["eligible_orgs"] == ["Uni A"]

    def test_host_organisation_excluded_from_both(self):
        orgs = [
            _org("Uni A", "Administering Organisation"),
            _org("University of Cambridge", "Host Organisation"),
        ]
        out = extract_grant_flat(_attrs(orgs), "FT000001")
        assert out["n_eligible_orgs"] == 1
        assert out["eligible_orgs"] == ["Uni A"]

    def test_other_organisation_and_bare_other_excluded_from_both(self):
        orgs = [
            _org("Uni A", "Administering Organisation"),
            _org("University of Auckland", "Other Organisation"),
            _org("University of Maryland", "Other"),
        ]
        out = extract_grant_flat(_attrs(orgs), "LP000001")
        assert out["n_eligible_orgs"] == 1
        assert out["eligible_orgs"] == ["Uni A"]

    def test_all_seven_role_names_together(self):
        # All 7 real ARC roleName values on one grant -- exactly the 3 in-scope ones survive
        # into eligible_orgs, exactly the 2 original ones survive into n_eligible_orgs.
        orgs = [
            _org("Admin Uni", "Administering Organisation"),
            _org("Eligible Uni", "Other Eligible Organisation"),
            _org("Collab Uni", "Collaborating Organisation"),
            _org("Partner Co", "Partner Organisation"),
            _org("Host Uni", "Host Organisation"),
            _org("Other Org Uni", "Other Organisation"),
            _org("Bare Other Uni", "Other"),
        ]
        out = extract_grant_flat(_attrs(orgs), "DP000001")
        assert out["n_eligible_orgs"] == 2
        assert out["eligible_orgs"] == ["Admin Uni", "Collab Uni", "Eligible Uni"]

    def test_duplicate_org_names_deduplicated(self):
        orgs = [
            _org("Uni A", "Administering Organisation"),
            _org("Uni A", "Other Eligible Organisation"),
        ]
        out = extract_grant_flat(_attrs(orgs), "DP000001")
        assert out["n_eligible_orgs"] == 1
        assert out["eligible_orgs"] == ["Uni A"]

    def test_no_orgs_at_all(self):
        out = extract_grant_flat(_attrs([]), "DP000001")
        assert out["n_eligible_orgs"] == 0
        assert out["eligible_orgs"] == []
