"""Tests for src/acif/output.py and build.set_aside_indigenous_research() (hand-built fixtures)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.acif.build import set_aside_indigenous_research
from src.acif.models import AwardCIFItem, AwardsCIF
from src.acif.output import acif_rows, for2020_union, record_rows

CHEM = {"code": "3401", "name": "Analytical chemistry", "is_primary": True, "confidence": 1.0}
IND_P = {"code": "4501", "name": "Aboriginal and Torres Strait Islander culture", "is_primary": True, "confidence": 1.0}
IND_S = dict(IND_P, is_primary=False)


def _item(uid, for2020, orcid=None, scopus_orcid=None, org="Uni A", year=2010):
    return AwardCIFItem(unique_id=uid, grant_code=uid.split("_")[0], first_name="Jan", family_name="Smith",
                        role_code="CI", orcid=orcid, admin_org=org, admin_orgs=[org], institution_oax_id=None,
                        funding_commence_year=year, for_name=None, for_code=None, full_name="Jan Smith",
                        full_name_keys=["jan_smith", "j_smith"], for2020_codes=for2020,
                        scopus_orcid=scopus_orcid)


def _acif(*items):
    return AwardsCIF(cluster_id=min(it.unique_id for it in items), items=list(items), cycle_stages=[1])


def test_primary_division_45_sets_aside():
    a = _acif(_item("DP1_jan_smith", [IND_P]))
    b = _acif(_item("DP2_jan_smith", [CHEM, IND_S]))
    assert set_aside_indigenous_research([a, b]) == 1
    assert (a.excluded, a.excluded_reason) == (True, "indigenous") and not b.excluded


def test_kept_acif_loses_non_primary_division_45():
    b = _acif(_item("DP2_jan_smith", [CHEM, IND_S]))
    assert [e["code"] for e in for2020_union(b)] == ["3401"]
    a = _acif(_item("DP1_jan_smith", [IND_P]))
    a.excluded = True
    assert [e["code"] for e in for2020_union(a)] == ["4501"]


def test_for2020_union_primary_on_any_grant():
    sec = dict(CHEM, is_primary=False, confidence=0.5)
    a = _acif(_item("DP1_jan_smith", [sec]), _item("DP2_jan_smith", [CHEM]))
    assert for2020_union(a) == [CHEM]


def test_acif_rows_coawardees_orcids_and_universities():
    a = _acif(_item("DP1_jan_smith", [CHEM], scopus_orcid="O1"), _item("DP2_jan_smith", [CHEM], year=2015))
    b = _acif(_item("DP1_ann_lee", [CHEM]))
    rows = acif_rows([a, b], single={"DP1"}, crosswalk={}, hep_names={"Uni A"}).set_index("cluster_id")
    r = rows.loc["DP1_jan_smith"]
    assert r.grant_codes == ["DP1", "DP2"] and (r.first_year, r.last_year) == (2010, 2015)
    assert r.orcids == ["O1"] and r.orcid_sources == ["scopus"]
    assert r.coawardee_acif_ids == ["DP1_ann_lee"] and rows.loc["DP1_ann_lee"].coawardee_acif_ids == ["DP1_jan_smith"]
    assert r.single_org_universities == ["Uni A"]
    assert list(record_rows([a, b]).cluster_id) == ["DP1_ann_lee", "DP1_jan_smith", "DP1_jan_smith"]
