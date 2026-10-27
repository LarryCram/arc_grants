import duckdb
import pandas as pd

from src.oeuvre.year_fix import fix_placeholder_years, orcid_doi_years


def _orcid_record(path, doi, year):
    return {"orcid-identifier": {"path": path},
            "activities-summary": {"works": {"group": [
                {"external-ids": {"external-id": [{"external-id-type": "doi", "external-id-value": f"https://doi.org/{doi}"}]},
                 "work-summary": [{"publication-date": {"year": {"value": str(year)}}}]}]}}}


def test_orcid_doi_years():
    assert orcid_doi_years(_orcid_record("o1", "10.1/AB", 2010)) == {"10.1/ab": 2010}


def test_fix_placeholder_years(tmp_path):
    kept = tmp_path / "kept.parquet"
    pd.DataFrame({"cluster_id": ["A", "A", "A", "B", "B"],
                  "work_idx": [1, 2, 3, 4, 5],
                  "doi": ["10.1/ab", "10.1/cd", None, "10.1/ef", "10.1/gh"],
                  "source_id": [10, 10, 20, 30, 10],
                  "publication_year": [1970, 1970, 1970, 1970, 2001]}).to_parquet(kept)
    docs = tmp_path / "docs.parquet"
    pd.DataFrame({"doi": ["10.1/CD", "10.1/ef"], "year": ["2011", "1970"]}).to_parquet(docs)
    works = tmp_path / "works"
    works.mkdir()
    pd.DataFrame({"work_idx": [9, 8, 7, 6], "source_id": [10, 20, 30, 30],
                  "publication_year": [1965, 2005, 1968, 1970]}).to_parquet(works / "w.parquet")
    acifs = pd.DataFrame({"cluster_id": ["A", "B"], "orcids": [["o1"], []]})
    cache = {"o1": _orcid_record("o1", "10.1/ab", 2010)}
    c = fix_placeholder_years(duckdb.connect(), kept, acifs, cache=cache, docs=docs, works=works)
    out = pd.read_parquet(kept).set_index("work_idx")
    assert out.loc[1, "publication_year"] == 2010 and out.loc[1, "year_fix"] == "orcid"       # ORCID record
    assert out.loc[2, "publication_year"] == 2011 and out.loc[2, "year_fix"] == "scopus"      # Scopus
    assert pd.isna(out.loc[3, "publication_year"]) and out.loc[3, "year_fix"] == "late"       # source starts 2005
    assert out.loc[4, "publication_year"] == 1970 and pd.isna(out.loc[4, "year_fix"])         # active in 1968, Scopus 1970
    assert out.loc[5, "publication_year"] == 2001 and out.loc[5, "publication_year_oax"] == 2001
    assert c == {"dated_placeholder": 4, "orcid": 1, "scopus": 1, "late": 1, "kept_placeholder": 1}
