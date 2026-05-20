"""
src/21_splink_demo.py

A small, self-contained demo of using Splink to link ARC Investigators (Left) 
to OpenAlex Authors (Right) using Name, Country, and Institution.
"""

import pandas as pd
from splink import Linker, SettingsCreator, DuckDBAPI, block_on
import splink.comparison_library as cl
from splink.datasets import splink_datasets

def main():
    # 1. Create synthetic Data mimicking ARC (Left) and OpenAlex (Right)
    # We will create ~16 scenarios
    data_arc = [
        {"id": "a01", "first_name": "Lucas",    "family_name": "Smith",     "country": "AU", "institution": "UNSW"},
        {"id": "a02", "first_name": "Rachel",   "family_name": "Ong",       "country": "AU", "institution": "UWA"},
        {"id": "a03", "first_name": "David",    "family_name": "Morrison",  "country": "AU", "institution": "ANU"},
        {"id": "a04", "first_name": "Jane",     "family_name": "Doe",       "country": "AU", "institution": "Monash"},
        {"id": "a05", "first_name": "Michael",  "family_name": "Johnson",   "country": "AU", "institution": "RMIT"},
        {"id": "a06", "first_name": "Sarah",    "family_name": "Williams",  "country": "AU", "institution": "Sydney"},
        {"id": "a07", "first_name": "Emma",     "family_name": "Brown",     "country": "AU", "institution": "Queensland"},
        {"id": "a08", "first_name": "William",  "family_name": "Jones",     "country": "AU", "institution": "Adelaide"},
        {"id": "a09", "first_name": "Oliver",   "family_name": "Garcia",    "country": "AU", "institution": "Macquarie"},
        {"id": "a10", "first_name": "Isabella", "family_name": "Martinez",  "country": "AU", "institution": "Curtin"},
        {"id": "a11", "first_name": "Thomas",   "family_name": "Rodriguez", "country": "AU", "institution": "Griffith"},
        {"id": "a12", "first_name": "Sophia",   "family_name": "Hernandez", "country": "AU", "institution": "Deakin"},
        {"id": "a13", "first_name": "James",    "family_name": "Lopez",     "country": "AU", "institution": "La Trobe"},
        {"id": "a14", "first_name": "Charlotte","family_name": "Gonzalez",  "country": "AU", "institution": "Flinders"},
        {"id": "a15", "first_name": "Benjamin", "family_name": "Wilson",    "country": "AU", "institution": "Newcastle"},
        {"id": "a16", "first_name": "Amelia",   "family_name": "Anderson",  "country": "AU", "institution": "Wollongong"},
    ]
    
    data_oax = [
        # 1. Perfect Match
        {"id": "x01", "first_name": "Lucas",    "family_name": "Smith",     "country": "AU", "institution": "UNSW"},
        # 2. Initial for first name, slight inst diff
        {"id": "x02", "first_name": "R.",       "family_name": "Ong",       "country": "AU", "institution": "Univ. of Western Australia"}, 
        # 3. Different country and inst (shows penalization)
        {"id": "x03", "first_name": "Dave",     "family_name": "Morrison",  "country": "GB", "institution": "Oxford"},
        # 4. Typo in family name
        {"id": "x04", "first_name": "Jane",     "family_name": "Doeh",      "country": "AU", "institution": "Monash"},
        # 5. Typo in first name
        {"id": "x05", "first_name": "Micheal",  "family_name": "Johnson",   "country": "AU", "institution": "RMIT"},
        # 6. Null institution in OAX
        {"id": "x06", "first_name": "Sarah",    "family_name": "Williams",  "country": "AU", "institution": None},
        # 7. Name collision (same name, diff person/country/inst)
        {"id": "x07", "first_name": "Emma",     "family_name": "Brown",     "country": "US", "institution": "Harvard"},
        # 8. First name nickname
        {"id": "x08", "first_name": "Bill",     "family_name": "Jones",     "country": "AU", "institution": "Adelaide"},
        # 9. Similar inst name
        {"id": "x09", "first_name": "Oliver",   "family_name": "Garcia",    "country": "AU", "institution": "Macquarie University"},
        # 10. Initial only for family name? (Rare, but possible)
        {"id": "x10", "first_name": "Isabella", "family_name": "M.",        "country": "AU", "institution": "Curtin"},
        # 11. Exact match with multiple components
        {"id": "x11", "first_name": "Thomas A.","family_name": "Rodriguez", "country": "AU", "institution": "Griffith"},
        # 12. Swapped names (won't match well without specific rule, but shows behavior)
        {"id": "x12", "first_name": "Hernandez","family_name": "Sophia",    "country": "AU", "institution": "Deakin"},
        # 13. Double barrelled family name
        {"id": "x13", "first_name": "James",    "family_name": "Lopez-Smith","country": "AU", "institution": "La Trobe"},
        # 14. Different spelling of first name
        {"id": "x14", "first_name": "Charlot",  "family_name": "Gonzalez",  "country": "AU", "institution": "Flinders"},
        # 15. Partial match all around
        {"id": "x15", "first_name": "Ben",      "family_name": "Willson",   "country": "NZ", "institution": "Newcastle"},
        # 16. Perfect Match
        {"id": "x16", "first_name": "Amelia",   "family_name": "Anderson",  "country": "AU", "institution": "Wollongong"},
    ]
    
    df_arc = pd.DataFrame(data_arc)
    df_oax = pd.DataFrame(data_oax)
    
    # 2. Define Link-Only Settings
    settings = SettingsCreator(
        unique_id_column_name="id",
        link_type="link_only",
        blocking_rules_to_generate_predictions=[
            # Multiple blocking rules (if it matches ANY of these, it forms a candidate pair)
            block_on("family_name"),
            block_on("first_name"),
            block_on("institution"),
        ],
        comparisons=[
            cl.NameComparison("first_name"),
            cl.NameComparison("family_name"),
            cl.ExactMatch("country"),
            cl.JaroWinklerAtThresholds("institution", [0.9, 0.7]),
        ],
        retain_matching_columns=True,
        retain_intermediate_calculation_columns=True,
    )
    
    db_api = DuckDBAPI()
    linker = Linker([df_arc, df_oax], settings, db_api=db_api, input_table_aliases=["arc", "oax"])
    
    # 3. Predict (Using default weights since dataset is too small for EM training)
    print("Running Splink Predictions...")
    df_predict = linker.inference.predict(threshold_match_probability=0.001)
    
    results = df_predict.as_pandas_dataframe()
    
    cols = [
        "id_l", "id_r",
        "match_probability", "match_weight",
        "first_name_l", "first_name_r",
        "family_name_l", "family_name_r",
        "institution_l", "institution_r",
        "country_l", "country_r"
    ]
    
    # Format the probability and weight to be more readable
    results["match_probability"] = results["match_probability"].round(4)
    results["match_weight"] = results["match_weight"].round(2)
    
    print("\nSplink Output:")
    print("-" * 120)
    print(results[cols].to_string(index=False, justify="left"))
    print("-" * 120)

if __name__ == "__main__":
    main()
