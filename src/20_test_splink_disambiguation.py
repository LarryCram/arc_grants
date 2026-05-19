"""
20_test_splink_disambiguation.py

Draft Splink model for linking ARC CIs and Fellows to OpenAlex Authors.
Uses DuckDB backend.

This is a link-only model linking two datasets:
- Dataset L: ARC Investigators
- Dataset R: OpenAlex Authors
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd
from splink import Linker, SettingsCreator, DuckDBAPI, block_on
import splink.comparison_library as cl

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA

def main():
    print("Drafting Splink Link-Only Model for ARC -> OpenAlex Disambiguation...")

    # 1. Connect to DuckDB
    # We can connect to an in-memory database or a persistent one if needed.
    con = duckdb.connect()
    
    # 2. Define Settings for Link-Only model
    settings = SettingsCreator(
        link_type="link_only",
        
        # Blocking rules to drastically reduce the search space
        # (Comparing 62k ARC invs vs ~600k OpenAlex authors requires blocking)
        blocking_rules_to_generate_predictions=[
            # Block 1: Exact ORCID match (highly specific)
            block_on("orcid"),
            
            # Block 2: Exact Family Name + Institution Code
            block_on("family_name", "hep_code"),
            
            # Block 3: Exact First Name + Family Name 
            # (Captures cases missing ORCID/HEP, but relies on name uniqueness)
            block_on("first_name", "family_name"),
        ],
        
        # Comparisons to score candidate pairs
        comparisons=[
            # First Name: Evaluate spelling variations or exact matches
            cl.NameComparison("first_name"),
            
            # Family Name: Evaluate spelling variations or exact matches
            cl.NameComparison("family_name"),
            
            # ORCID: Exact match evaluation (allows nulls to be handled probabilistically)
            cl.ExactMatch("orcid"),
            
            # Institution Code: Did they work at the same institution?
            cl.ExactMatch("hep_code").configure(term_frequency_adjustments=True),
            
            # Additional features could include FoR codes or publication years
            # cl.ExactMatch("for_code"),
        ],
        retain_intermediate_calculation_columns=True,
    )
    
    print("\nSettings initialized:")
    print(settings)
    
    # Example Workflow (commented out until data loading is wired up):
    """
    # Load data into DuckDB views
    con.execute(f"CREATE VIEW arc_investigators AS SELECT * FROM read_parquet('{PROCESSED_DATA}/investigators.parquet')")
    con.execute(f"CREATE VIEW oax_authors AS SELECT * FROM read_parquet('path/to/authors_au.parquet')")
    
    df_arc = con.table("arc_investigators")
    df_oax = con.table("oax_authors")

    # 3. Create Linker
    db_api = DuckDBAPI(connection=con)
    linker = Linker([df_arc, df_oax], settings, db_api=db_api, input_table_aliases=["arc", "oax"])
    
    # 4. Training (Estimating Parameters)
    deterministic_rules = [
        "l.orcid = r.orcid",
        "l.family_name = r.family_name and l.first_name = r.first_name and l.hep_code = r.hep_code"
    ]
    linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("family_name"))
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("first_name"))
    
    # 5. Prediction
    predictions = linker.inference.predict(threshold_match_probability=0.9)
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(predictions, threshold_match_probability=0.95)
    """

if __name__ == "__main__":
    main()
