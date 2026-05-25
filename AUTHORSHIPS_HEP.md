# Authorship HEP

## GOAL

- improve pre-filtering OAX authors compared with author entity

## CONTEXT

- the code is currently missing matches unnecessarily
- use duckdb and parquet
- persist expensive sql queries for reuse
- SQL to do some of this is copied into SQL.md. the parquets already exist

1. filter OAX authorship entity to pull out works_intermediate_hep - works with at least one author at a hep.
2. join works_intermediate to work + topics entities for publication_year, source_id, topics
3. join works_intermediate to authorships to get authorships_hep as authorships with work from hep
4. pick up orcids, display_name_alternatives from authors
5. groupby authors on authorships_hep join works_hep to get properties of all authors of papers that have a hep relationhsip 
6. continue with the current pipeline uing this new table once it is prepared for splink

## CONSTRAINTS

- end point is an author_hep parquet that can be fed into current process with minimal change 