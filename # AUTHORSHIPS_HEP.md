# Authorship HEP

## GOAL

- improve approach to pre-filtering OAX authors comnpared with using the author entity

## CONTEXT

- current missing matches inclduing many non-matches. new approach.

1. filter OAX authorship entity to pull out authorships_hep. works with at least one author at a hep. all coathors have a hep link
2. join to work entity on work_idx picking up publication_year, source_id, topics
3. construct authors_hep that can groupby on author_idx or author_name and include coauthors, topics from works, sources, publication_year range
4. can join on OAX authors entity to pick up oricds 

- use duckdb and parquet
- persist expensive sql queries for reuse

## CONSTRAINTS

- end point is an author_hep parquet that can be fed into current process with minimal change 