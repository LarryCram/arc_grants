-- SQL TO COPY WORKS THAT HAVE AT LEAST ONE HEP INSTITUTION
-----------------------------------------------------------
COPY (
WITH authorships_hep AS (
  SELECT DISTINCT
    institution_id[23:]::BIGINT AS institution_idx,
    hep_code
  FROM '/home/lc/m/working/WORKING_ARC_PROJECT/admin_orgs.csv'
  WHERE institution_idx IS NOT NULL AND hep_code IS NOT NULL
)

SELECT DISTINCT work_idx
FROM '/home/lc/m/openalex_feb26/parquet/authorships/*.parquet' p
INNER JOIN authorships_hep
USING (institution_idx)
) TO '/home/lc/m/working/WORKING_ARC_PROJECT/processed/works_intermediate_hep.parquet';

-- SQL TO COPY AUTHORSHIPS FOR WORKS WITH AT LEAST ONE HEP
----------------------------------------------------------
COPY (
SELECT *
  FROM '/home/lc/m/working/WORKING_ARC_PROJECT/processed/works_intermediate_hep.parquet'
  INNER JOIN '/home/lc/m/openalex_feb26/parquet/authorships/*.parquet'
  USING (work_idx)
  WHERE institution_idx IS NOT NULL
  ) TO '/home/lc/m/working/WORKING_ARC_PROJECT/processed/authorships_hep.parquet';

-- SQL TO COPY WORKS AND TOPICS FOR WORKS WITH AT LEAST ONE HEP
----------------------------------------------------------------
COPY (
  WITH 
    topics AS
      (SELECT *
         FROM '/home/lc/m/working/WORKING_ARC_PROJECT/processed/works_intermediate_hep.parquet'
         JOIN '/home/lc/m/openalex_feb26/parquet/topics/*.parquet'
         USING (work_idx)
      )
    
  SELECT t.*, "type", source_id, authors_count, institutions_distinct_count, cited_by_count
    FROM topics t
    INNER JOIN '/home/lc/m/openalex_feb26/parquet/works/*.parquet'
    USING (work_idx)
) TO '/home/lc/m/working/WORKING_ARC_PROJECT/processed/works_hep.parquet'