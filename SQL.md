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

-- SQL TO CONSTRUCT COUNTS OF DOMAINS PER AUTHOR. COULD USE SAME FOR FIELDS, SUBFIELDS OR TOPICS
------------------------------------------------------------------------------------------------
WITH unnested_counts AS (
    -- Step 1: Compute frequencies and unnest the map entries
    SELECT 
        author_name, 
        author_idx, 
        count(DISTINCT work_idx) AS works_count,
        unnest(map_entries(histogram(field_name))) AS entry
    FROM '/home/lc/m/working/WORKING_ARC_PROJECT/processed/works_hep.parquet' w
    INNER JOIN '/home/lc/m/working/WORKING_ARC_PROJECT/processed/authorships_hep.parquet' a
    USING (work_idx)
    GROUP BY ALL
),
calculated_fractions AS (
    -- Step 2: Calculate fractions using a window function
    SELECT 
        author_name,
        author_idx,
        works_count,
        entry.key AS field,
        entry.value AS field_count,
        -- Divides individual count by the total sum of counts for this specific author
        (entry.value::DOUBLE / SUM(entry.value) OVER (PARTITION BY author_idx)) AS field_fraction
    FROM unnested_counts
)
-- Step 3: Re-bundle into a structured list sorted by fraction descending
SELECT 
    author_name, 
    author_idx, 
    works_count,
    list({
        'field': field, 
        'count': field_count,
        'fraction': round(field_fraction, 4) -- Rounded to 4 decimal places for clean output
    } ORDER BY field_fraction DESC) AS sorted_fields
FROM calculated_fractions
GROUP BY ALL
ORDER BY works_count DESC;
