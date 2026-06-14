"""
Create reusable random samples of 10 and 1000 ARC persons from arc_oax_resolved.
Output: $OUTPUT_ROOT/analysis/samples.parquet  (arc_id, in_sample_10, in_sample_1000)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from config.settings import PROCESSED_DATA, OUTPUT_ROOT

ANALYSIS_OUT = OUTPUT_ROOT / "analysis"
ANALYSIS_OUT.mkdir(parents=True, exist_ok=True)

OUT      = str(ANALYSIS_OUT / "samples.parquet")
RESOLVED = str(PROCESSED_DATA / "arc_oax_resolved.parquet")
PERSONS  = str(PROCESSED_DATA / "arc_persons.parquet")

con = duckdb.connect()

con.execute(f"CREATE TABLE resolved AS SELECT arc_id FROM read_parquet('{RESOLVED}')")
con.execute("CREATE TABLE s10   AS SELECT arc_id FROM resolved USING SAMPLE 10   (reservoir, 42)")
con.execute("CREATE TABLE s1000 AS SELECT arc_id FROM resolved USING SAMPLE 1000 (reservoir, 42)")

con.execute(f"""
COPY (
    SELECT
        r.arc_id,
        (s10.arc_id  IS NOT NULL) AS in_sample_10,
        (s1k.arc_id  IS NOT NULL) AS in_sample_1000
    FROM resolved r
    LEFT JOIN s10        ON s10.arc_id = r.arc_id
    LEFT JOIN s1000 s1k  ON s1k.arc_id = r.arc_id
) TO '{OUT}' (FORMAT PARQUET)
""")
print(f"Written: {OUT}")

print("\n--- Sample-10 persons ---")
rows = con.execute(f"""
    SELECT s.arc_id, p.full_names[1] AS name, p.inst_arr[1] AS inst
    FROM s10 s
    JOIN read_parquet('{PERSONS}') p ON p.cluster_id = s.arc_id
    ORDER BY s.arc_id
""").fetchall()
for r in rows:
    print(f"  {r[0]:30s}  {str(r[1]):40s}  {r[2]}")
