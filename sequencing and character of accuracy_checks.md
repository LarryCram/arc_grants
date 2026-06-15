**** sequencing and character of accuracy_check

Types of checks
  1. ARC person - [oax_id]
  2. For each [oax_id]:
    - 2a. Work - earliest work when title dupe
    - 2b. Work - 1950 <= publication_year <= 2025 
    - 2c. Work - arc_for consistent
  3. ARC person - first_publication_year > 1950, academic age < 60
  4. Grant type - Fellowships can be early APD/APDI/DECRA/?, mid FF/QEII/?. senior FL?APD (make a full list)

Key checks:
  1. multiple ARC persons with same oax_idx -- check that each oax_idx refers to only one ARC person where not report for repair in /src  
  2a. Since each work_idx can be cited but the work materialized in its first year, save as a [] and use the earliest year is its publication_year.
  2b. Flag for hiding include 2026
  2c. this needs to check back to for_subfield map 
  3. oax_id or work problem? decide on basis of other works for this oax_id.
  4. Year continuity - need to examine this
  5. Grant type issues: ECR academic age at award <~ 5, MCR academic age at award > 5, senior academic age > 10, all with exceptions