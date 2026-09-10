we are building the filter_ class in 04_

the first step is to look at exemplars to see what functions and sequence is desirable in the class. 

this will assemble a list that can be tested and enumerated.

1. Sort the ACIFs by OAX candidate count ASC, then works DESC

This step is repeated many times - persist as a duckdb database, with a table of row-was provence decisions relating to decison about the ACIF - oax unit inclusion/exclusion.

2. We need a way to add provenance to each OAX unit attached to a CIF. someting like keep, drop, uncertain + a reason for drop. propose a solution and stop.

3. Enumerate the rate at which the top candidate in an ACIF  matches, missmatches, or is NULL for the ACIF orcid when it has one. -- <1% miss natch for 2-oax, around 30% for higher.

4. Print out the full ACIF and OAX data for the first miss-match.

>> the first filter is annotate a miss-match as that and flag it to be excluded. 

--- status 2026-09-10 ---

Both persistence needs from item 1/2 are built: oax_provenance.duckdb now holds two tables --
acif_oax_candidates (the prepared candidate pool, cached so populate_oax_candidates()/
dedup_oax_candidates() don't rerun every call) and oax_provenance (keep/drop/uncertain verdicts,
one row per cluster_id/oax_id).

orcid_veto() built and wired into flag_next_mismatch(), which walks in test-1 order, skips
already-annotated cases, and for the first unprocessed mismatch records
drop/orcid_mismatch/orcid_veto then prints the case.

Two cases run so far:
1. LP0989385_YukChuLiu / A5026782331 -- sub-HC (0.708), both orcids present and differ, correctly
   vetoed (the true match was never a candidate in this pool at all -- see the earlier session's
   HEP-authorship-intake-filter finding on this same person).
2. DP0342703_KimbalMarriott / A5085695563 -- high_confidence=True, match_probability=0.998, 11
   grants, ARC full_names include "Kimbal Marriott"/"Ken Marriott" against OAX "Kim Marriott" (a
   plausible nickname), orcids differ. This is the accepted-risk case the hard veto's own design
   named in advance (a stably-but-wrongly-recorded ARC orcid) -- surfaced on the very second case
   tested. Not yet resolved: should a high-confidence/many-grant match be vetoed the same way as a
   low-confidence/single-grant one? No further cases run pending this.