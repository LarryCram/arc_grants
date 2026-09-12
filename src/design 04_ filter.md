we are building the filter_ class in 04_

the first step is to look at exemplars to see what functions and sequence is desirable in the class. 

this will assemble a list that can be tested and enumerated.

1. Sort the ACIFs by OAX candidate count ASC, then works DESC

This step is repeated many times - persist as a duckdb database, with a table of row-was provence decisions relating to decison about the ACIF - oax unit inclusion/exclusion.

The ACIFs used here need to have a more precise treatment of the ARC inst and for data. Does the original ACIF allow the the prepared for 04_ to have a list of dicts of {count: admin org} over every grant in the ACIF - dropping other orgs since they cannot carry info into oax author/inst. likewise count the propensity of for codes (or their subfield equivalents).

2. We need a way to add provenance to each OAX unit attached to a CIF. someting like keep, drop, uncertain + a reason for drop. propose a solution and stop.

3. Enumerate the rate at which the top candidate in an ACIF  matches, missmatches, or is NULL for the ACIF orcid when it has one. -- <1% miss natch for 2-oax, around 30% for higher.

4. Print out the full ACIF and OAX data for the first miss-match.

5. you have built the orcid_veto(). I now expect that for the flow of arc-oax as sorted, all the orcid mismatches will be flagged and no longer revealed, and the nest print I see is NOT an inconsistent orcid arc/oax match.



