** Goals
To extract the oeuvres of ARC chief investigators and fellows (CIF) by linking ARC investigators to OpenAlex authors and then identifying the openalex works of those authors

** Context
ARC grant data since 2001 is available as a short file and a longer file. The grant data includes the names of the CIFs. A person can have more than one grant and nmore than one name form. Every grant has an administring organisation (AdminOrg) that may have changed in a few cases since the award. The first-named CIF (CIF1) is likley to be associaed with the AdminOrg. Other organisations may be associated with the grant and we don't know how they realte to the other CIs.
OpenAlex data comprises authors, institutions, and works entities. The works entities unpack to a work and an authorships part including name and organisation
* Constraints
Desktop system with 64GB memory, 24 CPU 4 Tb SSD. prefer duckdb/parquet.