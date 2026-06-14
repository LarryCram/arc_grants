"""
Shared synthetic DuckDB fixtures for oeuvres pipeline tests.

All data is in-memory — no parquet files required.

Schema matches the real OAX parquets:
  resolved:     arc_id, oax_id, secondary_oax_ids[]
  authorships:  work_idx, author_idx, author_name, institution_name, ror, country_code
  works:        work_idx, title, publication_year, cited_by_count, type, doi, authors_count
  topics:       work_idx, topic_idx, score, subfield_name, field_name, domain_name

People in synthetic data:
  DP001  → author_idx=100  (primary only)           works: 1001(2015,c=50), 1002(2018,c=20), 1003(2021,c=5)
  DP002  → author_idx=200  (primary)                works: 2001(2010,c=100), 2002(2016,c=30)
             author_idx=201  (secondary/split)       works: 2002(2016,c=30) [shared], 2003(2020,c=8)
  DP003  → author_idx=300  (primary, sparse career) works: 3001(2000,c=10), 3002(2006,c=4)  [gap 2001-2005]
  DP004  → author_idx=400  (primary, high H)        works: 4001(1998,c=200), 4002(2000,c=80),
                                                            4003(2005,c=50), 4004(2010,c=30),
                                                            4005(2015,c=20), 4006(2020,c=5)
"""

import pytest
import duckdb


@pytest.fixture
def con():
    """Fresh in-memory DuckDB connection populated with synthetic data."""
    c = duckdb.connect()

    # ── resolved ───────────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE resolved (
        arc_id            VARCHAR,
        oax_id            VARCHAR,
        secondary_oax_ids VARCHAR[],
        resolved_by       VARCHAR
    )""")
    c.executemany("INSERT INTO resolved VALUES (?, ?, ?, ?)", [
        # DP001: unique HC match — no secondary used
        ("DP001", "https://openalex.org/A100", [],                                    "unique_hc"),
        # DP002: oax_topic_dedup — secondary IS a genuine split record
        ("DP002", "https://openalex.org/A200", ["https://openalex.org/A201"],         "oax_topic_dedup"),
        # DP003: resolved by orcid — secondary is a disambiguation alternative (different person)
        ("DP003", "https://openalex.org/A300", ["https://openalex.org/A301"],         "orcid"),
        ("DP004", "https://openalex.org/A400", [],                                    "unique_hc"),
    ])

    # ── authorships ────────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE authorships (
        work_idx         BIGINT,
        author_idx       BIGINT,
        author_name      VARCHAR,
        institution_name VARCHAR,
        ror              VARCHAR,
        country_code     VARCHAR
    )""")
    c.executemany("INSERT INTO authorships VALUES (?, ?, ?, ?, ?, ?)", [
        # DP001
        (1001, 100, "Smith J",   "Univ A",   "r1", "AU"),
        (1002, 100, "Smith J",   "Univ A",   "r1", "AU"),
        (1003, 100, "Smith J",   "Univ A",   "r1", "AU"),
        # DP002 primary
        (2001, 200, "Jones M",   "Univ B",   "r2", "AU"),
        (2002, 200, "Jones M",   "Univ B",   "r2", "AU"),
        # DP002 secondary (split record) — shares work 2002, adds 2003
        (2002, 201, "M Jones",   "Univ B",   "r2", "AU"),
        (2003, 201, "M Jones",   "Univ B",   "r2", "AU"),
        # DP003 (primary author_idx=300)
        (3001, 300, "Lee S",     "Univ C",   "r3", "AU"),
        (3002, 300, "Lee S",     "Univ C",   "r3", "AU"),
        # author_idx=301 is DP003's secondary but resolved_by='orcid' — different person
        (3003, 301, "Sandra Lee","Univ X",   "r3x","AU"),
        # DP004
        (4001, 400, "Wang X",    "Univ D",   "r4", "AU"),
        (4002, 400, "Wang X",    "Univ D",   "r4", "AU"),
        (4003, 400, "Wang X",    "Univ D",   "r4", "AU"),
        (4004, 400, "Wang X",    "Univ D",   "r4", "AU"),
        (4005, 400, "Wang X",    "Univ D",   "r4", "AU"),
        (4006, 400, "Wang X",    "Univ D",   "r4", "AU"),
        # Co-authors (not ARC) on work 1001 and 2001
        (1001, 999, "Brown K",   "MIT",      "r9", "US"),
        (2001, 998, "Garcia L",  "Oxford",   "r8", "GB"),
    ])

    # ── works ──────────────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE works (
        work_idx         BIGINT,
        title            VARCHAR,
        publication_year BIGINT,
        cited_by_count   BIGINT,
        type             VARCHAR,
        doi              VARCHAR,
        authors_count    BIGINT
    )""")
    c.executemany("INSERT INTO works VALUES (?, ?, ?, ?, ?, ?, ?)", [
        (1001, "Smith 2015 A", 2015, 50,  "article", None, 2),
        (1002, "Smith 2018 B", 2018, 20,  "article", None, 1),
        (1003, "Smith 2021 C", 2021, 5,   "article", None, 1),
        (2001, "Jones 2010 D", 2010, 100, "article", None, 2),
        (2002, "Jones 2016 E", 2016, 30,  "article", None, 2),
        (2003, "Jones 2020 F", 2020, 8,   "article", None, 1),
        (3001, "Lee 2000 G",   2000, 10,  "article", None, 1),
        (3002, "Lee 2006 H",   2006, 4,   "article", None, 1),
        (3003, "Other 2012 X", 2012, 15,  "article", None, 1),  # belongs to A301, not DP003
        (4001, "Wang 1998 I",  1998, 200, "article", None, 1),
        (4002, "Wang 2000 J",  2000, 80,  "article", None, 1),
        (4003, "Wang 2005 K",  2005, 50,  "article", None, 1),
        (4004, "Wang 2010 L",  2010, 30,  "article", None, 1),
        (4005, "Wang 2015 M",  2015, 20,  "article", None, 1),
        (4006, "Wang 2020 N",  2020, 5,   "article", None, 1),
    ])

    # ── topics ─────────────────────────────────────────────────────────────
    c.execute("""
    CREATE TABLE topics (
        work_idx      BIGINT,
        topic_idx     BIGINT,
        score         FLOAT,
        subfield_name VARCHAR,
        field_name    VARCHAR,
        domain_name   VARCHAR
    )""")
    c.executemany("INSERT INTO topics VALUES (?, ?, ?, ?, ?, ?)", [
        # work 1001: two topics — score 0.9 wins
        (1001, 1, 0.9, "Astrophysics",  "Physics and Astronomy", "Physical Sciences"),
        (1001, 2, 0.6, "Spectroscopy",  "Chemistry",             "Physical Sciences"),
        (1002, 1, 0.8, "Astrophysics",  "Physics and Astronomy", "Physical Sciences"),
        (1003, 1, 0.7, "Astrophysics",  "Physics and Astronomy", "Physical Sciences"),
        (2001, 3, 0.95,"Cardiology",    "Medicine",              "Health Sciences"),
        (2002, 3, 0.70,"Cardiology",    "Medicine",              "Health Sciences"),
        (2003, 3, 0.80,"Cardiology",    "Medicine",              "Health Sciences"),
        (3001, 4, 0.85,"Ecology",       "Environmental Science", "Life Sciences"),
        (4001, 5, 0.90,"Bioinformatics","Biochemistry",          "Life Sciences"),
        (4002, 5, 0.88,"Bioinformatics","Biochemistry",          "Life Sciences"),
        (4003, 5, 0.82,"Bioinformatics","Biochemistry",          "Life Sciences"),
        (4004, 5, 0.79,"Bioinformatics","Biochemistry",          "Life Sciences"),
        (4005, 5, 0.75,"Bioinformatics","Biochemistry",          "Life Sciences"),
        (4006, 5, 0.70,"Bioinformatics","Biochemistry",          "Life Sciences"),
        # work 3002 has no topic (left-join test)
    ])

    return c
