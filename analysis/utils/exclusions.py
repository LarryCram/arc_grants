"""
analysis/utils/exclusions.py

Project-owned work-exclusion flag -- distinct from any OpenAlex-native flag
(is_retracted, is_paratext), which only cover a narrow slice of what shouldn't count
toward a person's genuine scholarly work count. Every excluded work carries a specific
reason (not just a bare boolean) so per-ECR exclusion counts can be reported and audited,
not just silently dropped.

Two exclusion reasons:
1. "type_not_allowed": OpenAlex `type` is not in ALLOWED_TYPES. Verified against the real
   type distribution across the full oeuvres corpus before settling this list -- excluded
   types are almost entirely non-original-research content (peer-review reports, editorials,
   paratext, library guides, errata, retraction notices, reference entries,
   supplementary-materials) plus datasets, ~5.5% of all rows.
2. "filename_artifact": type='article', no DOI, and the title is literally a bare filename
   (e.g. "3093538.pdf", "CrookHuman...Figures1-6.pdf"). Confirmed via direct inspection of
   several real OpenAlex records (abstract text describing itself as supplementary material,
   "Open MIND" as source, no real title ever assigned) that these are misclassified
   supplementary-material/data-file deposits, not independent scholarly outputs -- OpenAlex's
   own `type` field is simply wrong for these, not just badly titled.
"""

import re

import duckdb

ALLOWED_TYPES = [
    "article", "preprint", "report", "review", "book", "book-chapter",
    "dissertation", "software", "software-paper",
]

_FILENAME_TITLE = re.compile(r"^[A-Za-z0-9_-]+\.pdf$", re.IGNORECASE)


def exclude_reason(title: str | None, type_: str | None, doi: str | None) -> str | None:
    """None if the work should be kept; otherwise a short machine-readable reason."""
    if type_ not in ALLOWED_TYPES:
        return "type_not_allowed"
    if doi is None and title and _FILENAME_TITLE.match(title.strip()):
        return "filename_artifact"
    return None


def register(con: duckdb.DuckDBPyConnection) -> None:
    """Idempotent -- DuckDB doesn't allow re-registering a UDF name on the same
    connection, and callers (create_deduped_works, count_exclusions) may both run
    against the same connection in one session."""
    try:
        con.create_function(
            "exclude_reason", exclude_reason,
            ["VARCHAR", "VARCHAR", "VARCHAR"], "VARCHAR",
            null_handling="special",
        )
    except duckdb.NotImplementedException:
        pass
