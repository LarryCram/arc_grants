"""
Tests for analysis/utils/exclusions.py.
"""

from analysis.utils.exclusions import exclude_reason, ALLOWED_TYPES


def test_allowed_type_with_real_title_kept():
    assert exclude_reason("A genuine research article about something", "article", "10.1/x") is None
    assert exclude_reason("A genuine preprint about something else", "preprint", None) is None


def test_disallowed_type_excluded():
    assert exclude_reason("Some dataset title here", "dataset", None) == "type_not_allowed"
    assert exclude_reason("Editorial", "editorial", None) == "type_not_allowed"
    assert exclude_reason("A peer review report", "peer-review", None) == "type_not_allowed"


def test_all_declared_allowed_types_pass_type_check():
    for t in ALLOWED_TYPES:
        assert exclude_reason("A real-looking title with enough words in it", t, "10.1/x") is None


def test_filename_artifact_excluded():
    """type=article, no DOI, bare filename title -- confirmed real pattern: OpenAlex
    supplementary-material deposits misclassified as independent articles."""
    assert exclude_reason("3093538.pdf", "article", None) == "filename_artifact"
    assert exclude_reason("2929318.PDF", "article", None) == "filename_artifact"
    assert exclude_reason(
        "CrookHumanEffectsEcologicalConnectivityAquaticEcosystemsFigures1-6.pdf", "article", None
    ) == "filename_artifact"


def test_filename_pattern_requires_no_doi():
    """A real article that happens to have a DOI is never excluded on title shape alone."""
    assert exclude_reason("3093538.pdf", "article", "10.1000/real-doi") is None


def test_filename_pattern_requires_whole_string_match():
    """A real title that merely mentions a filename must not be excluded."""
    assert exclude_reason("Supplementary data available as 3093538.pdf in the appendix", "article", None) is None


def test_short_real_titles_not_excluded_by_filename_rule():
    """Short, real titles (not filenames) must not be caught by the filename check."""
    assert exclude_reason("Editorial", "article", None) is None
    assert exclude_reason("Introduction", "article", None) is None
    assert exclude_reason("Nonstandard Errors", "article", None) is None


def test_none_title():
    assert exclude_reason(None, "article", None) is None  # type check alone still applies
    assert exclude_reason(None, "dataset", None) == "type_not_allowed"
