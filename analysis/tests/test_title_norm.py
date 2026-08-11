"""
Tests for analysis/utils/title_norm.py.

Every case here was validated against a real title pair found in the ARC-linked oeuvre
corpus before being encoded as a test (see title_norm.py's module docstring) -- these
aren't hypothetical edge cases, they're the actual disagreements found by diffing the old
(strip-everything) normalisation against this one across the full ~23k-person cohort.
"""

from analysis.utils.title_norm import normalize_title


def test_none_and_empty():
    assert normalize_title(None) is None
    assert normalize_title("") is None


def test_case_insensitive():
    assert normalize_title("Quantum Repeaters") == normalize_title("quantum repeaters")


def test_hyphen_space_equivalence():
    """Real pair: preprint used spaces, article used hyphens."""
    a = normalize_title("A method for high throughput image based antifungal screening")
    b = normalize_title("A method for high-throughput image-based antifungal screening")
    assert a == b


def test_smart_vs_straight_quotes():
    """Real pair differing only in curly vs straight quote marks."""
    a = normalize_title("The X-Files: Past and Present Portrayals of China's Alien 'Legal System'")
    b = normalize_title("The X-Files: Past and Present Portrayals of China's Alien “Legal System”")
    assert a == b


def test_trailing_punctuation_insensitive():
    a = normalize_title("Does the Badal optometer stimulate accommodation accurately?")
    b = normalize_title("Does the Badal optometer stimulate accommodation accurately")
    assert a == b

    a2 = normalize_title(
        "Human skeletal muscle creatine transporter mRNA and protein expression in healthy, young males and females."
    )
    b2 = normalize_title(
        "Human skeletal muscle creatine transporter mRNA and protein expression in healthy, young males and females"
    )
    assert a2 == b2


def test_slash_adjacent_whitespace():
    """Real pair: "Micro-/Nano" (hyphen before slash) vs "Micro/Nano" (no hyphen) --
    hyphen->space conversion must not leave a stray space next to the slash."""
    a = normalize_title("Silicon Micro-/Nanomachining and Applications")
    b = normalize_title("Silicon Micro/Nanomachining and Applications")
    assert a == b


def test_html_entity_vs_raw_tag():
    a = normalize_title("Fluorescently Tagged &lt;em&gt;Verticillium dahliae&lt;/em&gt; test title")
    b = normalize_title("Fluorescently Tagged <em>Verticillium dahliae</em> test title")
    assert a == b


def test_latex_greek_letter_name():
    """"\\gamma" (LaTeX command) must match the plain spelled-out name "gamma"."""
    a = normalize_title("Matter-affected neutrino oscillations and \\gamma-ray bursts")
    b = normalize_title("Matter-affected neutrino oscillations and gamma-ray bursts")
    assert a == b


def test_latex_accent_escape():
    """"\\"a" (LaTeX umlaut escape) must match the plain (diacritic-stripped) letter."""
    a = normalize_title("Reply to Els\\\"asser Comment on laser physics questions")
    b = normalize_title("Reply to Elsasser Comment on laser physics questions")
    assert a == b


def test_latex_formatting_macro_stripped():
    """\\mathbb{}/\\textit{} etc. carry no content -- strip the macro, keep the argument."""
    a = normalize_title("Background Independence: \\mathbb{S}^1 spaces in Shape Theory")
    b = normalize_title("Background Independence: S1 spaces in Shape Theory")
    assert a == b


def test_genuinely_different_titles_stay_distinct():
    """A journal Letter and its differently-worded conference-proceedings precursor --
    same underlying research, real different title text, must NOT collapse."""
    a = normalize_title("Quantum enhancement of signal-to-noise ratio with a heralded linear amplifier")
    b = normalize_title(
        "Quantum enhancement of signal-to-noise ratio for arbitrary coherent states using heralded linear amplifiers"
    )
    assert a != b


def test_accepted_miss_oxford_comma():
    """Documented accepted limitation, not a regression target -- asserting the current
    (imperfect) behaviour so a future change to this is a deliberate choice, not a
    silent side effect."""
    a = normalize_title("Alcohol, Young Adults and the New Millennium")
    b = normalize_title("Alcohol, Young Adults, and the New Millennium")
    assert a != b
