"""
Scope definitions for the ARC grants productivity study.
Edit here to change which schemes and roles are included in analysis.
"""

# Investigator role codes to retain.
# Excludes travel/mobility fellowships (ECIF, MCIF, LXF*, ILF, DIA, LIF, RC-ATSI)
# and non-investigator roles (PI, NP, OI, IC, AC, TCD, MEN, HD, SUP, CD, etc.)
KEEP_ROLES = frozenset({
    "CI",       # Chief Investigator
    "CI-DORA",  # CI — Discovery Outstanding Researcher Award — corrected 2026-09-18, was
                #   mislabeled "Declaration on Research Assessment"; user-confirmed early-career.
    "DECRA",    # Discovery Early Career Researcher Award
    "FT",       # Future Fellow
    "FL",       # Laureate Fellow
    "FF",       # Federation Fellow
    "APD",      # Australian Postdoctoral Fellowship (early career)
    "APF",      # Australian Professorial Fellowship — SENIOR, not postdoctoral;
                #   corrected 2026-08-08, was mislabeled "Postdoctoral Fellowship (variant)"
    "ARF",      # Australian Research Fellowship
    "QEII",     # QEII Fellow
    "APDI",     # Australian Postdoctoral Fellowship (Industry) (early career)
    "ARFI",     # Australian Research Fellowship (Indigenous) — corrected 2026-08-08,
                #   was mislabeled "Australian Research Fellowship Industry"
    "DAATSIA",  # DAATSIA Fellowship
    "IRF",      # Indigenous Research(er) Fellowship — corrected 2026-08-08,
                #   was mislabeled "Industry Research Fellowship"
})

# Fellowship career-stage classification -- single canonical source, user-confirmed 2026-09-18.
# Every fellowship role_code in KEEP_ROLES appears exactly once here; CI itself is not a
# fellowship and has no tier. Built as one object (not three independently-maintained sets)
# specifically to avoid the duplicate-hardcoded-list drift this project has repeatedly found
# and had to fix elsewhere (e.g. 01a_diagnose.py's own SCHEMES_OF_INTEREST list silently
# drifting from this same file's KEEP_SCHEMES).
FELLOWSHIP_TIER: dict[str, str] = {
    "DECRA":   "Early-career",
    "APD":     "Early-career",
    "APDI":    "Early-career",
    "CI-DORA": "Early-career",  # Discovery Outstanding Researcher Award
    "IRF":     "Early-career",  # Indigenous Research(er) Fellowship
    "DAATSIA": "Early-career",
    "FT":      "Mid-career",    # Future Fellow
    "QEII":    "Mid-career",
    "ARF":     "Mid-career",
    "ARFI":    "Mid-career",    # Australian Research Fellowship (Indigenous)
    "FF":      "Senior",        # Federation Fellow
    "FL":      "Senior",        # Laureate Fellow
    "APF":     "Senior",        # Australian Professorial Fellowship -- confirmed senior
                                 #   2026-08-08, not postdoctoral despite the similar code.
}

# Derived sets, kept for callers that want a plain frozenset rather than the tier dict itself
# (analysis/07_analyse_ecr_fellowships.py, analysis/01_fetch_oeuvres.py --ecr).
ECR_ROLES = frozenset(c for c, t in FELLOWSHIP_TIER.items() if t == "Early-career")
MCR_ROLES = frozenset(c for c, t in FELLOWSHIP_TIER.items() if t == "Mid-career")
SRF_ROLES = frozenset(c for c, t in FELLOWSHIP_TIER.items() if t == "Senior")

# Grant scheme codes to retain (first two characters of grant_code).
# Excludes equipment/infrastructure (LE, IE), international mobility (LX, IN, IL),
# industry hubs (IH), Discovery Indigenous (DI -- dropped 2026-08-17: the scheme exists
# specifically to fund Indigenous-focused research and develop Indigenous researchers, so
# every grant under it is exactly the kind of research this project already deliberately keeps
# out of its bibliometric methods (see set_aside_indigenous_research()) -- relying on that
# downstream FOR2020-division-45 check to catch DI grants is unreliable, since a grant's own
# declared primary FOR code doesn't always resolve to "Indigenous Studies" even when the scheme
# and subject matter unambiguously are (confirmed on a real case, DI0347845_DonnaOxenham, primary
# FOR "Historical Studies")), and miscellaneous small schemes.
KEEP_SCHEMES = frozenset({
    "DP",   # Discovery Projects
    "LP",   # Linkage Projects
    "DE",   # Discovery Early Career Researcher Award
    "FT",   # Future Fellowships
    "FL",   # Laureate Fellowships
    "FF",   # Federation Fellowships
})
