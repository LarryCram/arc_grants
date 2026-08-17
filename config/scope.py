"""
Scope definitions for the ARC grants productivity study.
Edit here to change which schemes and roles are included in analysis.
"""

# Investigator role codes to retain.
# Excludes travel/mobility fellowships (ECIF, MCIF, LXF*, ILF, DIA, LIF, RC-ATSI)
# and non-investigator roles (PI, NP, OI, IC, AC, TCD, MEN, HD, SUP, CD, etc.)
KEEP_ROLES = frozenset({
    "CI",       # Chief Investigator
    "CI-DORA",  # CI — Declaration on Research Assessment
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

# Early-career fellowship cohort (analysis/07_analyse_ecr_fellowships.py, analysis/01_fetch_oeuvres.py --ecr).
# APF is deliberately excluded despite the similar code -- confirmed via ARC's own role_name
# field 2026-08-08 that APF is "Australian Professorial Fellowship" (senior), not postdoctoral.
ECR_ROLES = frozenset({
    "DECRA",    # Discovery Early Career Researcher Award
    "APD",      # Australian Postdoctoral Fellowship
    "APDI",     # Australian Postdoctoral Fellowship (Industry)
})

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
