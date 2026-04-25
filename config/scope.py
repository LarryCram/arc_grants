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
    "APD",      # Australian Postdoctoral Fellowship
    "APF",      # Australian Postdoctoral Fellowship (variant)
    "ARF",      # Australian Research Fellowship
    "QEII",     # QEII Fellow
    "APDI",     # Australian Postdoctoral Industry Fellowship
    "ARFI",     # Australian Research Fellowship Industry
    "DAATSIA",  # DAATSIA Fellowship
    "IRF",      # Industry Research Fellowship
})

# Grant scheme codes to retain (first two characters of grant_code).
# Excludes equipment/infrastructure (LE, IE), international mobility (LX, IN, IL),
# industry hubs (IH), and miscellaneous small schemes.
KEEP_SCHEMES = frozenset({
    "DP",   # Discovery Projects
    "LP",   # Linkage Projects
    "DE",   # Discovery Early Career Researcher Award
    "FT",   # Future Fellowships
    "FL",   # Laureate Fellowships
    "FF",   # Federation Fellowships
    "DI",   # Discovery Indigenous
})
