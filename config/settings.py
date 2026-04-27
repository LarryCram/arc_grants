"""
Project-wide settings and path configuration.
All scripts import from here rather than hardcoding paths.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Root paths - set in .env, never hardcoded
DATA_ROOT = Path(os.getenv("DATA_ROOT", ""))
OUTPUT_ROOT = Path(os.getenv("OUTPUT_ROOT", "output"))

# Data subdirectories
RAW_DATA = DATA_ROOT / "raw"
PROCESSED_DATA = DATA_ROOT / "processed"

# Output subdirectories
PROFILES_OUT = OUTPUT_ROOT / "profiles"

# Source data files
ARC_GRANTS_CSV      = RAW_DATA / "raw_json.csv"
GRANT_SUMMARIES_CSV = RAW_DATA / "grant_summaries.csv"
ADMIN_ORGS_CSV      = DATA_ROOT / "admin_orgs.csv"        # read-only concordance seed
FOR_OAX_CSV         = DATA_ROOT / "for_oax_concordance.csv"  # FoR 2-digit → OAX field

# OpenAlex
OPENALEX_DIR        = Path(os.getenv("OPENALEX_DIR", str(DATA_ROOT / "OPENALEX")))
OAX_AUTHORS         = OPENALEX_DIR / "authors"
OAX_AUTHORSHIPS     = OPENALEX_DIR / "authorships"

# OpenAlex API (required since March 2026)
OPENALEX_EMAIL   = os.getenv("OPENALEX_EMAIL", "")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "")

# Validate on import
if not DATA_ROOT:
    raise EnvironmentError("DATA_ROOT not set in .env")