"""
Project-wide settings and path configuration.
All scripts import from here rather than hardcoding paths.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Validate all required machine-local vars before constructing paths
_required = {
    "DATA_ROOT":    os.getenv("DATA_ROOT", ""),
    "OUTPUT_ROOT":  os.getenv("OUTPUT_ROOT", ""),
    "OPENALEX_DIR": os.getenv("OPENALEX_DIR", ""),
}
_missing = [k for k, v in _required.items() if not v]
if _missing:
    raise EnvironmentError(f"Not set in .env: {', '.join(_missing)}")

DATA_ROOT    = Path(_required["DATA_ROOT"])
OUTPUT_ROOT  = Path(_required["OUTPUT_ROOT"])
OPENALEX_DIR = Path(_required["OPENALEX_DIR"])

# Data subdirectories
RAW_DATA       = DATA_ROOT / "raw"
PROCESSED_DATA = DATA_ROOT / "processed"

# Output subdirectories
PROFILES_OUT = OUTPUT_ROOT / "profiles"

# Source data files
ARC_GRANTS_CSV      = RAW_DATA / "raw_json.csv"
GRANT_SUMMARIES_CSV = RAW_DATA / "grant_summaries.csv"
ADMIN_ORGS_CSV      = DATA_ROOT / "admin_orgs.csv"
FOR_OAX_CSV         = DATA_ROOT / "for_oax_concordance.csv"

# OpenAlex
OAX_AUTHORS     = OPENALEX_DIR / "authors"
OAX_AUTHORSHIPS = OPENALEX_DIR / "authorships"

# OpenAlex API (required since March 2026)
OPENALEX_EMAIL   = os.getenv("OPENALEX_EMAIL", "")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "")