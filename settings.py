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
ARC_GRANTS_CSV = RAW_DATA / "arc_grants.csv"

# Validate on import
if not DATA_ROOT:
    raise EnvironmentError("DATA_ROOT not set in .env")