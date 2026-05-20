"""
Path utilities. Ensures output directories exist before scripts write to them.
"""

from config.settings import PROFILES_OUT, PROCESSED_DATA

def ensure_dirs():
    """Create output directories if they don't exist."""
    for path in [PROFILES_OUT, PROCESSED_DATA]:
        path.mkdir(parents=True, exist_ok=True)