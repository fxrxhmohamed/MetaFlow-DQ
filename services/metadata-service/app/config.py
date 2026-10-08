import os
from pathlib import Path

LANDING_ROOT = Path(os.getenv("LANDING_ROOT", "data/landing"))
REPORTS_ROOT = Path(os.getenv("PROFILE_REPORTS_ROOT", "data/reports/profiling"))
