import os
from pathlib import Path

LANDING_ROOT = Path(os.getenv("LANDING_ROOT", "data/landing"))
DEFAULT_YEARS = {int(y) for y in os.getenv("INGEST_YEARS", "2024,2025,2026").split(",")}
