"""Gold: each table is a dbt model named in gold_control (the article's per-table notebook)."""
import subprocess
from pathlib import Path

from .audit import JobRun


def run_gold(cfg: dict, run: JobRun, dbt_project_dir: str | Path = "dbt") -> None:
    subprocess.run(
        ["dbt", "run", "--select", cfg["dbt_model"], "--project-dir", str(dbt_project_dir)],
        check=True,
    )
