"""Where the data-quality outputs of a run live, and the quarantine ledger.

A run is one ingestion (its ingestion id) or an ad hoc file (a timestamp).
Under the output root (data/):

    validated/<dataset>/<run>/valid.csv
        records that passed on the first validation
    quarantine/<dataset>/<run>/quarantine_status.csv
        the ledger: one row per quarantined record with its status
    quarantine/<dataset>/<run>/quarantine_history.csv
        every status change of the ledger, in order
    quarantine/<dataset>/<run>/round_<n>/
        quarantine_records.csv   records open in round n (cleaned values)
        quarantine_errors.csv    one row per failed rule and record
        correction_template.csv  written by "quarantine.py template"
    corrected/<dataset>/<run>/round_<n>/
        corrections.csv          imported manual corrections for round n
        import_rejections.csv    correction lines that were refused
        resolved.csv             records that passed re-validation
    quality/<dataset>/<run>/round_<n>/
        dq_result.json, remediation_audit.csv
    quality/<dataset>/<run>/correction_log.csv
        every manual correction, all rounds
    final/<dataset>/<run>/clean.csv
        silver: valid records plus resolved ones, one row per record

Round 1 is the first validation; re-validating the corrections of
round n produces round n + 1.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


# Record status in the quarantine ledger.
OPEN = "OPEN"
RESOLVED = "RESOLVED"
MANUAL_REVIEW = "MANUAL_REVIEW_REQUIRED"
DISCARDED = "DISCARDED"

STATUS_COLUMNS = ["record_id", "status", "round", "attempts", "updated_at"]
HISTORY_COLUMNS = ["record_id", "status", "round", "attempts", "changed_at"]

# Engine-added columns, never fed back into validation.
DQ_COLUMNS = ("record_id", "dq_errors", "dq_warnings")


class LayoutError(ValueError):
    """A run, round or ingestion that does not exist."""


@dataclass(frozen=True)
class Layout:
    root: Path
    dataset: str
    run_id: str

    def _dir(self, area: str) -> Path:
        return self.root / area / self.dataset / self.run_id

    @property
    def valid_file(self) -> Path:
        return self._dir("validated") / "valid.csv"

    @property
    def status_file(self) -> Path:
        return self._dir("quarantine") / "quarantine_status.csv"

    @property
    def history_file(self) -> Path:
        return self._dir("quarantine") / "quarantine_history.csv"

    @property
    def final_file(self) -> Path:
        return self._dir("final") / "clean.csv"

    @property
    def correction_log(self) -> Path:
        return self._dir("quality") / "correction_log.csv"

    def quarantine_round(self, round_number: int) -> Path:
        return self._dir("quarantine") / f"round_{round_number}"

    def corrected_round(self, round_number: int) -> Path:
        return self._dir("corrected") / f"round_{round_number}"

    def quality_round(self, round_number: int) -> Path:
        return self._dir("quality") / f"round_{round_number}"

    def run_dirs(self) -> list[Path]:
        return [
            self._dir(area)
            for area in ("validated", "quarantine", "corrected", "quality", "final")
        ]


def latest_run(root: Path, dataset: str) -> str:
    """Most recent run that has a quarantine ledger."""

    runs = list_runs(root, dataset)
    if not runs:
        raise LayoutError(f"No validated runs for {dataset} under {root}")
    return runs[-1]


# ============================================================
# Ingestions (raw layer)
# ============================================================

def ingestion_folder(raw_root: Path, dataset: str, ingestion_id: str) -> Path:
    """Folder of an ingestion; 'latest' picks the newest one."""

    dataset_root = raw_root / dataset
    if ingestion_id == "latest":
        folders = sorted(
            folder for folder in dataset_root.glob("ING_*")
            if (folder / "manifest.json").is_file()
        )
        if not folders:
            raise LayoutError(f"No ingestions under {dataset_root}")
        return folders[-1]

    folder = dataset_root / ingestion_id
    if not (folder / "manifest.json").is_file():
        raise LayoutError(f"No manifest for {ingestion_id} in {dataset_root}")
    return folder


def read_manifest(folder: Path) -> dict[str, Any]:
    """Manifest of an ingestion, after checking the raw copy is unchanged."""

    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    stored = folder / manifest["stored_file"]

    digest = hashlib.sha256()
    with stored.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)

    if digest.hexdigest() != manifest["sha256"]:
        raise LayoutError(
            f"{stored} no longer matches its manifest hash; the raw layer "
            "must not be modified."
        )

    return manifest


def record_ids(prefix: str, width: int, count: int) -> pd.Series:
    """<prefix><row number>, row numbers starting at 1."""

    return pd.Series(
        [f"{prefix}{n:0{width}d}" for n in range(1, count + 1)],
        dtype="string",
    )


# ============================================================
# CSV and ledger helpers
# ============================================================

def read_text_csv(path: Path) -> pd.DataFrame:
    """Read every column as text; empty cells become null."""

    frame = pd.read_csv(
        path, dtype="string", keep_default_na=False, encoding="utf-8-sig",
    )
    return frame.mask(frame.eq(""))


def write_csv(frame: pd.DataFrame, path: Path, excel: bool = False) -> None:
    """Write a CSV; excel=True adds a BOM so Excel reads UTF-8 correctly."""

    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig" if excel else "utf-8")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_status(layout: Layout) -> pd.DataFrame:
    if not layout.status_file.is_file():
        raise LayoutError(
            f"Run {layout.run_id} has no quarantine ledger; validate it first."
        )
    status = read_text_csv(layout.status_file)
    status["round"] = status["round"].astype(int)
    status["attempts"] = status["attempts"].astype(int)
    return status


def save_status(layout: Layout, status: pd.DataFrame) -> None:
    """Write the ledger and append the rows that changed to its history."""

    status = status[STATUS_COLUMNS]
    previous = (
        load_status(layout) if layout.status_file.is_file()
        else pd.DataFrame(columns=STATUS_COLUMNS)
    )

    # Ledgers written before the history existed: their current state
    # becomes its first entries.
    if not layout.history_file.is_file() and len(previous):
        _append_history(layout, previous)

    key = ["record_id", "status", "round", "attempts"]
    merged = status.merge(
        previous[key].astype({"round": int, "attempts": int}),
        on=key, how="left", indicator=True,
    )
    changed = status[merged["_merge"].eq("left_only").to_numpy()]

    write_csv(status, layout.status_file)
    if len(changed) or not layout.history_file.is_file():
        _append_history(layout, changed)


def _append_history(layout: Layout, rows: pd.DataFrame) -> None:
    history = rows.rename(columns={"updated_at": "changed_at"})[HISTORY_COLUMNS]
    path = layout.history_file
    path.parent.mkdir(parents=True, exist_ok=True)
    history.to_csv(
        path, mode="a", header=not path.is_file(), index=False,
        encoding="utf-8",
    )


def load_history(layout: Layout) -> pd.DataFrame:
    """Status changes in order; the ledger itself when there is no history."""

    if layout.history_file.is_file():
        history = read_text_csv(layout.history_file)
    else:
        history = load_status(layout).rename(columns={"updated_at": "changed_at"})
    history = history[HISTORY_COLUMNS].copy()
    history["round"] = history["round"].astype(int)
    history["attempts"] = history["attempts"].astype(int)
    return history


def list_runs(root: Path, dataset: str) -> list[str]:
    """Runs that have a quarantine ledger, oldest first."""

    return sorted(
        folder.name
        for folder in (root / "quarantine" / dataset).glob("*")
        if (folder / "quarantine_status.csv").is_file()
    )


def current_round(status: pd.DataFrame) -> int | None:
    """Round holding the open records (None when nothing is open)."""

    open_rounds = status.loc[status["status"].eq(OPEN), "round"]
    return int(open_rounds.max()) if len(open_rounds) else None


def data_columns(frame: pd.DataFrame) -> list[str]:
    return [c for c in frame.columns if c not in DQ_COLUMNS]
