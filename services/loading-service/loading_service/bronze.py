"""Bronze: land source data unchanged (as text) with ingestion metadata."""
import glob
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .audit import JobRun, now


def run_bronze(engine: Engine, cfg: dict, batch_id: str, run: JobRun,
               base_dir: str | Path = ".") -> None:
    if cfg["source_type"] == "kafka":
        # Kafka events are landed by kafka/consumer into the same bronze table.
        raise NotImplementedError("kafka bronze sources are landed by the Kafka consumer")

    pattern = str(Path(base_dir) / cfg["source_location"])
    with engine.connect() as conn:
        done = set(conn.execute(
            text("SELECT file_path FROM control.ingested_file WHERE bronze_id = :id"),
            {"id": cfg["id"]}).scalars())

    options = cfg.get("options") or {}
    for path in sorted(glob.glob(pattern)):
        if path in done:
            continue
        df = pd.read_csv(path, dtype=str, sep=options.get("delimiter", ","))
        df["_batch_id"] = batch_id
        df["_source_file"] = path
        df["_ingested_at"] = now()
        with engine.begin() as conn:
            df.to_sql(cfg["bronze_table"], conn, schema=cfg["bronze_schema"],
                      if_exists="append", index=False)
            conn.execute(text(
                "INSERT INTO control.ingested_file (bronze_id, file_path, batch_id, row_count) "
                "VALUES (:id, :path, :batch, :rows)"),
                {"id": cfg["id"], "path": path, "batch": batch_id, "rows": len(df)})
        run.rows_read += len(df)
        run.rows_written += len(df)
