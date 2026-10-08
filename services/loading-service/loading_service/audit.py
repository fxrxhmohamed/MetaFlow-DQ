import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


def now() -> datetime:
    return datetime.now(timezone.utc)


class JobRun:
    """Counts a step reports back for its job_run_audit row."""

    def __init__(self):
        self.rows_read = 0
        self.rows_written = 0


@contextmanager
def job_run(engine: Engine, *, batch_id: str, layer: str, pipeline_id: str,
            dag_id: str | None = None, task_id: str | None = None):
    run_id = uuid.uuid4()
    run = JobRun()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO audit.job_run_audit (job_run_id, batch_id, dag_id, task_id, layer, "
            "pipeline_id, job_status, task_start_ts) "
            "VALUES (:id, :batch, :dag, :task, :layer, :pipeline, 'RUNNING', :ts)"),
            {"id": run_id, "batch": batch_id, "dag": dag_id, "task": task_id, "layer": layer,
             "pipeline": pipeline_id, "ts": now()})
    status, error = "SUCCESS", None
    try:
        yield run
    except Exception as exc:
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"[:4000]
        raise
    finally:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE audit.job_run_audit SET job_status = :status, rows_read = :read, "
                "rows_written = :written, error_message = :error, task_end_ts = :ts "
                "WHERE job_run_id = :id"),
                {"status": status, "read": run.rows_read, "written": run.rows_written,
                 "error": error, "ts": now(), "id": run_id})


def write_dq_audit(conn: Connection, *, batch_id: str, pipeline_id: str, source_table: str,
                   target_table: str, result, src_count: int, target_count: int,
                   started: datetime) -> uuid.UUID:
    run_id = uuid.uuid4()
    conn.execute(text(
        "INSERT INTO audit.dq_run_audit (dq_task_run_id, batch_id, pipeline_id, source_table, "
        "target_table, pipeline_status, dq_check_outcome, src_rec_count, quarantine_count, "
        "warn_count, target_rec_count, dq_start_ts, dq_end_ts) VALUES (:id, :batch, :pipeline, "
        ":src_table, :tgt_table, 'SUCCESS', :outcome, :src, :quarantine, :warn, :tgt, "
        ":start, :end)"),
        {"id": run_id, "batch": batch_id, "pipeline": pipeline_id, "src_table": source_table,
         "tgt_table": target_table, "outcome": result.outcome, "src": src_count,
         "quarantine": len(result.quarantine), "warn": result.warn_count, "tgt": target_count,
         "start": started, "end": now()})
    if result.rule_results:
        conn.execute(text(
            "INSERT INTO audit.dq_rule_result (dq_task_run_id, assignment_id, rule_id, "
            "column_name, severity, checked_count, failed_count) VALUES (:run, :assignment_id, "
            ":rule_id, :column_name, :severity, :checked_count, :failed_count)"),
            [{"run": run_id, **vars(r)} for r in result.rule_results])
    return run_id
