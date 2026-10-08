"""One DAG built from the control tables: a task per bronze, silver and gold row.

Onboarding a dataset is a new config/pipelines/*.yaml loaded by CI; this file
does not change. Requires the repo installed in the Airflow image (pip install .).
"""
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from loading_service.driver import run_step
from loading_service.graph import build_graph
from metadata_service.db import get_engine
from metadata_service.repository import list_active

BASE_DIR = os.getenv("METAFLOW_BASE_DIR", "/opt/metaflow")
ALERT_EMAIL = [e for e in os.getenv("ALERT_EMAIL", "").split(",") if e]

log = logging.getLogger(__name__)


def _load_graph():
    try:
        engine = get_engine()
        return build_graph(list_active(engine, "bronze_control"),
                           list_active(engine, "silver_control"),
                           list_active(engine, "gold_control"))
    except Exception:
        # Keep the DAG importable when the control database is unreachable.
        log.exception("could not read control tables; DAG has no tasks")
        return [], []


def _run(layer, pipeline_id, **context):
    run_step(layer, pipeline_id, batch_id=context["run_id"], base_dir=BASE_DIR,
             dag_id=context["dag"].dag_id, task_id=context["task"].task_id)


with DAG(
    dag_id="metaflow_pipelines",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email": ALERT_EMAIL,
        "email_on_failure": bool(ALERT_EMAIL),
    },
    tags=["metaflow", "metadata-driven"],
) as dag:
    steps, edges = _load_graph()
    tasks = {
        step: PythonOperator(task_id=step.task_id, python_callable=_run,
                             op_kwargs={"layer": step.layer, "pipeline_id": step.id})
        for step in steps
    }
    for upstream, downstream in edges:
        tasks[upstream] >> tasks[downstream]
