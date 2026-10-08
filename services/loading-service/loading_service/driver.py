"""Run one pipeline step from its control-table metadata.

    python -m loading_service.driver --layer silver --id metro_bike_trips

Airflow calls run_step() once per task; the CLI is for local runs.
"""
import argparse
import uuid

from metadata_service.db import get_engine
from metadata_service.repository import get_one, list_active

from .audit import job_run
from .bronze import run_bronze
from .gold import run_gold
from .silver import run_silver


def run_step(layer: str, pipeline_id: str, *, batch_id: str | None = None,
             base_dir: str = ".", dag_id: str | None = None, task_id: str | None = None,
             engine=None) -> None:
    engine = engine or get_engine()
    batch_id = batch_id or str(uuid.uuid4())
    table = {"bronze": "bronze_control", "silver": "silver_control", "gold": "gold_control"}[layer]
    cfg = get_one(engine, table, pipeline_id)

    with job_run(engine, batch_id=batch_id, layer=layer, pipeline_id=pipeline_id,
                 dag_id=dag_id, task_id=task_id) as run:
        if layer == "bronze":
            run_bronze(engine, cfg, batch_id, run, base_dir)
        elif layer == "silver":
            bronze = get_one(engine, "bronze_control", cfg["bronze_id"])
            assignments = list_active(engine, "dq_rules_assignment", silver_id=pipeline_id)
            rules = {r["id"]: r for r in list_active(engine, "dq_rules")}
            run_silver(engine, cfg, bronze, assignments, rules, batch_id, run, base_dir)
        else:
            run_gold(cfg, run, dbt_project_dir=f"{base_dir}/dbt")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold"])
    parser.add_argument("--id", required=True, help="control table id")
    parser.add_argument("--batch-id")
    parser.add_argument("--base-dir", default=".")
    args = parser.parse_args(argv)
    run_step(args.layer, args.id, batch_id=args.batch_id, base_dir=args.base_dir)


if __name__ == "__main__":
    main()
