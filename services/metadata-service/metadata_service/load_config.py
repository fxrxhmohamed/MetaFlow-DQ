"""Load config/*.yaml into the control tables. Run from CI after merge.

    python -m metadata_service.load_config --config-dir config
    python -m metadata_service.load_config --config-dir config --dry-run   # validate only
"""
import argparse
from pathlib import Path

from sqlalchemy import text

from .config import TABLE_COLUMNS, load_config_dir
from .db import get_engine
from .scd2 import sync_table

DDL = Path(__file__).resolve().parents[3] / "sql" / "control" / "001_control_schema.sql"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config-dir", default="config")
    parser.add_argument("--dry-run", action="store_true", help="validate config, touch no database")
    parser.add_argument("--apply-ddl", action="store_true", help="create control/audit schema first")
    parser.add_argument("--db-url")
    args = parser.parse_args(argv)

    tables = load_config_dir(args.config_dir)
    if args.dry_run:
        for table, records in tables.items():
            print(f"{table}: {len(records)} records OK")
        return

    engine = get_engine(args.db_url)
    with engine.begin() as conn:
        if args.apply_ddl:
            conn.execute(text(DDL.read_text()))
        for table in TABLE_COLUMNS:
            changes = sync_table(conn, table, tables[table])
            print(f"{table}: {len(changes.inserts)} inserted, "
                  f"{len(changes.expire_ids)} expired, {changes.unchanged} unchanged")


if __name__ == "__main__":
    main()
