"""Read the active control rows the drivers run from."""
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import TABLE_COLUMNS


def list_active(engine: Engine, table: str, **filters) -> list[dict]:
    if table not in TABLE_COLUMNS:
        raise ValueError(f"unknown control table {table!r}")
    for col in filters:
        if col not in TABLE_COLUMNS[table]:
            raise ValueError(f"{table} has no column {col!r}")
    where = "".join(f" AND {col} = :{col}" for col in filters)
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT * FROM control.{table} WHERE record_is_active{where} ORDER BY id"),
            filters,
        ).mappings().all()
    return [dict(r) for r in rows]


def get_one(engine: Engine, table: str, id_: str) -> dict:
    rows = list_active(engine, table, id=id_)
    if not rows:
        raise LookupError(f"no active {table} row with id {id_!r}")
    return rows[0]
