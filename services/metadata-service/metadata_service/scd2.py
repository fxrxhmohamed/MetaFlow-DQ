"""SCD Type 2 upsert of config records into the control tables."""
import hashlib
import json
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.engine import Connection

from .config import TABLE_COLUMNS

JSON_COLUMNS = {"options", "business_keys", "params", "depends_on"}


def record_hash(record: dict) -> str:
    payload = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


@dataclass
class Changes:
    inserts: list[dict] = field(default_factory=list)
    expire_ids: list[str] = field(default_factory=list)
    unchanged: int = 0


def plan_changes(active: dict[str, str], incoming: list[dict]) -> Changes:
    """active: id -> record_hash of the currently active rows.

    New ids are inserted; changed ids are expired and re-inserted; ids no longer
    in config are expired.
    """
    changes = Changes()
    incoming_ids = set()
    for record in incoming:
        incoming_ids.add(record["id"])
        h = record_hash(record)
        current = active.get(record["id"])
        if current == h:
            changes.unchanged += 1
            continue
        if current is not None:
            changes.expire_ids.append(record["id"])
        changes.inserts.append({**record, "record_hash": h})
    changes.expire_ids.extend(sorted(set(active) - incoming_ids))
    return changes


def apply_changes(conn: Connection, table: str, changes: Changes) -> None:
    if changes.expire_ids:
        conn.execute(
            text(f"UPDATE control.{table} SET record_end_ts = now(), record_is_active = FALSE "
                 "WHERE record_is_active AND id = ANY(:ids)"),
            {"ids": changes.expire_ids},
        )
    if not changes.inserts:
        return
    cols = list(TABLE_COLUMNS[table]) + ["config_file_name", "record_hash"]
    values = ", ".join(
        f"CAST(:{c} AS jsonb)" if c in JSON_COLUMNS else f":{c}" for c in cols)
    stmt = text(f"INSERT INTO control.{table} ({', '.join(cols)}) VALUES ({values})")
    rows = [
        {c: json.dumps(r[c]) if c in JSON_COLUMNS else r[c] for c in cols}
        for r in changes.inserts
    ]
    conn.execute(stmt, rows)


def sync_table(conn: Connection, table: str, records: list[dict]) -> Changes:
    if table not in TABLE_COLUMNS:
        raise ValueError(f"unknown control table {table!r}")
    active = dict(conn.execute(
        text(f"SELECT id, record_hash FROM control.{table} WHERE record_is_active")).all())
    changes = plan_changes(active, records)
    apply_changes(conn, table, changes)
    return changes
