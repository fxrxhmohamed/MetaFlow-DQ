"""Silver: read new bronze rows, cast, run DQ, quarantine drops, load by load_type."""
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

from dq_service import validate

from .audit import JobRun, now, write_dq_audit
from .schema import cast_to_schema, load_schema, quote, silver_ddl


def _get_watermark(conn: Connection, pipeline_id: str) -> str | None:
    return conn.execute(
        text("SELECT watermark_value FROM control.watermark "
             "WHERE pipeline_id = :id AND layer = 'silver'"), {"id": pipeline_id}).scalar()


def _set_watermark(conn: Connection, pipeline_id: str, value: str) -> None:
    conn.execute(text(
        "INSERT INTO control.watermark (pipeline_id, layer, watermark_value) "
        "VALUES (:id, 'silver', :value) ON CONFLICT (pipeline_id, layer) "
        "DO UPDATE SET watermark_value = EXCLUDED.watermark_value, updated_at = now()"),
        {"id": pipeline_id, "value": value})


def _read_source(conn: Connection, bronze: dict, wm_col: str | None, wm: str | None):
    source = f"{quote(bronze['bronze_schema'])}.{quote(bronze['bronze_table'])}"
    if not inspect(conn).has_table(bronze["bronze_table"], schema=bronze["bronze_schema"]):
        return pd.DataFrame()
    if wm_col and wm is not None:
        return pd.read_sql(text(f"SELECT * FROM {source} WHERE {quote(wm_col)} > :wm"),
                           conn, params={"wm": wm})
    return pd.read_sql(text(f"SELECT * FROM {source}"), conn)


def _load(conn: Connection, cfg: dict, df: pd.DataFrame, columns: list[str]) -> None:
    schema, table, keys = cfg["silver_schema"], cfg["silver_table"], cfg["business_keys"]
    if cfg["load_type"] == "append":
        df.to_sql(table, conn, schema=schema, if_exists="append", index=False)
        return

    stage = f"_stg_{table}"
    target, staged = f"{quote(schema)}.{quote(table)}", f"{quote(schema)}.{quote(stage)}"
    if cfg["load_type"] == "scd2":
        df = df.assign(_row_hash=pd.util.hash_pandas_object(df[columns], index=False).astype(str))
    df.to_sql(stage, conn, schema=schema, if_exists="replace", index=False)
    cols = ", ".join(map(quote, df.columns))
    on_keys = " AND ".join(f"t.{quote(k)} = s.{quote(k)}" for k in keys)

    if cfg["load_type"] == "scd1":
        updates = ", ".join(f"{quote(c)} = EXCLUDED.{quote(c)}" for c in df.columns
                            if c not in keys)
        conn.execute(text(
            f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staged} "
            f"ON CONFLICT ({', '.join(map(quote, keys))}) DO UPDATE SET {updates}, "
            '"_loaded_at" = now()'))
    else:
        # Close current versions whose content changed, then insert a new current
        # version for every staged key that has none (new keys and just-closed ones).
        conn.execute(text(
            f"UPDATE {target} t SET _valid_to = now(), _is_current = FALSE FROM {staged} s "
            f"WHERE {on_keys} AND t._is_current AND t._row_hash <> s._row_hash"))
        s_cols = ", ".join(f"s.{quote(c)}" for c in df.columns)
        conn.execute(text(
            f"INSERT INTO {target} ({cols}) SELECT {s_cols} FROM {staged} s "
            f"LEFT JOIN {target} t ON {on_keys} AND t._is_current "
            f"WHERE t.{quote(keys[0])} IS NULL"))
    conn.execute(text(f"DROP TABLE {staged}"))


def run_silver(engine: Engine, cfg: dict, bronze: dict, assignments: list[dict],
               rules: dict[str, dict], batch_id: str, run: JobRun,
               base_dir: str | Path = ".") -> None:
    started = now()
    columns = load_schema(Path(base_dir) / cfg["schema_file"])
    wm_col = cfg.get("watermark_column")

    with engine.begin() as conn:
        src = _read_source(conn, bronze, wm_col, _get_watermark(conn, cfg["id"]))
        run.rows_read = len(src)
        if src.empty:
            return

        typed = cast_to_schema(src, columns)
        result = validate(typed, assignments, rules)

        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote(cfg['silver_schema'])}"))
        conn.execute(text(silver_ddl(cfg["silver_schema"], cfg["silver_table"], columns,
                                     cfg["load_type"], cfg["business_keys"])))

        if len(result.quarantine):
            # Quarantine the raw bronze values so remediation sees what actually arrived.
            raw = src.loc[result.quarantine.index].assign(
                _failed_rules=result.quarantine["_failed_rules"],
                _quarantine_batch_id=batch_id, _quarantined_at=now())
            raw.to_sql(cfg["quarantine_table"], conn, schema="quarantine",
                       if_exists="append", index=False)

        valid = result.valid.assign(_batch_id=batch_id)
        if len(valid):
            _load(conn, cfg, valid, list(columns))
        run.rows_written = len(valid)

        if wm_col:
            _set_watermark(conn, cfg["id"], str(src[wm_col].max()))
        write_dq_audit(conn, batch_id=batch_id, pipeline_id=cfg["id"],
                       source_table=f"{bronze['bronze_schema']}.{bronze['bronze_table']}",
                       target_table=f"{cfg['silver_schema']}.{cfg['silver_table']}",
                       result=result, src_count=len(src), target_count=len(valid),
                       started=started)
