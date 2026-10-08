"""Snowflake connection from environment variables (see .env.example).

Key-pair auth is preferred for services; password auth works for local dev.
"""
from __future__ import annotations

import os
from contextlib import contextmanager


def _private_key() -> bytes | None:
    path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not path:
        return None
    from cryptography.hazmat.primitives import serialization

    with open(path, "rb") as fh:
        key = serialization.load_pem_private_key(
            fh.read(), password=(os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or "").encode() or None)
    return key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


@contextmanager
def snowflake_connection():
    import snowflake.connector

    params = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.getenv("SNOWFLAKE_ROLE", "METAFLOW_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "METAFLOW_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "METAFLOW_DQ"),
        schema="RAW",
        application="metaflow-dq-loading",
        session_parameters={"TIMEZONE": "America/Los_Angeles", "QUERY_TAG": "metaflow-dq:loading"},
    )
    key = _private_key()
    if key:
        params["private_key"] = key
    else:
        params["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    conn = snowflake.connector.connect(**params)
    try:
        yield conn
    finally:
        conn.close()
