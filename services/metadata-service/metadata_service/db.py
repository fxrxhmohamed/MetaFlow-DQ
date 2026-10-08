import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine(url: str | None = None) -> Engine:
    """Engine from METAFLOW_DB_URL, or the POSTGRES_* variables in .env."""
    url = url or os.getenv("METAFLOW_DB_URL")
    if not url:
        user = os.getenv("POSTGRES_USER", "metaflow")
        password = os.getenv("POSTGRES_PASSWORD", "metaflow")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "metaflow")
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, future=True)
