"""Loading service: landing zone -> Snowflake RAW (PUT + COPY), audited in AUDIT.LOAD_LOG.

Run locally:  uvicorn app.main:app --reload --port 8003
"""
import logging

from fastapi import FastAPI

from .routes.load import router

logging.basicConfig(level=logging.INFO)
app = FastAPI(title="metaflow-dq loading-service")
app.include_router(router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
