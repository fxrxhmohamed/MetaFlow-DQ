"""Metadata service: profiles landed files and serves the results.

Run locally:  uvicorn app.main:app --reload --port 8002
CLI:          python -m app.profiler trips data/landing/trips --stations data/landing/stations
"""
from fastapi import FastAPI

from .routes.profile import router

app = FastAPI(title="metaflow-dq metadata-service")
app.include_router(router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
