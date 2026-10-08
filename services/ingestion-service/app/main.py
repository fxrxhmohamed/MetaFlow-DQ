"""Ingestion service: lands Metro Bike Share files in the bronze landing zone.

Run locally:  uvicorn app.main:app --reload --port 8001
CLI backfill: python -m app.cli trips --years 2024 2025 2026
"""
import logging

from fastapi import FastAPI

from .routes.ingest import router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

app = FastAPI(title="metaflow-dq ingestion-service")
app.include_router(router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
