FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Standard library only: no requirements to install.
COPY services/ingestion-service/src/ ./src/

# Mount data/ at /app/data at run time.
ENTRYPOINT ["python", "/app/src/ingest.py"]
