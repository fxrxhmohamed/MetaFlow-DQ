FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/dq-service/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY services/dq-service/src/ ./src/

# Mount config/ and data/ at /app/config and /app/data at run time.
ENTRYPOINT ["python", "/app/src/dq_engine.py"]
