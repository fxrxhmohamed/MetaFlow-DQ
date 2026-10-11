# Airflow with the data-quality pipeline's Python next to it.
#
# Airflow keeps its own libraries; the pipeline (dq.py and the services)
# runs in a separate virtual environment, so its pinned pandas/numpy/
# pyarrow never clash with Airflow's. The DAGs call it through
# METAFLOW_PYTHON (see docker-compose.yml).

FROM apache/airflow:3.0.6-python3.12

COPY services/dq-service/requirements.txt /tmp/requirements-dq.txt
COPY services/metadata-service/requirements.txt /tmp/requirements-metadata.txt

RUN python -m venv /home/airflow/metaflow-venv \
 && /home/airflow/metaflow-venv/bin/pip install --no-cache-dir \
        -r /tmp/requirements-dq.txt -r /tmp/requirements-metadata.txt
