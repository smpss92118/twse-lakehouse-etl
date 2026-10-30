# Runtime image = official Airflow + the pipeline's third-party libraries.
# Pipeline code (dags/ + twse_etl/) is NOT baked in: CD syncs it into the mounted DAGs folder,
# so a code change ships without rebuilding the image. Rebuild only when requirements change.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12
ARG AIRFLOW_VERSION

COPY docker/requirements.txt /tmp/requirements.txt
# Pinning apache-airflow in the same install keeps pip from upgrading Airflow's own dependencies.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
