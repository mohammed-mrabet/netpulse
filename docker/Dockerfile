# Airflow 2.10 + Java 17 (requis par Spark) + dépendances du pipeline
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
RUN ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java  # indépendant de l'architecture (amd64 / arm64)
ENV JAVA_HOME=/opt/java
RUN mkdir -p /opt/netpulse/data && chown -R airflow:root /opt/netpulse

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

COPY --chown=airflow:root netpulse /opt/netpulse/netpulse
COPY --chown=airflow:root sql /opt/netpulse/sql
COPY --chown=airflow:root dags /opt/airflow/dags
ENV PYTHONPATH=/opt/netpulse \
    NETPULSE_DATA_DIR=/opt/netpulse/data
