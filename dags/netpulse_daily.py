"""DAG Airflow : traitement quotidien des mesures de qualité de service réseau.

attente du fichier -> validation -> branchement (bloquer et alerter, ou continuer)
-> transformations PySpark -> détection d'anomalies -> chargement dans l'entrepôt -> synthèse
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor

DATA_DIR = os.environ.get("NETPULSE_DATA_DIR", "/opt/netpulse/data")


def _alert_on_failure(context):
    from netpulse.alerts import send_alert

    ti = context["task_instance"]
    send_alert("CRITICAL", f"Échec de la tâche {ti.task_id}", {"try": ti.try_number}, context["ds"])


default_args = {
    "owner": "data-network",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _alert_on_failure,
}


@dag(
    dag_id="netpulse_daily",
    description="Qualité de service réseau : validation, PySpark, anomalies, entrepôt",
    schedule="0 6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,  # les fenêtres glissantes dépendent des jours précédents
    default_args=default_args,
    tags=["network", "data-quality", "pyspark", "ml"],
)
def netpulse_daily():
    def _raw_file_present(ds: str) -> bool:
        return os.path.exists(f"{DATA_DIR}/raw/date={ds}/kpi.csv")

    wait_for_raw = PythonSensor(
        task_id="wait_for_raw_file",
        python_callable=_raw_file_present,
        op_kwargs={"ds": "{{ ds }}"},
        poke_interval=300,
        timeout=4 * 3600,
        mode="reschedule",  # libère le worker entre deux vérifications
    )

    @task
    def validate_input(ds=None) -> dict:
        from netpulse import validate

        return validate.run(ds).to_dict()

    @task.branch
    def quality_gate(report: dict, ds=None) -> str:
        from netpulse.alerts import send_alert

        if report["status"] == "CRITICAL":
            send_alert("CRITICAL", f"Lot du {ds} bloqué par la validation", report, ds)
            return "stop_pipeline"
        if report["status"] == "WARNING":
            send_alert("WARNING", f"Qualité dégradée le {ds}", {"checks": report["checks"]}, ds)
        return "spark_transform"

    stop = EmptyOperator(task_id="stop_pipeline")

    @task
    def spark_transform(ds=None) -> dict:
        from netpulse import transform

        spark = transform.get_spark(f"netpulse-{ds}")
        try:
            return transform.run(ds, spark)
        finally:
            spark.stop()

    @task
    def detect_anomalies(ds=None) -> dict:
        from netpulse import anomaly
        from netpulse.alerts import send_alert

        summary = anomaly.run(ds)
        if summary["critical"]:
            send_alert("CRITICAL", f"{summary['critical']} heures de coupure détectées le {ds}",
                       {"cells": summary["cells_impacted"]}, ds)
        return summary

    @task
    def load_warehouse(ds=None) -> dict:
        from netpulse import load

        return load.run(ds)

    @task
    def publish_summary(transformed: dict, anomalies: dict, loaded: dict, ds=None) -> None:
        print(f"[{ds}] {transformed['hourly_rows']} mesures, {anomalies['anomalies']} anomalies, chargé : {loaded}")

    report = validate_input()
    gate = quality_gate(report)
    wait_for_raw >> report
    transformed = spark_transform()
    gate >> [transformed, stop]
    anomalies = detect_anomalies()
    loaded = load_warehouse()
    transformed >> anomalies >> loaded
    publish_summary(transformed, anomalies, loaded)


netpulse_daily()
