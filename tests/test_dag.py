"""Intégrité du DAG Airflow (exécuté seulement si Airflow est installé)."""
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAG_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    return bag.get_dag("netpulse_daily")


def test_dag_structure(dag):
    assert dag is not None
    assert set(dag.task_ids) == {
        "wait_for_raw_file", "validate_input", "quality_gate", "stop_pipeline",
        "spark_transform", "detect_anomalies", "load_warehouse", "publish_summary",
    }


def test_dependencies(dag):
    down = lambda t: set(dag.get_task(t).downstream_task_ids)  # noqa: E731
    assert down("wait_for_raw_file") == {"validate_input"}
    assert down("validate_input") == {"quality_gate"}
    assert down("quality_gate") == {"spark_transform", "stop_pipeline"}
    assert "detect_anomalies" in down("spark_transform")
    assert "load_warehouse" in down("detect_anomalies")


def test_operational_settings(dag):
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] == 2
    assert dag.get_task("wait_for_raw_file").mode == "reschedule"
