"""Tests de bout en bout : génération -> validation -> PySpark -> anomalies -> entrepôt."""
import json

import duckdb
import pandas as pd
import pytest

from netpulse import anomaly, generate, pipeline, transform
from netpulse.config import paths


def _generate(days=4, corrupt_day=None):
    args = ["--start", "2026-10-01", "--days", str(days)]
    if corrupt_day:
        args += ["--corrupt-day", corrupt_day]
    generate.main(args)


def test_full_pipeline_over_several_days(data_dir, spark):
    _generate(days=4)
    results = [pipeline.run_day(f"2026-10-0{i}", spark) for i in range(1, 5)]

    last = results[-1]
    assert last["transform"]["history_days"] == 3          # le 4e jour est lu avec ses 3 jours d'historique
    assert last["transform"]["cells"] == 40
    assert last["load"]["fact_kpi_hourly"] == last["transform"]["hourly_rows"]

    con = duckdb.connect(str(paths().warehouse), read_only=True)
    assert con.execute("SELECT count(DISTINCT day) FROM agg_cell_daily").fetchone()[0] == 4
    assert con.execute("SELECT count(*) FROM v_region_daily").fetchone()[0] == 16   # 4 régions x 4 jours
    assert con.execute("SELECT count(*) FROM dq_reports").fetchone()[0] == 4
    con.close()


def test_rerun_is_idempotent(data_dir, spark):
    _generate(days=2)
    pipeline.run_day("2026-10-01", spark)
    first = pipeline.run_day("2026-10-02", spark)
    second = pipeline.run_day("2026-10-02", spark)
    assert first["load"] == second["load"]
    con = duckdb.connect(str(paths().warehouse), read_only=True)
    n = con.execute("SELECT count(*) FROM fact_kpi_hourly WHERE CAST(ts AS DATE) = DATE '2026-10-02'").fetchone()[0]
    con.close()
    assert n == first["load"]["fact_kpi_hourly"]


def test_corrupted_day_is_blocked_and_alerted(data_dir, spark):
    _generate(days=2, corrupt_day="2026-10-02")
    pipeline.run_day("2026-10-01", spark)
    with pytest.raises(pipeline.DataQualityError):
        pipeline.run_day("2026-10-02", spark)
    assert not (paths().staging / "date=2026-10-02").exists()      # rien n'a été chargé
    alerts = [json.loads(l) for l in paths().alerts.read_text(encoding="utf-8").splitlines()]
    assert any(a["severity"] == "CRITICAL" and a["day"] == "2026-10-02" for a in alerts)


def test_rolling_features_use_previous_hours_only(data_dir, spark):
    _generate(days=2)
    pipeline.run_day("2026-10-01", spark)
    pipeline.run_day("2026-10-02", spark)
    h = pd.read_parquet(paths().features / "hourly" / "date=2026-10-02")
    one = h[h["cell_id"] == "PAR-001"].sort_values("ts")
    staged = pd.concat([pd.read_parquet(paths().staging / f"date=2026-10-0{i}") for i in (1, 2)])
    staged = staged[staged["cell_id"] == "PAR-001"].sort_values("ts").reset_index(drop=True)
    first_ts = one["ts"].iloc[0]
    pos = staged.index[pd.to_datetime(staged["ts"]) == first_ts][0]
    expected = staged["latency_ms"].iloc[max(0, pos - 24):pos].mean()
    assert one["lat_mean_24h"].iloc[0] == pytest.approx(expected, rel=1e-6)


def test_anomaly_detection_finds_injected_incidents(data_dir, spark):
    _generate(days=3)
    for i in range(1, 4):
        pipeline.run_day(f"2026-10-0{i}", spark)
    summary = json.loads((paths().reports / "anomalies_2026-10-03.json").read_text())
    ev = summary["evaluation"]["is_anomaly"]
    assert ev["recall"] >= 0.6
    assert ev["precision"] >= 0.6


def test_outage_is_always_critical():
    df = pd.DataFrame({
        "latency_z": [0.1, 8.0], "throughput_z": [0.0, -9.0], "packet_loss_pct": [0.3, 80.0],
        "availability_pct": [99.9, 3.0], "jitter_ms": [4.0, 30.0], "latency_ms": [30.0, 190.0],
    })
    df = pd.concat([df.iloc[[0]]] * 60 + [df.iloc[[1]]], ignore_index=True)
    out = anomaly.detect(df)
    assert out["severity"].iloc[-1] == "CRITICAL"
    assert out["is_anomaly"].iloc[-1]
