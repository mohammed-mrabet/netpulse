from datetime import date

import numpy as np
import pandas as pd

from netpulse.generate import build_cells, corrupt, generate_day
from netpulse.validate import validate_frame

DAY = "2026-10-05"


def _day(seed=1):
    cells = build_cells()
    df, _ = generate_day(date.fromisoformat(DAY), cells, seed=seed)
    return df, cells


def test_generated_day_passes_with_quarantine_of_injected_defects():
    df, cells = _day()
    valid, quarantine, report = validate_frame(df, cells, DAY)
    assert report.status in ("OK", "WARNING")
    assert len(quarantine) > 0
    assert set(quarantine["reason"]).issuperset({"valeur_manquante", "cellule_inconnue"})
    assert any(r.startswith("hors_bornes") for r in quarantine["reason"])
    # aucune ligne valide ne viole les bornes
    assert valid["packet_loss_pct"].between(0, 100).all()
    assert (valid["throughput_mbps"] >= 0).all()


def test_duplicates_are_removed():
    df, cells = _day()
    valid, _, report = validate_frame(df, cells, DAY)
    assert report.duplicates_removed == 5
    assert not valid.duplicated(subset=["cell_id", "ts"]).any()


def test_rows_are_conserved():
    df, cells = _day()
    valid, quarantine, report = validate_frame(df, cells, DAY)
    assert report.rows_received == len(valid) + len(quarantine) + report.duplicates_removed


def test_corrupted_batch_is_blocked():
    df, cells = _day()
    _, _, report = validate_frame(corrupt(df), cells, DAY)
    assert report.status == "CRITICAL"
    assert {c.name: c.status for c in report.checks}["invalid_ratio"] == "CRITICAL"


def test_missing_column_is_critical():
    df, cells = _day()
    _, _, report = validate_frame(df.drop(columns=["latency_ms"]), cells, DAY)
    assert report.status == "CRITICAL"
    assert report.checks[0].name == "schema"


def test_low_completeness_is_detected():
    df, cells = _day()
    half = df.sample(frac=0.5, random_state=0)
    _, _, report = validate_frame(half, cells, DAY)
    assert {c.name: c.status for c in report.checks}["completeness"] == "CRITICAL"


def test_timestamp_outside_day_is_quarantined():
    df, cells = _day()
    df = df.copy()
    df.loc[0, "ts"] = "2026-10-04 23:00:00"
    _, quarantine, _ = validate_frame(df, cells, DAY)
    assert "hors_journee" in set(quarantine["reason"])


def test_non_numeric_value_is_quarantined():
    df, cells = _day()
    df = df.astype({"latency_ms": object})
    df.loc[3, "latency_ms"] = "N/A"
    _, quarantine, _ = validate_frame(df, cells, DAY)
    assert np.isnan(pd.to_numeric(quarantine["latency_ms"], errors="coerce")).any()
