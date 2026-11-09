"""Chargement dans l'entrepôt analytique DuckDB (idempotent par jour).

Les tables et les vues sont écrites en SQL standard pour rester portables vers un entrepôt
cloud de type BigQuery.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .config import paths

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

TABLES = {
    "fact_kpi_hourly": "hourly",
    "agg_cell_daily": "daily",
    "fact_anomalies": "anomalies",
}


def connect() -> duckdb.DuckDBPyConnection:
    p = paths()
    p.warehouse.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(p.warehouse))


def run(day: str) -> dict:
    p = paths()
    con = connect()
    counts = {}
    for table, folder in TABLES.items():
        src = p.features / folder / f"date={day}" / "*.parquet"
        if not list(src.parent.glob("*.parquet")):
            continue
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM read_parquet('{src}') LIMIT 0")
        date_col = "day" if table == "agg_cell_daily" else "CAST(ts AS DATE)"
        con.execute(f"DELETE FROM {table} WHERE {date_col} = DATE '{day}'")  # réexécution sans doublon
        con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM read_parquet('{src}')")
        counts[table] = con.execute(f"SELECT count(*) FROM {table} WHERE {date_col} = DATE '{day}'").fetchone()[0]

    quality = p.reports / f"quality_{day}.json"
    if quality.exists():
        rep = json.loads(quality.read_text(encoding="utf-8"))
        con.execute(
            "CREATE TABLE IF NOT EXISTS dq_reports (day DATE, status VARCHAR, rows_received INT, "
            "rows_valid INT, rows_quarantined INT, completeness DOUBLE)"
        )
        con.execute("DELETE FROM dq_reports WHERE day = ?", [day])
        con.execute(
            "INSERT INTO dq_reports VALUES (?, ?, ?, ?, ?, ?)",
            [day, rep["status"], rep["rows_received"], rep["rows_valid"], rep["rows_quarantined"], rep["completeness"]],
        )
    for view in sorted(SQL_DIR.glob("*.sql")):
        con.execute(view.read_text(encoding="utf-8"))
    con.close()
    return {"day": day, **counts}
