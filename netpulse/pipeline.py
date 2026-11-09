"""Exécution du pipeline hors Airflow (développement, CI) : python -m netpulse.pipeline 2026-10-05."""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date, timedelta

from . import anomaly, load, transform, validate
from .alerts import send_alert


class DataQualityError(RuntimeError):
    """Lot bloqué par la validation : rien n'est transformé ni chargé."""


def run_day(day: str, spark=None) -> dict:
    report = validate.run(day)
    if report.status == "CRITICAL":
        send_alert("CRITICAL", f"Lot du {day} bloqué par la validation", report.to_dict(), day)
        raise DataQualityError(f"Validation CRITICAL pour {day}")
    if report.status == "WARNING":
        send_alert("WARNING", f"Qualité dégradée le {day}", {"checks": [c.__dict__ for c in report.checks]}, day)

    feats = transform.run(day, spark)
    summary = anomaly.run(day)
    if summary["critical"]:
        send_alert("CRITICAL", f"{summary['critical']} heures de coupure détectées le {day}",
                   {"cells": summary["cells_impacted"]}, day)
    loaded = load.run(day)
    return {"quality": report.status, "transform": feats, "anomalies": summary, "load": loaded}


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("start")
    parser.add_argument("end", nargs="?")
    args = parser.parse_args(argv)
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else start

    spark = transform.get_spark()
    d = start
    while d <= end:
        try:
            print(json.dumps(run_day(d.isoformat(), spark), default=str))
        except DataQualityError as exc:
            print(f"BLOQUÉ : {exc}")
        d += timedelta(days=1)
    spark.stop()


if __name__ == "__main__":
    main()
