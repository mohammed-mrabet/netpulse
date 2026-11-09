"""Envoi d'alertes : journal JSON Lines, logs, et webhook optionnel (Slack, Teams, Mattermost...)."""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone

from .config import paths

log = logging.getLogger("netpulse.alerts")

SEVERITIES = ("INFO", "WARNING", "CRITICAL")


def send_alert(severity: str, title: str, details: dict | None = None, day: str | None = None) -> dict:
    if severity not in SEVERITIES:
        raise ValueError(f"Sévérité inconnue : {severity}")
    alert = {
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "day": day,
        "severity": severity,
        "title": title,
        "details": details or {},
    }
    target = paths().alerts
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as f:
        f.write(json.dumps(alert, ensure_ascii=False, default=str) + "\n")

    log.log(logging.ERROR if severity == "CRITICAL" else logging.WARNING, "[%s] %s", severity, title)

    webhook = os.environ.get("NETPULSE_WEBHOOK_URL")
    if webhook:
        payload = json.dumps({"text": f"[NetPulse][{severity}] {title}"}).encode()
        req = urllib.request.Request(webhook, data=payload, headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=5)
        except OSError as exc:  # une alerte non envoyée ne doit pas faire échouer le pipeline
            log.warning("Webhook indisponible : %s", exc)
    return alert
