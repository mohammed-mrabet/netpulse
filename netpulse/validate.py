"""Validation des données d'entrée d'un jour.

Contrôles appliqués avant toute transformation :
1. schéma : colonnes attendues présentes (sinon lot bloqué) ;
2. règles par ligne : valeurs manquantes, bornes physiques, cellule connue du référentiel,
   horodatage dans le jour traité ; les lignes en erreur partent en quarantaine avec leur motif ;
3. doublons (cell_id, ts) : on garde la première occurrence ;
4. complétude : mesures reçues / mesures attendues (nb de cellules x 24 h) ;
5. statut global OK / WARNING / CRITICAL selon des seuils configurables.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta

import pandas as pd

from . import config
from .config import paths
from .generate import COLUMNS

# Bornes physiques acceptées pour chaque indicateur
RANGES = {
    "latency_ms": (0, 5000),
    "jitter_ms": (0, 5000),
    "packet_loss_pct": (0, 100),
    "throughput_mbps": (0, 10_000),
    "availability_pct": (0, 100),
    "active_users": (0, 100_000),
}
NOT_NULL = ["ts", "cell_id", *RANGES.keys()]


@dataclass
class Check:
    name: str
    status: str          # OK | WARNING | CRITICAL
    value: float | int | str
    message: str


@dataclass
class ValidationReport:
    day: str
    rows_received: int
    rows_valid: int
    rows_quarantined: int
    duplicates_removed: int
    completeness: float
    status: str = "OK"
    checks: list[Check] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _worst(statuses: list[str]) -> str:
    order = {"OK": 0, "WARNING": 1, "CRITICAL": 2}
    return max(statuses, key=order.__getitem__) if statuses else "OK"


def validate_frame(df: pd.DataFrame, cells: pd.DataFrame, day: str) -> tuple[pd.DataFrame, pd.DataFrame, ValidationReport]:
    """Retourne (lignes valides, lignes en quarantaine avec motif, rapport)."""
    checks: list[Check] = []
    received = len(df)

    missing_cols = [c for c in COLUMNS if c not in df.columns]
    if missing_cols:
        checks.append(Check("schema", "CRITICAL", ",".join(missing_cols), "Colonnes manquantes"))
        report = ValidationReport(day, received, 0, received, 0, 0.0, "CRITICAL", checks)
        return df.iloc[0:0], df.assign(reason="schema"), report
    checks.append(Check("schema", "OK", len(COLUMNS), "Toutes les colonnes attendues sont présentes"))

    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    for col in RANGES:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Motif de rejet ligne par ligne (le premier motif rencontré est conservé)
    reason = pd.Series("", index=df.index, dtype="object")

    def flag(mask: pd.Series, why: str) -> int:
        new = mask & (reason == "")
        reason[new] = why
        return int(mask.sum())

    n_null = flag(df[NOT_NULL].isna().any(axis=1), "valeur_manquante")
    n_range = 0
    for col, (lo, hi) in RANGES.items():
        n_range += flag(df[col].notna() & ~df[col].between(lo, hi), f"hors_bornes:{col}")
    n_unknown = flag(~df["cell_id"].isin(cells["cell_id"]), "cellule_inconnue")
    start = datetime.fromisoformat(day)
    n_ts = flag(df["ts"].notna() & ~df["ts"].between(start, start + timedelta(hours=23, minutes=59)), "hors_journee")

    quarantine = df[reason != ""].assign(reason=reason[reason != ""])
    valid = df[reason == ""]

    before = len(valid)
    valid = valid.sort_values("ts").drop_duplicates(subset=["cell_id", "ts"], keep="first")
    duplicates = before - len(valid)

    invalid_ratio = len(quarantine) / received if received else 1.0
    status = "OK"
    if invalid_ratio > config.MAX_INVALID_RATIO_CRITICAL:
        status = "CRITICAL"
    elif invalid_ratio > config.MAX_INVALID_RATIO_WARNING:
        status = "WARNING"
    checks.append(Check(
        "invalid_ratio", status, round(invalid_ratio, 4),
        f"{len(quarantine)} lignes rejetées (manquantes : {n_null}, hors bornes : {n_range}, "
        f"cellule inconnue : {n_unknown}, hors journée : {n_ts})",
    ))

    expected = len(cells) * 24
    completeness = len(valid) / expected if expected else 0.0
    status = "OK"
    if completeness < config.MIN_COMPLETENESS_CRITICAL:
        status = "CRITICAL"
    elif completeness < config.MIN_COMPLETENESS_WARNING:
        status = "WARNING"
    checks.append(Check("completeness", status, round(completeness, 4),
                        f"{len(valid)} mesures valides sur {expected} attendues"))
    checks.append(Check("duplicates", "OK", duplicates, f"{duplicates} doublons supprimés"))

    report = ValidationReport(
        day=day,
        rows_received=received,
        rows_valid=len(valid),
        rows_quarantined=len(quarantine),
        duplicates_removed=duplicates,
        completeness=round(completeness, 4),
        status=_worst([c.status for c in checks]),
        checks=checks,
    )
    return valid.reset_index(drop=True), quarantine.reset_index(drop=True), report


def run(day: str) -> ValidationReport:
    """Valide le fichier brut du jour, écrit le staging, la quarantaine et le rapport."""
    p = paths()
    date.fromisoformat(day)  # contrôle du format de la date
    raw = p.raw_file(day)
    if not raw.exists():
        raise FileNotFoundError(f"Fichier brut absent : {raw}")
    cells = pd.read_csv(p.reference)
    df = pd.read_csv(raw)
    valid, quarantine, report = validate_frame(df, cells, day)

    if report.status != "CRITICAL":
        out = p.staging / f"date={day}"
        out.mkdir(parents=True, exist_ok=True)
        # horodatages en microsecondes : format lu par Spark (pandas écrit des nanosecondes par défaut)
        valid.to_parquet(out / "part-0.parquet", index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    p.quarantine.mkdir(parents=True, exist_ok=True)
    quarantine.to_csv(p.quarantine / f"{day}.csv", index=False)
    p.reports.mkdir(parents=True, exist_ok=True)
    (p.reports / f"quality_{day}.json").write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    return report
