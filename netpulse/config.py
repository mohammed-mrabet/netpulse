"""Chemins et seuils du pipeline, surchargeables par variables d'environnement."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def data_dir() -> Path:
    return Path(os.environ.get("NETPULSE_DATA_DIR", Path(__file__).resolve().parent.parent / "data"))


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def raw(self) -> Path:          # fichiers déposés par les sondes (zone d'atterrissage)
        return self.root / "raw"

    @property
    def reference(self) -> Path:    # référentiel des cellules radio
        return self.root / "reference" / "cells.csv"

    @property
    def labels(self) -> Path:       # vérité terrain des incidents injectés (données synthétiques)
        return self.root / "labels"

    @property
    def staging(self) -> Path:      # lignes validées, en Parquet partitionné par jour
        return self.root / "staging"

    @property
    def quarantine(self) -> Path:   # lignes rejetées, avec la raison du rejet
        return self.root / "quarantine"

    @property
    def features(self) -> Path:     # sortie des transformations PySpark
        return self.root / "features"

    @property
    def reports(self) -> Path:      # rapports qualité et anomalies (JSON)
        return self.root / "reports"

    @property
    def alerts(self) -> Path:
        return self.root / "alerts" / "alerts.jsonl"

    @property
    def warehouse(self) -> Path:    # entrepôt analytique DuckDB
        return self.root / "warehouse" / "netpulse.duckdb"

    def raw_file(self, day: str) -> Path:
        return self.raw / f"date={day}" / "kpi.csv"


def paths() -> Paths:
    return Paths(data_dir())


# Seuils de qualité des données
MAX_INVALID_RATIO_WARNING = 0.03    # > 3 % de lignes invalides : avertissement
MAX_INVALID_RATIO_CRITICAL = 0.08   # > 8 % : le lot est bloqué
MIN_COMPLETENESS_WARNING = 0.96     # < 96 % des mesures attendues : avertissement
MIN_COMPLETENESS_CRITICAL = 0.90    # < 90 % : le lot est bloqué

# Fenêtre d'historique utilisée pour les indicateurs glissants (en jours)
HISTORY_DAYS = 3
