"""Générateur de mesures réseau synthétiques.

Simule, heure par heure, les indicateurs de qualité de service de cellules radio 4G/5G
(latence, gigue, perte de paquets, débit, disponibilité, utilisateurs actifs), puis injecte :
- des incidents réseau connus (dégradations et coupures) dont la vérité terrain est conservée
  dans data/labels, ce qui permet de mesurer la détection d'anomalies ;
- des défauts de qualité de données (valeurs manquantes, hors bornes, doublons, cellule inconnue,
  heures manquantes), pour éprouver l'étape de validation.

Les données sont entièrement synthétiques : aucune donnée d'opérateur n'est utilisée.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from .config import paths

REGIONS = {"Paris": 14, "Lyon": 9, "Marseille": 9, "Lille": 8}
COLUMNS = [
    "ts", "cell_id", "region", "technology", "latency_ms", "jitter_ms",
    "packet_loss_pct", "throughput_mbps", "availability_pct", "active_users",
]


def build_cells(seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for region, n in REGIONS.items():
        for i in range(1, n + 1):
            tech = "5G" if rng.random() < 0.4 else "4G"
            rows.append({
                "cell_id": f"{region[:3].upper()}-{i:03d}",
                "region": region,
                "technology": tech,
                "capacity_mbps": 900 if tech == "5G" else 300,
            })
    return pd.DataFrame(rows)


def _traffic_profile(hour: int) -> float:
    """Charge relative selon l'heure : creux la nuit, pics à 12-14 h et 18-22 h."""
    base = 0.15 + 0.85 * np.exp(-((hour - 13) ** 2) / 18) * 0.7
    evening = 0.9 * np.exp(-((hour - 20) ** 2) / 6)
    return float(min(1.0, base + evening))


def generate_day(day: date, cells: pd.DataFrame, seed: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retourne (mesures brutes, vérité terrain des incidents) pour un jour."""
    rng = np.random.default_rng(seed)
    records, labels = [], []

    # Incidents du jour : environ 4 cellules dégradées et parfois une coupure franche
    incident_cells = rng.choice(cells["cell_id"], size=4, replace=False)
    incidents = {}
    for k, cell in enumerate(incident_cells):
        start = int(rng.integers(0, 20))
        duration = int(rng.integers(2, 6))
        kind = "outage" if k == 0 and rng.random() < 0.6 else "degradation"
        incidents[cell] = (start, start + duration, kind)

    for cell in cells.itertuples():
        cap = cell.capacity_mbps
        base_latency = 18 if cell.technology == "5G" else 32
        for hour in range(24):
            ts = datetime(day.year, day.month, day.day, hour)
            load = _traffic_profile(hour) * rng.uniform(0.85, 1.15)
            users = int(load * (420 if cell.technology == "5G" else 260) * rng.uniform(0.9, 1.1))
            latency = base_latency * (1 + 0.6 * load) * rng.normal(1, 0.08)
            jitter = latency * rng.uniform(0.08, 0.18)
            loss = max(0.0, rng.normal(0.25 + 0.4 * load, 0.12))
            throughput = cap * (1 - 0.55 * load) * rng.normal(1, 0.07)
            availability = min(100.0, rng.normal(99.95, 0.03))

            is_incident = 0
            if cell.cell_id in incidents:
                start, end, kind = incidents[cell.cell_id]
                if start <= hour < end:
                    is_incident = 1
                    if kind == "outage":
                        availability, throughput, users = rng.uniform(0, 15), 0.0, 0
                        latency, jitter, loss = latency * 6, jitter * 5, rng.uniform(40, 100)
                    else:
                        # dégradation partielle, volontairement moins visible qu'une coupure
                        latency *= rng.uniform(1.5, 2.6)
                        jitter *= rng.uniform(1.8, 3)
                        loss += rng.uniform(0.8, 3.5)
                        throughput *= rng.uniform(0.45, 0.75)
                        availability -= rng.uniform(0.05, 0.8)
            labels.append({"ts": ts, "cell_id": cell.cell_id, "is_incident": is_incident})
            records.append([
                ts, cell.cell_id, cell.region, cell.technology, round(latency, 2), round(jitter, 2),
                round(min(loss, 100), 3), round(max(throughput, 0), 1), round(max(availability, 0), 3), users,
            ])

    df = pd.DataFrame(records, columns=COLUMNS)
    df = _inject_quality_issues(df, rng)
    return df, pd.DataFrame(labels)


def _inject_quality_issues(df: pd.DataFrame, rng: np.random.Generator, rate: float = 0.008) -> pd.DataFrame:
    """Injecte des défauts réalistes de collecte, à faible taux (environ 0,8 % par type)."""
    n = len(df)
    df = df.copy()
    pick = lambda: rng.choice(n, size=max(1, int(n * rate)), replace=False)  # noqa: E731
    df.loc[pick(), "latency_ms"] = np.nan                       # sonde muette
    df.loc[pick(), "throughput_mbps"] = -1.0                    # valeur sentinelle d'erreur
    df.loc[pick(), "packet_loss_pct"] = 140.0                   # pourcentage impossible
    df.loc[rng.choice(n, size=2, replace=False), "cell_id"] = "UNK-999"  # cellule inconnue du référentiel
    df = df.drop(index=rng.choice(n, size=6, replace=False))    # mesures non remontées
    dups = df.sample(n=5, random_state=int(rng.integers(0, 10_000)))
    return pd.concat([df, dups]).sample(frac=1, random_state=1).reset_index(drop=True)  # doublons, ordre mélangé


def corrupt(df: pd.DataFrame, seed: int = 0) -> pd.DataFrame:
    """Simule un lot fortement dégradé (export défectueux) pour démontrer le blocage du pipeline."""
    rng = np.random.default_rng(seed)
    df = df.copy()
    idx = rng.choice(len(df), size=int(len(df) * 0.12), replace=False)
    df.loc[idx, "throughput_mbps"] = np.nan
    return df


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Génère des mesures réseau synthétiques.")
    parser.add_argument("--start", default="2026-10-01")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--corrupt-day", help="jour à corrompre volontairement (démonstration d'alerte)")
    args = parser.parse_args(argv)

    p = paths()
    cells = build_cells()
    p.reference.parent.mkdir(parents=True, exist_ok=True)
    cells.to_csv(p.reference, index=False)

    start = date.fromisoformat(args.start)
    for i in range(args.days):
        day = start + timedelta(days=i)
        df, labels = generate_day(day, cells, seed=args.seed + i)
        if args.corrupt_day == day.isoformat():
            df = corrupt(df)
        out = p.raw_file(day.isoformat())
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        p.labels.mkdir(parents=True, exist_ok=True)
        labels.to_csv(p.labels / f"{day.isoformat()}.csv", index=False)
        print(f"{day}: {len(df)} lignes -> {out}")


if __name__ == "__main__":
    main()
