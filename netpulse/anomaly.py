"""Détection d'anomalies sur les indicateurs horaires.

Deux approches sont comparées sur la vérité terrain des incidents injectés :
- une référence à base de règles (seuils sur les z-scores, la perte de paquets et la disponibilité) ;
- un modèle non supervisé IsolationForest (scikit-learn) sur les mêmes indicateurs.
La décision finale combine les deux : une mesure est anomale si les règles ET le modèle la
signalent (ce qui réduit fortement les fausses alertes), ou s'il s'agit d'une coupure franche.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, precision_score, recall_score

from .config import paths

FEATURES = ["latency_z", "throughput_z", "packet_loss_pct", "availability_pct", "jitter_ratio"]


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["jitter_ratio"] = df["jitter_ms"] / df["latency_ms"].clip(lower=1)
    return df


def rule_flags(df: pd.DataFrame) -> pd.Series:
    return (
        (df["latency_z"] > 3)
        | (df["throughput_z"] < -3)
        | (df["packet_loss_pct"] > 2.5)
        | (df["availability_pct"] < 98)
    )


def model_flags(df: pd.DataFrame, contamination: float = 0.02, seed: int = 0) -> tuple[pd.Series, np.ndarray]:
    x = df[FEATURES].to_numpy(dtype=float)
    model = IsolationForest(n_estimators=200, contamination=contamination, random_state=seed)
    pred = model.fit_predict(x)
    score = -model.score_samples(x)  # plus le score est haut, plus la mesure est atypique
    return pd.Series(pred == -1, index=df.index), score


def detect(df: pd.DataFrame) -> pd.DataFrame:
    df = prepare(df)
    rules = rule_flags(df)
    model, score = model_flags(df)
    outage = df["availability_pct"] < 50
    df["flag_rules"] = rules
    df["flag_model"] = model
    df["anomaly_score"] = np.round(score, 4)
    df["is_anomaly"] = (rules & model) | outage  # une coupure franche est toujours signalée
    df["severity"] = np.where(outage, "CRITICAL", np.where(df["is_anomaly"], "WARNING", "OK"))
    return df


def evaluate(df: pd.DataFrame, labels: pd.DataFrame) -> dict:
    """Précision / rappel / F1 de chaque approche face aux incidents réellement injectés."""
    labels = labels.assign(ts=pd.to_datetime(labels["ts"]))
    m = df.merge(labels, on=["ts", "cell_id"], how="inner")
    y = m["is_incident"].astype(int)
    out = {"measures": int(len(m)), "incidents": int(y.sum())}
    for name in ("flag_rules", "flag_model", "is_anomaly"):
        pred = m[name].astype(int)
        out[name] = {
            "precision": round(precision_score(y, pred, zero_division=0), 3),
            "recall": round(recall_score(y, pred, zero_division=0), 3),
            "f1": round(f1_score(y, pred, zero_division=0), 3),
            "alerts": int(pred.sum()),
        }
    return out


def run(day: str) -> dict:
    p = paths()
    feats = pd.read_parquet(p.features / "hourly" / f"date={day}")
    feats["ts"] = pd.to_datetime(feats["ts"])
    result = detect(feats)
    anomalies = result[result["is_anomaly"]]
    out = p.features / "anomalies" / f"date={day}"
    out.mkdir(parents=True, exist_ok=True)
    anomalies.to_parquet(out / "part-0.parquet", index=False)

    summary = {
        "day": day,
        "anomalies": int(len(anomalies)),
        "critical": int((anomalies["severity"] == "CRITICAL").sum()),
        "cells_impacted": sorted(anomalies["cell_id"].unique().tolist()),
    }
    label_file = p.labels / f"{day}.csv"
    if label_file.exists():  # vérité terrain disponible car données synthétiques
        summary["evaluation"] = evaluate(result, pd.read_csv(label_file))
    p.reports.mkdir(parents=True, exist_ok=True)
    (p.reports / f"anomalies_{day}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
