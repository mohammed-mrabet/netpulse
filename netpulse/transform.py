"""Transformations PySpark : indicateurs horaires enrichis et agrégats journaliers.

Pour chaque mesure du jour traité, on calcule des indicateurs glissants par cellule sur les
24 mesures précédentes (moyenne et écart-type de la latence et du débit), puis l'écart à la
normale (z-score). Le jour est lu avec son historique (HISTORY_DAYS jours) pour que les
fenêtres soient complètes dès la première heure.
"""
from __future__ import annotations

import shutil
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config
from .config import paths


def get_spark(app: str = "netpulse") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def staging_days(day: str) -> list[str]:
    """Jours à lire : le jour traité et son historique, s'ils existent en staging."""
    p = paths()
    d = date.fromisoformat(day)
    days = [(d - timedelta(days=i)).isoformat() for i in range(config.HISTORY_DAYS, -1, -1)]
    return [x for x in days if (p.staging / f"date={x}").exists()]


def hourly_features(df: DataFrame) -> DataFrame:
    """Ajoute les indicateurs glissants et les z-scores par cellule."""
    w = Window.partitionBy("cell_id").orderBy("ts").rowsBetween(-24, -1)
    out = (
        df.withColumn("hour", F.hour("ts"))
        .withColumn("is_busy_hour", F.col("hour").isin(12, 13, 18, 19, 20, 21).cast("int"))
        .withColumn("lat_mean_24h", F.avg("latency_ms").over(w))
        .withColumn("lat_std_24h", F.stddev("latency_ms").over(w))
        .withColumn("thr_mean_24h", F.avg("throughput_mbps").over(w))
        .withColumn("thr_std_24h", F.stddev("throughput_mbps").over(w))
    )
    # Écart à la normale ; écart-type plancher pour éviter les divisions par ~0
    out = out.withColumn(
        "latency_z", (F.col("latency_ms") - F.col("lat_mean_24h")) / F.greatest(F.col("lat_std_24h"), F.lit(1.0))
    ).withColumn(
        "throughput_z", (F.col("throughput_mbps") - F.col("thr_mean_24h")) / F.greatest(F.col("thr_std_24h"), F.lit(1.0))
    )
    return out.fillna(0.0, subset=["latency_z", "throughput_z", "lat_std_24h", "thr_std_24h"])


def daily_cell_aggregates(df: DataFrame) -> DataFrame:
    return df.groupBy(F.to_date("ts").alias("day"), "cell_id", "region", "technology").agg(
        F.round(F.avg("latency_ms"), 2).alias("latency_avg_ms"),
        F.round(F.percentile_approx("latency_ms", 0.95), 2).alias("latency_p95_ms"),
        F.round(F.avg("packet_loss_pct"), 3).alias("packet_loss_avg_pct"),
        F.round(F.avg("throughput_mbps"), 1).alias("throughput_avg_mbps"),
        F.round(F.avg("availability_pct"), 3).alias("availability_avg_pct"),
        F.sum("active_users").alias("user_hours"),
        F.count("*").alias("measures"),
    )


def run(day: str, spark: SparkSession | None = None) -> dict:
    p = paths()
    spark = spark or get_spark()
    days = staging_days(day)
    if day not in days:
        raise FileNotFoundError(f"Pas de données validées en staging pour {day}")

    df = spark.read.parquet(*[str(p.staging / f"date={d}") for d in days])
    df = df.withColumn("ts", F.col("ts").cast("timestamp"))
    feats = hourly_features(df).filter(F.to_date("ts") == F.lit(day).cast("date"))
    daily = daily_cell_aggregates(feats)

    out_h = p.features / "hourly" / f"date={day}"
    out_d = p.features / "daily" / f"date={day}"
    for out in (out_h, out_d):
        if out.exists():
            shutil.rmtree(out)  # réexécution idempotente d'un même jour
    feats.coalesce(1).write.mode("overwrite").parquet(str(out_h))
    daily.coalesce(1).write.mode("overwrite").parquet(str(out_d))
    return {"day": day, "history_days": len(days) - 1, "hourly_rows": feats.count(), "cells": daily.count()}
