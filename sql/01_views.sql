-- Vues de restitution pour les équipes réseau (SQL standard, portable vers BigQuery).

-- Qualité de service par région et par jour
CREATE OR REPLACE VIEW v_region_daily AS
SELECT
    day,
    region,
    ROUND(AVG(latency_avg_ms), 1)        AS latency_avg_ms,
    ROUND(MAX(latency_p95_ms), 1)        AS latency_p95_max_ms,
    ROUND(AVG(packet_loss_avg_pct), 3)   AS packet_loss_avg_pct,
    ROUND(AVG(throughput_avg_mbps), 1)   AS throughput_avg_mbps,
    ROUND(AVG(availability_avg_pct), 3)  AS availability_avg_pct,
    COUNT(*)                             AS cells
FROM agg_cell_daily
GROUP BY day, region;

-- Cellules les plus touchées par des anomalies sur toute la période
CREATE OR REPLACE VIEW v_top_cells_anomalies AS
SELECT
    cell_id,
    region,
    technology,
    COUNT(*)                                          AS anomalous_hours,
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_hours,
    ROUND(MAX(anomaly_score), 3)                      AS max_score
FROM fact_anomalies
GROUP BY cell_id, region, technology
ORDER BY anomalous_hours DESC;
