CREATE OR REPLACE VIEW main.mart.scorecard_dataset_quality_360 AS
WITH monitor_counts AS (
  SELECT
    dataset_name,
    COUNT(*) AS monitor_count
  FROM main.staging.bronze_bigeye_monitors
  WHERE dataset_name IS NOT NULL
  GROUP BY dataset_name
),
alert_counts AS (
  SELECT
    dataset_name,
    COUNT(*) AS alert_count_7d
  FROM main.staging.bronze_bigeye_alerts
  WHERE dataset_name IS NOT NULL
    AND ingested_at >= current_timestamp() - INTERVAL 7 DAYS
  GROUP BY dataset_name
),
alation_latest AS (
  SELECT
    dataset_name,
    max_by(alation_endorsed, ingested_at) AS alation_endorsed,
    max_by(alation_owner, ingested_at) AS alation_owner,
    max_by(alation_steward, ingested_at) AS alation_steward,
    max_by(alation_lineage_present, ingested_at) AS alation_lineage_present
  FROM main.staging.bronze_alation_assets
  WHERE dataset_name IS NOT NULL
  GROUP BY dataset_name
)
SELECT
  current_date() AS report_date,
  c.dataset_id,
  c.dataset_name,
  c.domain_name,
  c.business_owner,
  c.technical_owner,
  c.criticality,
  c.sla_freshness,
  c.contains_pii,
  c.is_active,
  coalesce(m.monitor_count, 0) AS bigeye_monitor_count,
  coalesce(a.alert_count_7d, 0) AS alert_count_7d,
  coalesce(l.alation_endorsed, false) AS alation_endorsed,
  l.alation_owner,
  l.alation_steward,
  coalesce(l.alation_lineage_present, false) AS alation_lineage_present,
  CASE WHEN coalesce(m.monitor_count, 0) > 0 THEN 1 ELSE 0 END AS monitored_dataset_flag
FROM main.staging.sliver_v_critical_datasets c
LEFT JOIN monitor_counts m ON c.dataset_name = m.dataset_name
LEFT JOIN alert_counts a ON c.dataset_name = a.dataset_name
LEFT JOIN alation_latest l ON c.dataset_name = l.dataset_name
WHERE coalesce(c.is_active, true);
