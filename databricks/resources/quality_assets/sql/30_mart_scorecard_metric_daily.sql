CREATE OR REPLACE VIEW main.mart.governance_scorecard_metric_daily AS
WITH base AS (
  SELECT * FROM main.mart.scorecard_dataset_quality_360
)
SELECT
  report_date,
  dataset_id,
  dataset_name,
  domain_name,
  'OBS_MONITOR_COVERAGE' AS metric_id,
  'bigeye_monitor_coverage' AS metric_name,
  20.0 AS weight,
  CAST(monitored_dataset_flag * 100 AS DOUBLE) AS metric_score
FROM base
UNION ALL
SELECT
  report_date,
  dataset_id,
  dataset_name,
  domain_name,
  'OBS_ALERT_HEALTH' AS metric_id,
  'bigeye_alert_health' AS metric_name,
  20.0 AS weight,
  CASE
    WHEN alert_count_7d = 0 THEN 100.0
    WHEN alert_count_7d <= 3 THEN 85.0
    ELSE 60.0
  END AS metric_score
FROM base
UNION ALL
SELECT
  report_date,
  dataset_id,
  dataset_name,
  domain_name,
  'GOV_ENDORSEMENT' AS metric_id,
  'alation_endorsed' AS metric_name,
  20.0 AS weight,
  CASE WHEN alation_endorsed THEN 100.0 ELSE 40.0 END AS metric_score
FROM base
UNION ALL
SELECT
  report_date,
  dataset_id,
  dataset_name,
  domain_name,
  'GOV_OWNERSHIP' AS metric_id,
  'alation_owner_steward_completeness' AS metric_name,
  20.0 AS weight,
  CASE
    WHEN alation_owner IS NOT NULL AND alation_steward IS NOT NULL THEN 100.0
    WHEN alation_owner IS NOT NULL OR alation_steward IS NOT NULL THEN 70.0
    ELSE 30.0
  END AS metric_score
FROM base
UNION ALL
SELECT
  report_date,
  dataset_id,
  dataset_name,
  domain_name,
  'GOV_LINEAGE' AS metric_id,
  'alation_lineage_coverage' AS metric_name,
  20.0 AS weight,
  CASE WHEN alation_lineage_present THEN 100.0 ELSE 50.0 END AS metric_score
FROM base;
