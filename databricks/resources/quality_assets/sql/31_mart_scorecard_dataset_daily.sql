CREATE OR REPLACE VIEW main.mart.governance_mart_scorecard_dataset_daily AS
SELECT
  report_date,
  dataset_id,
  dataset_name,
  domain_name,
  SUM(metric_score * weight) / NULLIF(SUM(weight), 0) AS dataset_score,
  CASE
    WHEN SUM(metric_score * weight) / NULLIF(SUM(weight), 0) >= 90 THEN 'Green'
    WHEN SUM(metric_score * weight) / NULLIF(SUM(weight), 0) >= 75 THEN 'Yellow'
    ELSE 'Red'
  END AS dataset_status
FROM main.mart.governance_scorecard_metric_daily
GROUP BY
  report_date,
  dataset_id,
  dataset_name,
  domain_name;

CREATE OR REPLACE VIEW main.mart.governance_mart_scorecard_domain_daily AS
SELECT
  report_date,
  domain_name,
  AVG(dataset_score) AS domain_score,
  CASE
    WHEN AVG(dataset_score) >= 90 THEN 'Green'
    WHEN AVG(dataset_score) >= 75 THEN 'Yellow'
    ELSE 'Red'
  END AS domain_status,
  COUNT(*) AS dataset_count
FROM main.mart.governance_mart_scorecard_dataset_daily
GROUP BY
  report_date,
  domain_name;
