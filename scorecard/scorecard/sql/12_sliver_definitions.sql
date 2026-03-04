CREATE OR REPLACE VIEW main.staging.sliver_v_scorecard_metric_definitions AS
SELECT
  ingested_at,
  get_json_object(payload, '$.metric_id') AS metric_id,
  get_json_object(payload, '$.metric_name') AS metric_name,
  get_json_object(payload, '$.section') AS section,
  CAST(get_json_object(payload, '$.weight') AS DOUBLE) AS weight,
  get_json_object(payload, '$.formula') AS formula,
  get_json_object(payload, '$.threshold_green') AS threshold_green,
  get_json_object(payload, '$.threshold_yellow') AS threshold_yellow,
  get_json_object(payload, '$.threshold_red') AS threshold_red,
  get_json_object(payload, '$.owner_team') AS owner_team,
  get_json_object(payload, '$.description') AS description
FROM main.staging.sliver_scorecard_metric_definitions_json;

CREATE OR REPLACE VIEW main.staging.sliver_v_critical_datasets AS
SELECT
  ingested_at,
  get_json_object(payload, '$.dataset_id') AS dataset_id,
  lower(trim(get_json_object(payload, '$.dataset_name'))) AS dataset_name,
  get_json_object(payload, '$.domain_name') AS domain_name,
  get_json_object(payload, '$.business_owner') AS business_owner,
  get_json_object(payload, '$.technical_owner') AS technical_owner,
  get_json_object(payload, '$.criticality') AS criticality,
  get_json_object(payload, '$.sla_freshness') AS sla_freshness,
  CAST(get_json_object(payload, '$.contains_pii') AS BOOLEAN) AS contains_pii,
  CAST(get_json_object(payload, '$.is_active') AS BOOLEAN) AS is_active,
  get_json_object(payload, '$.notes') AS notes
FROM main.staging.sliver_critical_datasets_json;
