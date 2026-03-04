CREATE OR REPLACE VIEW main.staging.bronze_bigeye_monitors AS
SELECT
  ingested_at,
  lower(trim(
    coalesce(
      get_json_object(payload, '$.dataset_name'),
      get_json_object(payload, '$.dataset'),
      get_json_object(payload, '$.table_name'),
      get_json_object(payload, '$.table')
    )
  )) AS dataset_name,
  payload
FROM main.raw.bigeye_monitors_json
WHERE payload IS NOT NULL;

CREATE OR REPLACE VIEW main.staging.bronze_bigeye_alerts AS
SELECT
  ingested_at,
  date(ingested_at) AS alert_date,
  lower(trim(
    coalesce(
      get_json_object(payload, '$.dataset_name'),
      get_json_object(payload, '$.dataset'),
      get_json_object(payload, '$.table_name'),
      get_json_object(payload, '$.table')
    )
  )) AS dataset_name,
  coalesce(
    get_json_object(payload, '$.severity'),
    get_json_object(payload, '$.priority')
  ) AS severity,
  payload
FROM main.raw.bigeye_alerts_json
WHERE payload IS NOT NULL;
