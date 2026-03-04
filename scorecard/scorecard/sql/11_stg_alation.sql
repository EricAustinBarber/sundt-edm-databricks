CREATE OR REPLACE VIEW main.staging.bronze_alation_assets AS
SELECT
  ingested_at,
  lower(trim(
    coalesce(
      get_json_object(payload, '$.name'),
      get_json_object(payload, '$.title'),
      get_json_object(payload, '$.dataset_name'),
      get_json_object(payload, '$.table_name')
    )
  )) AS dataset_name,
  CAST(
    coalesce(
      get_json_object(payload, '$.endorsed'),
      get_json_object(payload, '$.endorsed_flag'),
      'false'
    ) AS BOOLEAN
  ) AS alation_endorsed,
  coalesce(
    get_json_object(payload, '$.owner'),
    get_json_object(payload, '$.owner_name')
  ) AS alation_owner,
  coalesce(
    get_json_object(payload, '$.steward'),
    get_json_object(payload, '$.steward_name')
  ) AS alation_steward,
  CAST(
    coalesce(
      get_json_object(payload, '$.lineage_present'),
      get_json_object(payload, '$.has_lineage'),
      'false'
    ) AS BOOLEAN
  ) AS alation_lineage_present,
  payload
FROM main.raw.alation_assets_json
WHERE payload IS NOT NULL;
