# Databricks notebook source
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema_raw", "raw")
dbutils.widgets.text("schema_staging", "staging")
dbutils.widgets.text("schema_mart", "mart")

CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA_RAW = dbutils.widgets.get("schema_raw") or "raw"
SCHEMA_STAGING = dbutils.widgets.get("schema_staging") or "staging"
SCHEMA_MART = dbutils.widgets.get("schema_mart") or "mart"

from pyspark.sql.types import StringType, StructField, StructType, TimestampType


def _json_payload_schema() -> StructType:
    return StructType(
        [
            StructField("ingested_at", TimestampType(), True),
            StructField("payload", StringType(), True),
        ]
    )


def _create_json_table(full_name: str) -> None:
    df = spark.createDataFrame([], schema=_json_payload_schema())
    df.write.format("delta").mode("ignore").saveAsTable(full_name)


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_RAW}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_STAGING}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_MART}")

_create_json_table(f"{CATALOG}.{SCHEMA_RAW}.bigeye_monitors_json")
_create_json_table(f"{CATALOG}.{SCHEMA_RAW}.bigeye_alerts_json")
_create_json_table(f"{CATALOG}.{SCHEMA_RAW}.alation_assets_json")
_create_json_table(f"{CATALOG}.{SCHEMA_STAGING}.sliver_scorecard_metric_definitions_json")
_create_json_table(f"{CATALOG}.{SCHEMA_STAGING}.sliver_critical_datasets_json")

print(
    f"[quality-bootstrap] complete catalog={CATALOG} raw={SCHEMA_RAW} "
    f"staging={SCHEMA_STAGING} mart={SCHEMA_MART}"
)
