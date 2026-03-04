# Databricks notebook source
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema_bronze", "")
dbutils.widgets.text("schema_silver", "")
dbutils.widgets.text("schema_governance", "")

CATALOG = (dbutils.widgets.get("catalog") or "").strip()
SCHEMA_BRONZE = (dbutils.widgets.get("schema_bronze") or "").strip()
SCHEMA_SILVER = (dbutils.widgets.get("schema_silver") or "").strip()
SCHEMA_GOVERNANCE = (dbutils.widgets.get("schema_governance") or "").strip()

if not CATALOG or not SCHEMA_BRONZE or not SCHEMA_SILVER or not SCHEMA_GOVERNANCE:
    raise ValueError(
        "Missing required catalog/schema parameters. "
        "Expected catalog, schema_bronze, schema_silver, schema_governance."
    )
if CATALOG.lower() == "main":
    raise ValueError(
        "Refusing to deploy quality scorecard objects to catalog 'main'. "
        "Set target variable quality_catalog to dev/test/prod catalog."
    )

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


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOVERNANCE}")

_create_json_table(f"{CATALOG}.{SCHEMA_BRONZE}.bigeye_monitors_json")
_create_json_table(f"{CATALOG}.{SCHEMA_BRONZE}.bigeye_alerts_json")
_create_json_table(f"{CATALOG}.{SCHEMA_BRONZE}.alation_assets_json")
_create_json_table(f"{CATALOG}.{SCHEMA_SILVER}.sliver_scorecard_metric_definitions_json")
_create_json_table(f"{CATALOG}.{SCHEMA_SILVER}.sliver_critical_datasets_json")

print(
    f"[quality-bootstrap] complete catalog={CATALOG} bronze={SCHEMA_BRONZE} "
    f"silver={SCHEMA_SILVER} governance={SCHEMA_GOVERNANCE}"
)
