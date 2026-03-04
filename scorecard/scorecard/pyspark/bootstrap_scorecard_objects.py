"""
PySpark bootstrap for scorecard schemas and base JSON tables.

Run this on a Databricks cluster/job before scorecard SQL view deployment.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


def _json_payload_schema() -> StructType:
    return StructType(
        [
            StructField("ingested_at", TimestampType(), True),
            StructField("payload", StringType(), True),
        ]
    )


def _create_json_table(spark: SparkSession, full_name: str) -> None:
    df = spark.createDataFrame([], schema=_json_payload_schema())
    (
        df.write.format("delta")
        .mode("ignore")
        .saveAsTable(full_name)
    )


def bootstrap_scorecard_objects(
    spark: SparkSession,
    catalog: str = "main",
    schema_raw: str = "raw",
    schema_staging: str = "staging",
    schema_mart: str = "mart",
) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_raw}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_staging}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_mart}")

    _create_json_table(spark, f"{catalog}.{schema_raw}.bigeye_monitors_json")
    _create_json_table(spark, f"{catalog}.{schema_raw}.bigeye_alerts_json")
    _create_json_table(spark, f"{catalog}.{schema_raw}.alation_assets_json")
    _create_json_table(spark, f"{catalog}.{schema_staging}.sliver_scorecard_metric_definitions_json")
    _create_json_table(spark, f"{catalog}.{schema_staging}.sliver_critical_datasets_json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bootstrap scorecard schemas and JSON tables")
    parser.add_argument("--catalog", default="main")
    parser.add_argument("--schema-raw", default="raw")
    parser.add_argument("--schema-staging", default="staging")
    parser.add_argument("--schema-mart", default="mart")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    bootstrap_scorecard_objects(
        spark,
        catalog=args.catalog,
        schema_raw=args.schema_raw,
        schema_staging=args.schema_staging,
        schema_mart=args.schema_mart,
    )
