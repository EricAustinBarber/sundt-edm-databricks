# Databricks notebook source
# scorecard_status_loader_notebook.py
dbutils.widgets.text("env", "")
dbutils.widgets.text("updated_by", "notebook")

ENV = dbutils.widgets.get("env") or "unknown"
UPDATED_BY = dbutils.widgets.get("updated_by") or "notebook"

from pyspark.sql import functions as F
from pyspark.sql import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS governance_maturity")

spark.sql("""
CREATE TABLE IF NOT EXISTS governance_maturity.scorecard_check_status (
  check_id    STRING NOT NULL,
  status      STRING NOT NULL,
  notes       STRING,
  env         STRING,
  updated_at  TIMESTAMP NOT NULL,
  updated_by  STRING
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS governance_maturity.warehouse_telemetry_metrics (
  collected_at         TIMESTAMP NOT NULL,
  env                  STRING    NOT NULL,
  run_id               STRING,
  commit_sha           STRING,
  check_id             STRING    NOT NULL,
  dimension            STRING    NOT NULL,
  metric_name          STRING    NOT NULL,
  source_name          STRING,
  window_days          INT,
  metric_value_double  DOUBLE,
  metric_value_string  STRING,
  status_hint          STRING,
  notes                STRING,
  metric_sql           STRING,
  metric_json          STRING
)
USING DELTA
PARTITIONED BY (env)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS governance_maturity.warehouse_metric_catalog (
  metric_id             STRING NOT NULL,
  dimension             STRING NOT NULL,
  metric_name           STRING NOT NULL,
  description           STRING,
  why_it_matters        STRING,
  how_to_measure        STRING,
  improvement_signal    STRING,
  source_name           STRING,
  collection_method     STRING,
  implementation_status STRING,
  enabled_for_scorecard BOOLEAN,
  v1_candidate          BOOLEAN,
  direction             STRING,
  pass_threshold        DOUBLE,
  partial_threshold     DOUBLE,
  threshold_unit        STRING,
  updated_at            TIMESTAMP NOT NULL
)
USING DELTA
""")

now_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

default_thresholds = {
    "WH-01": {"direction": "high", "pass_value": 90.0, "partial_value": 75.0},
    "WH-02": {"direction": "high", "pass_value": 60.0, "partial_value": 35.0},
    "WH-03": {"direction": "high", "pass_value": 80.0, "partial_value": 50.0},
    "WH-04": {"direction": "low", "pass_value": 3.0, "partial_value": 10.0},
    "WH-05": {"direction": "low", "pass_value": 1.0, "partial_value": 5.0},
    "WH-06": {"direction": "high", "pass_value": 70.0, "partial_value": 40.0},
    "WH-07": {"direction": "low", "pass_value": 25.0, "partial_value": 75.0},
    "WH-08": {"direction": "high", "pass_value": 10.0, "partial_value": 3.0},
    "WH-09": {"direction": "high", "pass_value": 70.0, "partial_value": 40.0},
    "WH-10": {"direction": "high", "pass_value": 40.0, "partial_value": 15.0},
    "WH-11": {"direction": "low", "pass_value": 900.0, "partial_value": 1800.0},
    "WH-12": {"direction": "high", "pass_value": 95.0, "partial_value": 85.0},
}


catalog_rows = (
    spark.table("governance_maturity.warehouse_metric_catalog")
    .filter(F.col("enabled_for_scorecard") == F.lit(True))
    .select("metric_id", "direction", "pass_threshold", "partial_threshold")
    .collect()
)

score_thresholds = dict(default_thresholds)
for row in catalog_rows:
    score_thresholds[row["metric_id"]] = {
        "direction": row["direction"] or default_thresholds.get(row["metric_id"], {}).get("direction"),
        "pass_value": row["pass_threshold"] if row["pass_threshold"] is not None else default_thresholds.get(row["metric_id"], {}).get("pass_value"),
        "partial_value": row["partial_threshold"] if row["partial_threshold"] is not None else default_thresholds.get(row["metric_id"], {}).get("partial_value"),
    }


def classify_status(check_id: str, metric_value: float | None, status_hint: str | None):
    if metric_value is None:
        return "Unknown"
    thresholds = score_thresholds.get(check_id)
    if thresholds is None:
        if status_hint in ("Pass", "Partial", "Fail", "Unknown"):
            return status_hint
        return "Unknown"
    direction = thresholds["direction"]
    pass_value = thresholds["pass_value"]
    partial_value = thresholds["partial_value"]
    if direction == "high":
        if metric_value >= pass_value:
            return "Pass"
        if metric_value >= partial_value:
            return "Partial"
        return "Fail"
    if metric_value <= pass_value:
        return "Pass"
    if metric_value <= partial_value:
        return "Partial"
    return "Fail"


latest = (
    spark.table("governance_maturity.warehouse_telemetry_metrics")
    .filter(F.col("env") == ENV)
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("check_id").orderBy(F.col("collected_at").desc())
        ),
    )
    .filter(F.col("rn") == 1)
)

rows = latest.collect()
if not rows:
    raise Exception(f"[scorecard] No warehouse telemetry rows found for env={ENV}. Did maturity_collect run?")

status_rows = []
for row in rows:
    metric_value = row["metric_value_double"]
    status = classify_status(row["check_id"], metric_value, row["status_hint"])
    if metric_value is None:
        metric_summary = "metric unavailable"
    else:
        metric_summary = f"value={round(float(metric_value), 2)}"
    source_summary = row["source_name"] or "unknown source"
    extra_notes = row["notes"]
    notes = f"{metric_summary} from {source_summary}"
    if extra_notes:
        notes = f"{notes}; {extra_notes}"
    status_rows.append((row["check_id"], status, notes, ENV, now_ts, UPDATED_BY))

df = spark.createDataFrame(
    status_rows,
    ["check_id", "status", "notes", "env", "updated_at", "updated_by"],
)
df.write.mode("append").format("delta").saveAsTable("governance_maturity.scorecard_check_status")

print(f"[scorecard] derived {df.count()} telemetry-backed status rows for env={ENV}")
