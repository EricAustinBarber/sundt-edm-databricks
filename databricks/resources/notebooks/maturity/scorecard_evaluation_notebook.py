# Databricks notebook source
# scorecard_evaluation_notebook.py
dbutils.widgets.text("env", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("commit_sha", "")
dbutils.widgets.text("status_json", "")  # optional JSON array for overrides

ENV = dbutils.widgets.get("env") or "unknown"
RUN_ID = dbutils.widgets.get("run_id") or "manual"
COMMIT_SHA = dbutils.widgets.get("commit_sha") or None
STATUS_JSON = dbutils.widgets.get("status_json") or ""

import json
from pyspark.sql import functions as F
from pyspark.sql import Window

SCORECARD_SOURCE = "warehouse_telemetry"
SCORECARD_PATH = "governance_maturity.warehouse_telemetry_metrics"

spark.sql("CREATE SCHEMA IF NOT EXISTS governance_maturity")

spark.sql("""
CREATE TABLE IF NOT EXISTS governance_maturity.scorecard_definition (
  check_id        STRING NOT NULL,
  dimension       STRING NOT NULL,
  check_name      STRING NOT NULL,
  weight          INT    NOT NULL,
  pass_criteria   STRING,
  evidence_source STRING,
  updated_at      TIMESTAMP NOT NULL
)
USING DELTA
""")

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
CREATE TABLE IF NOT EXISTS governance_maturity.scorecard_results (
  collected_at    TIMESTAMP NOT NULL,
  env             STRING NOT NULL,
  run_id          STRING NOT NULL,
  commit_sha      STRING,
  total_score     DOUBLE NOT NULL,
  overall_status  STRING NOT NULL,
  blocked_reasons ARRAY<STRING>,
  warned_reasons  ARRAY<STRING>,
  details_json    STRING
)
USING DELTA
PARTITIONED BY (env)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS governance_maturity.scorecard_check_results (
  collected_at    TIMESTAMP NOT NULL,
  env             STRING NOT NULL,
  run_id          STRING NOT NULL,
  commit_sha      STRING,
  check_id        STRING NOT NULL,
  dimension       STRING NOT NULL,
  check_name      STRING NOT NULL,
  weight          INT    NOT NULL,
  status_norm     STRING NOT NULL,
  notes           STRING,
  score           DOUBLE NOT NULL,
  weighted_score  DOUBLE NOT NULL
)
USING DELTA
PARTITIONED BY (env)
""")

embedded_definitions = [
    {
        "check_id": "WH-01",
        "dimension": "Platform Feature Utilization",
        "check_name": "Curated warehouse tables are primarily stored in Delta format",
        "weight": 5,
        "pass_criteria": "Delta curated table coverage >= 90%",
        "evidence_source": "system.information_schema.tables + DESCRIBE DETAIL",
    },
    {
        "check_id": "WH-02",
        "dimension": "Platform Feature Utilization",
        "check_name": "Observed warehouse write workloads favor MERGE over rebuild-only logic",
        "weight": 6,
        "pass_criteria": "MERGE load ratio >= 60%",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-03",
        "dimension": "Platform Feature Utilization",
        "check_name": "Large tables use a layout strategy such as partitioning",
        "weight": 5,
        "pass_criteria": "Large table layout strategy coverage >= 80%",
        "evidence_source": "system.information_schema.tables + DESCRIBE DETAIL",
    },
    {
        "check_id": "WH-04",
        "dimension": "Performance and Efficiency",
        "check_name": "Only a small number of large tables show small-file issues",
        "weight": 5,
        "pass_criteria": "Small-file problem tables <= 3",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "WH-05",
        "dimension": "Performance and Efficiency",
        "check_name": "Only a limited number of tables show oversized file patterns",
        "weight": 3,
        "pass_criteria": "Oversized-file problem tables <= 1",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "WH-06",
        "dimension": "Platform Feature Utilization",
        "check_name": "Large Delta tables show recent OPTIMIZE usage",
        "weight": 5,
        "pass_criteria": "Large table OPTIMIZE coverage >= 70%",
        "evidence_source": "DESCRIBE HISTORY",
    },
    {
        "check_id": "WH-07",
        "dimension": "Performance and Efficiency",
        "check_name": "Large join workloads remain limited",
        "weight": 5,
        "pass_criteria": "Large join query count <= 25",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-08",
        "dimension": "Platform Feature Utilization",
        "check_name": "Broadcast join hints are used where appropriate",
        "weight": 3,
        "pass_criteria": "Broadcast join hint count >= 10",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-09",
        "dimension": "Platform Feature Utilization",
        "check_name": "Observed write workloads are primarily incremental",
        "weight": 6,
        "pass_criteria": "Incremental load coverage >= 70%",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-10",
        "dimension": "Platform Feature Utilization",
        "check_name": "Observed ingestion workloads use auto-trigger or streaming patterns",
        "weight": 5,
        "pass_criteria": "Auto-trigger or streaming load coverage >= 40%",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-11",
        "dimension": "Performance and Efficiency",
        "check_name": "Critical pipeline p95 runtime remains within the target window",
        "weight": 5,
        "pass_criteria": "P95 pipeline runtime <= 900 seconds",
        "evidence_source": "system.lakeflow.job_run_timeline",
    },
    {
        "check_id": "WH-12",
        "dimension": "Operational Reliability",
        "check_name": "Observed pipeline success rate remains above the reliability threshold",
        "weight": 5,
        "pass_criteria": "Pipeline success rate >= 95%",
        "evidence_source": "system.lakeflow.job_run_timeline",
    },
    {
        "check_id": "WH-13",
        "dimension": "Operational Reliability",
        "check_name": "Pipeline retry rate remains below the reliability threshold",
        "weight": 1,
        "pass_criteria": "Pipeline retry rate <= 5%",
        "evidence_source": "system.lakeflow.job_run_timeline",
    },
    {
        "check_id": "WH-14",
        "dimension": "Operational Reliability",
        "check_name": "Pipeline failure rate remains below the reliability threshold",
        "weight": 1,
        "pass_criteria": "Pipeline failure rate <= 5%",
        "evidence_source": "system.lakeflow.job_run_timeline",
    },
    {
        "check_id": "WH-15",
        "dimension": "Operational Reliability",
        "check_name": "Average recovery time after failure remains within the target window",
        "weight": 1,
        "pass_criteria": "Average recovery time <= 60 minutes",
        "evidence_source": "system.lakeflow.job_run_timeline",
    },
    {
        "check_id": "WH-16",
        "dimension": "Operational Reliability",
        "check_name": "Interrupted pipeline runs remain limited",
        "weight": 1,
        "pass_criteria": "Interrupted run count <= 5",
        "evidence_source": "system.lakeflow.job_run_timeline",
    },
    {
        "check_id": "WH-17",
        "dimension": "Operational Reliability",
        "check_name": "Manual rerun count remains limited",
        "weight": 1,
        "pass_criteria": "Manual rerun count <= 10",
        "evidence_source": "system.lakeflow.job_run_timeline",
    },
    {
        "check_id": "WH-18",
        "dimension": "Performance and Efficiency",
        "check_name": "Shuffle join query count remains limited",
        "weight": 1,
        "pass_criteria": "Shuffle join query count <= 50",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-19",
        "dimension": "Performance and Efficiency",
        "check_name": "Skewed join query count remains limited",
        "weight": 1,
        "pass_criteria": "Skewed join query count <= 5",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-20",
        "dimension": "Performance and Efficiency",
        "check_name": "Cartesian join query count remains limited",
        "weight": 1,
        "pass_criteria": "Cartesian join query count <= 1",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-21",
        "dimension": "Performance and Efficiency",
        "check_name": "Very large scan query count remains limited",
        "weight": 1,
        "pass_criteria": "Very large scan query count <= 10",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "WH-22",
        "dimension": "Performance and Efficiency",
        "check_name": "Average large-table file size remains healthy",
        "weight": 1,
        "pass_criteria": "Average large-table file size >= 128 MB",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "WH-23",
        "dimension": "Performance and Efficiency",
        "check_name": "Large-table file count growth remains controlled",
        "weight": 1,
        "pass_criteria": "Large-table file count growth <= 20%",
        "evidence_source": "DESCRIBE DETAIL + telemetry history",
    },
    {
        "check_id": "WH-24",
        "dimension": "Performance and Efficiency",
        "check_name": "Compaction candidate table count remains limited",
        "weight": 1,
        "pass_criteria": "Compaction candidate tables <= 5",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "CC-01",
        "dimension": "Cost Control",
        "check_name": "Full reload workload ratio remains below the cost threshold",
        "weight": 2,
        "pass_criteria": "Full reload workload ratio <= 20%",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-02",
        "dimension": "Cost Control",
        "check_name": "Full reload workload count remains below the cost threshold",
        "weight": 2,
        "pass_criteria": "Full reload workload count <= 25",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-03",
        "dimension": "Cost Control",
        "check_name": "SELECT * query count remains below the cost threshold",
        "weight": 2,
        "pass_criteria": "SELECT * query count <= 50",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-04",
        "dimension": "Cost Control",
        "check_name": "Long-running query count remains below the cost threshold",
        "weight": 2,
        "pass_criteria": "Long-running query count <= 50",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-05",
        "dimension": "Cost Control",
        "check_name": "Large table layout strategy coverage remains above the cost threshold",
        "weight": 2,
        "pass_criteria": "Large table layout strategy coverage >= 80%",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "CC-06",
        "dimension": "Cost Control",
        "check_name": "Small-file problem table count remains below the cost threshold",
        "weight": 2,
        "pass_criteria": "Small-file problem tables <= 3",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "CC-07",
        "dimension": "Cost Control",
        "check_name": "Oversized file problem table count remains below the cost threshold",
        "weight": 2,
        "pass_criteria": "Oversized file problem tables <= 1",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "CC-08",
        "dimension": "Cost Control",
        "check_name": "Large join query count remains below the cost threshold",
        "weight": 2,
        "pass_criteria": "Large join query count <= 25",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-09",
        "dimension": "Cost Control",
        "check_name": "Pipeline p95 runtime remains within the cost target",
        "weight": 2,
        "pass_criteria": "P95 pipeline runtime <= 900 seconds",
        "evidence_source": "system.lakeflow.job_run_timeline",
    },
    {
        "check_id": "CC-10",
        "dimension": "Cost Control",
        "check_name": "Large table count remains below the cost threshold",
        "weight": 1,
        "pass_criteria": "Large table count <= 200",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "CC-11",
        "dimension": "Cost Control",
        "check_name": "Non-Delta table count remains below the cost threshold",
        "weight": 1,
        "pass_criteria": "Non-Delta table count <= 10",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "CC-12",
        "dimension": "Cost Control",
        "check_name": "Large tables without partitioning remain below the cost threshold",
        "weight": 1,
        "pass_criteria": "Large tables without partitioning <= 20",
        "evidence_source": "DESCRIBE DETAIL",
    },
    {
        "check_id": "CC-13",
        "dimension": "Cost Control",
        "check_name": "Large Delta tables missing OPTIMIZE remain below the cost threshold",
        "weight": 1,
        "pass_criteria": "Large tables missing OPTIMIZE <= 20",
        "evidence_source": "DESCRIBE HISTORY",
    },
    {
        "check_id": "CC-14",
        "dimension": "Cost Control",
        "check_name": "P95 query runtime remains within the cost threshold",
        "weight": 1,
        "pass_criteria": "P95 query runtime <= 60 seconds",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-15",
        "dimension": "Cost Control",
        "check_name": "High scan byte query count remains below the cost threshold",
        "weight": 1,
        "pass_criteria": "High scan byte query count <= 50",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-16",
        "dimension": "Cost Control",
        "check_name": "High scan-to-output ratio query count remains below the cost threshold",
        "weight": 1,
        "pass_criteria": "High scan-to-output ratio query count <= 30",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-17",
        "dimension": "Cost Control",
        "check_name": "Spill-to-disk query count remains below the cost threshold",
        "weight": 1,
        "pass_criteria": "Spill-to-disk query count <= 10",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-18",
        "dimension": "Cost Control",
        "check_name": "High shuffle byte query count remains below the cost threshold",
        "weight": 1,
        "pass_criteria": "High shuffle byte query count <= 10",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-19",
        "dimension": "Cost Control",
        "check_name": "SELECT * query ratio remains below the cost threshold",
        "weight": 1,
        "pass_criteria": "SELECT * query ratio <= 5%",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-20",
        "dimension": "Cost Control",
        "check_name": "Broadcast join ratio remains above the cost threshold",
        "weight": 1,
        "pass_criteria": "Broadcast join ratio >= 10%",
        "evidence_source": "system.query.history",
    },
    {
        "check_id": "CC-21",
        "dimension": "Cost Control",
        "check_name": "Write workload count baseline is available",
        "weight": 1,
        "pass_criteria": "Write workload count observed in last 30 days",
        "evidence_source": "system.query.history",
    },
]


def load_scorecard_definition():
    df = spark.createDataFrame(embedded_definitions)
    return df.withColumn("updated_at", F.current_timestamp())


definition_df = load_scorecard_definition()
if definition_df.filter(F.col("check_id").isNull() | F.col("dimension").isNull() | F.col("check_name").isNull()).count() > 0:
    raise Exception("scorecard_definition contains null required fields (check_id, dimension, check_name)")

duplicate_ids = (
    definition_df.groupBy("check_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
if duplicate_ids > 0:
    raise Exception("scorecard_definition contains duplicate check_id values")

weight_sum = definition_df.agg(F.sum("weight").alias("weight_sum")).collect()[0]["weight_sum"] or 0
if int(weight_sum) != 100:
    raise Exception(f"scorecard_definition weights must sum to 100, got {weight_sum}")

spark.sql("DROP TABLE IF EXISTS governance_maturity.scorecard_definition")
definition_df.write.mode("overwrite").format("delta").saveAsTable("governance_maturity.scorecard_definition")


def normalize_status(value: str):
    v = (value or "").strip().lower()
    if v in ("pass", "passed"):
        return "Pass"
    if v in ("partial", "partially"):
        return "Partial"
    if v in ("fail", "failed"):
        return "Fail"
    return "Unknown"


def align_to_table_schema(df, table_name: str):
    try:
        target = spark.table(table_name)
    except Exception:
        return df
    for field in target.schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df


status_rows = []
now_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
if STATUS_JSON.strip():
    try:
        payload = json.loads(STATUS_JSON)
    except json.JSONDecodeError as exc:
        raise Exception(f"Invalid status_json payload: {exc}")
    if not isinstance(payload, list):
        raise Exception("status_json must be a JSON array of {check_id, status, notes}")
    for item in payload:
        status_rows.append((
            item.get("check_id"),
            normalize_status(item.get("status")),
            item.get("notes"),
            ENV,
            now_ts,
            "status_json",
        ))

if status_rows:
    status_df = spark.createDataFrame(
        status_rows,
        schema="check_id string, status string, notes string, env string, updated_at timestamp, updated_by string",
    )
    status_df = align_to_table_schema(status_df, "governance_maturity.scorecard_check_status")
    status_df.write.mode("append").format("delta").saveAsTable("governance_maturity.scorecard_check_status")


latest_status = (
    spark.table("governance_maturity.scorecard_check_status")
    .filter((F.col("env") == ENV) | (F.col("env").isNull()))
    .withColumn("env_rank", F.when(F.col("env") == ENV, F.lit(2)).otherwise(F.lit(1)))
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("check_id").orderBy(F.col("env_rank").desc(), F.col("updated_at").desc())
        ),
    )
    .filter(F.col("rn") == 1)
    .select("check_id", "status", "notes")
)

definition = spark.table("governance_maturity.scorecard_definition")

joined = (
    definition.join(latest_status, on="check_id", how="left")
    .withColumn("status_norm", F.when(F.col("status").isNull(), F.lit("Unknown")).otherwise(F.col("status")))
)

print("[scorecard] latest telemetry-backed status snapshot:")
(
    joined.select("check_id", "dimension", "check_name", "weight", "status_norm", "notes")
    .orderBy("check_id")
    .show(50, False)
)


def status_score(col):
    return (
        F.when(F.col(col) == "Pass", F.lit(1.0))
        .when(F.col(col) == "Partial", F.lit(0.5))
        .when(F.col(col) == "Fail", F.lit(0.0))
        .otherwise(F.lit(0.0))
    )


scored = (
    joined.withColumn("score", status_score("status_norm"))
    .withColumn("weighted_score", F.col("score") * F.col("weight"))
    .withColumn(
        "available_weight",
        F.when(F.col("status_norm") == "Unknown", F.lit(0.0)).otherwise(F.col("weight").cast("double")),
    )
)

print("[scorecard] dimension summary:")
(
    scored.groupBy("dimension")
    .agg(
        F.sum("weighted_score").alias("dimension_weighted_score"),
        F.sum("available_weight").alias("dimension_available_weight"),
        F.sum("weight").alias("dimension_weight"),
        F.sum(F.when(F.col("status_norm") == "Pass", F.lit(1)).otherwise(F.lit(0))).alias("pass_count"),
        F.sum(F.when(F.col("status_norm") == "Partial", F.lit(1)).otherwise(F.lit(0))).alias("partial_count"),
        F.sum(F.when(F.col("status_norm") == "Fail", F.lit(1)).otherwise(F.lit(0))).alias("fail_count"),
        F.sum(F.when(F.col("status_norm") == "Unknown", F.lit(1)).otherwise(F.lit(0))).alias("unknown_count"),
    )
    .orderBy("dimension")
    .show(50, False)
)

score_totals = scored.agg(
    F.sum("weighted_score").alias("weighted_total"),
    F.sum("available_weight").alias("available_weight_total"),
).collect()[0]
weighted_total = score_totals["weighted_total"] or 0.0
available_weight_total = score_totals["available_weight_total"] or 0.0
total_score = 0.0 if float(available_weight_total) <= 0.0 else 100.0 * float(weighted_total) / float(available_weight_total)

reliability_fail = scored.filter((F.col("dimension") == "Operational Reliability") & (F.col("status_norm") == "Fail")).count() > 0

blocked = []
warned = []
if reliability_fail:
    blocked.append("Operational reliability metric failed")

unknown_count = scored.filter(F.col("status_norm") == "Unknown").count()
if unknown_count > 0:
    warned.append(f"{unknown_count} checks missing telemetry")
if float(available_weight_total) < 100.0:
    warned.append(f"Score normalized over {round(float(available_weight_total), 1)} observed weight")
if float(available_weight_total) <= 0.0:
    warned.append("No observed warehouse telemetry metrics were available for scoring")
if total_score < 75.0:
    warned.append(f"Total score below target: {round(float(total_score), 2)}")

overall_status = "Not Ready" if blocked else "Ready"

details_json = (
    scored.select(
        F.struct(
            F.col("check_id"),
            F.col("dimension"),
            F.col("check_name"),
            F.col("weight"),
            F.col("status_norm"),
            F.col("notes"),
            F.col("weighted_score"),
        ).alias("check")
    )
    .agg(
        F.to_json(
            F.struct(
                F.collect_list("check").alias("checks"),
                F.lit(SCORECARD_SOURCE).alias("scorecard_source"),
                F.lit(SCORECARD_PATH).alias("scorecard_path"),
                F.lit(float(available_weight_total)).alias("available_weight_total"),
                F.lit(float(weighted_total)).alias("weighted_total"),
            )
        ).alias("details_json")
    )
    .collect()[0]["details_json"]
)

result_df = spark.createDataFrame([(
    now_ts,
    ENV,
    RUN_ID,
    COMMIT_SHA,
    float(total_score),
    overall_status,
    blocked,
    warned,
    details_json,
)], schema="""
    collected_at timestamp, env string, run_id string, commit_sha string,
    total_score double, overall_status string, blocked_reasons array<string>,
    warned_reasons array<string>, details_json string
""")

result_df = align_to_table_schema(result_df, "governance_maturity.scorecard_results")
result_df.write.mode("append").format("delta").saveAsTable("governance_maturity.scorecard_results")

check_results_df = (
    scored.select(
        F.lit(now_ts).alias("collected_at"),
        F.lit(ENV).alias("env"),
        F.lit(RUN_ID).alias("run_id"),
        F.lit(COMMIT_SHA).alias("commit_sha"),
        F.col("check_id"),
        F.col("dimension"),
        F.col("check_name"),
        F.col("weight"),
        F.col("status_norm"),
        F.col("notes"),
        F.col("score"),
        F.col("weighted_score"),
    )
)
check_results_df = align_to_table_schema(check_results_df, "governance_maturity.scorecard_check_results")
check_results_df.write.mode("append").format("delta").saveAsTable("governance_maturity.scorecard_check_results")

print(f"[scorecard] env={ENV} run_id={RUN_ID} total_score={total_score} status={overall_status}")
if blocked:
    print(f"[scorecard] blocked: {blocked}")
if warned:
    print(f"[scorecard] warned: {warned}")
