# Databricks notebook source
# scorecard_evaluation_notebook.py
dbutils.widgets.text("env", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("commit_sha", "")
dbutils.widgets.text("status_json", "")               # optional JSON array for overrides (unused in default workflow)

ENV = dbutils.widgets.get("env") or "unknown"
RUN_ID = dbutils.widgets.get("run_id") or "manual"
COMMIT_SHA = dbutils.widgets.get("commit_sha") or None
STATUS_JSON = dbutils.widgets.get("status_json") or ""

import json
from pyspark.sql import functions as F
from pyspark.sql import Window

SCORECARD_SOURCE = "embedded"
SCORECARD_PATH = ""

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
        "check_id": "DBX-01",
        "dimension": "Standards",
        "check_name": "Notebook parameterization by environment and source object",
        "weight": 6,
        "pass_criteria": "Parameterization is standard across notebooks (widgets or equivalent) with minimal exceptions documented",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-02",
        "dimension": "Standards",
        "check_name": "Delta tables used for curated layers",
        "weight": 7,
        "pass_criteria": "Curated layers consistently use Delta tables; exceptions documented",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-03",
        "dimension": "Standards",
        "check_name": "Incremental pipelines use deterministic merge/upsert",
        "weight": 8,
        "pass_criteria": "Merge/upsert patterns are standard for incremental loads with coverage mapped by dataset",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-04",
        "dimension": "Standards",
        "check_name": "Data sanitization rules explicitly coded and tested",
        "weight": 7,
        "pass_criteria": "Sanitization rules exist per domain and are validated/tested",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-05",
        "dimension": "Performance",
        "check_name": "Table hygiene tasks defined and executed",
        "weight": 12,
        "pass_criteria": "OPTIMIZE/VACUUM coverage mapped to all large/critical tables with defined cadence",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-06",
        "dimension": "Performance",
        "check_name": "Partitioning/clustering documented for large tables",
        "weight": 10,
        "pass_criteria": "Partitioning/clustering standards exist and are applied to all large/critical tables",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-07",
        "dimension": "Performance",
        "check_name": "File-size thresholds and compaction strategy present",
        "weight": 18,
        "pass_criteria": "Explicit file-size policy and compaction thresholds defined and enforced",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-08",
        "dimension": "Reliability",
        "check_name": "Error handling and retry/idempotency patterns present",
        "weight": 7,
        "pass_criteria": "Standardized retry/idempotency utilities are used by all production jobs",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-09",
        "dimension": "Security",
        "check_name": "Secrets accessed securely (Key Vault/secret scopes)",
        "weight": 20,
        "pass_criteria": "All secrets retrieved via secret scopes or Key Vault; no literal credentials",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
    {
        "check_id": "DBX-10",
        "dimension": "Observability",
        "check_name": "Observability hooks and logging included in jobs",
        "weight": 5,
        "pass_criteria": "Job-level logging/metrics are standardized and mapped to all production jobs",
        "evidence_source": "assessments/databricks-transformation-review.md",
    },
]


def load_scorecard_definition():
    df = spark.createDataFrame(embedded_definitions)
    return df.withColumn("updated_at", F.current_timestamp())


definition_df = load_scorecard_definition()
if definition_df.filter(F.col("check_id").isNull() | F.col("dimension").isNull() | F.col("check_name").isNull()).count() > 0:
    raise Exception("scorecard_definition contains null required fields (check_id, dimension, check_name)")

duplicate_ids = (definition_df.groupBy("check_id")
                 .count()
                 .filter(F.col("count") > 1)
                 .count())
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
            "status_json"
        ))

if status_rows:
    status_df = spark.createDataFrame(
        status_rows,
        schema="check_id string, status string, notes string, env string, updated_at timestamp, updated_by string"
    )
    status_df = align_to_table_schema(
        status_df, "governance_maturity.scorecard_check_status"
    )
    status_df.write.mode("append").format("delta").saveAsTable("governance_maturity.scorecard_check_status")


latest_status = (spark.table("governance_maturity.scorecard_check_status")
                 .filter((F.col("env") == ENV) | (F.col("env").isNull()))
                 .withColumn("env_rank", F.when(F.col("env") == ENV, F.lit(2)).otherwise(F.lit(1)))
                 .withColumn("rn", F.row_number().over(
                     Window.partitionBy("check_id").orderBy(F.col("env_rank").desc(), F.col("updated_at").desc())
                 ))
                 .filter(F.col("rn") == 1)
                 .select("check_id", "status", "notes"))

definition = spark.table("governance_maturity.scorecard_definition")

joined = (definition.join(latest_status, on="check_id", how="left")
          .withColumn("status_norm", F.when(F.col("status").isNull(), F.lit("Unknown")).otherwise(F.col("status"))))

print("[scorecard] latest status snapshot (env-scoped, newest by check_id):")
(joined
 .select("check_id", "dimension", "check_name", "weight", "status_norm", "notes")
 .orderBy("check_id")
 .show(50, False))

def status_score(col):
    return (F.when(F.col(col) == "Pass", F.lit(1.0))
            .when(F.col(col) == "Partial", F.lit(0.5))
            .when(F.col(col) == "Fail", F.lit(0.0))
            .otherwise(F.lit(0.0)))

scored = (joined
          .withColumn("score", status_score("status_norm"))
          .withColumn("weighted_score", F.col("score") * F.col("weight")))

print("[scorecard] dimension summary:")
(scored.groupBy("dimension")
 .agg(
     F.sum("weighted_score").alias("dimension_score"),
     F.sum("weight").alias("dimension_weight"),
     F.sum(F.when(F.col("status_norm") == "Pass", F.lit(1)).otherwise(F.lit(0))).alias("pass_count"),
     F.sum(F.when(F.col("status_norm") == "Partial", F.lit(1)).otherwise(F.lit(0))).alias("partial_count"),
     F.sum(F.when(F.col("status_norm") == "Fail", F.lit(1)).otherwise(F.lit(0))).alias("fail_count"),
     F.sum(F.when(F.col("status_norm") == "Unknown", F.lit(1)).otherwise(F.lit(0))).alias("unknown_count"),
 )
 .orderBy("dimension")
 .show(50, False))

total_score = scored.agg(F.sum("weighted_score").alias("total")).collect()[0]["total"] or 0.0

security_fail = scored.filter((F.col("dimension") == "Security") & (F.col("status_norm") == "Fail")).count() > 0

blocked = []
warned = []
if security_fail:
    blocked.append("Security check failed")

unknown_count = scored.filter(F.col("status_norm") == "Unknown").count()
if unknown_count > 0:
    warned.append(f"{unknown_count} checks missing status")

overall_status = "Not Ready" if blocked else "Ready"

details_json = scored.select(
    F.struct(
        F.col("check_id"),
        F.col("dimension"),
        F.col("check_name"),
        F.col("weight"),
        F.col("status_norm"),
        F.col("notes"),
        F.col("weighted_score"),
    ).alias("check")
).agg(
    F.to_json(
        F.struct(
            F.collect_list("check").alias("checks"),
            F.lit(SCORECARD_SOURCE).alias("scorecard_source"),
            F.lit(SCORECARD_PATH).alias("scorecard_path"),
        )
    ).alias("details_json")
).collect()[0]["details_json"]

result_df = spark.createDataFrame([(
    now_ts, ENV, RUN_ID, COMMIT_SHA, float(total_score),
    overall_status, blocked, warned, details_json
)], schema="""
    collected_at timestamp, env string, run_id string, commit_sha string,
    total_score double, overall_status string, blocked_reasons array<string>,
    warned_reasons array<string>, details_json string
""")

result_df = align_to_table_schema(
    result_df, "governance_maturity.scorecard_results"
)
result_df.write.mode("append").format("delta").saveAsTable("governance_maturity.scorecard_results")

check_results_df = (scored
                    .select(
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
                    )) 
check_results_df = align_to_table_schema(
    check_results_df, "governance_maturity.scorecard_check_results"
)

check_results_df.write.mode("append").format("delta").saveAsTable("governance_maturity.scorecard_check_results")

print(f"[scorecard] env={ENV} run_id={RUN_ID} total_score={total_score} status={overall_status}")
if blocked:
    print(f"[scorecard] blocked: {blocked}")
if warned:
    print(f"[scorecard] warned: {warned}")
