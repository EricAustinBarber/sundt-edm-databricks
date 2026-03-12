# Databricks notebook source
# maturity_collector_notebook.py
dbutils.widgets.text("env", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("commit_sha", "")
dbutils.widgets.text("repo", "")
dbutils.widgets.text("git_ref", "")
dbutils.widgets.text("workflow_run_id", "")
dbutils.widgets.text("bundle_name", "")

ENV = dbutils.widgets.get("env") or "unknown"
RUN_ID = dbutils.widgets.get("run_id") or "unknown"
COMMIT_SHA = dbutils.widgets.get("commit_sha") or None
REPO = dbutils.widgets.get("repo") or None
GIT_REF = dbutils.widgets.get("git_ref") or None
WORKFLOW_RUN_ID = dbutils.widgets.get("workflow_run_id") or None
BUNDLE_NAME = dbutils.widgets.get("bundle_name") or "unknown"

import json
from decimal import Decimal
from pyspark.sql import functions as F

LARGE_TABLE_BYTES = 10 * 1024 * 1024 * 1024
SMALL_FILE_AVG_BYTES = 32 * 1024 * 1024
OVERSIZED_FILE_AVG_BYTES = 1024 * 1024 * 1024
LARGE_JOIN_RUNTIME_MS = 120000
OPTIMIZE_LOOKBACK_DAYS = 90

spark.sql("CREATE SCHEMA IF NOT EXISTS governance_maturity")

spark.sql("""
CREATE TABLE IF NOT EXISTS governance_maturity.bundle_deployments (
  deployed_at        TIMESTAMP NOT NULL,
  env                STRING    NOT NULL,
  repo               STRING,
  bundle_name        STRING,
  git_sha            STRING,
  git_ref            STRING,
  workflow_run_id    STRING,
  run_id             STRING
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

now_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

metric_catalog = [
    {
        "metric_id": "WH-01",
        "dimension": "Platform Feature Utilization",
        "metric_name": "delta_curated_table_coverage_pct",
        "description": "Percent of curated warehouse tables stored in Delta format.",
        "why_it_matters": "Delta is the baseline feature set for reliable incremental processing, maintenance, and optimization.",
        "how_to_measure": "Inventory non-system tables and inspect table format with DESCRIBE DETAIL.",
        "improvement_signal": "Migrate non-Delta curated tables to Delta.",
        "source_name": "system.information_schema.tables + DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "high",
        "pass_threshold": 90.0,
        "partial_threshold": 75.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-02",
        "dimension": "Platform Feature Utilization",
        "metric_name": "merge_load_ratio_pct_30d",
        "description": "Percent of observed write workloads using MERGE logic instead of full replacement patterns.",
        "why_it_matters": "MERGE indicates more mature incremental load design than rebuild-only patterns.",
        "how_to_measure": "Parse recent warehouse write statements from system.query.history.",
        "improvement_signal": "Replace create-or-replace rebuilds with keyed incremental MERGE logic.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "high",
        "pass_threshold": 60.0,
        "partial_threshold": 35.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-03",
        "dimension": "Platform Feature Utilization",
        "metric_name": "large_table_layout_strategy_coverage_pct",
        "description": "Percent of large tables with partitioning present.",
        "why_it_matters": "Large tables without layout strategy are harder to prune, compact, and operate efficiently.",
        "how_to_measure": "Inspect partition columns from DESCRIBE DETAIL for large tables.",
        "improvement_signal": "Add partitioning or clustering to large tables with broad scan patterns.",
        "source_name": "system.information_schema.tables + DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "high",
        "pass_threshold": 80.0,
        "partial_threshold": 50.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-04",
        "dimension": "Performance and Efficiency",
        "metric_name": "small_file_problem_table_count",
        "description": "Count of large tables whose average file size suggests compaction issues.",
        "why_it_matters": "Small files increase planning overhead, metadata pressure, and runtime inefficiency.",
        "how_to_measure": "Compare DESCRIBE DETAIL sizeInBytes and numFiles to average file size thresholds.",
        "improvement_signal": "Run compaction or adjust write patterns to reduce small files.",
        "source_name": "DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "low",
        "pass_threshold": 3.0,
        "partial_threshold": 10.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-05",
        "dimension": "Performance and Efficiency",
        "metric_name": "oversized_file_problem_table_count",
        "description": "Count of tables whose average file size is large enough to suggest write or compaction imbalance.",
        "why_it_matters": "Oversized files can reduce parallelism and create skew in downstream processing.",
        "how_to_measure": "Compare DESCRIBE DETAIL sizeInBytes and numFiles to oversized file thresholds.",
        "improvement_signal": "Adjust writer settings or partition strategy to improve parallelism.",
        "source_name": "DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "low",
        "pass_threshold": 1.0,
        "partial_threshold": 5.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-06",
        "dimension": "Platform Feature Utilization",
        "metric_name": "large_table_optimize_coverage_pct",
        "description": "Percent of large Delta tables with recent OPTIMIZE activity.",
        "why_it_matters": "OPTIMIZE usage indicates active Delta maintenance on the tables most likely to need it.",
        "how_to_measure": "Inspect DESCRIBE HISTORY for recent OPTIMIZE operations on large Delta tables.",
        "improvement_signal": "Schedule OPTIMIZE for large Delta tables with write churn or file health issues.",
        "source_name": "DESCRIBE HISTORY",
        "collection_method": "table_history_scan",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "high",
        "pass_threshold": 70.0,
        "partial_threshold": 40.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-07",
        "dimension": "Performance and Efficiency",
        "metric_name": "large_join_query_count_30d",
        "description": "Count of long-running join queries observed over the last 30 days.",
        "why_it_matters": "Frequent expensive joins are a signal for data model, partitioning, or broadcast tuning opportunities.",
        "how_to_measure": "Parse recent query history for join statements above the large-join runtime threshold.",
        "improvement_signal": "Reduce shuffle-heavy joins with layout improvements, broadcast, or model changes.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "low",
        "pass_threshold": 25.0,
        "partial_threshold": 75.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-08",
        "dimension": "Platform Feature Utilization",
        "metric_name": "broadcast_join_hint_count_30d",
        "description": "Count of recent queries that explicitly used a broadcast join hint.",
        "why_it_matters": "Broadcast usage shows that teams are applying Databricks join optimization features where useful.",
        "how_to_measure": "Parse recent query history for broadcast join hints.",
        "improvement_signal": "Evaluate selective broadcast opportunities on small dimensions in expensive joins.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "high",
        "pass_threshold": 10.0,
        "partial_threshold": 3.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-09",
        "dimension": "Platform Feature Utilization",
        "metric_name": "incremental_load_coverage_pct_30d",
        "description": "Percent of observed write workloads using incremental patterns.",
        "why_it_matters": "Incremental pipelines are generally cheaper, faster, and easier to operate than repeated full rebuilds.",
        "how_to_measure": "Parse recent write statements from system.query.history and classify incremental patterns.",
        "improvement_signal": "Convert full reload pipelines to keyed or append-based incremental processing.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "high",
        "pass_threshold": 70.0,
        "partial_threshold": 40.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-10",
        "dimension": "Platform Feature Utilization",
        "metric_name": "auto_trigger_streaming_load_coverage_pct_30d",
        "description": "Percent of observed ingestion workloads using streaming or auto-trigger patterns.",
        "why_it_matters": "Auto-trigger and streaming patterns indicate higher warehouse automation maturity.",
        "how_to_measure": "Parse recent query history for trigger available now, streaming, or cloud_files patterns.",
        "improvement_signal": "Move suitable ingestion flows to streaming or auto-trigger patterns.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "high",
        "pass_threshold": 40.0,
        "partial_threshold": 15.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-11",
        "dimension": "Performance and Efficiency",
        "metric_name": "critical_pipeline_p95_runtime_seconds_30d",
        "description": "P95 runtime of observed Lakeflow or job runs over the last 30 days.",
        "why_it_matters": "Long critical-path runtime increases cost, latency, and operational fragility.",
        "how_to_measure": "Use system.lakeflow.job_run_timeline when available.",
        "improvement_signal": "Tune or split long-running jobs and reduce expensive table operations.",
        "source_name": "system.lakeflow.job_run_timeline",
        "collection_method": "job_run_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "low",
        "pass_threshold": 900.0,
        "partial_threshold": 1800.0,
        "threshold_unit": "seconds",
    },
    {
        "metric_id": "WH-12",
        "dimension": "Operational Reliability",
        "metric_name": "pipeline_success_rate_pct_30d",
        "description": "Success rate of observed Lakeflow or job runs over the last 30 days.",
        "why_it_matters": "Pipeline success rate is the clearest warehouse reliability indicator in the scorecard.",
        "how_to_measure": "Use system.lakeflow.job_run_timeline and classify successful outcomes.",
        "improvement_signal": "Reduce failing jobs and add retry, testing, and alerting where missing.",
        "source_name": "system.lakeflow.job_run_timeline",
        "collection_method": "job_run_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": True,
        "direction": "high",
        "pass_threshold": 95.0,
        "partial_threshold": 85.0,
        "threshold_unit": "percent",
    },
]


def align_to_table_schema(df, table_name: str):
    try:
        target = spark.table(table_name)
    except Exception:
        return df
    for field in target.schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df


def quote_ident(value: str):
    return f"`{value.replace('`', '``')}`"


def fq_table_name(catalog: str, schema: str, table: str):
    return ".".join([quote_ident(catalog), quote_ident(schema), quote_ident(table)])


def table_columns(table_name: str):
    try:
        return {field.name.lower() for field in spark.table(table_name).schema.fields}
    except Exception:
        return set()


def choose_column(table_name: str, candidates: list[str]):
    cols = table_columns(table_name)
    for candidate in candidates:
        if candidate.lower() in cols:
            return candidate
    return None


def metric_row(metric: dict, *, metric_value_double=None, metric_value_string=None, status_hint="Observed", notes=None, metric_sql=None, metric_json=None):
    window_days = 1 if metric["collection_method"] in ("table_profile_scan", "table_history_scan") else 30
    return (
        now_ts,
        ENV,
        RUN_ID,
        COMMIT_SHA,
        metric["metric_id"],
        metric["dimension"],
        metric["metric_name"],
        metric["source_name"],
        window_days,
        metric_value_double,
        metric_value_string,
        status_hint,
        notes,
        metric_sql,
        json.dumps(_json_safe(metric_json or {}), sort_keys=True),
    )


def unknown_metric_row(metric: dict, notes: str, metric_sql=None, metric_json=None):
    return metric_row(
        metric,
        status_hint="Unknown",
        notes=notes,
        metric_sql=metric_sql,
        metric_json=metric_json,
    )


def safe_collect_one(sql_text: str):
    return spark.sql(sql_text).collect()[0]


def _json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "asDict"):
        return _json_safe(value.asDict(recursive=True))
    return value


table_profiles_cache = None


def get_table_profiles():
    global table_profiles_cache
    if table_profiles_cache is not None:
        return table_profiles_cache

    inventory_sql = """
        SELECT table_catalog, table_schema, table_name, table_type
        FROM system.information_schema.tables
        WHERE table_catalog NOT IN ('system', 'samples')
          AND table_schema <> 'information_schema'
          AND UPPER(table_type) NOT LIKE '%VIEW%'
    """

    inventory_rows = spark.sql(inventory_sql).collect()
    profiles = []
    for row in inventory_rows:
        profile = {
            "table_catalog": row["table_catalog"],
            "table_schema": row["table_schema"],
            "table_name": row["table_name"],
            "table_type": row["table_type"],
            "full_name": fq_table_name(row["table_catalog"], row["table_schema"], row["table_name"]),
        }
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {profile['full_name']}").collect()[0].asDict(recursive=True)
            format_name = str(detail.get("format") or "").lower()
            num_files = detail.get("numFiles")
            size_in_bytes = detail.get("sizeInBytes")
            avg_file_size = None
            if num_files and size_in_bytes is not None and float(num_files) > 0:
                avg_file_size = float(size_in_bytes) / float(num_files)
            partition_columns = detail.get("partitionColumns") or []
            profile.update(
                {
                    "format": format_name,
                    "num_files": float(num_files) if num_files is not None else None,
                    "size_in_bytes": float(size_in_bytes) if size_in_bytes is not None else None,
                    "avg_file_size_bytes": avg_file_size,
                    "partition_columns": partition_columns,
                    "partition_column_count": len(partition_columns),
                    "detail_error": None,
                }
            )
        except Exception as exc:
            profile.update(
                {
                    "format": None,
                    "num_files": None,
                    "size_in_bytes": None,
                    "avg_file_size_bytes": None,
                    "partition_columns": [],
                    "partition_column_count": 0,
                    "detail_error": str(exc),
                }
            )
        profiles.append(profile)

    table_profiles_cache = profiles
    return table_profiles_cache


def profiled_tables():
    return [profile for profile in get_table_profiles() if not profile["detail_error"]]


def large_tables():
    return [
        profile
        for profile in profiled_tables()
        if profile["size_in_bytes"] is not None and profile["size_in_bytes"] >= LARGE_TABLE_BYTES
    ]


def large_delta_tables():
    return [profile for profile in large_tables() if profile["format"] == "delta"]


def collect_table_profile_metric(metric: dict):
    profiled = profiled_tables()
    skipped = len(get_table_profiles()) - len(profiled)

    if metric["metric_id"] == "WH-01":
        if not profiled:
            return unknown_metric_row(metric, "No describable warehouse tables found", "DESCRIBE DETAIL inventory scan")
        delta_tables = sum(1 for profile in profiled if profile["format"] == "delta")
        value = 100.0 * delta_tables / len(profiled)
        notes = f"{delta_tables} of {len(profiled)} described tables are Delta; skipped {skipped} tables"
        return metric_row(metric, metric_value_double=float(value), notes=notes, metric_sql="DESCRIBE DETAIL inventory scan", metric_json={"described_tables": len(profiled), "delta_tables": delta_tables, "skipped_tables": skipped})

    if metric["metric_id"] == "WH-03":
        large = large_tables()
        if not large:
            return unknown_metric_row(metric, "No large tables found using the current size threshold", "DESCRIBE DETAIL inventory scan")
        with_layout = sum(1 for profile in large if profile["partition_column_count"] > 0)
        value = 100.0 * with_layout / len(large)
        notes = f"{with_layout} of {len(large)} large tables have partition columns; liquid clustering not yet inspected"
        return metric_row(metric, metric_value_double=float(value), notes=notes, metric_sql="DESCRIBE DETAIL inventory scan", metric_json={"large_tables": len(large), "large_tables_with_partitioning": with_layout})

    if metric["metric_id"] == "WH-04":
        candidates = [
            profile
            for profile in large_tables()
            if profile["avg_file_size_bytes"] is not None and profile["avg_file_size_bytes"] < SMALL_FILE_AVG_BYTES
        ]
        notes = f"{len(candidates)} large tables below {int(SMALL_FILE_AVG_BYTES / (1024 * 1024))} MB average file size"
        return metric_row(metric, metric_value_double=float(len(candidates)), notes=notes, metric_sql="DESCRIBE DETAIL inventory scan", metric_json={"problem_tables": [profile["full_name"] for profile in candidates[:25]], "problem_table_count": len(candidates)})

    if metric["metric_id"] == "WH-05":
        candidates = [
            profile
            for profile in profiled_tables()
            if profile["avg_file_size_bytes"] is not None and profile["avg_file_size_bytes"] > OVERSIZED_FILE_AVG_BYTES
        ]
        notes = f"{len(candidates)} tables above {int(OVERSIZED_FILE_AVG_BYTES / (1024 * 1024 * 1024))} GB average file size"
        return metric_row(metric, metric_value_double=float(len(candidates)), notes=notes, metric_sql="DESCRIBE DETAIL inventory scan", metric_json={"problem_tables": [profile["full_name"] for profile in candidates[:25]], "problem_table_count": len(candidates)})

    if metric["metric_id"] == "WH-06":
        candidates = large_delta_tables()
        if not candidates:
            return unknown_metric_row(metric, "No large Delta tables found for OPTIMIZE inspection", "DESCRIBE HISTORY scan")
        optimized = 0
        unavailable = 0
        for profile in candidates:
            try:
                history = spark.sql(f"DESCRIBE HISTORY {profile['full_name']}")
                recent = history.filter(
                    (F.col("timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {OPTIMIZE_LOOKBACK_DAYS} DAYS"))
                    & (F.upper(F.col("operation")) == "OPTIMIZE")
                )
                if recent.limit(1).count() > 0:
                    optimized += 1
            except Exception:
                unavailable += 1
        observed = len(candidates) - unavailable
        if observed <= 0:
            return unknown_metric_row(metric, "DESCRIBE HISTORY unavailable for large Delta tables", "DESCRIBE HISTORY scan", {"candidate_tables": len(candidates), "history_unavailable": unavailable})
        value = 100.0 * optimized / observed
        notes = f"{optimized} of {observed} large Delta tables showed recent OPTIMIZE; history unavailable for {unavailable}"
        return metric_row(metric, metric_value_double=float(value), notes=notes, metric_sql="DESCRIBE HISTORY scan", metric_json={"candidate_tables": len(candidates), "observed_tables": observed, "optimized_tables": optimized, "history_unavailable": unavailable})

    raise Exception(f"Unhandled table profile metric: {metric['metric_id']}")


def collect_query_history_metric(metric: dict):
    table_name = "system.query.history"
    start_col = choose_column(table_name, ["start_time"])
    text_col = choose_column(table_name, ["statement_text", "query_text", "statement_text_truncated", "query_text_truncated"])
    duration_col = choose_column(table_name, ["total_duration_ms"])
    if start_col is None:
        return unknown_metric_row(metric, "system.query.history is unavailable or missing start_time")
    if text_col is None:
        return unknown_metric_row(metric, "system.query.history is missing a statement text column")

    text_expr = f"UPPER(COALESCE({text_col}, ''))"
    write_predicate = f"""({text_expr} LIKE '%MERGE INTO%'
        OR {text_expr} LIKE '%CREATE OR REPLACE%'
        OR {text_expr} LIKE '%REPLACE TABLE%'
        OR {text_expr} LIKE '%INSERT INTO%'
        OR {text_expr} LIKE '%COPY INTO%'
        OR {text_expr} LIKE '%TRUNCATE TABLE%'
        OR {text_expr} LIKE '%DELETE FROM%'
        OR {text_expr} LIKE '%UPDATE %')"""
    incremental_predicate = f"""({text_expr} LIKE '%MERGE INTO%'
        OR {text_expr} LIKE '%INSERT INTO%'
        OR {text_expr} LIKE '%DELETE FROM%'
        OR {text_expr} LIKE '%UPDATE %')"""
    rebuild_predicate = f"""({text_expr} LIKE '%CREATE OR REPLACE%'
        OR {text_expr} LIKE '%REPLACE TABLE%'
        OR {text_expr} LIKE '%TRUNCATE TABLE%')"""
    auto_trigger_predicate = f"""({text_expr} LIKE '%AVAILABLE NOW%'
        OR {text_expr} LIKE '%TRIGGER%'
        OR {text_expr} LIKE '%STREAM%'
        OR {text_expr} LIKE '%CLOUD_FILES%'
        OR {text_expr} LIKE '%AUTO LOADER%')"""
    join_predicate = f"({text_expr} LIKE '% JOIN %')"
    broadcast_predicate = f"({text_expr} LIKE '%BROADCAST%')"

    if metric["metric_id"] == "WH-02":
        sql_text = f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN NULL
                   ELSE 100.0 * SUM(CASE WHEN {text_expr} LIKE '%MERGE INTO%' THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS relevant_workloads,
              SUM(CASE WHEN {text_expr} LIKE '%MERGE INTO%' THEN 1 ELSE 0 END) AS merge_workloads,
              SUM(CASE WHEN {rebuild_predicate} THEN 1 ELSE 0 END) AS rebuild_workloads
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {write_predicate}
        """
    elif metric["metric_id"] == "WH-07":
        duration_predicate = "TRUE" if duration_col is None else f"{duration_col} >= {LARGE_JOIN_RUNTIME_MS}"
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value,
              COUNT(*) AS large_join_queries
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {join_predicate}
              AND {duration_predicate}
        """
    elif metric["metric_id"] == "WH-08":
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value,
              COUNT(*) AS broadcast_queries
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {broadcast_predicate}
        """
    elif metric["metric_id"] == "WH-09":
        sql_text = f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN NULL
                   ELSE 100.0 * SUM(CASE WHEN {incremental_predicate} THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS relevant_workloads,
              SUM(CASE WHEN {incremental_predicate} THEN 1 ELSE 0 END) AS incremental_workloads,
              SUM(CASE WHEN {rebuild_predicate} THEN 1 ELSE 0 END) AS rebuild_workloads
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {write_predicate}
        """
    elif metric["metric_id"] == "WH-10":
        sql_text = f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN NULL
                   ELSE 100.0 * SUM(CASE WHEN {auto_trigger_predicate} THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS ingestion_workloads,
              SUM(CASE WHEN {auto_trigger_predicate} THEN 1 ELSE 0 END) AS auto_trigger_workloads
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {write_predicate}
        """
    else:
        raise Exception(f"Unhandled query history metric: {metric['metric_id']}")

    try:
        result = safe_collect_one(sql_text).asDict(recursive=True)
        metric_value = result.get("metric_value")
        if metric_value is None:
            return unknown_metric_row(metric, "Metric query returned no measurable value", sql_text, result)
        notes = ", ".join([f"{key}={value}" for key, value in result.items() if key != "metric_value" and value is not None])
        return metric_row(metric, metric_value_double=float(metric_value), notes=notes or None, metric_sql=sql_text, metric_json=result)
    except Exception as exc:
        return unknown_metric_row(metric, str(exc), sql_text, {"table_name": table_name})


def collect_job_metric(metric: dict):
    table_name = "system.lakeflow.job_run_timeline"
    start_col = choose_column(table_name, ["period_start_time", "start_time"])
    end_col = choose_column(table_name, ["period_end_time", "end_time"])
    state_col = choose_column(table_name, ["result_state", "run_result_state"])
    if start_col is None:
        return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is unavailable or missing start timestamp")

    runtime_expr = None
    if end_col is not None:
        runtime_expr = f"CAST(unix_timestamp(COALESCE({end_col}, current_timestamp())) - unix_timestamp({start_col}) AS DOUBLE)"

    if metric["metric_id"] == "WH-11":
        if runtime_expr is None:
            return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is missing end timestamp columns")
        sql_text = f"""
            SELECT
              percentile_approx({runtime_expr}, 0.95) AS metric_value,
              COUNT(*) AS run_count
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
        """
    elif metric["metric_id"] == "WH-12":
        if state_col is None:
            return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is missing result state columns")
        sql_text = f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN NULL
                   ELSE 100.0 * SUM(CASE WHEN UPPER(COALESCE({state_col}, '')) IN ('SUCCESS', 'SUCCEEDED') THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS run_count,
              SUM(CASE WHEN UPPER(COALESCE({state_col}, '')) IN ('SUCCESS', 'SUCCEEDED') THEN 1 ELSE 0 END) AS success_count
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
        """
    else:
        raise Exception(f"Unhandled job metric: {metric['metric_id']}")

    try:
        result = safe_collect_one(sql_text).asDict(recursive=True)
        metric_value = result.get("metric_value")
        if metric_value is None:
            return unknown_metric_row(metric, "Metric query returned no measurable value", sql_text, result)
        notes = ", ".join([f"{key}={value}" for key, value in result.items() if key != "metric_value" and value is not None])
        return metric_row(metric, metric_value_double=float(metric_value), notes=notes or None, metric_sql=sql_text, metric_json=result)
    except Exception as exc:
        return unknown_metric_row(metric, str(exc), sql_text, {"table_name": table_name})


catalog_rows = [
    (
        metric["metric_id"],
        metric["dimension"],
        metric["metric_name"],
        metric["description"],
        metric["why_it_matters"],
        metric["how_to_measure"],
        metric["improvement_signal"],
        metric["source_name"],
        metric["collection_method"],
        metric["implementation_status"],
        metric["enabled_for_scorecard"],
        metric["v1_candidate"],
        metric["direction"],
        metric["pass_threshold"],
        metric["partial_threshold"],
        metric["threshold_unit"],
        now_ts,
    )
    for metric in metric_catalog
]

catalog_df = spark.createDataFrame(
    catalog_rows,
    schema="""
        metric_id string, dimension string, metric_name string, description string,
        why_it_matters string, how_to_measure string, improvement_signal string, source_name string,
        collection_method string, implementation_status string, enabled_for_scorecard boolean,
        v1_candidate boolean, direction string, pass_threshold double,
        partial_threshold double, threshold_unit string, updated_at timestamp
    """,
)
catalog_df = align_to_table_schema(catalog_df, "governance_maturity.warehouse_metric_catalog")
catalog_df.write.mode("overwrite").format("delta").saveAsTable("governance_maturity.warehouse_metric_catalog")

rows = []
for metric in metric_catalog:
    try:
        if metric["collection_method"] in ("table_profile_scan", "table_history_scan"):
            rows.append(collect_table_profile_metric(metric))
        elif metric["collection_method"] == "query_history_sql":
            rows.append(collect_query_history_metric(metric))
        elif metric["collection_method"] == "job_run_sql":
            rows.append(collect_job_metric(metric))
        else:
            rows.append(unknown_metric_row(metric, f"Unsupported collection method: {metric['collection_method']}"))
    except Exception as exc:
        rows.append(unknown_metric_row(metric, str(exc), metric_json={"metric_id": metric["metric_id"]}))

metrics_df = spark.createDataFrame(
    rows,
    schema="""
        collected_at timestamp, env string, run_id string, commit_sha string,
        check_id string, dimension string, metric_name string, source_name string,
        window_days int, metric_value_double double, metric_value_string string,
        status_hint string, notes string, metric_sql string, metric_json string
    """,
)
metrics_df = align_to_table_schema(metrics_df, "governance_maturity.warehouse_telemetry_metrics")
metrics_df.write.mode("append").format("delta").saveAsTable("governance_maturity.warehouse_telemetry_metrics")

deploy_df = spark.createDataFrame([(
    now_ts, ENV, REPO, BUNDLE_NAME, COMMIT_SHA, GIT_REF, WORKFLOW_RUN_ID, RUN_ID
)], schema="""
    deployed_at timestamp, env string, repo string, bundle_name string,
    git_sha string, git_ref string, workflow_run_id string, run_id string
""")
deploy_df = align_to_table_schema(deploy_df, "governance_maturity.bundle_deployments")
deploy_df.write.mode("append").format("delta").saveAsTable("governance_maturity.bundle_deployments")

unknown_count = metrics_df.filter(F.col("status_hint") == "Unknown").count()
print(f"[maturity] published {catalog_df.count()} warehouse metric catalog rows")
print(f"[maturity] wrote {metrics_df.count()} warehouse telemetry metrics for env={ENV} run_id={RUN_ID}")
print(f"[maturity] metrics with unknown telemetry: {unknown_count}")
print(f"[maturity] recorded deployment metadata for env={ENV} bundle={BUNDLE_NAME}")
