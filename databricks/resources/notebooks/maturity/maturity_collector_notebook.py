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
LONG_QUERY_RUNTIME_MS = 300000
LARGE_SCAN_BYTES = 50 * 1024 * 1024 * 1024
HIGH_SCAN_TO_OUTPUT_RATIO = 50.0
HIGH_SHUFFLE_BYTES = 10 * 1024 * 1024 * 1024
HIGH_SPILL_BYTES = 2 * 1024 * 1024 * 1024
VERY_LARGE_SCAN_BYTES = 200 * 1024 * 1024 * 1024
VERY_LARGE_SCAN_ROWS = 1_000_000_000
COMPACTION_FILE_COUNT = 10000

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
    {
        "metric_id": "WH-13",
        "dimension": "Operational Reliability",
        "metric_name": "pipeline_retry_rate_pct_30d",
        "description": "Percent of pipeline runs that required at least one retry in the last 30 days.",
        "why_it_matters": "High retry rates indicate instability even when runs eventually succeed.",
        "how_to_measure": "Use job run attempt metadata when available to count retries.",
        "improvement_signal": "Reduce retried runs by addressing root-cause failures and flaky dependencies.",
        "source_name": "system.lakeflow.job_run_timeline",
        "collection_method": "job_run_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 5.0,
        "partial_threshold": 10.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-14",
        "dimension": "Operational Reliability",
        "metric_name": "pipeline_failure_rate_pct_30d",
        "description": "Average failure rate across pipelines over the last 30 days.",
        "why_it_matters": "Persistent failures undermine trust in warehouse operations and increase manual toil.",
        "how_to_measure": "Calculate failure rates per pipeline and average across observed pipelines.",
        "improvement_signal": "Stabilize the least reliable pipelines and reduce recurring failures.",
        "source_name": "system.lakeflow.job_run_timeline",
        "collection_method": "job_run_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 5.0,
        "partial_threshold": 10.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-15",
        "dimension": "Operational Reliability",
        "metric_name": "pipeline_recovery_time_avg_minutes_30d",
        "description": "Average minutes to recover from a failed pipeline run to the next success.",
        "why_it_matters": "Long recovery times signal operational gaps and delayed downstream data availability.",
        "how_to_measure": "Measure time between a failure and the next successful run per pipeline.",
        "improvement_signal": "Reduce recovery time with alerting, retries, and operational playbooks.",
        "source_name": "system.lakeflow.job_run_timeline",
        "collection_method": "job_run_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 60.0,
        "partial_threshold": 180.0,
        "threshold_unit": "minutes",
    },
    {
        "metric_id": "WH-16",
        "dimension": "Operational Reliability",
        "metric_name": "orphaned_or_interrupted_run_count_30d",
        "description": "Count of interrupted or canceled pipeline runs in the last 30 days.",
        "why_it_matters": "Interrupted runs create data gaps and indicate unstable execution behavior.",
        "how_to_measure": "Count runs with canceled, timed out, or interrupted result states.",
        "improvement_signal": "Reduce interrupted runs through stability and capacity tuning.",
        "source_name": "system.lakeflow.job_run_timeline",
        "collection_method": "job_run_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 5.0,
        "partial_threshold": 15.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-17",
        "dimension": "Operational Reliability",
        "metric_name": "manual_rerun_count_30d",
        "description": "Count of pipeline runs triggered manually or rerun due to failure.",
        "why_it_matters": "Manual reruns represent hidden operational cost and fragile pipelines.",
        "how_to_measure": "Inspect run trigger metadata to identify manual or retry-triggered runs.",
        "improvement_signal": "Reduce manual reruns through reliability and automated recovery.",
        "source_name": "system.lakeflow.job_run_timeline",
        "collection_method": "job_run_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 10.0,
        "partial_threshold": 30.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-18",
        "dimension": "Performance and Efficiency",
        "metric_name": "shuffle_join_query_count_30d",
        "description": "Count of join queries using shuffle-based strategies.",
        "why_it_matters": "Shuffle joins are expensive and often indicate optimization opportunities.",
        "how_to_measure": "Use query plan text when available to detect shuffle-based join operators.",
        "improvement_signal": "Reduce shuffle joins with layout, pruning, or broadcast tuning.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 50.0,
        "partial_threshold": 150.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-19",
        "dimension": "Performance and Efficiency",
        "metric_name": "skewed_join_query_count_30d",
        "description": "Count of join queries exhibiting skew indicators.",
        "why_it_matters": "Skewed joins create long tails and inefficient resource usage.",
        "how_to_measure": "Use query plan text when available to identify skew join indicators.",
        "improvement_signal": "Address skewed joins with salting, repartitioning, or model changes.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 5.0,
        "partial_threshold": 20.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-20",
        "dimension": "Performance and Efficiency",
        "metric_name": "cartesian_join_query_count_30d",
        "description": "Count of queries using explicit cross or cartesian joins.",
        "why_it_matters": "Cartesian joins are high-risk for performance and correctness.",
        "how_to_measure": "Parse query history for CROSS JOIN or cartesian join indicators.",
        "improvement_signal": "Eliminate accidental cartesian joins with proper predicates.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 1.0,
        "partial_threshold": 5.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-21",
        "dimension": "Performance and Efficiency",
        "metric_name": "very_large_scan_query_count_30d",
        "description": "Count of queries scanning extremely large row or byte volumes.",
        "why_it_matters": "Very large scans are costly and often indicate inefficient access patterns.",
        "how_to_measure": "Use scan byte or row counters from query history when available.",
        "improvement_signal": "Reduce extreme scans through pruning, layout, and access pattern changes.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 10.0,
        "partial_threshold": 30.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "WH-22",
        "dimension": "Performance and Efficiency",
        "metric_name": "avg_large_table_file_size_mb",
        "description": "Average file size (MB) across large tables.",
        "why_it_matters": "Healthy average file sizes reduce metadata overhead and improve performance.",
        "how_to_measure": "Use DESCRIBE DETAIL size and file counts for large tables.",
        "improvement_signal": "Adjust write patterns and compaction to target healthier file sizes.",
        "source_name": "DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "high",
        "pass_threshold": 128.0,
        "partial_threshold": 64.0,
        "threshold_unit": "MB",
    },
    {
        "metric_id": "WH-23",
        "dimension": "Performance and Efficiency",
        "metric_name": "large_table_file_count_growth_pct_30d",
        "description": "Percent change in total file count for large tables versus the previous snapshot.",
        "why_it_matters": "Rapid file count growth signals fragmentation and rising maintenance needs.",
        "how_to_measure": "Compare total file counts across large tables between scorecard runs.",
        "improvement_signal": "Stabilize file growth with compaction and write pattern fixes.",
        "source_name": "DESCRIBE DETAIL + governance_maturity.warehouse_telemetry_metrics",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 20.0,
        "partial_threshold": 50.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "WH-24",
        "dimension": "Performance and Efficiency",
        "metric_name": "compaction_candidate_table_count",
        "description": "Count of large tables that likely need compaction based on file size and count.",
        "why_it_matters": "Compaction candidates represent immediate performance and cost remediation opportunities.",
        "how_to_measure": "Identify large tables with too-small files or very high file counts.",
        "improvement_signal": "Reduce compaction candidates with OPTIMIZE or write tuning.",
        "source_name": "DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 5.0,
        "partial_threshold": 20.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-01",
        "dimension": "Cost Control",
        "metric_name": "full_reload_workload_ratio_pct_30d",
        "description": "Percent of observed write workloads using rebuild-only patterns.",
        "why_it_matters": "Full reloads on large data sets drive higher compute and longer warehouse runtimes.",
        "how_to_measure": "Parse recent query history for CREATE OR REPLACE / REPLACE TABLE / TRUNCATE patterns.",
        "improvement_signal": "Replace rebuild-only logic with incremental MERGE or append patterns.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 20.0,
        "partial_threshold": 40.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "CC-02",
        "dimension": "Cost Control",
        "metric_name": "full_reload_workload_count_30d",
        "description": "Count of rebuild-only write workloads in the last 30 days.",
        "why_it_matters": "Frequent rebuilds increase warehouse cost and contention.",
        "how_to_measure": "Parse recent query history for CREATE OR REPLACE / REPLACE TABLE / TRUNCATE patterns.",
        "improvement_signal": "Reduce full reload frequency or migrate to incremental processing.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 25.0,
        "partial_threshold": 75.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-03",
        "dimension": "Cost Control",
        "metric_name": "select_star_query_count_30d",
        "description": "Count of queries using SELECT * patterns over the last 30 days.",
        "why_it_matters": "SELECT * on large tables increases scan volume and waste.",
        "how_to_measure": "Parse query history for SELECT * patterns.",
        "improvement_signal": "Replace SELECT * with projected columns needed by downstream workloads.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 50.0,
        "partial_threshold": 150.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-04",
        "dimension": "Cost Control",
        "metric_name": "long_running_query_count_30d",
        "description": "Count of queries exceeding the long-run threshold in the last 30 days.",
        "why_it_matters": "Long-running queries drive sustained compute utilization and cost spikes.",
        "how_to_measure": "Use query history total duration with a long-run threshold.",
        "improvement_signal": "Optimize or rewrite long-running queries and improve data layout.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 50.0,
        "partial_threshold": 150.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-05",
        "dimension": "Cost Control",
        "metric_name": "large_table_layout_strategy_coverage_pct",
        "description": "Alias for WH-03: percent of large tables with a layout strategy.",
        "why_it_matters": "Poor layout forces larger scans and higher compute cost.",
        "how_to_measure": "Reuse WH-03 metric from table profiling.",
        "improvement_signal": "Add partitioning or clustering for large tables.",
        "source_name": "alias",
        "collection_method": "alias_metric",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "high",
        "pass_threshold": 80.0,
        "partial_threshold": 50.0,
        "threshold_unit": "percent",
        "alias_of": "WH-03",
    },
    {
        "metric_id": "CC-06",
        "dimension": "Cost Control",
        "metric_name": "small_file_problem_table_count",
        "description": "Alias for WH-04: count of large tables with small-file issues.",
        "why_it_matters": "Small files increase planning overhead and I/O cost.",
        "how_to_measure": "Reuse WH-04 metric from table profiling.",
        "improvement_signal": "Compact files and adjust write patterns.",
        "source_name": "alias",
        "collection_method": "alias_metric",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 3.0,
        "partial_threshold": 10.0,
        "threshold_unit": "count",
        "alias_of": "WH-04",
    },
    {
        "metric_id": "CC-07",
        "dimension": "Cost Control",
        "metric_name": "oversized_file_problem_table_count",
        "description": "Alias for WH-05: count of tables with oversized file patterns.",
        "why_it_matters": "Oversized files reduce parallelism and increase runtime.",
        "how_to_measure": "Reuse WH-05 metric from table profiling.",
        "improvement_signal": "Adjust writer settings or partition strategy.",
        "source_name": "alias",
        "collection_method": "alias_metric",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 1.0,
        "partial_threshold": 5.0,
        "threshold_unit": "count",
        "alias_of": "WH-05",
    },
    {
        "metric_id": "CC-08",
        "dimension": "Cost Control",
        "metric_name": "large_join_query_count_30d",
        "description": "Alias for WH-07: count of long-running join queries.",
        "why_it_matters": "Large joins drive shuffle and compute cost.",
        "how_to_measure": "Reuse WH-07 metric from query history.",
        "improvement_signal": "Reduce large joins through layout and modeling.",
        "source_name": "alias",
        "collection_method": "alias_metric",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 25.0,
        "partial_threshold": 75.0,
        "threshold_unit": "count",
        "alias_of": "WH-07",
    },
    {
        "metric_id": "CC-09",
        "dimension": "Cost Control",
        "metric_name": "critical_pipeline_p95_runtime_seconds_30d",
        "description": "Alias for WH-11: p95 pipeline runtime.",
        "why_it_matters": "Long runtimes drive compute cost and cluster time.",
        "how_to_measure": "Reuse WH-11 metric from job run telemetry.",
        "improvement_signal": "Optimize or split long-running jobs.",
        "source_name": "alias",
        "collection_method": "alias_metric",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 900.0,
        "partial_threshold": 1800.0,
        "threshold_unit": "seconds",
        "alias_of": "WH-11",
    },
    {
        "metric_id": "CC-10",
        "dimension": "Cost Control",
        "metric_name": "large_table_count",
        "description": "Count of large tables in the warehouse.",
        "why_it_matters": "Large tables dominate compute and storage costs.",
        "how_to_measure": "Count tables with sizeInBytes above the large table threshold.",
        "improvement_signal": "Prioritize optimization and layout work on these tables.",
        "source_name": "DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 200.0,
        "partial_threshold": 400.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-11",
        "dimension": "Cost Control",
        "metric_name": "non_delta_table_count",
        "description": "Count of non-Delta tables in the warehouse.",
        "why_it_matters": "Non-Delta tables miss optimization features and can increase cost.",
        "how_to_measure": "Count tables whose format is not Delta.",
        "improvement_signal": "Migrate high-value non-Delta tables to Delta.",
        "source_name": "DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 10.0,
        "partial_threshold": 50.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-12",
        "dimension": "Cost Control",
        "metric_name": "large_tables_without_partition_count",
        "description": "Count of large tables without partition columns.",
        "why_it_matters": "Large unpartitioned tables drive expensive scans.",
        "how_to_measure": "Inspect partitionColumns in DESCRIBE DETAIL for large tables.",
        "improvement_signal": "Add partitioning or clustering to large tables.",
        "source_name": "DESCRIBE DETAIL",
        "collection_method": "table_profile_scan",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 20.0,
        "partial_threshold": 60.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-13",
        "dimension": "Cost Control",
        "metric_name": "large_tables_missing_optimize_count",
        "description": "Count of large Delta tables without recent OPTIMIZE usage.",
        "why_it_matters": "Large tables without OPTIMIZE accumulate file overhead and cost.",
        "how_to_measure": "Review DESCRIBE HISTORY for OPTIMIZE in the lookback window.",
        "improvement_signal": "Schedule OPTIMIZE for large Delta tables.",
        "source_name": "DESCRIBE HISTORY",
        "collection_method": "table_history_scan",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 20.0,
        "partial_threshold": 60.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-14",
        "dimension": "Cost Control",
        "metric_name": "p95_query_runtime_seconds_30d",
        "description": "P95 query runtime in seconds over the last 30 days.",
        "why_it_matters": "Longer queries drive higher compute utilization.",
        "how_to_measure": "Use query history duration statistics.",
        "improvement_signal": "Tune the slowest queries and data layout.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 60.0,
        "partial_threshold": 180.0,
        "threshold_unit": "seconds",
    },
    {
        "metric_id": "CC-15",
        "dimension": "Cost Control",
        "metric_name": "high_scan_bytes_query_count_30d",
        "description": "Count of queries that scanned above the large-scan threshold.",
        "why_it_matters": "Large scans are a direct driver of compute cost.",
        "how_to_measure": "Use query history scan bytes when available.",
        "improvement_signal": "Partition and optimize tables to reduce scan volume.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 50.0,
        "partial_threshold": 150.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-16",
        "dimension": "Cost Control",
        "metric_name": "high_scan_to_output_ratio_query_count_30d",
        "description": "Count of queries with scan-to-output ratios above the threshold.",
        "why_it_matters": "High scan-to-output ratios indicate wasted I/O.",
        "how_to_measure": "Use query history scan and output bytes when available.",
        "improvement_signal": "Reduce scans with pruning and column projection.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 30.0,
        "partial_threshold": 100.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-17",
        "dimension": "Cost Control",
        "metric_name": "spill_to_disk_query_count_30d",
        "description": "Count of queries with high spill-to-disk usage.",
        "why_it_matters": "Spill indicates memory pressure and expensive shuffles.",
        "how_to_measure": "Use spill bytes from query history when available.",
        "improvement_signal": "Reduce shuffle and memory pressure in heavy queries.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 10.0,
        "partial_threshold": 30.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-18",
        "dimension": "Cost Control",
        "metric_name": "high_shuffle_bytes_query_count_30d",
        "description": "Count of queries with high shuffle bytes.",
        "why_it_matters": "High shuffle volume is a key compute cost driver.",
        "how_to_measure": "Use shuffle bytes from query history when available.",
        "improvement_signal": "Reduce shuffle with better join strategies and layout.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_best_effort",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 10.0,
        "partial_threshold": 30.0,
        "threshold_unit": "count",
    },
    {
        "metric_id": "CC-19",
        "dimension": "Cost Control",
        "metric_name": "select_star_query_ratio_pct_30d",
        "description": "Percent of queries using SELECT * patterns.",
        "why_it_matters": "SELECT * increases scan volume and compute cost.",
        "how_to_measure": "Parse query history for SELECT * and divide by total queries.",
        "improvement_signal": "Use explicit column projection.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 5.0,
        "partial_threshold": 15.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "CC-20",
        "dimension": "Cost Control",
        "metric_name": "broadcast_join_ratio_pct_30d",
        "description": "Percent of join queries that used broadcast hints.",
        "why_it_matters": "Broadcast can reduce shuffle cost when used appropriately.",
        "how_to_measure": "Parse query history for broadcast hints and join patterns.",
        "improvement_signal": "Tune joins where broadcast is appropriate.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "high",
        "pass_threshold": 10.0,
        "partial_threshold": 3.0,
        "threshold_unit": "percent",
    },
    {
        "metric_id": "CC-21",
        "dimension": "Cost Control",
        "metric_name": "write_workload_count_30d",
        "description": "Count of write workloads observed in query history.",
        "why_it_matters": "Baseline workload volume helps normalize cost signals.",
        "how_to_measure": "Count write-pattern queries in query history.",
        "improvement_signal": "Use as a baseline for per-workload cost trends.",
        "source_name": "system.query.history",
        "collection_method": "query_history_sql",
        "implementation_status": "implemented_proxy",
        "enabled_for_scorecard": True,
        "v1_candidate": False,
        "direction": "low",
        "pass_threshold": 999999.0,
        "partial_threshold": 999999.0,
        "threshold_unit": "count",
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

    if metric["metric_id"] == "WH-22":
        large = large_tables()
        sizes = [profile["avg_file_size_bytes"] for profile in large if profile["avg_file_size_bytes"] is not None]
        if not sizes:
            return unknown_metric_row(metric, "No large table file sizes available", "DESCRIBE DETAIL inventory scan")
        avg_mb = sum(sizes) / len(sizes) / (1024 * 1024)
        notes = f"average across {len(sizes)} large tables"
        return metric_row(
            metric,
            metric_value_double=float(avg_mb),
            notes=notes,
            metric_sql="DESCRIBE DETAIL inventory scan",
            metric_json={"large_table_count": len(large), "observed_tables": len(sizes), "avg_file_size_mb": avg_mb},
        )

    if metric["metric_id"] == "WH-23":
        large = large_tables()
        file_counts = [profile["num_files"] for profile in large if profile["num_files"] is not None]
        if not file_counts:
            return unknown_metric_row(metric, "No large table file counts available", "DESCRIBE DETAIL inventory scan")
        current_total = float(sum(file_counts))
        previous_total = None
        previous_ts = None
        try:
            prev_row = (
                spark.table("governance_maturity.warehouse_telemetry_metrics")
                .filter((F.col("env") == ENV) & (F.col("check_id") == metric["metric_id"]) & (F.col("metric_json").isNotNull()))
                .orderBy(F.col("collected_at").desc())
                .limit(1)
                .collect()
            )
            if prev_row:
                previous_ts = prev_row[0]["collected_at"]
                try:
                    prev_json = json.loads(prev_row[0]["metric_json"] or "{}")
                    previous_total = prev_json.get("current_total_files")
                except Exception:
                    previous_total = None
        except Exception:
            previous_total = None
        if previous_total is None or float(previous_total) <= 0.0:
            return unknown_metric_row(
                metric,
                "Previous file count snapshot not available",
                "DESCRIBE DETAIL inventory scan",
                {"current_total_files": current_total},
            )
        growth_pct = 100.0 * (current_total - float(previous_total)) / float(previous_total)
        notes = f"current_files={int(current_total)}, previous_files={int(previous_total)}"
        if previous_ts is not None:
            notes = f"{notes}, previous_collected_at={previous_ts}"
        return metric_row(
            metric,
            metric_value_double=float(growth_pct),
            notes=notes,
            metric_sql="DESCRIBE DETAIL inventory scan",
            metric_json={"current_total_files": current_total, "previous_total_files": float(previous_total), "growth_pct": growth_pct},
        )

    if metric["metric_id"] == "WH-24":
        candidates = [
            profile
            for profile in large_tables()
            if (
                (profile["avg_file_size_bytes"] is not None and profile["avg_file_size_bytes"] < SMALL_FILE_AVG_BYTES)
                or (profile["num_files"] is not None and profile["num_files"] > COMPACTION_FILE_COUNT)
            )
        ]
        notes = f"{len(candidates)} large tables flagged for compaction based on file size or file count"
        return metric_row(
            metric,
            metric_value_double=float(len(candidates)),
            notes=notes,
            metric_sql="DESCRIBE DETAIL inventory scan",
            metric_json={"problem_tables": [profile["full_name"] for profile in candidates[:25]], "problem_table_count": len(candidates)},
        )

    if metric["metric_id"] == "CC-10":
        large = large_tables()
        return metric_row(
            metric,
            metric_value_double=float(len(large)),
            notes=f"{len(large)} large tables above {int(LARGE_TABLE_BYTES / (1024 * 1024 * 1024))} GB",
            metric_sql="DESCRIBE DETAIL inventory scan",
            metric_json={"large_table_count": len(large)},
        )

    if metric["metric_id"] == "CC-11":
        profiled = profiled_tables()
        non_delta = [profile for profile in profiled if profile["format"] != "delta"]
        return metric_row(
            metric,
            metric_value_double=float(len(non_delta)),
            notes=f"{len(non_delta)} of {len(profiled)} tables are non-Delta",
            metric_sql="DESCRIBE DETAIL inventory scan",
            metric_json={"non_delta_count": len(non_delta), "profiled_tables": len(profiled)},
        )

    if metric["metric_id"] == "CC-12":
        large = large_tables()
        without_partition = [profile for profile in large if profile["partition_column_count"] == 0]
        return metric_row(
            metric,
            metric_value_double=float(len(without_partition)),
            notes=f"{len(without_partition)} of {len(large)} large tables lack partition columns",
            metric_sql="DESCRIBE DETAIL inventory scan",
            metric_json={"large_tables": len(large), "without_partition": len(without_partition)},
        )

    if metric["metric_id"] == "CC-13":
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
        missing = max(observed - optimized, 0)
        if observed <= 0:
            return unknown_metric_row(metric, "DESCRIBE HISTORY unavailable for large Delta tables", "DESCRIBE HISTORY scan", {"candidate_tables": len(candidates), "history_unavailable": unavailable})
        notes = f"{missing} of {observed} large Delta tables missing OPTIMIZE; history unavailable for {unavailable}"
        return metric_row(
            metric,
            metric_value_double=float(missing),
            notes=notes,
            metric_sql="DESCRIBE HISTORY scan",
            metric_json={"candidate_tables": len(candidates), "observed_tables": observed, "optimized_tables": optimized, "missing_optimize": missing, "history_unavailable": unavailable},
        )

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
    plan_col = choose_column(table_name, ["query_plan", "plan", "spark_plan", "physical_plan", "executed_plan", "logical_plan"])
    plan_expr = f"UPPER(COALESCE({plan_col}, ''))" if plan_col is not None else None
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
    select_star_predicate = f"({text_expr} LIKE '%SELECT *%')"
    shuffle_join_predicate = None
    skew_join_predicate = None
    if plan_expr is not None:
        shuffle_join_predicate = f"({plan_expr} LIKE '%SORTMERGEJOIN%' OR {plan_expr} LIKE '%SHUFFLEDHASHJOIN%' OR ({plan_expr} LIKE '%SHUFFLE%' AND {plan_expr} LIKE '%JOIN%'))"
        skew_join_predicate = f"({plan_expr} LIKE '%SKEW%')"
    cartesian_predicate = f"({text_expr} LIKE '%CROSS JOIN%' OR {text_expr} LIKE '%CARTESIAN%')"
    if plan_expr is not None:
        cartesian_predicate = f"({cartesian_predicate} OR {plan_expr} LIKE '%CARTESIAN%')"
    query_count_sql = f"""
        SELECT CAST(COUNT(*) AS DOUBLE) AS metric_value
        FROM {table_name}
        WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
    """
    scan_bytes_col = choose_column(table_name, ["total_scan_bytes", "total_bytes", "read_bytes", "total_read_bytes", "bytes_read"])
    scan_rows_col = choose_column(table_name, ["total_scan_rows", "rows_read", "read_rows", "input_rows", "total_read_rows"])
    output_bytes_col = choose_column(table_name, ["output_bytes", "result_bytes", "bytes_written"])
    shuffle_bytes_col = choose_column(table_name, ["shuffle_bytes", "shuffle_read_bytes", "shuffle_write_bytes"])
    spill_bytes_col = choose_column(table_name, ["spill_bytes", "spill_to_disk_bytes", "spilled_bytes"])

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
    elif metric["metric_id"] == "WH-18":
        if shuffle_join_predicate is None:
            return unknown_metric_row(metric, "system.query.history is missing a plan column for shuffle join detection", query_count_sql, {"table_name": table_name})
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value,
              COUNT(*) AS shuffle_join_queries
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {shuffle_join_predicate}
        """
    elif metric["metric_id"] == "WH-19":
        if skew_join_predicate is None:
            return unknown_metric_row(metric, "system.query.history is missing a plan column for skew join detection", query_count_sql, {"table_name": table_name})
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value,
              COUNT(*) AS skew_join_queries
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {skew_join_predicate}
        """
    elif metric["metric_id"] == "WH-20":
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value,
              COUNT(*) AS cartesian_join_queries
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {cartesian_predicate}
        """
    elif metric["metric_id"] == "WH-21":
        if scan_rows_col is None and scan_bytes_col is None:
            return unknown_metric_row(metric, "system.query.history is missing scan row/byte columns", query_count_sql, {"table_name": table_name})
        if scan_rows_col is not None:
            sql_text = f"""
                SELECT
                  CAST(COUNT(*) AS DOUBLE) AS metric_value
                FROM {table_name}
                WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
                  AND {scan_rows_col} >= {VERY_LARGE_SCAN_ROWS}
            """
        else:
            sql_text = f"""
                SELECT
                  CAST(COUNT(*) AS DOUBLE) AS metric_value
                FROM {table_name}
                WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
                  AND {scan_bytes_col} >= {VERY_LARGE_SCAN_BYTES}
            """
    elif metric["metric_id"] == "CC-01":
        sql_text = f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN NULL
                   ELSE 100.0 * SUM(CASE WHEN {rebuild_predicate} THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS relevant_workloads,
              SUM(CASE WHEN {rebuild_predicate} THEN 1 ELSE 0 END) AS rebuild_workloads
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {write_predicate}
        """
    elif metric["metric_id"] == "CC-02":
        sql_text = f"""
            SELECT
              CAST(SUM(CASE WHEN {rebuild_predicate} THEN 1 ELSE 0 END) AS DOUBLE) AS metric_value,
              COUNT(*) AS relevant_workloads,
              SUM(CASE WHEN {rebuild_predicate} THEN 1 ELSE 0 END) AS rebuild_workloads
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {write_predicate}
        """
    elif metric["metric_id"] == "CC-03":
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {select_star_predicate}
        """
    elif metric["metric_id"] == "CC-04":
        duration_predicate = "TRUE" if duration_col is None else f"{duration_col} >= {LONG_QUERY_RUNTIME_MS}"
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {duration_predicate}
        """
    elif metric["metric_id"] == "CC-14":
        if duration_col is None:
            return unknown_metric_row(metric, "system.query.history is missing duration column", query_count_sql, {"table_name": table_name})
        sql_text = f"""
            SELECT
              percentile_approx({duration_col} / 1000.0, 0.95) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {duration_col} IS NOT NULL
        """
    elif metric["metric_id"] == "CC-15":
        if scan_bytes_col is None:
            return unknown_metric_row(metric, "system.query.history is missing scan-bytes column", query_count_sql, {"table_name": table_name})
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {scan_bytes_col} >= {LARGE_SCAN_BYTES}
        """
    elif metric["metric_id"] == "CC-16":
        if scan_bytes_col is None or output_bytes_col is None:
            return unknown_metric_row(metric, "system.query.history is missing scan/output byte columns", query_count_sql, {"table_name": table_name})
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {output_bytes_col} > 0
              AND ({scan_bytes_col} / {output_bytes_col}) >= {HIGH_SCAN_TO_OUTPUT_RATIO}
        """
    elif metric["metric_id"] == "CC-17":
        if spill_bytes_col is None:
            return unknown_metric_row(metric, "system.query.history is missing spill-bytes column", query_count_sql, {"table_name": table_name})
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {spill_bytes_col} >= {HIGH_SPILL_BYTES}
        """
    elif metric["metric_id"] == "CC-18":
        if shuffle_bytes_col is None:
            return unknown_metric_row(metric, "system.query.history is missing shuffle-bytes column", query_count_sql, {"table_name": table_name})
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {shuffle_bytes_col} >= {HIGH_SHUFFLE_BYTES}
        """
    elif metric["metric_id"] == "CC-19":
        sql_text = f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN NULL
                   ELSE 100.0 * SUM(CASE WHEN {select_star_predicate} THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS total_queries,
              SUM(CASE WHEN {select_star_predicate} THEN 1 ELSE 0 END) AS select_star_queries
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
        """
    elif metric["metric_id"] == "CC-20":
        sql_text = f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN NULL
                   ELSE 100.0 * SUM(CASE WHEN {broadcast_predicate} THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS total_queries,
              SUM(CASE WHEN {broadcast_predicate} THEN 1 ELSE 0 END) AS broadcast_queries
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {join_predicate}
        """
    elif metric["metric_id"] == "CC-21":
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value,
              COUNT(*) AS write_workloads
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
    attempt_col = choose_column(table_name, ["run_attempt", "attempt_number", "attempt", "run_attempt_number"])
    trigger_col = choose_column(table_name, ["trigger_type", "run_trigger", "run_source", "run_type", "trigger"])
    pipeline_col = choose_column(table_name, ["pipeline_id", "job_id", "job_definition_id", "job_run_id", "run_id"])
    if start_col is None:
        return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is unavailable or missing start timestamp")

    runtime_expr = None
    if end_col is not None:
        runtime_expr = f"CAST(unix_timestamp(COALESCE({end_col}, current_timestamp())) - unix_timestamp({start_col}) AS DOUBLE)"

    success_predicate = None
    failure_predicate = None
    interrupted_predicate = None
    if state_col is not None:
        success_predicate = f"UPPER(COALESCE({state_col}, '')) IN ('SUCCESS', 'SUCCEEDED')"
        failure_predicate = f"UPPER(COALESCE({state_col}, '')) IN ('FAILED', 'FAILURE', 'ERROR')"
        interrupted_predicate = f"UPPER(COALESCE({state_col}, '')) IN ('CANCELED', 'CANCELLED', 'TIMEDOUT', 'TIMEOUT', 'TERMINATED', 'SKIPPED', 'INTERNAL_ERROR', 'BLOCKED')"

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
                   ELSE 100.0 * SUM(CASE WHEN {success_predicate} THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS run_count,
              SUM(CASE WHEN {success_predicate} THEN 1 ELSE 0 END) AS success_count
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
        """
    elif metric["metric_id"] == "WH-13":
        if attempt_col is None:
            return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is missing attempt metadata", f"SELECT COUNT(*) FROM {table_name}")
        sql_text = f"""
            SELECT
              CASE WHEN COUNT(*) = 0 THEN NULL
                   ELSE 100.0 * SUM(CASE WHEN CAST({attempt_col} AS INT) > 1 THEN 1 ELSE 0 END) / COUNT(*)
              END AS metric_value,
              COUNT(*) AS run_count,
              SUM(CASE WHEN CAST({attempt_col} AS INT) > 1 THEN 1 ELSE 0 END) AS retry_run_count
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
        """
    elif metric["metric_id"] == "WH-14":
        if state_col is None:
            return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is missing result state columns")
        if pipeline_col is None:
            sql_text = f"""
                SELECT
                  CASE WHEN COUNT(*) = 0 THEN NULL
                       ELSE 100.0 * SUM(CASE WHEN {failure_predicate} THEN 1 ELSE 0 END) / COUNT(*)
                  END AS metric_value,
                  COUNT(*) AS run_count,
                  SUM(CASE WHEN {failure_predicate} THEN 1 ELSE 0 END) AS failure_count
                FROM {table_name}
                WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
            """
        else:
            sql_text = f"""
                WITH pipeline_runs AS (
                  SELECT
                    {pipeline_col} AS pipeline_id,
                    COUNT(*) AS run_count,
                    SUM(CASE WHEN {failure_predicate} THEN 1 ELSE 0 END) AS failure_count
                  FROM {table_name}
                  WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
                  GROUP BY {pipeline_col}
                )
                SELECT
                  CASE WHEN COUNT(*) = 0 THEN NULL
                       ELSE 100.0 * AVG(CASE WHEN run_count = 0 THEN 0.0 ELSE failure_count / run_count END)
                  END AS metric_value,
                  COUNT(*) AS pipeline_count
                FROM pipeline_runs
            """
    elif metric["metric_id"] == "WH-15":
        if state_col is None or pipeline_col is None or end_col is None:
            return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is missing pipeline, state, or end timestamp metadata")
        sql_text = f"""
            WITH base AS (
              SELECT
                {pipeline_col} AS pipeline_id,
                {start_col} AS start_time,
                {end_col} AS end_time,
                UPPER(COALESCE({state_col}, '')) AS result_state
              FROM {table_name}
              WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
            ),
            ordered AS (
              SELECT
                *,
                MIN(CASE WHEN result_state IN ('SUCCESS', 'SUCCEEDED') THEN start_time END)
                  OVER (PARTITION BY pipeline_id ORDER BY start_time ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS next_success_start
              FROM base
            )
            SELECT
              AVG(CASE WHEN result_state IN ('FAILED', 'FAILURE', 'ERROR') AND next_success_start IS NOT NULL
                       THEN (unix_timestamp(next_success_start) - unix_timestamp(end_time)) / 60.0
                  END) AS metric_value,
              COUNT(*) AS observed_failures
            FROM ordered
            WHERE result_state IN ('FAILED', 'FAILURE', 'ERROR')
        """
    elif metric["metric_id"] == "WH-16":
        if state_col is None:
            return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is missing result state columns")
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND {interrupted_predicate}
        """
    elif metric["metric_id"] == "WH-17":
        if trigger_col is None:
            return unknown_metric_row(metric, "system.lakeflow.job_run_timeline is missing trigger metadata")
        sql_text = f"""
            SELECT
              CAST(COUNT(*) AS DOUBLE) AS metric_value
            FROM {table_name}
            WHERE {start_col} >= current_timestamp() - INTERVAL 30 DAYS
              AND UPPER(COALESCE({trigger_col}, '')) IN ('MANUAL', 'RETRY', 'RERUN', 'RESTARTED', 'RERUN_FROM_FAILURE', 'RUN_NOW', 'ADHOC')
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
row_by_id = {}
alias_metrics = []
for metric in metric_catalog:
    if metric.get("collection_method") == "alias_metric":
        alias_metrics.append(metric)
        continue
    try:
        if metric["collection_method"] in ("table_profile_scan", "table_history_scan"):
            row = collect_table_profile_metric(metric)
        elif metric["collection_method"] == "query_history_sql":
            row = collect_query_history_metric(metric)
        elif metric["collection_method"] == "job_run_sql":
            row = collect_job_metric(metric)
        else:
            row = unknown_metric_row(metric, f"Unsupported collection method: {metric['collection_method']}")
    except Exception as exc:
        row = unknown_metric_row(metric, str(exc), metric_json={"metric_id": metric["metric_id"]})
    rows.append(row)
    row_by_id[metric["metric_id"]] = row

for metric in alias_metrics:
    alias_of = metric.get("alias_of")
    base_row = row_by_id.get(alias_of)
    if not base_row:
        rows.append(unknown_metric_row(metric, f"Alias source not available: {alias_of}", metric_json={"alias_of": alias_of}))
        continue
    rows.append(
        metric_row(
            metric,
            metric_value_double=base_row[9],
            metric_value_string=base_row[10],
            status_hint=base_row[11],
            notes=f"alias_of={alias_of}; {base_row[12] or 'no base notes'}",
            metric_sql=None,
            metric_json={"alias_of": alias_of, "base_metric": alias_of},
        )
    )

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

