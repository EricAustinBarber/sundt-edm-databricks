# Propose Metric Set

##  Core Table Design

  - Percent of curated tables stored as Delta.
  - Percent of pipelines using CREATE OR REPLACE vs incremental MERGE.
  - Percent of tables with primary business keys defined in logic.
  - Percent of tables with partition columns.
  - Percent of large tables partitioned appropriately.
  - Percent of tables using liquid clustering or clustering strategy, if applicable.
  - Percent of tables with comments, owners, and tags.

##  Ingestion and Load Patterns

  - Number of auto-trigger or scheduled incremental loads.
  - Number of full reload pipelines vs incremental pipelines.
  - Number of loads using MERGE INTO.
  - Number of loads using append-only pattern.
  - Number of loads truncating/rebuilding targets.
  - Number of streaming tables or Auto Loader pipelines.
  - Pipelines with checkpointing enabled.
  - Pipelines with schema evolution handling.

##  Delta Optimization Usage
 
  - Tables with OPTIMIZE usage.
  - Tables with VACUUM usage.
  - Tables with auto optimize / optimized writes enabled.
  - Tables with ZORDER usage, if used.
  - Tables with change data feed enabled where it makes sense.
  - Tables with time travel retention aligned to policy.
 
##  File Health
 
  - Tables with too many small files.
  - Tables with oversized files.
  - Average file size by table.
  - File count growth trend by table.
  - Tables needing compaction.
  - Tables with skew between partition sizes.
 
##  Join and Query Characteristics
 
  - Number of large joins by workload.
  - Number of broadcast joins.
  - Number of shuffle joins.
  - Queries with excessive shuffle volume.
  - Queries with skewed joins.
  - Queries with cartesian or accidental cross joins.
  - Repeated joins on non-selective keys.
  - Queries with high spill to disk.
  - Queries with high scan-to-output ratio.
 
##  Performance Patterns
 
  - Median and p95 runtime by pipeline.
  - Median and p95 runtime by major table build.
  - Long-running queries over threshold.
  - Queries scanning very large row/file volumes.
  - Pipelines with repeated full table scans.
  - Pipelines not benefiting from partition pruning.
  - Pipelines not benefiting from data skipping.
  - Queries with repeated SELECT * on large tables.

##  Pipeline Engineering Maturity

  - Pipelines parameterized by environment.
  - Pipelines with retry logic.
  - Pipelines with idempotent write logic.
  - Pipelines with checkpoint or watermark logic where needed.
  - Pipelines with explicit error handling.
  - Pipelines with test/validation steps.
  - Pipelines with source-to-target row count reconciliation.
  - Pipelines with SLA metadata.

##  Operational Reliability
 
  - Pipeline success rate.
  - Retry rate.
  - Failure rate by pipeline.
  - Average recovery time after failure.
  - Number of orphaned/interrupted runs.
  - Number of manual reruns required.
  - Number of jobs with alerting configured.
 
##  Utilization / Adoption

  - Number of active tables by domain.
  - Number of active pipelines by domain.
  - Number of distinct users querying the warehouse.
  - Number of scheduled jobs hitting the warehouse.
  - Number of BI/reporting workloads using the warehouse.
  - Number of service principals or applications using it.

##  Improvement-Focused Anti-Pattern Metrics
 
  - Tables built with CREATE OR REPLACE when they should be incremental.
  - Large tables without partitioning/clustering.
  - Large tables with small-file problems.
  - Pipelines using full reloads on high-volume data.
  - Queries with repeated large shuffle joins.
  - Pipelines lacking OPTIMIZE/compaction hygiene.
  - Broadcast opportunities missed.
  - Tables with poor documentation or ownership metadata.