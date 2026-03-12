# Cost Control Metrics (Warehouse)

This list flags metrics from `docs/metric_areas.md` that are most likely to
reduce compute cost when improved. The focus is on reducing wasted scans,
shuffles, retries, and unnecessary full reload work.

## Highest-Impact Cost Levers

- Percent of pipelines using CREATE OR REPLACE vs incremental MERGE.
- Number of full reload pipelines vs incremental pipelines.
- Pipelines using full reloads on high-volume data.
- Pipelines with repeated full table scans.
- Percent of large tables partitioned appropriately.
- Percent of tables using liquid clustering or clustering strategy, if applicable.
- Tables with too many small files.
- Tables needing compaction.
- Tables with oversized files.
- Queries with excessive shuffle volume.
- Queries with skewed joins.
- Queries with high spill to disk.
- Queries with high scan-to-output ratio.
- Queries scanning very large row/file volumes.
- Long-running queries over threshold.
- Median and p95 runtime by pipeline.
- Number of large joins by workload.

## Secondary Cost Levers

- Tables with OPTIMIZE usage.
- Tables with auto optimize / optimized writes enabled.
- Tables with ZORDER usage, if used.
- Tables with skew between partition sizes.
- Queries with repeated SELECT * on large tables.
- Queries with repeated joins on non-selective keys.
- Pipelines with checkpoint or watermark logic where needed.
- Pipelines with retry logic (reduces rerun cost when tuned).
- Retry rate.
- Failure rate by pipeline.
- Average recovery time after failure.
- Number of manual reruns required.

## Lower Cost Signal (Indirect)

- Percent of curated tables stored as Delta.
- Percent of tables with partition columns.
- Number of streaming tables or Auto Loader pipelines.
- Number of auto-trigger or scheduled incremental loads.

## Notes

- The “Highest-Impact” group should be the first wave of cost-control metrics.
- The “Secondary” group is still valuable but often requires more context to
  interpret.
- The “Lower Cost Signal” group is useful for maturity tracking but usually
  doesn’t drive immediate cost reductions on its own.
