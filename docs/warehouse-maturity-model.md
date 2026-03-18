# Databricks Warehouse Maturity Model

## Status

This is a living design document for the Databricks warehouse maturity
scorecard. Update it whenever the scorecard definition, telemetry sources,
thresholds, or scoring approach changes.

Current phase:

- Databricks-only maturity model
- Telemetry-backed scoring
- Initial warehouse engineering metrics backlog defined
- Proposed v1 metric set identified for first implementation pass

## Purpose

Define what "maturity" means for the Databricks warehouse in this repository and
document how we will evolve the scorecard over time.

This document is intentionally broader than the executable scorecard definition.
It captures:

- Why the scorecard exists
- What we are trying to measure
- What is currently implemented
- What we want to measure next
- What assumptions and gaps remain

## Goals

- Measure warehouse maturity from Databricks-native telemetry instead of
  external tools or manual evidence.
- Track platform adoption, engineering quality, performance behavior, and
  operational reliability.
- Make the scorecard repeatable and environment-specific.
- Support CI/CD review and release readiness decisions with objective evidence.
- Surface specific pipeline improvement opportunities, not just a single score.

## Non-Goals

- This scorecard does not currently measure Bigeye or Alation adoption.
- This scorecard is not a broad enterprise data maturity model.
- This scorecard is not yet a direct chargeback or cost allocation model.
- This scorecard does not replace deeper architecture or code reviews.

## What Maturity Means Here

For this repository, a mature Databricks warehouse environment demonstrates:

- Strong adoption by real workloads and users
- Meaningful ongoing utilization
- Reliable query and job execution
- Efficient use of Delta and warehouse optimization features
- Governed warehouse assets with managed and documented objects
- Pipeline patterns that favor incremental, maintainable, and performant data
  movement

## Current Dimensions

### Adoption

- Are teams, users, and workloads using the warehouse regularly?
- Is the warehouse active enough to be considered part of normal operations?

### Utilization

- Is the warehouse handling enough successful activity to represent a meaningful
  shared platform?

### Reliability

- Are jobs and queries succeeding consistently enough to trust production use?

### Performance

- Are workloads efficient in runtime, wait time, file layout, and join strategy?

### Governance

- Are tables managed, documented, and structured in a way that supports stable
  enterprise operations?

### Pipeline Engineering

- Are data pipelines using mature patterns such as incremental loads, merge
  logic, partitioning, and operational safeguards?

## Management Rollup Areas

These rollup areas are intended for manager and leadership review. They provide
an understandable summary layer above the detailed engineering metrics.

| Rollup Area | What It Means | Manager Question It Answers |
|---|---|---|
| Adoption and Business Use | Whether the warehouse is actually being used by teams, workloads, and reporting consumers. | Are we getting meaningful adoption from the platform? |
| Pipeline Engineering Quality | Whether pipelines are built with scalable, maintainable, and incremental patterns. | Are we building pipelines the right way or creating future rework? |
| Performance and Efficiency | Whether data layout, query behavior, and execution patterns are efficient. | Are we using the warehouse efficiently, or are we creating avoidable cost and latency? |
| Operational Reliability | Whether the warehouse and pipelines run successfully and recover cleanly. | Can the business rely on this platform to run consistently? |
| Data Asset Governance | Whether tables are managed, documented, and stewarded in a controlled way. | Are warehouse assets governed well enough for enterprise use? |
| Platform Feature Utilization | Whether we are actually using core Databricks and Delta capabilities. | Are we taking advantage of the platform we are paying for? |

## Rollup Mapping

This mapping shows how the detailed backlog ladders into manager-readable
rollups.

| Rollup Area | Included Detailed Measures |
|---|---|
| Adoption and Business Use | Active query days, distinct query users, active tables by domain, active pipelines by domain, scheduled jobs hitting the warehouse, BI/reporting workloads, service principals or applications using the warehouse |
| Pipeline Engineering Quality | `MERGE` vs rebuild patterns, append-only vs truncate/rebuild behavior, incremental load coverage, auto-trigger loads, streaming or Auto Loader usage, checkpointing, schema evolution handling, idempotent writes, retry logic, environment parameterization, test and validation steps, reconciliation logic, SLA metadata |
| Performance and Efficiency | P95 runtime, wait-for-compute time, large joins, shuffle joins, broadcast joins, excessive shuffle volume, skewed joins, scan-to-output ratio, partition pruning effectiveness, data skipping effectiveness, `SELECT *` overuse, small-file issues, oversized-file issues, compaction candidates, file growth trends, partition skew |
| Operational Reliability | Pipeline success rate, retry rate, failure rate by pipeline, recovery time after failure, orphaned or interrupted runs, manual reruns, jobs with alerting configured |
| Data Asset Governance | Managed table coverage, comments and ownership metadata, tags, time-travel retention alignment, owner coverage, documentation gaps, ownerless tables, critical asset stewardship coverage |
| Platform Feature Utilization | Delta table coverage, `MERGE INTO` usage, partitioning coverage, clustering usage, `OPTIMIZE`, `VACUUM`, auto optimize, optimized writes, ZORDER, change data feed, streaming features, Unity Catalog metadata usage |

## Current Implemented Metrics

The authoritative, deployed scorecard definition is maintained in
`docs/databricks-environment-scorecard.md`.

As of the current scorecard definition, the implementation includes:

- `WH-01` through `WH-24` (Platform Feature Utilization, Performance and Efficiency, Operational Reliability)
- `CC-01` through `CC-21` (Cost Control)

Use the scorecard definition document for exact check names, thresholds, and
weights.

## Current Scoring Model

- `Pass` = `1.0`
- `Partial` = `0.5`
- `Fail` = `0.0`
- `Unknown` = `0.0`

Weighted scoring is defined in the executable scorecard and currently sums to
`100`.

Current blocking rules:

- Any `Fail` in `Governance`
- Any `Fail` in `Reliability`

Current warning rule:

- Total score below `75`

## Data Sources

Current implemented sources:

- `system.query.history`
- `system.information_schema.tables`
- `governance_maturity.warehouse_telemetry_metrics`
- `governance_maturity.scorecard_check_status`
- `governance_maturity.scorecard_results`
- `governance_maturity.scorecard_check_results`
- `governance_maturity.bundle_deployments`

Planned sources:

- `system.billing.usage`
- `system.compute.warehouse_events`
- Unity Catalog ownership and tag metadata
- Databricks job and pipeline metadata
- Delta transaction history where accessible
- Query plans or execution details where accessible

## Metrics Backlog

This backlog is the working list of candidate maturity metrics. It is larger
than the first scorecard release by design.

| Metric Name | Why It Matters | How To Measure | Improvement Signal |
|---|---|---|---|
| Percent of curated tables using Delta | Demonstrates whether the warehouse is using Databricks-native storage and optimization features consistently. | Count curated tables and divide Delta-format tables by total curated tables. | Increase toward full Delta coverage in curated layers. |
| Percent of pipelines using `CREATE OR REPLACE` vs incremental `MERGE` | Shows whether pipelines are using rebuild patterns instead of maintainable incremental logic. | Parse pipeline SQL or notebook logic and classify write pattern by target table. | Reduce full rebuild patterns and increase incremental merge usage where appropriate. |
| Percent of tables with primary business keys defined in logic | Indicates whether data models support deterministic matching and incremental maintenance. | Detect merge keys, join keys, or declared business keys in transformation logic and metadata. | Increase explicit key usage for curated and incremental tables. |
| Percent of tables with partition columns | Shows whether physical table design is taking advantage of partition-aware access patterns. | Inspect table metadata for partition columns across warehouse tables. | Increase partition coverage for large and time-series tables. |
| Percent of large tables partitioned appropriately | Moves beyond presence of partitions to whether partitioning is applied where it matters. | Compare table size thresholds against partition metadata and expected access pattern. | Reduce large unpartitioned tables or poorly partitioned large tables. |
| Percent of tables using liquid clustering or clustering strategy | Demonstrates use of newer layout strategies where partitioning alone is not enough. | Inspect table properties and DDL history for liquid clustering or clustering definitions. | Increase clustering coverage for large or high-selectivity access tables. |
| Percent of tables with comments, owners, and tags | Indicates basic stewardship and usability of warehouse assets. | Inspect Unity Catalog metadata for comments, owner values, and tags. | Increase metadata completeness and stewardship coverage. |
| Number of auto-trigger or scheduled incremental loads | Shows whether the platform is operating as a reliable warehouse rather than ad hoc notebooks. | Count production jobs/pipelines with recurring schedules or triggered incremental loads. | Increase repeatable scheduled ingestion and reduce manual-only refreshes. |
| Number of full reload pipelines vs incremental pipelines | Highlights operational inefficiency and unnecessary compute consumption. | Classify pipeline write behavior from SQL and notebook logic. | Shift high-volume pipelines toward incremental patterns. |
| Number of loads using `MERGE INTO` | Measures adoption of Delta-native upsert logic. | Parse workload logic and count `MERGE INTO` usage by pipeline or target table. | Increase merge-based ingestion where source changes are incremental. |
| Number of loads using append-only pattern | Helps differentiate append pipelines from upsert and rebuild pipelines. | Parse target write logic and classify append-only patterns. | Use append-only where data shape supports it; do not overuse rebuilds. |
| Number of loads truncating or rebuilding targets | Flags pipelines that may be expensive, brittle, or hard to scale. | Detect `TRUNCATE`, delete-all, overwrite, or replace patterns in pipeline code. | Reduce rebuild-heavy pipelines for large or frequent loads. |
| Number of streaming tables or Auto Loader pipelines | Shows adoption of Databricks-native ingestion patterns for continuous data movement. | Count streaming jobs, Auto Loader usage, or streaming table definitions. | Increase use where near-real-time ingestion is a business requirement. |
| Pipelines with checkpointing enabled | Indicates resilience and restartability in streaming or incremental patterns. | Inspect streaming pipeline configs and notebook code for checkpoint locations. | Increase checkpoint coverage for stateful ingestion paths. |
| Pipelines with schema evolution handling | Demonstrates whether ingestion can tolerate expected source drift. | Detect schema evolution options, rescue columns, or explicit evolution handling in jobs. | Increase managed schema drift handling for volatile sources. |
| Tables with `OPTIMIZE` usage | Measures whether large Delta tables are being actively maintained. | Inspect job code, SQL history, or table maintenance patterns for `OPTIMIZE`. | Increase optimization coverage for large and frequently queried tables. |
| Tables with `VACUUM` usage | Measures storage hygiene and retention maintenance. | Detect `VACUUM` execution or scheduled maintenance jobs by table. | Increase vacuum coverage aligned to retention policy. |
| Tables with auto optimize or optimized writes enabled | Shows proactive use of Databricks write optimizations. | Inspect table properties or session configs used by write jobs. | Increase automatic write optimization for heavy ingestion tables. |
| Tables with ZORDER usage | Demonstrates additional layout optimization for selective access patterns. | Inspect maintenance SQL history for `OPTIMIZE ... ZORDER BY`. | Increase targeted use for high-value query patterns. |
| Tables with change data feed enabled where appropriate | Indicates readiness for downstream incremental consumption. | Inspect Delta table properties for CDF enablement on change-heavy curated tables. | Enable CDF where downstream consumers need incremental change access. |
| Tables with time travel retention aligned to policy | Reflects governance and recovery posture. | Compare table retention properties to platform policy expectations. | Reduce inconsistent retention settings across managed tables. |
| Tables with too many small files | Small files degrade read performance and operational efficiency. | Analyze file counts and average file sizes per table against thresholds. | Reduce small-file tables through compaction and ingestion tuning. |
| Tables with oversized files | Oversized files can reduce parallelism and hurt performance. | Analyze average and max file sizes by table against thresholds. | Reduce oversized files through optimized write and compaction strategy. |
| Average file size by table | Provides a direct physical health metric for Delta layout. | Use file metadata from table detail or Delta history to calculate file size distribution. | Move file size distribution toward target range. |
| File count growth trend by table | Detects fragmentation before performance degrades significantly. | Track daily file counts and growth rate by table. | Flatten excessive file-count growth through maintenance and ingestion changes. |
| Tables needing compaction | Highlights tables with immediate physical optimization needs. | Combine small-file thresholds, file count growth, and recent optimize history. | Shrink the backlog of compaction candidates. |
| Tables with skew between partition sizes | Indicates partition strategy problems that hurt performance and manageability. | Compare row count or storage distribution across partitions. | Reduce skewed partition distributions for large tables. |
| Number of large joins by workload | Large joins drive cost and performance problems when not well designed. | Analyze query history and plans for joins above a row or byte threshold. | Reduce unnecessary large joins or optimize the highest-cost ones. |
| Number of broadcast joins | Shows whether the engine is able to exploit smaller dimension-side joins efficiently. | Use query plan analysis or SQL execution details to count broadcast joins. | Increase intentional broadcast join usage where dimension tables are small enough. |
| Number of shuffle joins | Highlights join patterns that require expensive redistribution. | Analyze query plans for shuffle-based join strategies. | Reduce avoidable shuffle joins through data model or query changes. |
| Queries with excessive shuffle volume | Identifies expensive distributed execution patterns. | Measure shuffle bytes or shuffle read/write metrics from query execution telemetry. | Reduce high-shuffle workloads through partitioning, filtering, or join changes. |
| Queries with skewed joins | Detects data-distribution problems that create long tails and task imbalance. | Use execution metrics or skew indicators from query plans. | Reduce skewed joins through salting, repartitioning, or model changes. |
| Queries with cartesian or accidental cross joins | Flags correctness and performance anti-patterns. | Parse logical plans or query history for cross join behavior without selective predicates. | Eliminate accidental cross joins entirely. |
| Repeated joins on non-selective keys | Indicates weak data model design or expensive repetitive query patterns. | Analyze join keys and cardinality/selectivity from plans and SQL patterns. | Shift joins toward selective keys or remodel dimensions/facts. |
| Queries with high spill to disk | Spill is a strong indicator of memory pressure and inefficient execution. | Use execution metrics for spill bytes or spill events by query. | Reduce spilling through join tuning, partitioning, and warehouse sizing. |
| Queries with high scan-to-output ratio | Highlights wasteful scans and poor filtering. | Compare bytes or rows scanned versus rows returned. | Reduce inefficient scans through pruning, filtering, and model changes. |
| Median and p95 runtime by pipeline | Provides a stable operational performance signal at the pipeline level. | Track job duration distribution by pipeline over rolling windows. | Lower p95 runtime and reduce variance on critical jobs. |
| Median and p95 runtime by major table build | Surfaces which table builds are bottlenecks. | Track runtime by target table or major transformation unit. | Improve the slowest table builds first. |
| Long-running queries over threshold | Identifies slow workloads that warrant investigation. | Count queries exceeding defined runtime thresholds. | Reduce the count of long-running queries. |
| Queries scanning very large row or file volumes | Highlights workloads that may need architectural review. | Use query history scan metrics where available. | Reduce extreme scan-heavy workloads or isolate them intentionally. |
| Pipelines with repeated full table scans | Indicates poor incremental strategy or missing pruning opportunities. | Inspect plans or execution metrics for repeated full scans of large tables. | Reduce full-scan dependence for routine transformations. |
| Pipelines not benefiting from partition pruning | Shows where partitioning exists but is not helping. | Compare query predicates to partition columns and scan behavior. | Increase effective partition pruning on large tables. |
| Pipelines not benefiting from data skipping | Indicates missed layout or clustering opportunities. | Analyze scan behavior relative to file skipping statistics where available. | Increase data skipping effectiveness through clustering and layout strategy. |
| Queries with repeated `SELECT *` on large tables | Highlights low-discipline query behavior that increases scan cost. | Parse SQL history and flag broad projection queries on large tables. | Reduce broad projection patterns in production workloads. |
| Pipelines parameterized by environment | Demonstrates engineering discipline and deployability. | Inspect notebooks/jobs for environment parameters or configuration injection. | Increase reusable environment-aware pipeline design. |
| Pipelines with retry logic | Indicates resilience to transient platform or source failures. | Inspect job configs and code for retry behavior. | Increase retry coverage for transient-failure-prone jobs. |
| Pipelines with idempotent write logic | Idempotency reduces risk during retries and reruns. | Inspect write patterns and rerun-safe logic in transformations. | Increase idempotent pipeline coverage for production jobs. |
| Pipelines with checkpoint or watermark logic where needed | Improves replay safety and correctness in incremental or streaming jobs. | Inspect code and configs for watermark or checkpoint patterns. | Increase restart-safe incremental logic. |
| Pipelines with explicit error handling | Demonstrates operational discipline and observability. | Inspect code for structured failure capture, logging, and fallback behavior. | Increase explicit error handling in production jobs. |
| Pipelines with test or validation steps | Shows whether data pipelines include quality or completeness checks. | Count pipelines with assertions, validation notebooks, or reconciliation steps. | Increase pipeline-integrated validation coverage. |
| Pipelines with source-to-target row count reconciliation | A concrete data movement control for completeness. | Inspect jobs for reconciliation logic or post-load validation queries. | Increase reconciliation coverage on critical loads. |
| Pipelines with SLA metadata | Helps distinguish critical workloads from ad hoc jobs. | Inspect job metadata, tags, configs, or scorecard metadata tables. | Increase formal SLA metadata on production workloads. |
| Pipeline success rate | Core indicator of operational health. | Measure successful runs divided by total runs over a rolling period. | Increase success rate and reduce instability. |
| Retry rate | Indicates instability even when runs eventually succeed. | Measure retried runs divided by total runs. | Reduce retry dependence through root-cause fixes. |
| Failure rate by pipeline | Helps target improvement to the least stable workloads. | Track failed runs by pipeline over time. | Reduce failure concentration in the worst pipelines. |
| Average recovery time after failure | Measures operational recovery quality. | Compare time from failed run to next successful run. | Reduce recovery time for critical pipelines. |
| Number of orphaned or interrupted runs | Indicates operational hygiene issues. | Count canceled, hung, or incomplete runs. | Reduce incomplete execution patterns. |
| Number of manual reruns required | Signals fragility and hidden operational labor. | Track reruns initiated after failures or partial loads. | Reduce manual intervention for normal operations. |
| Number of jobs with alerting configured | Demonstrates operational observability and readiness. | Inspect jobs for notification or alert configuration. | Increase alerting coverage on critical workloads. |
| Number of active tables by domain | Shows breadth of warehouse adoption across the business. | Count active queried or refreshed tables grouped by domain. | Increase adoption breadth where strategic domains are underrepresented. |
| Number of active pipelines by domain | Shows engineering coverage of the warehouse by business area. | Count active scheduled or recently run pipelines by domain. | Increase balanced pipeline coverage across critical domains. |
| Number of distinct users querying the warehouse | Direct signal of user-level adoption. | Count distinct users in query history over a rolling period. | Increase sustained usage by real consumers. |
| Number of scheduled jobs hitting the warehouse | Shows whether the warehouse is part of production operations. | Count scheduled jobs that read from or write to warehouse objects. | Increase productionized warehouse usage over ad hoc execution. |
| Number of BI or reporting workloads using the warehouse | Indicates business-facing consumption. | Identify BI-originated queries or service accounts associated with reporting tools. | Increase direct reporting use where the warehouse is intended as the serving layer. |
| Number of service principals or applications using it | Measures system-to-system adoption beyond human querying. | Count distinct service principals or application identities in access/query telemetry. | Increase platform integration where expected. |
| Tables built with `CREATE OR REPLACE` when they should be incremental | Explicit anti-pattern metric for engineering improvement. | Compare table size/frequency to pipeline write pattern classification. | Reduce inappropriate rebuild patterns. |
| Large tables without partitioning or clustering | Explicit anti-pattern metric for physical design. | Identify large tables and check absence of partitioning or clustering metadata. | Reduce the number of large tables lacking layout strategy. |
| Large tables with small-file problems | Explicit anti-pattern metric tied to performance debt. | Combine size thresholds with file-size analysis. | Reduce the number of large fragmented tables. |
| Pipelines using full reloads on high-volume data | Explicit anti-pattern metric for cost and runtime risk. | Join pipeline classifications to volume thresholds and refresh cadence. | Reduce high-volume full reload patterns. |
| Queries with repeated large shuffle joins | Explicit anti-pattern metric for execution inefficiency. | Aggregate query plans and identify repeated shuffle-heavy join patterns. | Reduce the count and frequency of heavy shuffle joins. |
| Pipelines lacking `OPTIMIZE` or compaction hygiene | Explicit anti-pattern metric for Delta maintenance gaps. | Compare table maintenance history to table size and workload criticality. | Reduce maintenance gaps on critical Delta tables. |
| Broadcast opportunities missed | Explicit anti-pattern metric for join strategy tuning. | Find joins where one side is small enough for broadcast but plans do not use it. | Increase effective broadcast usage where beneficial. |
| Tables with poor documentation or ownership metadata | Explicit anti-pattern metric for stewardship weakness. | Count tables missing comments, owners, tags, or other key metadata. | Reduce undocumented or ownerless critical tables. |

## Proposed V1 Metrics

These are the recommended first implementation metrics for a practical warehouse
maturity scorecard.

| Metric Name | Why It Is In V1 |
|---|---|
| Percent of curated tables using Delta | Core indicator of whether Databricks-native table functionality is actually being used. |
| Percent of loads using `MERGE` instead of full rebuild | Strong signal of incremental maturity and maintainable data movement. |
| Percent of large tables with partition or clustering strategy | Captures physical design discipline for large data assets. |
| Number of small-file problem tables | High-value operational metric tied directly to performance improvement. |
| Number of oversized-file problem tables | Complements small-file detection and shows poor write/layout behavior. |
| Percent of large tables with `OPTIMIZE` history | Measures whether critical Delta tables are being maintained. |
| Number of large shuffle joins | Direct signal of expensive query behavior and join inefficiency. |
| Number of broadcast joins | Demonstrates whether the warehouse is taking advantage of efficient join strategies. |
| Percent of pipelines using incremental loads | Broad engineering maturity signal beyond individual SQL statements. |
| Percent of pipelines using auto-trigger or streaming where appropriate | Captures more advanced Databricks-native ingestion behavior. |
| P95 pipeline runtime for critical jobs | Operational performance signal that is easy to understand and trend. |
| Pipeline success rate | Essential reliability metric for any production maturity model. |

## Proposed V1 Rollup View

For manager-level review, the current v1 shortlist can be rolled into these
areas:

| Rollup Area | Proposed V1 Measures |
|---|---|
| Platform Feature Utilization | Percent of curated tables using Delta, percent of loads using `MERGE`, percent of pipelines using incremental loads, percent of pipelines using auto-trigger or streaming |
| Performance and Efficiency | Percent of large tables with partition or clustering strategy, number of small-file problem tables, number of oversized-file problem tables, percent of large tables with `OPTIMIZE` history, number of large shuffle joins, number of broadcast joins, P95 pipeline runtime for critical jobs |
| Operational Reliability | Pipeline success rate |

This gives managers a compact story:

- Are we using the platform well?
- Are we engineering efficient pipelines?
- Are operations stable?

## Implementation Map

Current implementation lives in:

- Model overview: `docs/databricks-environment-scorecard.md`
- Operational runbook: `docs/databricks-scorecard-runbook.md`
- Metric collection: `databricks/resources/notebooks/maturity/maturity_collector_notebook.py`
- Metric-to-status derivation: `databricks/resources/notebooks/maturity/scorecard_status_loader_notebook.py`
- Weighted evaluation: `databricks/resources/notebooks/maturity/scorecard_evaluation_notebook.py`
- CI/CD gate: `databricks/resources/notebooks/maturity/maturity_ci_check_notebook.py`
- Bundle orchestration: `.github/workflows/deploy.yml`

## Development Roadmap

### Near Term

- Validate query-history and metadata-table availability in each workspace.
- Decide which metrics can be sourced from telemetry alone and which require SQL
  or notebook parsing.
- Tune thresholds with real telemetry rather than placeholder assumptions.
- Add report outputs that summarize score trends, top anti-patterns, and
  failing checks.

### Next Metrics To Add After Current Implementation

- Cost and spend trend from `system.billing.usage`
- Queue and startup behavior from `system.compute.warehouse_events`
- Warehouse idle efficiency
- Table owner coverage
- Tag coverage for critical assets
- Catalog/schema growth trends
- Workload mix by user, service principal, and query type
- SQL-pattern parsing for merge vs replace behavior
- SQL-pattern parsing for join characteristics
- Delta file-layout analysis for small and large file issues

### Later Enhancements

- Time-series trend scoring
- Environment-to-environment comparison
- Domain or workload segmentation
- Score history dashboards
- Promotion gates that are stricter in `prod` than `dev`
- Per-domain or per-pipeline engineering scorecards

## Open Questions

- What is the right minimum utilization threshold for this warehouse?
- Should CI warnings differ by environment (`dev`, `test`, `prod`)?
- Should governance failures block only `prod` or all environments?
- Which metrics can be pulled reliably from Databricks system tables alone?
- Which metrics require SQL or notebook code parsing?
- What file-size thresholds should define "small" and "large" file problems?
- How should we define a "large join" and a "broadcast opportunity missed"?
- What cost metrics are trustworthy enough to score without creating noise?

## Assumptions

- Databricks system tables are available or can be enabled in target
  environments.
- Warehouse maturity should be evaluated from platform behavior and pipeline
  implementation patterns, not external catalog tooling.
- Unknown telemetry should not break the pipeline, but it should reduce trust in
  the scorecard result.
- Some engineering-pattern metrics will require parsing SQL or notebook code in
  addition to querying system tables.

## Change Log

- `2026-03-18`: Added operational reliability and performance telemetry checks
  (WH-13 through WH-24) covering retry/failure recovery, join patterns, very
  large scans, file size health, file growth, and compaction candidates.
- `2026-03-11`: Initial Databricks-only warehouse maturity model documented and
  aligned to telemetry-backed `WH-*` scorecard checks.
- `2026-03-11`: Expanded with a warehouse engineering metrics backlog and a
  proposed v1 metric shortlist focused on table design, load patterns, Delta
  optimization, join behavior, and operational reliability.
