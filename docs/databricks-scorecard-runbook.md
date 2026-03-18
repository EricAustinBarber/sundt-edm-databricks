# Databricks Warehouse Scorecard Runbook

## Purpose

Run and refresh the Databricks warehouse maturity scorecard from Databricks
telemetry, store results, and use the latest score for review or CI warnings.

Related documents:

- `docs/databricks-environment-scorecard.md`
- `docs/warehouse-maturity-model.md`

## Output Tables

- `governance_maturity.warehouse_metric_catalog`
- `governance_maturity.warehouse_telemetry_metrics`
- `governance_maturity.scorecard_definition`
- `governance_maturity.scorecard_check_status`
- `governance_maturity.scorecard_results`
- `governance_maturity.scorecard_check_results`
- `governance_maturity.bundle_deployments`

## Bundle Jobs

- `maturity-collect-<env>`
- `maturity-scorecard-status-load-<env>`
- `maturity-scorecard-eval-<env>`
- `maturity-ci-check-<env>`

## Execution Steps

1. Deploy the bundle for the target environment.
2. Run `maturity_collect` to capture the latest warehouse telemetry snapshot.
   This also refreshes the deployed metric catalog used by the scorecard.
3. Run `maturity_scorecard_status_load` to convert telemetry into status rows.
4. Run `maturity_scorecard_eval` to compute weighted scorecard results.
5. Review `governance_maturity.scorecard_results` and
   `governance_maturity.scorecard_check_results`.
6. If used in CI, run `maturity_ci_check` after evaluation.

## Quick SQL

```sql
-- deployed metric catalog
select metric_id, dimension, metric_name, direction, pass_threshold, partial_threshold, updated_at
from governance_maturity.warehouse_metric_catalog
order by metric_id;

-- latest telemetry snapshot
select collected_at, env, check_id, metric_name, metric_value_double, status_hint, notes
from governance_maturity.warehouse_telemetry_metrics
where env = 'test'
order by collected_at desc, check_id;

-- latest derived statuses
with s as (
  select *,
         row_number() over (partition by check_id order by updated_at desc) rn
  from governance_maturity.scorecard_check_status
  where env = 'test'
)
select check_id, status, notes, updated_at, updated_by
from s
where rn = 1
order by check_id;

-- latest scorecard summary
select collected_at, env, run_id, total_score, overall_status, blocked_reasons, warned_reasons
from governance_maturity.scorecard_results
where env = 'test'
order by collected_at desc
limit 20;

-- latest check-level results
select collected_at, env, run_id, check_id, dimension, weight, status_norm, score, weighted_score, notes
from governance_maturity.scorecard_check_results
where env = 'test'
order by collected_at desc, check_id;
```

## Notes

- The current scorecard is Databricks-only. Bigeye and Alation are no longer in
  the warehouse maturity scoring path.
- The current scorecard covers 24 warehouse maturity metrics plus 21 cost-control
  signals spanning Delta usage, pipeline patterns, file layout health, join
  behavior, and operational reliability.
- Metrics degrade to `Unknown` when a system table or column is unavailable in a
  workspace. That preserves the pipeline, keeps the bundle deployable, and
  surfaces the missing telemetry as observed-weight warnings instead of hard
  score penalties.
