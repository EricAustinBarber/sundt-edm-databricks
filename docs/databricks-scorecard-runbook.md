# Databricks Scorecard Runbook

## Purpose

Run and refresh the Databricks environment scorecard, store results, and keep the baseline current.

This workflow is **non-blocking** and should not gate deployments until approval workflows are finalized.

## Inputs

- Scorecard definition: embedded in `databricks/resources/notebooks/maturity/scorecard_evaluation_notebook.py`
- Assessment evidence: `EnterpriseDataMaturity/assessments/databricks-transformation-review.md`
- Assessment evidence: `EnterpriseDataMaturity/assessments/assessment-tracker.csv`
- Assessment evidence: `EnterpriseDataMaturity/assessments/databricks-notebook-inventory.csv`
- Assessment evidence: `EnterpriseDataMaturity/assessments/databricks-function-inventory.csv`

## Output Tables (Delta)

- `governance_maturity.scorecard_definition`
- `governance_maturity.scorecard_check_status`
- `governance_maturity.scorecard_results`
- `governance_maturity.scorecard_check_results`

## Execution Steps

1. Update assessment evidence in `EnterpriseDataMaturity/assessments/`.
2. Update scorecard definition if needed:
   - Edit the embedded list in `databricks/resources/notebooks/maturity/scorecard_evaluation_notebook.py`.
   - Keep `docs/databricks-environment-scorecard.md` in sync.
3. Maintain scorecard status via loader notebook:
   - Job: `maturity-scorecard-status-load-test`
   - Baseline statuses are embedded in `scorecard_status_loader_notebook.py`
4. Run the evaluation job in Databricks (test environment): `maturity-scorecard-eval-test`
5. Review outputs:
   - Check `governance_maturity.scorecard_results` for total score, blocked reasons, and run metadata.
6. Record the snapshot:
   - Update `docs/databricks-environment-scorecard.md` baseline table and date.

## Status Conventions

- `Pass` = 1.0
- `Partial` = 0.5
- `Fail` = 0.0
- Any `Fail` in the **Security** dimension blocks readiness.

## Quick SQL: What Is Being Scored

```sql
-- scorecard checks + weights
select check_id, dimension, check_name, weight, pass_criteria
from governance_maturity.scorecard_definition
order by check_id;

-- latest status used by env
with s as (
  select *,
         row_number() over (partition by check_id order by updated_at desc) rn
  from governance_maturity.scorecard_check_status
  where env = 'test' or env is null
)
select check_id, status, notes, updated_at, updated_by
from s
where rn = 1
order by check_id;

-- most recent result summary
select collected_at, env, run_id, commit_sha, total_score, overall_status, blocked_reasons, warned_reasons
from governance_maturity.scorecard_results
where env = 'test'
order by collected_at desc
limit 20;

-- most recent per-check scoring details
select collected_at, env, run_id, check_id, dimension, weight, status_norm, score, weighted_score, notes
from governance_maturity.scorecard_check_results
where env = 'test'
order by collected_at desc, check_id;
```

## Manual Overrides

Update the embedded baseline list in `scorecard_status_loader_notebook.py` and rerun the loader job.

## CI Integration

CI bundle jobs run after deploy:

- `quality-scorecard-deploy-<env>` (bootstrap + definition/SQL deploy)
- `maturity-scorecard-status-load-<env>`
- `maturity-scorecard-eval-<env>`

## Evidence Stub Notebook

Use `scorecard_evidence_stub_notebook.py` to prototype evidence-derived statuses without enforcing gates. Set `write_mode=append` only when you are ready to persist results.

Job: `maturity-scorecard-evidence-stub-test`
