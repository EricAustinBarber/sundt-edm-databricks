# Sundt EDM Databricks Warehouse Maturity

This repository is the production Databricks Asset Bundle (DAB) for assessing
and tracking Databricks warehouse maturity.

## What this repo provides

- Bundle-managed Databricks jobs and notebooks under `databricks/`
- CI/CD workflows that validate, deploy, and run post-deploy maturity checks
- Databricks telemetry-backed maturity jobs and notebooks under `databricks/resources/notebooks/maturity/`
- Warehouse maturity documentation under `docs/`
- Runbooks for workflow validation and promotion

## Required GitHub Environments

Create these environments in GitHub UI (`Settings -> Environments`):

- `DataBricks-Dev`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`, `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLUSTER_ID`
- `DataBricks-Test`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`, `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLUSTER_ID`
- `DataBricks-Prod`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`, `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLUSTER_ID`

## Deployment Model

GitHub Actions deploys via Databricks Asset Bundles only. CI/CD is the source
of truth for environment deployment:

1. Validate and deploy bundle to the target (`dev`, `test`, `prod`)
2. Run post-deploy smoke and warehouse maturity bundle jobs in Databricks

Workflow split:

- PR checks: `.github/workflows/pr-checks.yml`
- Deployments: `.github/workflows/deploy.yml`
- Legacy shim: `.github/workflows/ci.yml` (manual only)

## Warehouse Maturity Docs

- `docs/warehouse-maturity-model.md`
- `docs/databricks-environment-scorecard.md`
- `docs/databricks-scorecard-runbook.md`

## Notes

- The Databricks asset validation notebook path used in the workflow must exist
  in your workspace.
- This repo intentionally avoids storing secret values.
- The reusable workflow is pinned to an immutable commit SHA; update it whenever
  `sundt-edm-cicd-platform` is intentionally upgraded.
- Helper script for pin updates:
  `scripts/update-pipeline-workflow-pin.ps1 -PipelineSha <new_sha>`.
- Branch governance setup is documented in
  `docs/branch-protection-checklist.md`.
- Dev/test workflow execution is documented in
  `docs/workflow-validation-runbook.md`.
- End-to-end promotion path (`dev` -> `test` -> `prod`) is documented in
  `docs/promotion-runbook.md`.
- Asset build and decommission workflow is documented in
  `docs/databricks-asset-bundles-lifecycle.md`.
