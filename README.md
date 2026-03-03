# Sundt EDM Databricks Maturity (Extending to Quality)

This repository is the production Databricks Asset Bundle (DAB) for assessing
and tracking Databricks environment maturity. It now also includes the
`sundt-edm-quality` toolchain for Bigeye + Alation + Databricks scorecarding.

## What this repo provides

- Bundle-managed Databricks jobs and notebooks under `databricks/`
- CI/CD workflows that validate, deploy, and run post-deploy maturity checks
- Quality ingestion and scorecard deploy tooling under `src/sundt_edm_quality/`
- YAML/SQL/PySpark scorecard assets under `scorecard/`
- Runbooks for workflow validation and promotion

## Required GitHub Environments (in this repo)

Create these environments in GitHub UI (`Settings -> Environments`):

- `DataBricks-Dev`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`, `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLUSTER_ID`
- `DataBricks-Test`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`, `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLUSTER_ID`
- `DataBricks-Prod`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`, `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLUSTER_ID` (with required approvers)

## Quality deployment flow

Post-deploy CI jobs now run:

```bash
just scorecard-bootstrap-deploy config="config/sources.template.yaml"
```

This executes:
1. Databricks PySpark bootstrap for scorecard schemas/tables
2. Scorecard definition load + SQL model/view deployment

## Notes

- The Databricks asset validation notebook path used in the workflow must exist in your workspace.
- This repo intentionally avoids storing secret values.
- The reusable workflow is pinned to an immutable commit SHA; update it whenever `sundt-edm-cicd-platform` is intentionally upgraded.
- Helper script for pin updates: `scripts/update-pipeline-workflow-pin.ps1 -PipelineSha <new_sha>`.
- Branch governance setup is documented in `docs/branch-protection-checklist.md`.
- Dev/test workflow execution is documented in `docs/workflow-validation-runbook.md`.
- End-to-end promotion path (`dev` -> `test` -> `prod`) is documented in `docs/promotion-runbook.md`.
- Asset build and decommission workflow is documented in `docs/databricks-asset-bundles-lifecycle.md`.
