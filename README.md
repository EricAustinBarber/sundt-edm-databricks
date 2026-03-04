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

## Quality Environment Variables (bundle targets)

Quality scorecard writes are controlled by bundle target variables in
`databricks/databricks.yml`:

- `quality_catalog`
- `quality_schema_bronze`
- `quality_schema_silver`
- `quality_schema_governance`

Ownership:

- Data Platform Engineering owns these values in version control.
- Changes require PR review from platform maintainers and data governance.
- `prod` changes should only merge alongside an approved release/promotion PR.
## Deployment model (best practice)

GitHub Actions deploys via Databricks Asset Bundles only. CI/CD is the source of
truth for environment deployment:

1. Validate and deploy bundle to the target (`dev`, `test`, `prod`)
2. Run post-deploy smoke and scorecard bundle jobs in Databricks

This keeps deployment logic inside the bundle contract and avoids runner-local
or ad hoc deploy steps.

For local development only, you can still run:

```bash
just scorecard-bootstrap-deploy config/sources.template.yaml
```

## Notes

- The Databricks asset validation notebook path used in the workflow must exist in your workspace.
- This repo intentionally avoids storing secret values.
- The reusable workflow is pinned to an immutable commit SHA; update it whenever `sundt-edm-cicd-platform` is intentionally upgraded.
- Helper script for pin updates: `scripts/update-pipeline-workflow-pin.ps1 -PipelineSha <new_sha>`.
- Branch governance setup is documented in `docs/branch-protection-checklist.md`.
- Dev/test workflow execution is documented in `docs/workflow-validation-runbook.md`.
- End-to-end promotion path (`dev` -> `test` -> `prod`) is documented in `docs/promotion-runbook.md`.
- Asset build and decommission workflow is documented in `docs/databricks-asset-bundles-lifecycle.md`.
