# Workflow Validation Runbook

Use this runbook to execute one end-to-end validation in `dev` and `test`.

For full promotion through production, use `docs/promotion-runbook.md`.

## Prerequisites

- `gh` CLI installed.
- Valid GitHub authentication (`gh auth login`).
- GitHub Environments configured:
  - `DataBricks-Dev`
  - `DataBricks-Test`
  - `DataBricks-Prod`
- Required environment secrets present.

## Trigger validation runs

1. Validate `dev`:
   - `gh workflow run "CICD-Databricks" --ref dev`
   - `gh run list --workflow "CICD-Databricks" --limit 1`
2. Validate `test`:
   - `gh workflow run "CICD-Databricks" --ref test`
   - `gh run list --workflow "CICD-Databricks" --limit 1`

## Success criteria

- `unit-tests` passes.
- `ci-dev` passes on `dev`.
- `ci-test` passes on `test`.
- Databricks deploy + smoke + validation steps complete successfully.
