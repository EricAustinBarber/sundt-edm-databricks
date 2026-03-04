# Databricks Asset Bundles Lifecycle (Build + Decommission)

This repo uses Databricks Asset Bundles (DAB) to create and maintain Databricks assets in a repeatable, CI/CD-safe way. The goal is to have **one source of truth** in `databricks/` that CI can validate, deploy, and cleanly decommission without manual drift.

This document is written to pass the CI/CD gates in `sundt-edm-cicd-platform` and the unit tests in this repo.

## Scope

Covered assets:

- Jobs defined in `databricks/resources/resources.jobs.yml`
- Notebooks under `databricks/resources/notebooks/**` that are deployed by the bundle

## Principles

- All deployable assets are declared in bundle configuration and committed to the repo.
- CI validates and deploys from the repo, never from ad-hoc workspace edits.
- Decommissioning is a **two-step** process: remove from bundle config, then destroy for the target.

## Build (Create/Update) Workflow

1. Define or update assets in the bundle files:
   - Jobs: `databricks/resources/resources.jobs.yml`
   - Notebooks: `databricks/resources/notebooks/**`
   - Bundle root: `databricks/databricks.yml`
2. Commit and open a PR.
3. CI runs unit tests and bundle validation before deployment:
   - `unit_tests` job runs `pytest -q`
   - Deployment jobs require tests to pass
4. Merge to the target branch (`dev`, `test`, or `prod`) to trigger deployment.

Notes:

- `ci-test` additionally runs post-deploy bundle jobs (maturity checks) defined in `.github/workflows/ci.yml`.
- If a notebook changes, the bundle deploy updates it in the target workspace root path.

## Decommission Workflow (Repeatable and CI-Safe)

Use this when removing a job, notebook, or other bundle-managed asset.

1. Remove the asset from bundle configuration:
   - Jobs: delete the job entry from `databricks/resources/resources.jobs.yml`
   - Notebooks: delete or move the notebook file under `databricks/resources/notebooks/**`
2. Commit and merge the change.
3. Run a bundle destroy against the target to remove the asset from Databricks:
   - This should be executed by your CI/CD runner or an approved admin workflow
   - Use the same `bundle_path` and `target` as CI
4. Verify that post-deploy jobs and validation still pass.

Reasoning:

- Removing the definition alone prevents future deployments from re-creating the asset.
- The destroy step actually decommissions it in the workspace.

## CI/CD Compatibility Requirements

To keep CI green for `sundt-edm-cicd-platform`:

- Keep bundle state consistent with the repo.
- Do not introduce manual workspace-only artifacts.
- Ensure unit tests pass before deploy (the deploy jobs are gated).
- Keep maturity scorecard definitions embedded in
  `databricks/resources/notebooks/maturity/scorecard_evaluation_notebook.py`.

## Example: Decommission a Scorecard Job

Goal: remove `maturity_scorecard_evidence_stub` cleanly.

1. Remove the job entry from `databricks/resources/resources.jobs.yml`.
2. Remove the notebook file if it is no longer used:
   - `databricks/resources/notebooks/maturity/scorecard_evidence_stub_notebook.py`
3. Merge to the target branch.
4. Run bundle destroy for the target to remove the job from Databricks.
5. Confirm `ci-test` still runs and the remaining post-deploy jobs succeed.
