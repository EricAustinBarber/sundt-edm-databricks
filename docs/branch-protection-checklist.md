# Branch Protection Checklist (Sundt EDM Databricks Maturity)

Use this checklist to enforce safe promotion from `dev` -> `test` -> `prod`.

## Global repository settings

- Protect branches: `dev`, `test`, `prod`, `main`.
- Require pull requests before merge.
- Require at least 1 approval (`prod` requires 2).
- Dismiss stale reviews on push.
- Block force pushes and branch deletion.
- Enable secret scanning and push protection.

## Required status checks

Branch `dev`:
- `unit-tests`
- `ci-dev`

Branch `test`:
- `unit-tests`
- `ci-test`

Branch `prod`:
- `unit-tests`
- `ci-test` (runs for PRs targeting `prod`)
- Enforce deployment approvals in `DataBricks-Prod` environment for `cd-prod`.

## Environment approvals

- `DataBricks-Dev`: no manual approval.
- `DataBricks-Test`: optional approvals for high-risk changes.
- `DataBricks-Prod`: required approvers from platform + data owners.

## Release / hotfix controls

- Merge `release/*` into `prod` via PR only.
- Allow emergency `hotfix/*` with same required checks.
- Back-merge hotfixes to `dev` after prod deployment.
