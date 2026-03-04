# Promotion Runbook (dev -> test -> prod)

Use this runbook to promote changes through build, test, and production using the reusable platform workflow.

## Preconditions

- Branches exist: `dev`, `test`, `prod`, `main`
- GitHub Environments exist with secrets:
  - `DataBricks-Dev`
  - `DataBricks-Test`
  - `DataBricks-Prod`
- `DataBricks-Prod` has required approvers
- Branch protection required checks:
  - `unit-tests`
  - `ci-dev` on `dev`
  - `ci-test` on `test`
  - `cd-prod` via merge to `prod`

## Phase 1: Build + Dev Deploy

1. Create a feature branch from `dev`.
2. Make a small code change (for example a notebook or job definition).
3. Open PR to `dev`.
4. Verify pipeline result:
   - `unit-tests` passes
   - `ci-dev` passes
   - Databricks dev deploy and smoke succeed
5. Merge PR into `dev`.

## Phase 2: Test Validation

1. Open PR from `dev` to `test`.
2. Verify pipeline result:
   - `unit-tests` passes
   - `ci-test` passes
   - Test deploy succeeds
   - Smoke job (`smoke`) succeeds
   - Asset validation notebook succeeds
3. Merge PR into `test`.

## Phase 3: Production Deploy

1. Open PR from `test` to `prod`.
2. Verify required checks and approvals on PR.
3. Merge PR into `prod`.
4. Approve environment deployment in `DataBricks-Prod` when prompted.
5. Verify `cd-prod` completed:
   - Prod deploy succeeded
   - Smoke job succeeded
   - Post-deploy summary succeeded

## Suggested `gh` Commands

```bash
gh pr create --base dev --head <feature-branch> --title "Feature for dev validation" --body "Promote to dev"
gh pr checks <pr-number> --watch
gh pr merge <pr-number> --merge

gh pr create --base test --head dev --title "Promote dev to test" --body "Test validation run"
gh pr checks <pr-number> --watch
gh pr merge <pr-number> --merge

gh pr create --base prod --head test --title "Promote test to prod" --body "Production promotion"
gh pr checks <pr-number> --watch
gh pr merge <pr-number> --merge
```

## Evidence to Capture for Handoff

- PR links for `dev`, `test`, `prod` promotions
- GitHub run URLs for `ci-dev`, `ci-test`, `cd-prod`
- Databricks run IDs for smoke and validation steps
- Commit SHA promoted to production
