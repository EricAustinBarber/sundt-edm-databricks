# Databricks Environment Scorecard

## Purpose

Define the exact Databricks environment scorecard used by this repo. It converts the current assessment evidence into a repeatable, weighted checklist with explicit scoring rules.

## Scope

This scorecard covers the Databricks notebook transformation environment referenced in:

- `C:\Users\eabarber\CodeX\EnterpriseDataMaturity\assessments\databricks-transformation-review.md`
- `C:\Users\eabarber\CodeX\EnterpriseDataMaturity\assessments\databricks-implementation-summary.md`
- `C:\Users\eabarber\CodeX\EnterpriseDataMaturity\assessments\databricks-notebook-inventory.csv`
- `C:\Users\eabarber\CodeX\EnterpriseDataMaturity\assessments\databricks-function-inventory.csv`
- `C:\Users\eabarber\CodeX\EnterpriseDataMaturity\assessments\assessment-tracker.csv`

## Scoring Rules

- Each check is scored as:
  - `Pass` = 1.0
  - `Partial` = 0.5
  - `Fail` = 0.0
- Weights emphasize **Security** and **Performance**.
- Total score = sum of (check score * weight). Maximum = 100.
- If any **Security** check is `Fail`, overall status is **Not Ready** regardless of total score.

## Scorecard Checks (Exact Definition)

| Check ID | Dimension | Check | Evidence Source | Pass Criteria | Weight |
|---|---|---|---|---|---:|
| DBX-01 | Standards | Notebook parameterization by environment and source object | `assessments/databricks-transformation-review.md` | Parameterization is standard across notebooks (widgets or equivalent) with minimal exceptions documented | 6 |
| DBX-02 | Standards | Delta tables used for curated layers | `assessments/databricks-transformation-review.md` | Curated layers consistently use Delta tables; exceptions documented | 7 |
| DBX-03 | Standards | Incremental pipelines use deterministic merge/upsert | `assessments/databricks-transformation-review.md` | Merge/upsert patterns are standard for incremental loads with coverage mapped by dataset | 8 |
| DBX-04 | Standards | Data sanitization rules explicitly coded and tested | `assessments/databricks-transformation-review.md` | Sanitization rules exist per domain and are validated/tested | 7 |
| DBX-05 | Performance | Table hygiene tasks defined and executed | `assessments/databricks-transformation-review.md` | `OPTIMIZE`/`VACUUM` coverage mapped to all large/critical tables with defined cadence | 12 |
| DBX-06 | Performance | Partitioning/clustering documented for large tables | `assessments/databricks-transformation-review.md` | Partitioning/clustering standards exist and are applied to all large/critical tables | 10 |
| DBX-07 | Performance | File-size thresholds and compaction strategy present | `assessments/databricks-transformation-review.md` | Explicit file-size policy and compaction thresholds defined and enforced | 18 |
| DBX-08 | Reliability | Error handling and retry/idempotency patterns present | `assessments/databricks-transformation-review.md` | Standardized retry/idempotency utilities are used by all production jobs | 7 |
| DBX-09 | Security | Secrets accessed securely (Key Vault/secret scopes) | `assessments/databricks-transformation-review.md` | All secrets retrieved via secret scopes or Key Vault; no literal credentials | 20 |
| DBX-10 | Observability | Observability hooks and logging included in jobs | `assessments/databricks-transformation-review.md` | Job-level logging/metrics are standardized and mapped to all production jobs | 5 |

## Baseline Assessment Snapshot (February 22, 2026)

The baseline statuses below reflect the initial assessment run documented in the assessment workspace.

| Check ID | Status | Notes |
|---|---|---|
| DBX-01 | Pass | Strong evidence via widespread widget usage |
| DBX-02 | Partial | Delta indicators present; table-level validation pending |
| DBX-03 | Partial | Merge usage present; coverage by dataset not yet measured |
| DBX-04 | Partial | Rule inventory per domain not yet captured |
| DBX-05 | Partial | Optimize/Vacuum present; compliance mapping incomplete |
| DBX-06 | Partial | Partition usage present; completeness by table unknown |
| DBX-07 | Fail | No file-size threshold policy evidence captured |
| DBX-08 | Partial | Patterns exist in utilities; job-level coverage pending |
| DBX-09 | Partial | Secure retrieval present; potential literals need triage |
| DBX-10 | Partial | Needs per-job mapping to logging/metrics standard |

## Remediation Items That Affect Score

Tracked in `C:\Users\eabarber\CodeX\EnterpriseDataMaturity\assessments\assessment-tracker.csv`:

- `DBX-R01` (H): Validate/remediate credential literal findings (impacts DBX-09).
- `DBX-R02` (H): Define/enforce Delta file-size compaction policy (impacts DBX-07).
- `DBX-R03` (M): Build dataset-level compliance matrix (impacts DBX-02, DBX-03, DBX-05, DBX-06).

## Execution Cadence

- Update the scorecard after each remediation sprint or quarterly at minimum.
- Evidence refresh should include updated notebook inventory and targeted manual reviews for the `Partial` checks.

## Canonical Definition

The canonical scorecard definition is embedded in
`databricks/resources/notebooks/maturity/scorecard_evaluation_notebook.py`.
Keep this document aligned with the embedded list.
