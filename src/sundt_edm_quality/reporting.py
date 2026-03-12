from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from databricks import sql as dbsql
except Exception:  # noqa: BLE001
    dbsql = None

from sundt_edm_quality.config import AppConfig


DATASET_DETAIL_FIELDS = [
    "report_date",
    "dataset_id",
    "dataset_name",
    "domain_name",
    "business_owner",
    "technical_owner",
    "criticality",
    "sla_freshness",
    "contains_pii",
    "is_active",
    "bigeye_monitor_count",
    "alert_count_7d",
    "alation_endorsed",
    "alation_owner",
    "alation_steward",
    "alation_lineage_present",
    "dataset_score",
    "dataset_status",
]

DOMAIN_SUMMARY_FIELDS = [
    "report_date",
    "domain_name",
    "domain_score",
    "domain_status",
    "dataset_count",
]

METRIC_SUMMARY_FIELDS = [
    "report_date",
    "metric_id",
    "metric_name",
    "avg_metric_score",
    "dataset_count",
]

MATURITY_CHECK_FIELDS = [
    "collected_at",
    "env",
    "run_id",
    "check_id",
    "dimension",
    "check_name",
    "weight",
    "status_norm",
    "score",
    "weighted_score",
    "notes",
]


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round((numerator / denominator) * 100.0, 2)


def _avg(values: list[float | None]) -> float | None:
    usable = [value for value in values if value is not None]
    if not usable:
        return None
    return round(sum(usable) / len(usable), 2)


def _coerce_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(payload, list):
            return [str(item) for item in payload]
    return [text]


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)


def _quote_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _fetch_rows(cursor: Any, sql_text: str) -> list[dict[str, Any]]:
    cursor.execute(sql_text)
    columns = [column[0] for column in (cursor.description or [])]
    rows = cursor.fetchall()
    return [
        {columns[idx]: row[idx] for idx in range(len(columns))}
        for row in rows
    ]


def _fetch_optional(
    cursor: Any,
    sql_text: str,
    warnings: list[str],
    label: str,
) -> list[dict[str, Any]]:
    try:
        return _fetch_rows(cursor, sql_text)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"{label}: {exc}")
        return []


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    field: (
                        json.dumps(row.get(field), default=_json_default)
                        if isinstance(row.get(field), (list, dict, tuple, set))
                        else row.get(field)
                    )
                    for field in fieldnames
                }
            )


def _render_markdown(payload: dict[str, Any], top_n: int) -> str:
    warnings = payload.get("warnings", [])
    bigeye = payload["sources"]["bigeye"]
    alation = payload["sources"]["alation"]
    governance = payload["sources"]["databricks_governance"]
    maturity = payload["sources"]["databricks_maturity"]

    lines = [
        "# Data Governance and Maturity Review",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Databricks catalog: `{payload['catalog']}`",
        f"- Databricks maturity env: `{payload['databricks_env']}`",
        "",
        "## Bigeye",
        "",
        f"- Raw monitor rows ingested: `{bigeye['raw_row_count']}`",
        f"- Latest Bigeye ingest: `{bigeye['max_ingested_at'] or 'n/a'}`",
        f"- Critical datasets in review set: `{bigeye['dataset_count']}`",
        f"- Datasets with monitors: `{bigeye['monitored_dataset_count']}` ({bigeye['monitored_dataset_pct'] or 'n/a'}%)",
        f"- Total monitor assignments across datasets: `{bigeye['monitor_count_total']}`",
        f"- Bigeye alerts in last 7 days: `{bigeye['alert_count_7d_total']}`",
        "",
        "## Alation",
        "",
        f"- Raw asset rows ingested: `{alation['raw_row_count']}`",
        f"- Latest Alation ingest: `{alation['max_ingested_at'] or 'n/a'}`",
        f"- Datasets endorsed: `{alation['endorsed_dataset_count']}` ({alation['endorsed_dataset_pct'] or 'n/a'}%)",
        (
            f"- Datasets with owner and steward assigned: "
            f"`{alation['owner_steward_complete_count']}` ({alation['owner_steward_complete_pct'] or 'n/a'}%)"
        ),
        f"- Datasets with lineage present: `{alation['lineage_present_count']}` ({alation['lineage_present_pct'] or 'n/a'}%)",
        "",
        "## Databricks Governance Scorecard",
        "",
        f"- Report date: `{governance['report_date'] or 'n/a'}`",
        f"- Average dataset score: `{governance['avg_dataset_score'] or 'n/a'}`",
        f"- Dataset status counts: `Green={governance['status_counts']['Green']}` `Yellow={governance['status_counts']['Yellow']}` `Red={governance['status_counts']['Red']}`",
        f"- Domains scored: `{governance['domain_count']}`",
        "",
        f"### Lowest-Scoring Domains (Top {top_n})",
        "",
    ]

    lowest_domains = governance.get("lowest_scoring_domains", [])
    if lowest_domains:
        for row in lowest_domains:
            lines.append(
                f"- `{row.get('domain_name')}` score=`{row.get('domain_score')}` status=`{row.get('domain_status')}` datasets=`{row.get('dataset_count')}`"
            )
    else:
        lines.append("- No domain score rows were available.")

    lines.extend(
        [
            "",
            f"### Lowest-Scoring Datasets (Top {top_n})",
            "",
        ]
    )

    lowest_datasets = governance.get("lowest_scoring_datasets", [])
    if lowest_datasets:
        for row in lowest_datasets:
            lines.append(
                f"- `{row.get('dataset_name')}` domain=`{row.get('domain_name')}` score=`{row.get('dataset_score')}` status=`{row.get('dataset_status')}`"
            )
    else:
        lines.append("- No dataset score rows were available.")

    lines.extend(
        [
            "",
            "## Databricks Environment Maturity",
            "",
            f"- Latest evaluation time: `{maturity.get('collected_at') or 'n/a'}`",
            f"- Overall status: `{maturity.get('overall_status') or 'n/a'}`",
            f"- Total score: `{maturity.get('total_score') or 'n/a'}`",
        ]
    )

    blocked = maturity.get("blocked_reasons", [])
    warned = maturity.get("warned_reasons", [])
    lines.append(f"- Blocked reasons: `{', '.join(blocked) if blocked else 'none'}`")
    lines.append(f"- Warned reasons: `{', '.join(warned) if warned else 'none'}`")
    lines.extend(
        [
            "",
            "### Dimension Summary",
            "",
        ]
    )

    dimensions = maturity.get("dimension_summary", [])
    if dimensions:
        for row in dimensions:
            lines.append(
                f"- `{row.get('dimension')}` score=`{row.get('dimension_score')}` pass=`{row.get('pass_count')}` partial=`{row.get('partial_count')}` fail=`{row.get('fail_count')}` unknown=`{row.get('unknown_count')}`"
            )
    else:
        lines.append("- No Databricks maturity rows were available.")

    lines.extend(
        [
            "",
            f"### Non-Passing Maturity Checks (Top {top_n})",
            "",
        ]
    )

    maturity_issues = maturity.get("non_passing_checks", [])
    if maturity_issues:
        for row in maturity_issues[:top_n]:
            lines.append(
                f"- `{row.get('check_id')}` `{row.get('check_name')}` status=`{row.get('status_norm')}` weight=`{row.get('weight')}` notes=`{row.get('notes') or 'n/a'}`"
            )
    else:
        lines.append("- No non-passing maturity checks were returned.")

    if warnings:
        lines.extend(
            [
                "",
                "## Warnings",
                "",
            ]
        )
        for warning in warnings:
            lines.append(f"- {warning}")

    lines.append("")
    return "\n".join(lines)


def _build_report_payload(
    *,
    catalog: str,
    databricks_env: str,
    raw_summary: dict[str, dict[str, Any]],
    dataset_detail: list[dict[str, Any]],
    domain_summary: list[dict[str, Any]],
    metric_summary: list[dict[str, Any]],
    maturity_result: dict[str, Any] | None,
    maturity_dimensions: list[dict[str, Any]],
    maturity_checks: list[dict[str, Any]],
    warnings: list[str],
    top_n: int,
) -> dict[str, Any]:
    status_counts = {"Green": 0, "Yellow": 0, "Red": 0}
    for row in dataset_detail:
        status = str(row.get("dataset_status") or "").strip()
        if status in status_counts:
            status_counts[status] += 1

    dataset_scores = [_as_float(row.get("dataset_score")) for row in dataset_detail]
    monitored_dataset_count = sum(
        1 for row in dataset_detail if int(row.get("bigeye_monitor_count") or 0) > 0
    )
    endorsed_dataset_count = sum(1 for row in dataset_detail if _as_bool(row.get("alation_endorsed")))
    owner_steward_complete_count = sum(
        1
        for row in dataset_detail
        if str(row.get("alation_owner") or "").strip() and str(row.get("alation_steward") or "").strip()
    )
    lineage_present_count = sum(
        1 for row in dataset_detail if _as_bool(row.get("alation_lineage_present"))
    )

    report_date = None
    if dataset_detail:
        report_date = dataset_detail[0].get("report_date")

    lowest_domains = sorted(
        domain_summary,
        key=lambda row: (_as_float(row.get("domain_score")) is None, _as_float(row.get("domain_score")) or 999999.0, str(row.get("domain_name") or "")),
    )[:top_n]
    lowest_datasets = sorted(
        dataset_detail,
        key=lambda row: (_as_float(row.get("dataset_score")) is None, _as_float(row.get("dataset_score")) or 999999.0, str(row.get("dataset_name") or "")),
    )[:top_n]

    maturity = maturity_result or {}
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "catalog": catalog,
        "databricks_env": databricks_env,
        "sources": {
            "bigeye": {
                "raw_row_count": int(raw_summary.get("bigeye_monitors", {}).get("row_count") or 0),
                "max_ingested_at": raw_summary.get("bigeye_monitors", {}).get("max_ingested_at"),
                "dataset_count": len(dataset_detail),
                "monitored_dataset_count": monitored_dataset_count,
                "monitored_dataset_pct": _pct(monitored_dataset_count, len(dataset_detail)),
                "monitor_count_total": sum(int(row.get("bigeye_monitor_count") or 0) for row in dataset_detail),
                "alert_count_7d_total": sum(int(row.get("alert_count_7d") or 0) for row in dataset_detail),
            },
            "alation": {
                "raw_row_count": int(raw_summary.get("alation_assets", {}).get("row_count") or 0),
                "max_ingested_at": raw_summary.get("alation_assets", {}).get("max_ingested_at"),
                "dataset_count": len(dataset_detail),
                "endorsed_dataset_count": endorsed_dataset_count,
                "endorsed_dataset_pct": _pct(endorsed_dataset_count, len(dataset_detail)),
                "owner_steward_complete_count": owner_steward_complete_count,
                "owner_steward_complete_pct": _pct(owner_steward_complete_count, len(dataset_detail)),
                "lineage_present_count": lineage_present_count,
                "lineage_present_pct": _pct(lineage_present_count, len(dataset_detail)),
            },
            "databricks_governance": {
                "report_date": report_date,
                "avg_dataset_score": _avg(dataset_scores),
                "dataset_count": len(dataset_detail),
                "domain_count": len(domain_summary),
                "status_counts": status_counts,
                "metric_averages": metric_summary,
                "lowest_scoring_domains": lowest_domains,
                "lowest_scoring_datasets": lowest_datasets,
            },
            "databricks_maturity": {
                "collected_at": maturity.get("collected_at"),
                "env": maturity.get("env"),
                "run_id": maturity.get("run_id"),
                "commit_sha": maturity.get("commit_sha"),
                "total_score": _as_float(maturity.get("total_score")),
                "overall_status": maturity.get("overall_status"),
                "blocked_reasons": _coerce_string_list(maturity.get("blocked_reasons")),
                "warned_reasons": _coerce_string_list(maturity.get("warned_reasons")),
                "dimension_summary": maturity_dimensions,
                "non_passing_checks": [
                    row for row in maturity_checks if str(row.get("status_norm") or "") != "Pass"
                ],
            },
        },
        "warnings": warnings,
    }
    return payload


def generate_reports(
    cfg: AppConfig,
    *,
    server_hostname: str,
    http_path: str,
    access_token: str,
    output_dir: str,
    databricks_env: str,
    top_n: int = 10,
) -> dict[str, Any]:
    if dbsql is None:
        raise ImportError(
            "databricks-sql-connector is required for generate_reports; install the databricks SQL connector"
        )
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    catalog = cfg.databricks.catalog
    env_sql = _quote_sql_string(databricks_env)
    raw_schema = cfg.databricks.schema_raw
    mart_schema = "mart"

    raw_summary: dict[str, dict[str, Any]] = {}
    dataset_detail: list[dict[str, Any]] = []
    domain_summary: list[dict[str, Any]] = []
    metric_summary: list[dict[str, Any]] = []
    maturity_result: dict[str, Any] | None = None
    maturity_dimensions: list[dict[str, Any]] = []
    maturity_checks: list[dict[str, Any]] = []
    warnings: list[str] = []

    with dbsql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    ) as conn:
        with conn.cursor() as cursor:
            raw_objects = {
                "bigeye_monitors": f"{catalog}.{raw_schema}.{cfg.databricks.table_bigeye_monitors_json}",
                "bigeye_alerts": f"{catalog}.{raw_schema}.{cfg.databricks.table_bigeye_alerts_json}",
                "alation_assets": f"{catalog}.{raw_schema}.{cfg.databricks.table_alation_assets_json}",
            }
            for label, full_name in raw_objects.items():
                rows = _fetch_optional(
                    cursor,
                    (
                        f"SELECT COUNT(*) AS row_count, "
                        f"CAST(MAX(ingested_at) AS STRING) AS max_ingested_at "
                        f"FROM {full_name}"
                    ),
                    warnings,
                    f"{label} raw summary",
                )
                raw_summary[label] = rows[0] if rows else {"row_count": 0, "max_ingested_at": None}

            dataset_detail = _fetch_optional(
                cursor,
                f"""
                WITH latest AS (
                  SELECT MAX(report_date) AS report_date
                  FROM {catalog}.{mart_schema}.scorecard_dataset_quality_360
                )
                SELECT
                  q.report_date,
                  q.dataset_id,
                  q.dataset_name,
                  q.domain_name,
                  q.business_owner,
                  q.technical_owner,
                  q.criticality,
                  q.sla_freshness,
                  q.contains_pii,
                  q.is_active,
                  q.bigeye_monitor_count,
                  q.alert_count_7d,
                  q.alation_endorsed,
                  q.alation_owner,
                  q.alation_steward,
                  q.alation_lineage_present,
                  d.dataset_score,
                  d.dataset_status
                FROM {catalog}.{mart_schema}.scorecard_dataset_quality_360 q
                LEFT JOIN {catalog}.{mart_schema}.governance_mart_scorecard_dataset_daily d
                  ON q.report_date = d.report_date
                 AND q.dataset_id = d.dataset_id
                WHERE q.report_date = (SELECT report_date FROM latest)
                ORDER BY d.dataset_score ASC, q.dataset_name ASC
                """,
                warnings,
                "dataset governance detail",
            )

            domain_summary = _fetch_optional(
                cursor,
                f"""
                WITH latest AS (
                  SELECT MAX(report_date) AS report_date
                  FROM {catalog}.{mart_schema}.governance_mart_scorecard_domain_daily
                )
                SELECT
                  report_date,
                  domain_name,
                  domain_score,
                  domain_status,
                  dataset_count
                FROM {catalog}.{mart_schema}.governance_mart_scorecard_domain_daily
                WHERE report_date = (SELECT report_date FROM latest)
                ORDER BY domain_score ASC, domain_name ASC
                """,
                warnings,
                "domain governance summary",
            )

            metric_summary = _fetch_optional(
                cursor,
                f"""
                WITH latest AS (
                  SELECT MAX(report_date) AS report_date
                  FROM {catalog}.{mart_schema}.governance_scorecard_metric_daily
                )
                SELECT
                  report_date,
                  metric_id,
                  metric_name,
                  ROUND(AVG(metric_score), 2) AS avg_metric_score,
                  COUNT(*) AS dataset_count
                FROM {catalog}.{mart_schema}.governance_scorecard_metric_daily
                WHERE report_date = (SELECT report_date FROM latest)
                GROUP BY report_date, metric_id, metric_name
                ORDER BY avg_metric_score ASC, metric_name ASC
                """,
                warnings,
                "metric governance summary",
            )

            maturity_rows = _fetch_optional(
                cursor,
                f"""
                SELECT
                  collected_at,
                  env,
                  run_id,
                  commit_sha,
                  total_score,
                  overall_status,
                  blocked_reasons,
                  warned_reasons
                FROM governance_maturity.scorecard_results
                WHERE env = '{env_sql}'
                ORDER BY collected_at DESC
                LIMIT 1
                """,
                warnings,
                "databricks maturity result",
            )
            maturity_result = maturity_rows[0] if maturity_rows else None

            maturity_dimensions = _fetch_optional(
                cursor,
                f"""
                WITH latest AS (
                  SELECT MAX(collected_at) AS collected_at
                  FROM governance_maturity.scorecard_check_results
                  WHERE env = '{env_sql}'
                )
                SELECT
                  dimension,
                  ROUND(SUM(weighted_score), 2) AS dimension_score,
                  SUM(weight) AS dimension_weight,
                  SUM(CASE WHEN status_norm = 'Pass' THEN 1 ELSE 0 END) AS pass_count,
                  SUM(CASE WHEN status_norm = 'Partial' THEN 1 ELSE 0 END) AS partial_count,
                  SUM(CASE WHEN status_norm = 'Fail' THEN 1 ELSE 0 END) AS fail_count,
                  SUM(CASE WHEN status_norm = 'Unknown' THEN 1 ELSE 0 END) AS unknown_count
                FROM governance_maturity.scorecard_check_results
                WHERE env = '{env_sql}'
                  AND collected_at = (SELECT collected_at FROM latest)
                GROUP BY dimension
                ORDER BY dimension ASC
                """,
                warnings,
                "databricks maturity dimensions",
            )

            maturity_checks = _fetch_optional(
                cursor,
                f"""
                WITH latest AS (
                  SELECT MAX(collected_at) AS collected_at
                  FROM governance_maturity.scorecard_check_results
                  WHERE env = '{env_sql}'
                )
                SELECT
                  collected_at,
                  env,
                  run_id,
                  check_id,
                  dimension,
                  check_name,
                  weight,
                  status_norm,
                  score,
                  weighted_score,
                  notes
                FROM governance_maturity.scorecard_check_results
                WHERE env = '{env_sql}'
                  AND collected_at = (SELECT collected_at FROM latest)
                ORDER BY weighted_score ASC, check_id ASC
                """,
                warnings,
                "databricks maturity checks",
            )

    payload = _build_report_payload(
        catalog=catalog,
        databricks_env=databricks_env,
        raw_summary=raw_summary,
        dataset_detail=dataset_detail,
        domain_summary=domain_summary,
        metric_summary=metric_summary,
        maturity_result=maturity_result,
        maturity_dimensions=maturity_dimensions,
        maturity_checks=maturity_checks,
        warnings=warnings,
        top_n=top_n,
    )

    files = {
        "summary_markdown": output_path / "data-governance-review.md",
        "summary_json": output_path / "report_summary.json",
        "bigeye_json": output_path / "bigeye_report.json",
        "alation_json": output_path / "alation_report.json",
        "databricks_governance_json": output_path / "databricks_governance_report.json",
        "databricks_maturity_json": output_path / "databricks_maturity_report.json",
        "dataset_detail_csv": output_path / "dataset_governance_detail.csv",
        "domain_summary_csv": output_path / "domain_governance_summary.csv",
        "metric_summary_csv": output_path / "metric_governance_summary.csv",
        "maturity_checks_csv": output_path / "databricks_maturity_checks.csv",
    }

    _write_json(files["summary_json"], payload)
    _write_json(files["bigeye_json"], payload["sources"]["bigeye"])
    _write_json(files["alation_json"], payload["sources"]["alation"])
    _write_json(files["databricks_governance_json"], payload["sources"]["databricks_governance"])
    _write_json(files["databricks_maturity_json"], payload["sources"]["databricks_maturity"])
    files["summary_markdown"].write_text(
        _render_markdown(payload, top_n=top_n),
        encoding="utf-8",
    )
    _write_csv(files["dataset_detail_csv"], dataset_detail, DATASET_DETAIL_FIELDS)
    _write_csv(files["domain_summary_csv"], domain_summary, DOMAIN_SUMMARY_FIELDS)
    _write_csv(files["metric_summary_csv"], metric_summary, METRIC_SUMMARY_FIELDS)
    _write_csv(files["maturity_checks_csv"], maturity_checks, MATURITY_CHECK_FIELDS)

    return {
        "ok": True,
        "catalog": catalog,
        "databricks_env": databricks_env,
        "output_dir": str(output_path),
        "files": {key: str(path) for key, path in files.items()},
        "warnings": warnings,
        "dataset_count": payload["sources"]["databricks_governance"]["dataset_count"],
        "domain_count": payload["sources"]["databricks_governance"]["domain_count"],
        "maturity_status": payload["sources"]["databricks_maturity"]["overall_status"],
    }
