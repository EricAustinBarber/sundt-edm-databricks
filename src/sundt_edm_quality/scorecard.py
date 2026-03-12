from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from sundt_edm_quality.clients.databricks import DatabricksPublisher
from sundt_edm_quality.config import AppConfig


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_sql_files(sql_dir: str) -> list[tuple[str, str]]:
    root = Path(sql_dir)
    files = sorted(root.glob("*.sql"))
    return [(f.name, f.read_text(encoding="utf-8")) for f in files]


def _rewrite_sql_catalogs(sql_text: str, catalog: str, schema_raw: str, schema_staging: str) -> str:
    rewritten = sql_text
    rewritten = rewritten.replace("main.raw.", f"{catalog}.{schema_raw}.")
    rewritten = rewritten.replace("main.staging.", f"{catalog}.{schema_staging}.")
    rewritten = rewritten.replace("main.mart.", f"{catalog}.mart.")
    return rewritten


def _definition_rows(path: str, key: str) -> list[dict[str, Any]]:
    payload = _load_yaml(path)
    rows = payload.get(key, [])
    if not isinstance(rows, list):
        raise RuntimeError(f"Expected list at '{key}' in {path}")
    return [r for r in rows if isinstance(r, dict)]


def deploy_scorecard(
    cfg: AppConfig,
    databricks_server_hostname: str,
    databricks_http_path: str,
    databricks_access_token: str,
    sql_dir: str,
    metrics_yaml_path: str,
    critical_datasets_yaml_path: str,
) -> dict[str, int]:
    metric_rows = _definition_rows(metrics_yaml_path, "metrics")
    dataset_rows = _definition_rows(critical_datasets_yaml_path, "datasets")
    sql_files = [
        (
            name,
            _rewrite_sql_catalogs(
                sql_text,
                catalog=cfg.databricks.catalog,
                schema_raw=cfg.databricks.schema_raw,
                schema_staging=cfg.databricks.schema_staging,
            ),
        )
        for name, sql_text in _load_sql_files(sql_dir)
    ]

    publisher = DatabricksPublisher(
        cfg=cfg.databricks,
        server_hostname=databricks_server_hostname,
        http_path=databricks_http_path,
        access_token=databricks_access_token,
    )

    # Ensure raw/staging/mart objects exist before creating dependent views.
    publisher.ensure_scorecard_definition_tables()
    publisher.execute_sql_files(sql_files)
    publisher.replace_json_rows(
        schema=cfg.databricks.schema_staging,
        table="sliver_scorecard_metric_definitions_json",
        rows=metric_rows,
    )
    publisher.replace_json_rows(
        schema=cfg.databricks.schema_staging,
        table="sliver_critical_datasets_json",
        rows=dataset_rows,
    )

    return {
        "metric_definition_rows": len(metric_rows),
        "critical_dataset_rows": len(dataset_rows),
        "sql_files_executed": len(sql_files),
    }
