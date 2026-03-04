# Databricks notebook source
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema_raw", "raw")
dbutils.widgets.text("schema_staging", "staging")
dbutils.widgets.text("schema_mart", "mart")

CATALOG = dbutils.widgets.get("catalog") or "main"
SCHEMA_RAW = dbutils.widgets.get("schema_raw") or "raw"
SCHEMA_STAGING = dbutils.widgets.get("schema_staging") or "staging"
SCHEMA_MART = dbutils.widgets.get("schema_mart") or "mart"

import json
from pathlib import Path

from pyspark.sql import functions as F


def _workspace_files_root() -> Path:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # Job context can return /Shared/...; local file access needs /Workspace/...
    if not notebook_path.startswith("/Workspace/"):
        if notebook_path.startswith("/"):
            notebook_path = f"/Workspace{notebook_path}"
        else:
            notebook_path = f"/Workspace/{notebook_path}"
    marker = "/files/resources/notebooks/quality/"
    if marker not in notebook_path:
        raise RuntimeError(f"Unable to resolve workspace files root from notebook path: {notebook_path}")
    return Path(notebook_path.split(marker, 1)[0] + "/files")


def _rewrite_catalog_and_schemas(sql_text: str) -> str:
    rewritten = sql_text
    rewritten = rewritten.replace("main.raw.", f"{CATALOG}.{SCHEMA_RAW}.")
    rewritten = rewritten.replace("main.staging.", f"{CATALOG}.{SCHEMA_STAGING}.")
    rewritten = rewritten.replace("main.mart.", f"{CATALOG}.{SCHEMA_MART}.")
    return rewritten


def _run_sql_file(path: Path) -> int:
    sql_text = _rewrite_catalog_and_schemas(path.read_text(encoding="utf-8"))
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    for stmt in statements:
        spark.sql(stmt)
    return len(statements)


def _replace_json_rows(table_name: str, rows: list[dict]) -> int:
    spark.sql(f"TRUNCATE TABLE {table_name}")
    payload_rows = [(json.dumps(row),) for row in rows]
    if not payload_rows:
        return 0
    df = (
        spark.createDataFrame(payload_rows, ["payload"])
        .withColumn("ingested_at", F.current_timestamp())
        .select("ingested_at", "payload")
    )
    df.write.mode("append").format("delta").saveAsTable(table_name)
    return len(payload_rows)


files_root = _workspace_files_root()
assets_candidates = [
    files_root / "resources" / "quality_assets",
    Path(str(files_root).replace("/Workspace/", "/", 1)) / "resources" / "quality_assets",
]
assets_root = next((p for p in assets_candidates if p.exists()), assets_candidates[0])
definitions_root = assets_root / "definitions"
sql_root = assets_root / "sql"

if not definitions_root.exists() or not sql_root.exists():
    raise FileNotFoundError(
        "Unable to locate quality assets directory. "
        f"Tried: {[str(p) for p in assets_candidates]}"
    )

metrics_payload = json.loads((definitions_root / "metrics.json").read_text(encoding="utf-8"))
datasets_payload = json.loads(
    (definitions_root / "critical_datasets.template.json").read_text(encoding="utf-8")
)

metric_rows = metrics_payload.get("metrics", [])
dataset_rows = datasets_payload.get("datasets", [])
if not isinstance(metric_rows, list) or not isinstance(dataset_rows, list):
    raise RuntimeError("Invalid definition JSON payloads. Expected 'metrics' and 'datasets' arrays.")

metric_table = f"{CATALOG}.{SCHEMA_STAGING}.sliver_scorecard_metric_definitions_json"
dataset_table = f"{CATALOG}.{SCHEMA_STAGING}.sliver_critical_datasets_json"

metric_count = _replace_json_rows(metric_table, metric_rows)
dataset_count = _replace_json_rows(dataset_table, dataset_rows)

sql_files = sorted(sql_root.glob("*.sql"))
statement_count = 0
for sql_file in sql_files:
    executed = _run_sql_file(sql_file)
    statement_count += executed
    print(f"[quality-deploy] executed {executed} statements from {sql_file.name}")

print(
    f"[quality-deploy] complete metrics={metric_count} datasets={dataset_count} "
    f"sql_files={len(sql_files)} sql_statements={statement_count} "
    f"catalog={CATALOG} raw={SCHEMA_RAW} staging={SCHEMA_STAGING} mart={SCHEMA_MART}"
)
