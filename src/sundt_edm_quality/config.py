from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import yaml


@dataclass
class BigeyeConfig:
    base_url: str
    monitors_path: str
    alerts_path: str
    endpoint_candidates: list[str]
    timeout_seconds: int


@dataclass
class AlationConfig:
    base_url: str
    assets_path: str
    endpoint_candidates: list[str]
    timeout_seconds: int


@dataclass
class DatabricksConfig:
    catalog: str
    schema_raw: str
    schema_staging: str
    table_bigeye_monitors_json: str
    table_bigeye_alerts_json: str
    table_alation_assets_json: str
    table_dataset_quality_seed: str


@dataclass
class AppConfig:
    bigeye: BigeyeConfig
    alation: AlationConfig
    databricks: DatabricksConfig


def _read_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_config(path: str) -> AppConfig:
    payload = _read_yaml(path)

    bigeye = payload["bigeye"]
    alation = payload["alation"]
    databricks = payload["databricks"]

    return AppConfig(
        bigeye=BigeyeConfig(
            base_url=(
                os.getenv("BIGEYE_API_BASE_URL", "").strip()
                or os.getenv("BIGEYE_BASE_URL", "").strip()
                or bigeye["base_url"]
            ),
            monitors_path=bigeye["endpoints"]["monitors"],
            alerts_path=bigeye["endpoints"]["alerts"],
            endpoint_candidates=bigeye.get(
                "endpoint_candidates",
                [
                    "/api/v1/monitors",
                    "/api/v1/alerts",
                    "/api/login",
                    "/api/v1/login",
                    "/api/session",
                    "/monitors",
                    "/alerts",
                    "/status",
                ],
            ),
            timeout_seconds=int(bigeye.get("timeout_seconds", 60)),
        ),
        alation=AlationConfig(
            base_url=(
                os.getenv("ALATION_API_BASE_URL", "").strip()
                or os.getenv("ALATION_BASE_URL", "").strip()
                or alation["base_url"]
            ),
            assets_path=alation["endpoints"]["assets"],
            endpoint_candidates=alation.get(
                "endpoint_candidates",
                [
                    "/api/v1/assets",
                    "/integration/v1/assets",
                    "/api/v1/table",
                    "/api/v1/dataset",
                    "/api/v1/search",
                ],
            ),
            timeout_seconds=int(alation.get("timeout_seconds", 60)),
        ),
        databricks=DatabricksConfig(
            catalog=databricks["catalog"],
            schema_raw=databricks["schema_raw"],
            schema_staging=databricks["schema_staging"],
            table_bigeye_monitors_json=databricks["table_bigeye_monitors_json"],
            table_bigeye_alerts_json=databricks["table_bigeye_alerts_json"],
            table_alation_assets_json=databricks["table_alation_assets_json"],
            table_dataset_quality_seed=databricks["table_dataset_quality_seed"],
        ),
    )


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
