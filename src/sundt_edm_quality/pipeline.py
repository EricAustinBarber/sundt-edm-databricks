from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sundt_edm_quality.clients.alation import AlationClient
from sundt_edm_quality.clients.bigeye import BigeyeClient
from sundt_edm_quality.clients.databricks import DatabricksPublisher
from sundt_edm_quality.config import AppConfig


def _normalize_dataset_name(value: Any) -> str:
    return str(value or "").strip().lower()


def _to_dataset_quality_seed(
    bigeye_monitors: Iterable[dict[str, Any]],
    bigeye_alerts: Iterable[dict[str, Any]],
    alation_assets: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    # Build a simple seed set to support the first 360-quality mart model.
    bigeye_by_dataset: dict[str, dict[str, Any]] = {}
    for m in bigeye_monitors:
        dataset_name = _normalize_dataset_name(m.get("dataset_name") or m.get("dataset"))
        if not dataset_name:
            continue
        row = bigeye_by_dataset.setdefault(
            dataset_name,
            {
                "dataset_name": dataset_name,
                "bigeye_monitor_count": 0,
                "bigeye_alert_count": 0,
            },
        )
        row["bigeye_monitor_count"] += 1

    for a in bigeye_alerts:
        dataset_name = _normalize_dataset_name(a.get("dataset_name") or a.get("dataset"))
        if not dataset_name:
            continue
        row = bigeye_by_dataset.setdefault(
            dataset_name,
            {
                "dataset_name": dataset_name,
                "bigeye_monitor_count": 0,
                "bigeye_alert_count": 0,
            },
        )
        row["bigeye_alert_count"] += 1

    alation_by_dataset: dict[str, dict[str, Any]] = {}
    for a in alation_assets:
        dataset_name = _normalize_dataset_name(
            a.get("dataset_name") or a.get("name") or a.get("title")
        )
        if not dataset_name:
            continue
        alation_by_dataset[dataset_name] = {
            "alation_endorsed": bool(a.get("endorsed_flag") or a.get("endorsed")),
            "alation_owner": a.get("owner"),
            "alation_steward": a.get("steward"),
            "alation_lineage_present": bool(a.get("lineage_present")),
        }

    all_dataset_names = sorted(set(bigeye_by_dataset) | set(alation_by_dataset))
    rows: list[dict[str, Any]] = []
    for name in all_dataset_names:
        b = bigeye_by_dataset.get(
            name,
            {"bigeye_monitor_count": 0, "bigeye_alert_count": 0},
        )
        a = alation_by_dataset.get(
            name,
            {
                "alation_endorsed": False,
                "alation_owner": None,
                "alation_steward": None,
                "alation_lineage_present": False,
            },
        )
        rows.append(
            {
                "dataset_name": name,
                **b,
                **a,
            }
        )
    return rows


def run_pipeline(
    cfg: AppConfig,
    bigeye_token: str,
    bigeye_username: str,
    bigeye_password: str,
    alation_token: str,
    alation_auth_type: str,
    databricks_server_hostname: str,
    databricks_http_path: str,
    databricks_access_token: str,
) -> dict[str, int]:
    bigeye = BigeyeClient(
        cfg.bigeye,
        api_token=bigeye_token or None,
        username=bigeye_username or None,
        password=bigeye_password or None,
    )
    alation = AlationClient(cfg.alation, alation_token, auth_type=alation_auth_type)

    bigeye_monitors = bigeye.fetch_monitors()
    bigeye_alerts = bigeye.fetch_alerts()
    alation_assets = alation.fetch_assets()
    seed_rows = _to_dataset_quality_seed(bigeye_monitors, bigeye_alerts, alation_assets)

    publisher = DatabricksPublisher(
        cfg=cfg.databricks,
        server_hostname=databricks_server_hostname,
        http_path=databricks_http_path,
        access_token=databricks_access_token,
    )
    publisher.publish_json_payloads(
        bigeye_monitors=bigeye_monitors,
        bigeye_alerts=bigeye_alerts,
        alation_assets=alation_assets,
        seed_rows=seed_rows,
    )

    return {
        "bigeye_monitors": len(bigeye_monitors),
        "bigeye_alerts": len(bigeye_alerts),
        "alation_assets": len(alation_assets),
        "seed_rows": len(seed_rows),
    }
