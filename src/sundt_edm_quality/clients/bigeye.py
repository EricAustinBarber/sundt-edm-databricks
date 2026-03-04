from __future__ import annotations

import base64
import json
import os
from typing import Any

import requests

from sundt_edm_quality.config import BigeyeConfig


class BigeyeClient:
    def __init__(
        self,
        cfg: BigeyeConfig,
        api_token: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.sdk_client: Any | None = None
        self.auth_mode = "unknown"
        if api_token:
            self.session.headers.update({"Authorization": f"Bearer {api_token}"})
            self.auth_mode = "token"
        elif username and password:
            # Preferred for username/password: Bigeye SDK auth flow.
            # Fallback to basic auth over HTTP only if SDK is unavailable.
            try:
                from bigeye_sdk.authentication.api_authentication import ApiAuth
                from bigeye_sdk.client.datawatch_client import datawatch_client_factory
            except Exception:
                self.session.auth = (username, password)
                self.auth_mode = "basic_auth_fallback"
            else:
                creds = json.dumps(
                    {
                        "base_url": self.cfg.base_url,
                        "user": username,
                        "password": password,
                    }
                ).encode("utf-8")
                api_auth = ApiAuth.load_from_base64(base64.b64encode(creds))
                ws_raw = os.getenv("BIGEYE_WORKSPACE_ID", "").strip()
                workspace_id = int(ws_raw) if ws_raw else None
                self.sdk_client = datawatch_client_factory(api_auth, workspace_id=workspace_id)
                self.auth_mode = "sdk_user_password"
        else:
            raise RuntimeError(
                "Bigeye auth missing. Set BIGEYE_API_TOKEN or BIGEYE_USERNAME and BIGEYE_PASSWORD."
            )

    def _get(self, path: str) -> list[dict[str, Any]]:
        url = f"{self.cfg.base_url.rstrip('/')}{path}"
        response = self.session.get(url, timeout=self.cfg.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("results", "data", "items"):
                if isinstance(payload.get(key), list):
                    return payload[key]
            return [payload]
        return []

    def fetch_monitors(self) -> list[dict[str, Any]]:
        if self.sdk_client is not None:
            # Method names differ by SDK version; try common candidates.
            for method_name in (
                "get_monitors",
                "list_monitors",
                "get_metrics",
                "list_metrics",
            ):
                method = getattr(self.sdk_client, method_name, None)
                if callable(method):
                    payload = method()
                    return self._normalize_sdk_payload(payload)
            raise RuntimeError(
                "Bigeye SDK authenticated, but no monitor/metric listing method was found on DatawatchClient."
            )
        return self._get(self.cfg.monitors_path)

    def fetch_alerts(self) -> list[dict[str, Any]]:
        if self.sdk_client is not None:
            for method_name in (
                "get_alerts",
                "list_alerts",
                "get_issues",
                "list_issues",
            ):
                method = getattr(self.sdk_client, method_name, None)
                if callable(method):
                    payload = method()
                    return self._normalize_sdk_payload(payload)
            # Alerts endpoint may not be available in all SDK versions.
            return []
        return self._get(self.cfg.alerts_path)

    def smoke_test(self) -> dict[str, object]:
        monitors_count = 0
        alerts_count = 0
        workspace_count = None

        if self.sdk_client is not None:
            get_workspaces = getattr(self.sdk_client, "get_workspaces", None)
            if callable(get_workspaces):
                workspaces_payload = get_workspaces()
                workspaces = getattr(workspaces_payload, "workspaces", None)
                if isinstance(workspaces, list):
                    workspace_count = len(workspaces)
            # Best-effort counts
            try:
                monitors_count = len(self.fetch_monitors())
            except Exception:
                monitors_count = 0
            try:
                alerts_count = len(self.fetch_alerts())
            except Exception:
                alerts_count = 0
        else:
            monitors_count = len(self.fetch_monitors())
            alerts_count = len(self.fetch_alerts())

        return {
            "ok": True,
            "base_url": self.cfg.base_url,
            "auth_mode_used": self.auth_mode,
            "workspace_count_sampled": workspace_count,
            "monitors_count_sampled": monitors_count,
            "alerts_count_sampled": alerts_count,
        }

    def discover_endpoints(self) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for path in self.cfg.endpoint_candidates:
            url = f"{self.cfg.base_url.rstrip('/')}{path}"
            try:
                response = self.session.get(url, timeout=self.cfg.timeout_seconds)
                entry: dict[str, object] = {
                    "path": path,
                    "status_code": response.status_code,
                    "ok_http": 200 <= response.status_code < 300,
                    "content_type": response.headers.get("content-type", ""),
                }
                if "application/json" in entry["content_type"]:
                    try:
                        payload = response.json()
                        if isinstance(payload, dict):
                            entry["keys"] = sorted(payload.keys())[:10]
                        elif isinstance(payload, list):
                            entry["sample_count"] = len(payload)
                    except ValueError:
                        pass
                results.append(entry)
            except Exception as exc:  # noqa: BLE001
                results.append({"path": path, "ok_http": False, "error": str(exc)})
        return results

    @staticmethod
    def _normalize_sdk_payload(payload: Any) -> list[dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return [p if isinstance(p, dict) else {"value": str(p)} for p in payload]
        if isinstance(payload, dict):
            for key in ("items", "results", "data", "monitors", "alerts", "issues", "metrics"):
                val = payload.get(key)
                if isinstance(val, list):
                    return [p if isinstance(p, dict) else {"value": str(p)} for p in val]
            return [payload]

        # SDK model object: inspect common attributes.
        for key in ("items", "results", "data", "monitors", "alerts", "issues", "metrics"):
            val = getattr(payload, key, None)
            if isinstance(val, list):
                return [p if isinstance(p, dict) else {"value": str(p)} for p in val]

        return [{"value": str(payload)}]
