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
        self.page_size = max(1, int(os.getenv("BIGEYE_PAGE_SIZE", "500")))
        self.max_rows_monitors = max(1, int(os.getenv("BIGEYE_MONITORS_MAX_ROWS", "5000")))
        self.max_rows_alerts = max(1, int(os.getenv("BIGEYE_ALERTS_MAX_ROWS", "5000")))
        self.monitors_request_json = self._json_env("BIGEYE_MONITORS_REQUEST_JSON")
        self.alerts_request_json = self._json_env("BIGEYE_ALERTS_REQUEST_JSON")
        workspace_id = os.getenv("BIGEYE_WORKSPACE_ID", "").strip()
        if workspace_id:
            self.session.headers.update({"X-Bigeye-Workspace-Id": workspace_id})
        if api_token:
            # Bigeye API keys use `Authorization: apikey <token>`.
            # Keep compatibility with bearer tokens if a tenant is configured that way.
            if api_token.startswith("bigeye_"):
                self.session.headers.update({"Authorization": f"apikey {api_token}"})
                self.auth_mode = "api_key"
            else:
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

    @staticmethod
    def _json_env(name: str) -> dict[str, Any]:
        raw = os.getenv(name, "").strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON in {name}: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"{name} must decode to a JSON object")
        return payload

    @staticmethod
    def _extract_rows(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [p if isinstance(p, dict) else {"value": str(p)} for p in payload]
        if isinstance(payload, dict):
            for key in ("results", "data", "items", "metrics", "issues", "issue", "alerts"):
                val = payload.get(key)
                if isinstance(val, list):
                    return [p if isinstance(p, dict) else {"value": str(p)} for p in val]
            return [payload]
        return []

    def _request_json(self, path: str, method: str = "GET", payload: dict[str, Any] | None = None) -> Any:
        url = f"{self.cfg.base_url.rstrip('/')}{path}"
        method_upper = method.upper()
        if method_upper == "POST":
            response = self.session.post(url, json=(payload or {}), timeout=self.cfg.timeout_seconds)
        else:
            response = self.session.get(url, timeout=self.cfg.timeout_seconds)
        response.raise_for_status()
        return response.json()

    def _request(self, path: str, method: str = "GET", payload: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return self._extract_rows(self._request_json(path, method=method, payload=payload))

    def _fetch_paginated(
        self,
        path: str,
        payload: dict[str, Any] | None,
        max_rows: int,
        default_page_size: int | None = None,
    ) -> list[dict[str, Any]]:
        body = dict(payload or {})
        collected: list[dict[str, Any]] = []
        page: int | None = int(body["page"]) if "page" in body and isinstance(body["page"], int) else None
        while True:
            raw = self._request_json(path, method="POST", payload=body)
            rows = self._extract_rows(raw)
            if not rows:
                break
            remaining = max_rows - len(collected)
            if remaining <= 0:
                break
            collected.extend(rows[:remaining])
            if len(collected) >= max_rows:
                break

            pagination = raw.get("paginationInfo") if isinstance(raw, dict) else None
            if not isinstance(pagination, dict):
                break
            current = pagination.get("currentPage")
            total = pagination.get("totalPages")
            if isinstance(current, int) and isinstance(total, int):
                if current >= total:
                    break
                page = current + 1
            else:
                next_page = pagination.get("nextPage")
                if isinstance(next_page, int):
                    page = next_page
                else:
                    break
            body["page"] = page
            if default_page_size is not None and "pageSize" not in body and "limit" not in body:
                body["pageSize"] = default_page_size
        return collected

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
        method = self._method_for_path(self.cfg.monitors_path)
        if method == "POST":
            return self._fetch_paginated(
                self.cfg.monitors_path,
                payload=self.monitors_request_json,
                max_rows=self.max_rows_monitors,
                default_page_size=None,
            )
        return self._request(self.cfg.monitors_path, method=method)

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
        method = self._method_for_path(self.cfg.alerts_path)
        if method == "POST":
            return self._fetch_paginated(
                self.cfg.alerts_path,
                payload=self.alerts_request_json,
                max_rows=self.max_rows_alerts,
                default_page_size=self.page_size,
            )
        return self._request(self.cfg.alerts_path, method=method)

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
    @staticmethod
    def _method_for_path(path: str) -> str:
        lowered = path.strip().lower()
        if lowered.endswith("/fetch") or lowered.endswith("/metrics/info"):
            return "POST"
        return "GET"
