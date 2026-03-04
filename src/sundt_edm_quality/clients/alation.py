from __future__ import annotations

import base64
from typing import Any

import requests

from sundt_edm_quality.config import AlationConfig


class AlationClient:
    def __init__(self, cfg: AlationConfig, api_token: str, auth_type: str = "token") -> None:
        self.cfg = cfg
        self.session = requests.Session()
        headers = {"Accept": "application/json"}
        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {api_token}"
        else:
            headers["Token"] = api_token
        self.session.headers.update(headers)

    def fetch_assets(self) -> list[dict[str, Any]]:
        url = f"{self.cfg.base_url.rstrip('/')}{self.cfg.assets_path}"
        response = self.session.get(url, timeout=self.cfg.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("results", "data", "objects"):
                if isinstance(payload.get(key), list):
                    return payload[key]
            return [payload]
        return []

    def smoke_test(self) -> dict[str, object]:
        assets = self.fetch_assets()
        return {
            "ok": True,
            "base_url": self.cfg.base_url,
            "assets_count_sampled": len(assets),
        }

    def discover_endpoints(self) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for path in self.cfg.endpoint_candidates:
            url = f"{self.cfg.base_url.rstrip('/')}{path}"
            try:
                response = self.session.get(url, timeout=self.cfg.timeout_seconds)
                status = response.status_code
                entry: dict[str, object] = {
                    "path": path,
                    "status_code": status,
                    "ok_http": 200 <= status < 300,
                }
                try:
                    payload = response.json()
                    if isinstance(payload, list):
                        entry["payload_type"] = "list"
                        entry["sample_count"] = len(payload)
                    elif isinstance(payload, dict):
                        entry["payload_type"] = "dict"
                        entry["keys"] = sorted(payload.keys())[:10]
                    else:
                        entry["payload_type"] = type(payload).__name__
                except ValueError:
                    entry["payload_type"] = "non_json"
                results.append(entry)
            except Exception as exc:  # noqa: BLE001
                results.append(
                    {
                        "path": path,
                        "ok_http": False,
                        "error": str(exc),
                    }
                )
        return results

    @staticmethod
    def get_oauth2_client_credentials_token(
        base_url: str,
        client_id: str,
        client_secret: str,
        timeout_seconds: int = 60,
    ) -> str:
        url = f"{base_url.rstrip('/')}/oauth/v2/token"
        basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {"grant_type": "client_credentials"}
        response = requests.post(url, headers=headers, data=data, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("OAuth2 token response missing access_token")
        return str(token)

    @staticmethod
    def get_oauth1_refresh_token_access_token(
        base_url: str,
        refresh_token: str,
        client_id: str | None = None,
        client_secret: str | None = None,
        timeout_seconds: int = 60,
    ) -> str:
        url = f"{base_url.rstrip('/')}/oauth/v1/token"
        headers = {"Accept": "application/json"}
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        if client_id and client_secret:
            basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
            headers["Authorization"] = f"Basic {basic}"
        elif client_id and client_secret:
            data["client_id"] = client_id
            data["client_secret"] = client_secret
        response = requests.post(url, headers=headers, data=data, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("OAuth1 refresh response missing access_token")
        return str(token)
