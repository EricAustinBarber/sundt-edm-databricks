from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

from sundt_edm_quality.clients.alation import AlationClient
from sundt_edm_quality.clients.bigeye import BigeyeClient
from sundt_edm_quality.clients.databricks import DatabricksPublisher
from sundt_edm_quality.config import load_config, require_env
from sundt_edm_quality.databricks_bootstrap import run_scorecard_pyspark_bootstrap
from sundt_edm_quality.pipeline import run_pipeline
from sundt_edm_quality.scorecard import deploy_scorecard


def _load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            key, value = raw.split("=", 1)
            if key and key not in os.environ:
                os.environ[key] = value


def _bigeye_auth() -> dict[str, str | None]:
    token = os.getenv("BIGEYE_API_TOKEN", "").strip() or None
    username = (
        os.getenv("BIGEYE_USERNAME", "").strip()
        or os.getenv("BIGEYE_USER", "").strip()
        or None
    )
    password = (
        os.getenv("BIGEYE_PASSWORD", "").strip()
        or os.getenv("BIGEYE_PASS", "").strip()
        or None
    )
    return {
        "api_token": token,
        "username": username,
        "password": password,
    }


def _resolve_alation_auth(cfg: object) -> tuple[str, str, str]:
    # Returns: (token, auth_type, mode_used)
    mode = os.getenv("ALATION_AUTH_MODE", "auto").strip().lower()
    api_token = os.getenv("ALATION_API_TOKEN", "").strip()
    api_token_secret = os.getenv("ALATION_API_TOKEN_SECRET", "").strip()
    client_id = os.getenv("ALATION_CLIENT_ID", "").strip()
    client_secret = os.getenv("ALATION_CLIENT_SECRET", "").strip()
    refresh_token = (
        os.getenv("ALATION_REFRESH_TOKEN", "").strip()
        or os.getenv("ALATION_REFRESH_TOKEN_SECRET", "").strip()
    )

    # 1) Direct API token modes
    if mode in ("auto", "token", "api_token"):
        token = api_token or api_token_secret
        if token:
            return token, "token", "api_token"
        if mode in ("token", "api_token"):
            raise RuntimeError(
                "ALATION_AUTH_MODE=token but ALATION_API_TOKEN/ALATION_API_TOKEN_SECRET is missing."
            )

    # 2) OAuth2 client credentials
    if mode in ("auto", "oauth2", "oauth2_client_credentials"):
        if client_id and client_secret:
            token = AlationClient.get_oauth2_client_credentials_token(
                base_url=cfg.alation.base_url,
                client_id=client_id,
                client_secret=client_secret,
                timeout_seconds=cfg.alation.timeout_seconds,
            )
            return token, "bearer", "oauth2_client_credentials"
        if mode in ("oauth2", "oauth2_client_credentials"):
            raise RuntimeError(
                "ALATION_AUTH_MODE=oauth2 requires ALATION_CLIENT_ID and ALATION_CLIENT_SECRET."
            )

    # 3) OAuth1 refresh-token flow
    if mode in ("auto", "oauth1_refresh", "refresh_token"):
        if refresh_token:
            token = AlationClient.get_oauth1_refresh_token_access_token(
                base_url=cfg.alation.base_url,
                refresh_token=refresh_token,
                client_id=client_id or None,
                client_secret=client_secret or None,
                timeout_seconds=cfg.alation.timeout_seconds,
            )
            return token, "bearer", "oauth1_refresh"
        if mode in ("oauth1_refresh", "refresh_token"):
            raise RuntimeError(
                "ALATION_AUTH_MODE=oauth1_refresh requires ALATION_REFRESH_TOKEN or ALATION_REFRESH_TOKEN_SECRET."
            )

    raise RuntimeError(
        "Unable to resolve Alation auth. Provide ALATION_API_TOKEN/ALATION_API_TOKEN_SECRET "
        "or ALATION_CLIENT_ID+ALATION_CLIENT_SECRET or ALATION_REFRESH_TOKEN."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="sundt-edm-quality pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_cmd = subparsers.add_parser("run", help="Run extract + normalize + publish flow")
    run_cmd.add_argument(
        "--config",
        required=True,
        help="Path to sources YAML config",
    )
    smoke_cmd = subparsers.add_parser("smoke", help="Run connector smoke tests")
    smoke_cmd.add_argument(
        "--config",
        required=True,
        help="Path to sources YAML config",
    )
    smoke_cmd.add_argument(
        "--json-out",
        required=False,
        help="Optional path to write smoke-test JSON output",
    )
    discover_alation_cmd = subparsers.add_parser(
        "discover-alation",
        help="Probe candidate Alation endpoints and return HTTP/payload diagnostics",
    )
    discover_alation_cmd.add_argument(
        "--config",
        required=True,
        help="Path to sources YAML config",
    )
    discover_alation_cmd.add_argument(
        "--json-out",
        required=False,
        help="Optional path to write discovery JSON output",
    )
    discover_bigeye_cmd = subparsers.add_parser(
        "discover-bigeye",
        help="Probe candidate Bigeye endpoints and return HTTP/payload diagnostics",
    )
    discover_bigeye_cmd.add_argument(
        "--config",
        required=True,
        help="Path to sources YAML config",
    )
    discover_bigeye_cmd.add_argument(
        "--json-out",
        required=False,
        help="Optional path to write discovery JSON output",
    )
    scorecard_cmd = subparsers.add_parser(
        "deploy-scorecard",
        help="Deploy scorecard definitions and SQL models to Databricks",
    )
    scorecard_cmd.add_argument("--config", required=True, help="Path to sources YAML config")
    scorecard_cmd.add_argument(
        "--sql-dir",
        default="scorecard/sql",
        help="Directory containing scorecard SQL files",
    )
    scorecard_cmd.add_argument(
        "--metrics-yaml",
        default="scorecard/definitions/metrics.yaml",
        help="Metric definition YAML path",
    )
    scorecard_cmd.add_argument(
        "--critical-datasets-yaml",
        default="scorecard/definitions/critical_datasets.template.yaml",
        help="Critical dataset YAML path",
    )
    bootstrap_cmd = subparsers.add_parser(
        "bootstrap-scorecard",
        help="Run Databricks PySpark bootstrap for scorecard schemas/tables",
    )
    bootstrap_cmd.add_argument("--config", required=True, help="Path to sources YAML config")
    bootstrap_cmd.add_argument(
        "--cluster-id",
        required=False,
        help="Databricks cluster ID (defaults to DATABRICKS_CLUSTER_ID env var)",
    )
    bootstrap_cmd.add_argument(
        "--bootstrap-script",
        default="scorecard/pyspark/bootstrap_scorecard_objects.py",
        help="Local path to PySpark bootstrap script",
    )

    args = parser.parse_args(argv)
    _load_env_file()

    if args.command == "run":
        cfg = load_config(args.config)
        auth = _bigeye_auth()
        alation_token, alation_auth_type, _ = _resolve_alation_auth(cfg)
        result = run_pipeline(
            cfg=cfg,
            bigeye_token=auth["api_token"] or "",
            bigeye_username=auth["username"] or "",
            bigeye_password=auth["password"] or "",
            alation_token=alation_token,
            alation_auth_type=alation_auth_type,
            databricks_server_hostname=require_env("DATABRICKS_SERVER_HOSTNAME"),
            databricks_http_path=require_env("DATABRICKS_HTTP_PATH"),
            databricks_access_token=require_env("DATABRICKS_ACCESS_TOKEN"),
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "smoke":
        cfg = load_config(args.config)
        results: dict[str, object] = {"ok": True}

        try:
            auth = _bigeye_auth()
            bigeye = BigeyeClient(
                cfg.bigeye,
                api_token=auth["api_token"],
                username=auth["username"],
                password=auth["password"],
            )
            results["bigeye"] = bigeye.smoke_test()
        except Exception as exc:  # noqa: BLE001
            results["ok"] = False
            err = str(exc)
            hint = None
            if "401" in err:
                hint = "Credentials rejected by endpoint. Verify Bigeye username/password and auth method."
            elif "404" in err:
                hint = "Endpoint path not found. Run discover-bigeye."
            elif "500" in err:
                hint = "Likely hitting UI path instead of API path/host. Run discover-bigeye and verify BIGEYE_BASE_URL."
            discovery: object = []
            try:
                auth = _bigeye_auth()
                bigeye_discovery = BigeyeClient(
                    cfg.bigeye,
                    api_token=auth["api_token"],
                    username=auth["username"],
                    password=auth["password"],
                )
                discovery = bigeye_discovery.discover_endpoints()
            except Exception as inner_exc:  # noqa: BLE001
                discovery = [{"error": str(inner_exc)}]
            results["bigeye"] = {"ok": False, "error": err, "hint": hint, "discovery": discovery}

        try:
            alation_token, alation_auth_type, alation_mode_used = _resolve_alation_auth(cfg)
            alation = AlationClient(
                cfg.alation,
                alation_token,
                auth_type=alation_auth_type,
            )
            alation_result = alation.smoke_test()
            alation_result["auth_mode_used"] = alation_mode_used
            results["alation"] = alation_result
        except Exception as exc:  # noqa: BLE001
            results["ok"] = False
            err = str(exc)
            hint = None
            if "403" in err:
                hint = (
                    "Endpoint is reachable but token is forbidden. "
                    "Request Alation API permissions/scopes for table/schema/search endpoints."
                )
            elif "404" in err:
                hint = "Endpoint path is likely incorrect for this tenant. Run discover-alation."
            discovery: object = []
            try:
                alation_token, alation_auth_type, alation_mode_used = _resolve_alation_auth(cfg)
                alation_discovery = AlationClient(
                    cfg.alation,
                    alation_token,
                    auth_type=alation_auth_type,
                )
                discovery = alation_discovery.discover_endpoints()
            except Exception as inner_exc:  # noqa: BLE001
                discovery = [{"error": str(inner_exc)}]
            results["alation"] = {
                "ok": False,
                "error": err,
                "hint": hint,
                "auth_mode_used": locals().get("alation_mode_used"),
                "discovery": discovery,
            }

        try:
            publisher = DatabricksPublisher(
                cfg=cfg.databricks,
                server_hostname=require_env("DATABRICKS_SERVER_HOSTNAME"),
                http_path=require_env("DATABRICKS_HTTP_PATH"),
                access_token=require_env("DATABRICKS_ACCESS_TOKEN"),
            )
            results["databricks"] = publisher.smoke_test()
        except Exception as exc:  # noqa: BLE001
            results["ok"] = False
            results["databricks"] = {"ok": False, "error": str(exc)}

        rendered = json.dumps(results, indent=2)
        print(rendered)
        if args.json_out:
            out_path = Path(args.json_out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(rendered + "\n", encoding="utf-8")
        return 0 if results["ok"] else 2

    if args.command == "discover-alation":
        cfg = load_config(args.config)
        alation_token, alation_auth_type, alation_mode_used = _resolve_alation_auth(cfg)
        client = AlationClient(cfg.alation, alation_token, auth_type=alation_auth_type)
        results = {
            "ok": True,
            "base_url": cfg.alation.base_url,
            "auth_mode_used": alation_mode_used,
            "candidates": client.discover_endpoints(),
        }
        rendered = json.dumps(results, indent=2)
        print(rendered)
        if args.json_out:
            out_path = Path(args.json_out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(rendered + "\n", encoding="utf-8")
        return 0

    if args.command == "discover-bigeye":
        cfg = load_config(args.config)
        auth = _bigeye_auth()
        client = BigeyeClient(
            cfg.bigeye,
            api_token=auth["api_token"],
            username=auth["username"],
            password=auth["password"],
        )
        results = {
            "ok": True,
            "base_url": cfg.bigeye.base_url,
            "auth_mode_used": client.auth_mode,
            "candidates": client.discover_endpoints(),
        }
        rendered = json.dumps(results, indent=2)
        print(rendered)
        if args.json_out:
            out_path = Path(args.json_out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(rendered + "\n", encoding="utf-8")
        return 0

    if args.command == "deploy-scorecard":
        cfg = load_config(args.config)
        result = deploy_scorecard(
            cfg=cfg,
            databricks_server_hostname=require_env("DATABRICKS_SERVER_HOSTNAME"),
            databricks_http_path=require_env("DATABRICKS_HTTP_PATH"),
            databricks_access_token=require_env("DATABRICKS_ACCESS_TOKEN"),
            sql_dir=args.sql_dir,
            metrics_yaml_path=args.metrics_yaml,
            critical_datasets_yaml_path=args.critical_datasets_yaml,
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "bootstrap-scorecard":
        cfg = load_config(args.config)
        cluster_id = (args.cluster_id or os.getenv("DATABRICKS_CLUSTER_ID", "")).strip()
        if not cluster_id:
            raise RuntimeError(
                "Missing cluster ID. Provide --cluster-id or set DATABRICKS_CLUSTER_ID."
            )
        result = run_scorecard_pyspark_bootstrap(
            cfg=cfg,
            server_hostname=require_env("DATABRICKS_SERVER_HOSTNAME"),
            access_token=require_env("DATABRICKS_ACCESS_TOKEN"),
            cluster_id=cluster_id,
            local_bootstrap_script=args.bootstrap_script,
        )
        print(json.dumps(result, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
