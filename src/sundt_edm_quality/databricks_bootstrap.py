from __future__ import annotations

import base64
import json
import time
from pathlib import Path

import requests

from sundt_edm_quality.config import AppConfig


class DatabricksJobRunner:
    def __init__(self, server_hostname: str, access_token: str, timeout_seconds: int = 60) -> None:
        self.base_url = f"https://{server_hostname}"
        self.timeout_seconds = timeout_seconds
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    def _post(self, path: str, payload: dict) -> dict:
        response = requests.post(
            f"{self.base_url}{path}",
            headers=self.headers,
            data=json.dumps(payload),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def _get(self, path: str, params: dict) -> dict:
        response = requests.get(
            f"{self.base_url}{path}",
            headers=self.headers,
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def upload_file_to_dbfs(self, local_path: str, dbfs_path: str) -> None:
        content = Path(local_path).read_bytes()
        payload = {
            "path": dbfs_path,
            "contents": base64.b64encode(content).decode("ascii"),
            "overwrite": True,
        }
        self._post("/api/2.0/dbfs/put", payload)

    def submit_pyspark_file(
        self,
        cluster_id: str,
        python_file_dbfs_path: str,
        parameters: list[str],
        run_name: str = "sundt-edm-quality scorecard bootstrap",
    ) -> int:
        payload = {
            "run_name": run_name,
            "tasks": [
                {
                    "task_key": "bootstrap_scorecard",
                    "existing_cluster_id": cluster_id,
                    "spark_python_task": {
                        "python_file": python_file_dbfs_path,
                        "parameters": parameters,
                    },
                }
            ],
        }
        result = self._post("/api/2.1/jobs/runs/submit", payload)
        return int(result["run_id"])

    def wait_for_run(self, run_id: int, poll_seconds: int = 5) -> dict:
        while True:
            status = self._get("/api/2.1/jobs/runs/get", {"run_id": run_id})
            state = status.get("state", {})
            life_cycle = state.get("life_cycle_state")
            if life_cycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
                return status
            time.sleep(poll_seconds)


def run_scorecard_pyspark_bootstrap(
    cfg: AppConfig,
    server_hostname: str,
    access_token: str,
    cluster_id: str,
    local_bootstrap_script: str,
    dbfs_bootstrap_path: str = "dbfs:/tmp/sundt-edm-quality/bootstrap_scorecard_objects.py",
) -> dict[str, object]:
    runner = DatabricksJobRunner(server_hostname=server_hostname, access_token=access_token)
    runner.upload_file_to_dbfs(
        local_path=local_bootstrap_script,
        dbfs_path=dbfs_bootstrap_path.replace("dbfs:", ""),
    )
    run_id = runner.submit_pyspark_file(
        cluster_id=cluster_id,
        python_file_dbfs_path=dbfs_bootstrap_path,
        parameters=[
            "--catalog",
            cfg.databricks.catalog,
            "--schema-raw",
            cfg.databricks.schema_raw,
            "--schema-staging",
            cfg.databricks.schema_staging,
            "--schema-mart",
            "mart",
        ],
    )
    result = runner.wait_for_run(run_id)
    state = result.get("state", {})
    return {
        "run_id": run_id,
        "life_cycle_state": state.get("life_cycle_state"),
        "result_state": state.get("result_state"),
        "state_message": state.get("state_message"),
    }
