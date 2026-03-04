from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from databricks import sql

from sundt_edm_quality.config import DatabricksConfig


class DatabricksPublisher:
    def __init__(
        self,
        cfg: DatabricksConfig,
        server_hostname: str,
        http_path: str,
        access_token: str,
    ) -> None:
        self.cfg = cfg
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token

    def _table_name(self, schema: str, table: str) -> str:
        return f"{self.cfg.catalog}.{schema}.{table}"

    def _ensure_schema(self, cursor: Any, schema: str) -> None:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.cfg.catalog}.{schema}")

    def _ensure_json_table(self, cursor: Any, schema: str, table: str) -> None:
        self._ensure_schema(cursor, schema)
        full_name = self._table_name(schema, table)
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {full_name} (
              ingested_at TIMESTAMP,
              payload STRING
            )
            """
            )

    @staticmethod
    def _split_sql_statements(sql_text: str) -> list[str]:
        parts = [p.strip() for p in sql_text.split(";")]
        return [p for p in parts if p]

    def _insert_json_rows(
        self,
        cursor: Any,
        schema: str,
        table: str,
        rows: Iterable[dict[str, Any]],
    ) -> None:
        full_name = self._table_name(schema, table)
        values = [(json.dumps(row, ensure_ascii=True),) for row in rows]
        if not values:
            return
        cursor.executemany(
            f"INSERT INTO {full_name} (ingested_at, payload) VALUES (current_timestamp(), ?)",
            values,
        )

    def publish_json_payloads(
        self,
        bigeye_monitors: list[dict[str, Any]],
        bigeye_alerts: list[dict[str, Any]],
        alation_assets: list[dict[str, Any]],
        seed_rows: list[dict[str, Any]],
    ) -> None:
        with sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
        ) as conn:
            with conn.cursor() as cursor:
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_bigeye_monitors_json,
                )
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_bigeye_alerts_json,
                )
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_alation_assets_json,
                )
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_staging,
                    self.cfg.table_dataset_quality_seed,
                )
                self._insert_json_rows(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_bigeye_monitors_json,
                    bigeye_monitors,
                )
                self._insert_json_rows(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_bigeye_alerts_json,
                    bigeye_alerts,
                )
                self._insert_json_rows(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_alation_assets_json,
                    alation_assets,
                )
                self._insert_json_rows(
                    cursor,
                    self.cfg.schema_staging,
                    self.cfg.table_dataset_quality_seed,
                    seed_rows,
                )
            conn.commit()

    def ensure_scorecard_definition_tables(self) -> None:
        with sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
        ) as conn:
            with conn.cursor() as cursor:
                self._ensure_schema(cursor, self.cfg.schema_raw)
                self._ensure_schema(cursor, self.cfg.schema_staging)
                self._ensure_schema(cursor, "mart")
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_bigeye_monitors_json,
                )
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_bigeye_alerts_json,
                )
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_raw,
                    self.cfg.table_alation_assets_json,
                )
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_staging,
                    "sliver_scorecard_metric_definitions_json",
                )
                self._ensure_json_table(
                    cursor,
                    self.cfg.schema_staging,
                    "sliver_critical_datasets_json",
                )
            conn.commit()

    def replace_json_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> None:
        with sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
        ) as conn:
            with conn.cursor() as cursor:
                self._ensure_json_table(cursor, schema, table)
                full_name = self._table_name(schema, table)
                cursor.execute(f"DELETE FROM {full_name}")
                self._insert_json_rows(cursor, schema, table, rows)
            conn.commit()

    def execute_sql_files(self, sql_files: list[tuple[str, str]]) -> None:
        with sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
        ) as conn:
            with conn.cursor() as cursor:
                for _, sql_text in sql_files:
                    for stmt in self._split_sql_statements(sql_text):
                        cursor.execute(stmt)
            conn.commit()

    def smoke_test(self) -> dict[str, object]:
        with sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_catalog(), current_timestamp()")
                row = cursor.fetchone()
                catalog = row[0] if row else None
        return {
            "ok": True,
            "server_hostname": self.server_hostname,
            "current_catalog": catalog,
        }
