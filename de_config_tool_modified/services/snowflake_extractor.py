"""
snowflake_extractor.py — Snowflake Pipeline Config Extractor
-------------------------------------------------------------
Supports:
  - Snowflake Tasks + DAG Pipelines
  - Snowflake Streams (CDC)
  - Stored Procedure Pipelines
  - Demo / Mock mode
"""

from __future__ import annotations
import json
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

SNOWFLAKE_MOCK_TEMPLATES = {
    "sf-cdc-pipeline": {
        "framework_type": "Snowflake CDC Pipeline (Streams + Tasks)",
        "pipeline_name": "sf-cdc-pipeline",
        "source_config": {
            "type": "Snowflake_Stream",
            "account": "myorg-myaccount",
            "database": "PROD_DB",
            "schema": "RAW",
            "stream_name": "ORDERS_STREAM",
            "base_table": "ORDERS",
            "stream_type": "STANDARD",  # STANDARD | APPEND_ONLY | INSERT_ONLY
            "created_on": "2025-01-15T10:00:00Z",
            "data_retention_days": 14,
        },
        "ingestion_config": {
            "pipeline_type": "Snowflake_Task_DAG",
            "tasks": [
                {
                    "name": "VALIDATE_ORDERS_TASK",
                    "schedule": "USING CRON 0 */2 * * * UTC",
                    "warehouse": "COMPUTE_WH",
                    "condition": "SYSTEM$STREAM_HAS_DATA('ORDERS_STREAM')",
                    "definition": "CALL VALIDATE_ORDERS_SP();",
                    "predecessor": None,
                },
                {
                    "name": "TRANSFORM_ORDERS_TASK",
                    "warehouse": "TRANSFORM_WH",
                    "condition": None,
                    "definition": "INSERT INTO SILVER.ORDERS SELECT ... FROM RAW.ORDERS_STREAM",
                    "predecessor": "VALIDATE_ORDERS_TASK",
                },
                {
                    "name": "AGGREGATE_ORDERS_TASK",
                    "warehouse": "COMPUTE_WH",
                    "definition": "CALL AGGREGATE_ORDERS_GOLD_SP();",
                    "predecessor": "TRANSFORM_ORDERS_TASK",
                },
            ],
            "batch_mode": "micro_batch",
            "watermark": {"enabled": True, "column": "UPDATED_AT"},
        },
        "dq_config": {
            "framework": "Snowflake_Stored_Procedure_DQ",
            "procedure": "VALIDATE_ORDERS_SP",
            "checks": [
                {"column": "ORDER_ID", "rule": "NOT NULL", "action": "REJECT", "severity": "CRITICAL"},
                {"column": "CUSTOMER_ID", "rule": "NOT NULL", "action": "REJECT", "severity": "CRITICAL"},
                {"column": "ORDER_AMOUNT", "rule": "BETWEEN 0 AND 1000000", "action": "QUARANTINE", "severity": "HIGH"},
                {"column": "ORDER_DATE", "rule": "NOT NULL AND >= '2000-01-01'", "action": "REJECT", "severity": "CRITICAL"},
                {"table": "ORDERS", "rule": "ROW_COUNT > 0", "action": "ALERT", "severity": "MEDIUM"},
            ],
            "quarantine_table": "RAW.ORDERS_QUARANTINE",
            "dq_log_table": "DQ.DQ_RESULTS_LOG",
        },
        "raw_metadata": {
            "account": "myorg-myaccount",
            "database": "PROD_DB",
            "warehouse": "COMPUTE_WH",
            "role": "SYSADMIN",
            "region": "us-east-1",
            "sink_schema": "SILVER",
            "sink_table": "ORDERS",
        },
    },
    "sf-dbt-pipeline": {
        "framework_type": "Snowflake dbt Pipeline",
        "pipeline_name": "sf-dbt-pipeline",
        "source_config": {
            "type": "Snowflake_Table",
            "account": "myorg-myaccount",
            "database": "RAW_DB",
            "schema": "SALESFORCE",
            "tables": ["ACCOUNT", "CONTACT", "OPPORTUNITY", "LEAD"],
            "extraction_mode": "incremental",
            "unique_key": "ID",
            "updated_at": "SYSTEMMODSTAMP",
        },
        "ingestion_config": {
            "pipeline_type": "dbt_on_Snowflake",
            "dbt_version": "1.7.0",
            "models": [
                {"name": "stg_sf_accounts", "layer": "staging", "materialized": "view"},
                {"name": "stg_sf_contacts", "layer": "staging", "materialized": "view"},
                {"name": "int_crm_unified", "layer": "intermediate", "materialized": "ephemeral"},
                {"name": "fct_opportunities", "layer": "marts", "materialized": "incremental"},
            ],
            "task_schedule": "USING CRON 0 6 * * * UTC",
            "warehouse": "TRANSFORM_WH",
            "threads": 8,
        },
        "dq_config": {
            "framework": "dbt_tests",
            "tests": [
                {"model": "stg_sf_accounts", "test": "not_null", "column": "account_id"},
                {"model": "stg_sf_accounts", "test": "unique", "column": "account_id"},
                {"model": "fct_opportunities", "test": "not_null", "column": "opportunity_id"},
                {"model": "fct_opportunities", "test": "accepted_values", "column": "stage", "values": ["Prospecting", "Qualification", "Closed Won", "Closed Lost"]},
                {"model": "fct_opportunities", "test": "relationships", "column": "account_id", "ref": "stg_sf_accounts"},
            ],
            "severity": "warn",
            "store_failures": True,
            "failure_schema": "dbt_test_failures",
        },
        "raw_metadata": {
            "account": "myorg-myaccount",
            "target_database": "ANALYTICS_DB",
            "target_schema": "dbt_prod",
            "profile": "snowflake_prod",
            "tags": ["crm", "salesforce", "daily"],
        },
    },
    "sf-dynamic-tables": {
        "framework_type": "Snowflake Dynamic Tables",
        "pipeline_name": "sf-dynamic-tables",
        "source_config": {
            "type": "Snowflake_External_Stage",
            "stage": "@MY_S3_STAGE",
            "path": "raw/events/",
            "file_format": "PARQUET",
            "pattern": ".*\\.parquet",
        },
        "ingestion_config": {
            "pipeline_type": "Dynamic_Tables",
            "lag": "1 hour",
            "warehouse": "INGEST_WH",
            "tables": [
                {
                    "name": "DT_EVENTS_BRONZE",
                    "query": "SELECT $1:event_id::STRING, $1:user_id::STRING, $1:timestamp::TIMESTAMP FROM @MY_S3_STAGE",
                    "lag": "1 hour",
                },
                {
                    "name": "DT_EVENTS_SILVER",
                    "query": "SELECT * FROM DT_EVENTS_BRONZE WHERE event_id IS NOT NULL AND user_id IS NOT NULL",
                    "lag": "2 hours",
                },
            ],
            "snowpipe": {"enabled": True, "auto_ingest": True, "sqs_arn": "arn:aws:sqs:us-east-1:123:snowpipe-queue"},
        },
        "dq_config": {
            "framework": "Snowflake_Constraints",
            "constraints": [
                {"table": "DT_EVENTS_SILVER", "column": "EVENT_ID", "type": "NOT NULL"},
                {"table": "DT_EVENTS_SILVER", "column": "USER_ID", "type": "NOT NULL"},
            ],
            "monitoring": {"enabled": True, "alert_on_zero_rows": True, "check_interval_minutes": 30},
        },
        "raw_metadata": {
            "account": "myorg-myaccount",
            "database": "EVENTS_DB",
            "schema": "RAW",
            "integration": "MY_S3_INTEGRATION",
        },
    },
}

SNOWFLAKE_PIPELINE_LIST = [
    {"name": "sf-cdc-pipeline", "type": "Streams + Tasks", "status": "Active", "schedule": "0 */2 * * *"},
    {"name": "sf-dbt-pipeline", "type": "dbt on Snowflake", "status": "Active", "schedule": "0 6 * * *"},
    {"name": "sf-dynamic-tables", "type": "Dynamic Tables + Snowpipe", "status": "Active", "schedule": "1 hour lag"},
]


def extract_snowflake_mock(pipeline_name: str) -> dict[str, Any]:
    key = None
    for k in SNOWFLAKE_MOCK_TEMPLATES:
        if pipeline_name.lower() in k.lower() or k.lower() in pipeline_name.lower():
            key = k
            break
    if not key:
        key = list(SNOWFLAKE_MOCK_TEMPLATES.keys())[0]
    t = SNOWFLAKE_MOCK_TEMPLATES[key]
    return {
        "pipeline_name": pipeline_name,
        "framework": t["framework_type"],
        "source_config": t["source_config"],
        "ingestion_config": t["ingestion_config"],
        "dq_config": t["dq_config"],
        "raw_metadata": t.get("raw_metadata", {}),
        "_demo": True,
    }


# ── Real Snowflake Extractor ───────────────────────────────────────────────────

class SnowflakeExtractor:
    """
    Extracts task/stream/procedure config from Snowflake account.
    """

    def __init__(
        self,
        pipeline_name: str,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
    ):
        self.pipeline_name = pipeline_name
        self.conn_params = {k: v for k, v in {
            "account": account, "user": user, "password": password,
            "warehouse": warehouse, "database": database, "schema": schema, "role": role,
        }.items() if v}

    def _get_connection(self):
        try:
            import snowflake.connector
            return snowflake.connector.connect(**self.conn_params)
        except ImportError:
            raise ImportError("snowflake-connector-python required: pip install snowflake-connector-python")

    def extract(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "pipeline_name": self.pipeline_name,
            "framework": "Snowflake",
            "source_config": {},
            "ingestion_config": {},
            "dq_config": {},
            "raw_metadata": {},
        }
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            # Discover tasks
            tasks = self._get_tasks(cur)
            streams = self._get_streams(cur)
            result["ingestion_config"] = {"tasks": tasks}
            result["source_config"] = {"streams": streams}
            result["raw_metadata"] = {"task_count": len(tasks), "stream_count": len(streams)}
            cur.close()
            conn.close()
        except Exception as exc:
            logger.warning("Snowflake extraction failed: %s", exc)
            result["raw_metadata"]["extraction_error"] = str(exc)
        return result

    def _get_tasks(self, cur) -> list:
        cur.execute(f"SHOW TASKS LIKE '%{self.pipeline_name.upper()}%'")
        rows = cur.fetchall()
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in rows[:20]]

    def _get_streams(self, cur) -> list:
        cur.execute(f"SHOW STREAMS LIKE '%{self.pipeline_name.upper()}%'")
        rows = cur.fetchall()
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in rows[:20]]
