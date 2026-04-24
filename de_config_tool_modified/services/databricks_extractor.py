"""
databricks_extractor.py — Databricks Pipeline Config Extractor
---------------------------------------------------------------
Supports:
  - Databricks Jobs
  - Databricks Notebooks (code-level parsing)
  - Delta Live Tables pipelines
  - Demo / Mock mode
"""

from __future__ import annotations
import json
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DQ_PATTERNS = re.compile(
    r"(expectation|dlt\.expect|quality|validate|not_null|is_not_null|"
    r"constraint|reject|warn|fail|quarantine|row_count|unique|"
    r"greatexpectations|soda|deequ|assert)",
    re.IGNORECASE,
)

# ── Mock Templates ─────────────────────────────────────────────────────────────

DATABRICKS_MOCK_TEMPLATES = {
    "dlt-bronze-silver": {
        "framework_type": "Databricks Delta Live Tables",
        "job_name": "dlt-bronze-silver",
        "source_config": {
            "type": "ADLS_Gen2",
            "path": "abfss://raw@datalakeprod.dfs.core.windows.net/events/",
            "format": "json",
            "schema_hints": {"event_id": "STRING", "user_id": "STRING", "timestamp": "TIMESTAMP"},
            "auto_loader": True,
            "cloud_files_schema_location": "abfss://checkpoints@datalakeprod.dfs.core.windows.net/events_schema/",
        },
        "ingestion_config": {
            "pipeline_type": "DLT",
            "mode": "TRIGGERED",
            "cluster": {"num_workers": 4, "node_type": "Standard_DS3_v2", "spark_version": "13.3.x-scala2.12"},
            "tables": [
                {"name": "bronze_events", "layer": "bronze", "comment": "Raw ingested events"},
                {"name": "silver_events_clean", "layer": "silver", "comment": "Cleaned and validated events"},
                {"name": "gold_events_agg", "layer": "gold", "comment": "Aggregated hourly metrics"},
            ],
            "schedule": {"quartz_cron_expression": "0 0 * * * ?", "timezone_id": "UTC"},
            "continuous": False,
        },
        "dq_config": {
            "framework": "Delta_Live_Tables_Expectations",
            "expectations": [
                {"name": "valid_event_id", "constraint": "event_id IS NOT NULL", "action": "drop"},
                {"name": "valid_user_id", "constraint": "user_id IS NOT NULL", "action": "drop"},
                {"name": "valid_timestamp", "constraint": "timestamp >= '2020-01-01'", "action": "quarantine"},
                {"name": "valid_event_type", "constraint": "event_type IN ('click','view','purchase')", "action": "warn"},
            ],
            "quarantine_table": "quarantine_events",
            "monitoring": {"enabled": True, "alert_email": "data-team@company.com"},
        },
        "raw_metadata": {
            "workspace": "adb-prod-workspace",
            "pipeline_id": "dlt-pipe-abc123",
            "storage": "abfss://dlt@datalakeprod.dfs.core.windows.net/",
            "target_schema": "hive_metastore.silver",
            "tags": {"env": "prod", "team": "data-engineering"},
        },
    },
    "spark-batch-job": {
        "framework_type": "Databricks Spark Batch Job",
        "job_name": "spark-batch-job",
        "source_config": {
            "type": "Snowflake",
            "sfUrl": "myorg-myaccount.snowflakecomputing.com",
            "sfDatabase": "PROD_DB",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "TRANSFORM_WH",
            "query": "SELECT * FROM TRANSACTIONS WHERE dt = :run_date",
            "extraction_mode": "incremental",
            "watermark_column": "updated_at",
        },
        "ingestion_config": {
            "pipeline_type": "SparkBatch",
            "notebook": "/Shared/ETL/transform_transactions",
            "cluster": {"num_workers": 8, "node_type": "Standard_DS5_v2", "autoscale": {"min_workers": 2, "max_workers": 16}},
            "schedule": {"quartz_cron_expression": "0 0 2 * * ?", "timezone_id": "America/New_York"},
            "parameters": {"run_date": "{{date}}", "env": "prod", "batch_size": "50000"},
            "libraries": [
                {"maven": {"coordinates": "net.snowflake:spark-snowflake_2.12:2.14.0-spark_3.3"}},
                {"pypi": {"package": "great-expectations==0.18.0"}},
            ],
        },
        "dq_config": {
            "framework": "Great_Expectations",
            "suite_name": "transactions_suite",
            "expectations": [
                {"type": "expect_column_values_to_not_be_null", "column": "transaction_id"},
                {"type": "expect_column_values_to_not_be_null", "column": "customer_id"},
                {"type": "expect_column_values_to_be_between", "column": "amount", "min_value": 0, "max_value": 1000000},
                {"type": "expect_table_row_count_to_be_between", "min_value": 100, "max_value": 10000000},
                {"type": "expect_column_values_to_be_unique", "column": "transaction_id"},
            ],
            "checkpoint_name": "transactions_checkpoint",
            "data_docs_site": "s3://data-docs-bucket/transactions/",
            "on_failure": "fail_pipeline",
        },
        "raw_metadata": {
            "workspace": "adb-prod-workspace",
            "job_id": 12345,
            "run_type": "JOB_RUN",
            "format": "delta",
            "sink": "abfss://silver@datalakeprod.dfs.core.windows.net/transactions/",
            "tags": {"env": "prod", "domain": "finance"},
        },
    },
    "notebook-pipeline": {
        "framework_type": "Databricks Notebook Pipeline",
        "job_name": "notebook-pipeline",
        "source_config": {
            "type": "REST_API",
            "base_url": "https://api.salesforce.com/services/data/v58.0",
            "auth_type": "OAuth2",
            "object": "Account",
            "soql": "SELECT Id, Name, Email, CreatedDate FROM Account WHERE LastModifiedDate > :watermark",
            "incremental": True,
            "watermark_column": "LastModifiedDate",
        },
        "ingestion_config": {
            "pipeline_type": "Notebook",
            "notebook": "/Shared/Ingestion/salesforce_ingestor",
            "cluster": {"num_workers": 2, "node_type": "Standard_DS3_v2"},
            "schedule": {"quartz_cron_expression": "0 0 */4 * * ?", "timezone_id": "UTC"},
            "widgets": {"env": "prod", "source_object": "Account", "batch_size": "1000"},
        },
        "dq_config": {
            "framework": "Custom_Notebook",
            "checks": [
                {"column": "Id", "rule": "not_null", "action": "drop"},
                {"column": "Email", "rule": "regex", "pattern": r"^[^@]+@[^@]+\.[^@]+$", "action": "warn"},
                {"table": "Account", "rule": "row_count_min", "value": 100, "action": "alert"},
            ],
        },
        "raw_metadata": {
            "workspace": "adb-prod-workspace",
            "sink": "abfss://raw@datalakeprod.dfs.core.windows.net/salesforce/accounts/",
            "format": "parquet",
        },
    },
}

DATABRICKS_JOB_LIST = [
    {"name": "dlt-bronze-silver", "type": "Delta Live Tables", "status": "Running", "cluster": "DLT-Cluster"},
    {"name": "spark-batch-job", "type": "Spark Batch", "status": "Active", "cluster": "Standard_DS5_v2"},
    {"name": "notebook-pipeline", "type": "Notebook", "status": "Active", "cluster": "Standard_DS3_v2"},
]


def extract_databricks_mock(job_name: str) -> dict[str, Any]:
    key = None
    for k in DATABRICKS_MOCK_TEMPLATES:
        if job_name.lower() in k.lower() or k.lower() in job_name.lower():
            key = k
            break
    if not key:
        key = list(DATABRICKS_MOCK_TEMPLATES.keys())[0]
    t = DATABRICKS_MOCK_TEMPLATES[key]
    return {
        "pipeline_name": job_name,
        "framework": t["framework_type"],
        "source_config": t["source_config"],
        "ingestion_config": t["ingestion_config"],
        "dq_config": t["dq_config"],
        "raw_metadata": t.get("raw_metadata", {}),
        "_demo": True,
    }


# ── Real Databricks Extractor ──────────────────────────────────────────────────

class DatabricksExtractor:
    """
    Extracts job/pipeline config from Databricks workspace.
    """

    def __init__(
        self,
        job_name: str,
        host: Optional[str] = None,
        token: Optional[str] = None,
    ):
        self.job_name = job_name
        self.host = host
        self.token = token

    def _get_client(self):
        try:
            from databricks.sdk import WorkspaceClient
            if self.host and self.token:
                return WorkspaceClient(host=self.host, token=self.token)
            return WorkspaceClient()
        except ImportError:
            raise ImportError("databricks-sdk required: pip install databricks-sdk")

    def extract(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "pipeline_name": self.job_name,
            "framework": "Databricks",
            "source_config": {},
            "ingestion_config": {},
            "dq_config": {},
            "raw_metadata": {},
        }
        try:
            client = self._get_client()
            # Search by name
            jobs = list(client.jobs.list(name=self.job_name))
            if not jobs:
                result["raw_metadata"]["error"] = f"Job '{self.job_name}' not found"
                return result

            job = client.jobs.get(job_id=jobs[0].job_id)
            raw = job.as_dict()
            result["raw_metadata"] = {
                "job_id": raw.get("job_id"),
                "created_time": raw.get("created_time"),
                "creator_user_name": raw.get("creator_user_name"),
            }
            result["ingestion_config"] = self._extract_ingestion(raw)
            result["source_config"] = self._infer_source(raw)
            result["dq_config"] = self._infer_dq(raw)
        except Exception as exc:
            logger.warning("Databricks extraction failed: %s", exc)
            result["raw_metadata"]["extraction_error"] = str(exc)
        return result

    def _extract_ingestion(self, raw: dict) -> dict:
        settings = raw.get("settings", {})
        tasks = settings.get("tasks", [])
        return {
            "tasks": [{"key": t.get("task_key"), "type": list(t.keys())[2] if len(t) > 2 else "unknown"} for t in tasks],
            "schedule": settings.get("schedule", {}),
            "max_concurrent_runs": settings.get("max_concurrent_runs", 1),
            "job_clusters": [
                {"key": c.get("job_cluster_key"), "node_type": c.get("new_cluster", {}).get("node_type_id")}
                for c in settings.get("job_clusters", [])
            ],
        }

    def _infer_source(self, raw: dict) -> dict:
        """Infer source from task parameters and notebook content."""
        settings = raw.get("settings", {})
        for task in settings.get("tasks", []):
            params = task.get("notebook_task", {}).get("base_parameters", {})
            if params:
                source_type = params.get("source_type") or params.get("SOURCE_TYPE", "unknown")
                return {
                    "type": source_type,
                    "parameters": params,
                }
        return {"type": "unknown", "inferred_from": "task_parameters"}

    def _infer_dq(self, raw: dict) -> dict:
        """Detect DQ framework from libraries or notebook names."""
        settings = raw.get("settings", {})
        dq: dict = {"framework": "unknown", "rules": []}
        for cluster in settings.get("job_clusters", []):
            libs = cluster.get("new_cluster", {}).get("libraries", [])
            for lib in libs:
                pkg = lib.get("pypi", {}).get("package", "")
                if "great_expectations" in pkg.lower() or "great-expectations" in pkg.lower():
                    dq["framework"] = "great_expectations"
                elif "deequ" in pkg.lower():
                    dq["framework"] = "deequ"
                elif "soda" in pkg.lower():
                    dq["framework"] = "soda"
        return dq
