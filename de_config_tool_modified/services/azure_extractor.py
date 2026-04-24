"""
azure_extractor.py — Azure Pipeline Config Extractor
------------------------------------------------------
Supports:
  - Azure Data Factory (ADF) pipelines
  - Synapse Analytics pipelines
  - ADLS Gen2 config discovery
  - Demo / Mock mode (no Azure credentials needed)
"""

from __future__ import annotations
import json
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Pattern matchers (reused from lambda_extractor style) ──────────────────────
_INGESTION_PATTERNS = re.compile(
    r"(source|sink|dataset|linkedservice|pipeline|activity|trigger|schedule|"
    r"copy|dataflow|wrangling|mapping|partition|batch|interval|frequency|"
    r"container|path|format|compression|encoding|table|database|schema)",
    re.IGNORECASE,
)
_DQ_PATTERNS = re.compile(
    r"(quality|validate|assertion|expectation|null|unique|range|regex|"
    r"completeness|accuracy|consistency|threshold|reject|quarantine|"
    r"constraint|rule|check|monitor|alert|warn|fail|pass)",
    re.IGNORECASE,
)


# ── Mock Templates ─────────────────────────────────────────────────────────────

ADF_MOCK_TEMPLATES = {
    "adf-blob-to-sql": {
        "framework_type": "Azure Data Factory",
        "pipeline_name": "adf-blob-to-sql",
        "pipeline_id": "/subscriptions/sub-123/resourceGroups/rg-data/providers/Microsoft.DataFactory/factories/adf-prod/pipelines/blob-to-sql",
        "source_config": {
            "type": "AzureBlobStorage",
            "linked_service": "BlobStorageLS",
            "container": "raw-data",
            "folder_path": "incoming/sales/",
            "file_format": "DelimitedText",
            "column_delimiter": ",",
            "row_delimiter": "\n",
            "encoding": "UTF-8",
            "first_row_as_header": True,
            "compression": {"type": "GZip"},
        },
        "ingestion_config": {
            "activity_type": "Copy",
            "batch_mode": "sequential",
            "parallelism": 4,
            "timeout": "01:00:00",
            "retry": {"count": 3, "interval": "00:05:00"},
            "schedule": {"type": "ScheduleTrigger", "frequency": "Hour", "interval": 6},
            "watermark": {"enabled": True, "column": "ModifiedDate", "store": "AzureSQL"},
            "staging": {"enabled": True, "container": "adf-staging"},
        },
        "sink_config": {
            "type": "AzureSqlDatabase",
            "linked_service": "AzureSQLLS",
            "table_name": "sales_staging",
            "schema": "stg",
            "write_behavior": "upsert",
            "upsert_keys": ["SalesOrderId"],
            "pre_copy_script": "TRUNCATE TABLE stg.sales_staging",
        },
        "dq_config": {
            "framework": "ADF_Validation_Activity",
            "rules": [
                {"column": "SalesOrderId", "type": "not_null", "severity": "Critical"},
                {"column": "OrderDate", "type": "not_null", "severity": "Critical"},
                {"column": "Amount", "type": "range", "min": 0, "max": 1000000, "severity": "Warning"},
                {"column": "CustomerEmail", "type": "regex", "pattern": r"^[^@]+@[^@]+\.[^@]+$", "severity": "Warning"},
            ],
            "on_failure": "quarantine",
            "quarantine_container": "bad-data",
        },
        "raw_metadata": {
            "resource_group": "rg-data",
            "factory_name": "adf-prod",
            "subscription_id": "sub-123",
            "region": "eastus",
            "tags": {"env": "production", "team": "data-engineering"},
        },
    },
    "synapse-spark-pipeline": {
        "framework_type": "Azure Synapse Analytics",
        "pipeline_name": "synapse-spark-pipeline",
        "pipeline_id": "/subscriptions/sub-123/resourceGroups/rg-analytics/providers/Microsoft.Synapse/workspaces/synapse-prod/pipelines/spark-transform",
        "source_config": {
            "type": "ADLS_Gen2",
            "account": "datalakeprod",
            "container": "bronze",
            "path": "events/clickstream/",
            "format": "parquet",
            "partition_by": ["year", "month", "day"],
        },
        "ingestion_config": {
            "activity_type": "SparkJob",
            "spark_pool": "sparkpool01",
            "node_size": "Medium",
            "min_nodes": 3,
            "max_nodes": 10,
            "auto_scale": True,
            "notebook": "transform_clickstream",
            "parameters": {"env": "prod", "batch_date": "@pipeline().TriggerTime"},
            "schedule": {"type": "TumblingWindow", "frequency": "Hour", "interval": 1},
        },
        "sink_config": {
            "type": "SynapseDedicatedPool",
            "pool": "DataWarehouse",
            "schema": "silver",
            "table": "clickstream_events",
            "write_mode": "append",
            "polybase_staging": "datalakeprod/polybase-staging/",
        },
        "dq_config": {
            "framework": "Great_Expectations_Synapse",
            "expectations": [
                {"type": "expect_column_values_to_not_be_null", "column": "event_id"},
                {"type": "expect_column_values_to_not_be_null", "column": "user_id"},
                {"type": "expect_table_row_count_to_be_between", "min_value": 1000, "max_value": 10000000},
                {"type": "expect_column_values_to_be_unique", "column": "event_id"},
            ],
            "on_failure": "alert_and_continue",
            "alert_email": "data-team@company.com",
        },
        "raw_metadata": {
            "workspace": "synapse-prod",
            "resource_group": "rg-analytics",
            "region": "westus2",
            "tags": {"env": "production", "cost_center": "data-platform"},
        },
    },
    "adf-api-ingestion": {
        "framework_type": "Azure Data Factory (REST API Ingestion)",
        "pipeline_name": "adf-api-ingestion",
        "source_config": {
            "type": "RestAPI",
            "linked_service": "CRMRestLS",
            "base_url": "https://crm-api.company.com/v3",
            "auth_type": "ServicePrincipal",
            "relative_url": "/contacts?modified_after=@{pipeline().parameters.WatermarkDate}",
            "request_method": "GET",
            "pagination": {"type": "NextPageUrl", "next_page_url_path": "$.pagination.next"},
        },
        "ingestion_config": {
            "activity_type": "Copy",
            "batch_size": 1000,
            "parallelism": 2,
            "timeout": "02:00:00",
            "watermark": {"column": "ModifiedAt", "type": "datetime"},
            "schedule": {"frequency": "Hour", "interval": 4},
        },
        "sink_config": {
            "type": "ADLS_Gen2",
            "account": "datalakeprod",
            "container": "raw",
            "path": "crm/contacts/@{formatDateTime(utcnow(),'yyyy/MM/dd')}/",
            "format": "json",
            "compression": "gzip",
        },
        "dq_config": {
            "framework": "ADF_DataFlow_Validation",
            "rules": [
                {"column": "ContactId", "type": "not_null", "severity": "Critical"},
                {"column": "Email", "type": "regex", "pattern": r"^[^@]+@[^@]+\.[^@]+$", "severity": "Warning"},
            ],
            "row_count_min": 10,
            "on_failure": "quarantine",
        },
        "raw_metadata": {
            "factory_name": "adf-prod",
            "region": "eastus",
            "key_vault": "kv-data-secrets",
        },
    },
}

ADF_PIPELINE_LIST = [
    {"name": "adf-blob-to-sql", "type": "Azure Data Factory", "status": "Active", "last_run": "2026-04-09T10:00:00Z"},
    {"name": "synapse-spark-pipeline", "type": "Azure Synapse Analytics", "status": "Active", "last_run": "2026-04-09T09:00:00Z"},
    {"name": "adf-api-ingestion", "type": "Azure Data Factory (REST API)", "status": "Active", "last_run": "2026-04-09T08:00:00Z"},
]


# ── Mock extractor ─────────────────────────────────────────────────────────────

def extract_adf_mock(pipeline_name: str) -> dict[str, Any]:
    key = None
    for k in ADF_MOCK_TEMPLATES:
        if pipeline_name.lower() in k.lower() or k.lower() in pipeline_name.lower():
            key = k
            break
    if not key:
        key = list(ADF_MOCK_TEMPLATES.keys())[0]
    template = ADF_MOCK_TEMPLATES[key]
    return {
        "pipeline_name": pipeline_name,
        "framework": template["framework_type"],
        "source_config": template["source_config"],
        "ingestion_config": template["ingestion_config"],
        "dq_config": template["dq_config"],
        "raw_metadata": {
            **template.get("raw_metadata", {}),
            "pipeline_id": template.get("pipeline_id", ""),
            "sink_config": template.get("sink_config", {}),
        },
        "_demo": True,
    }


# ── Real ADF Extractor ─────────────────────────────────────────────────────────

class AzureExtractor:
    """
    Extracts pipeline config from Azure Data Factory / Synapse.
    Falls back to LLM inference for unknown patterns.
    """

    def __init__(
        self,
        pipeline_name: str,
        subscription_id: Optional[str] = None,
        resource_group: Optional[str] = None,
        factory_name: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        self.pipeline_name = pipeline_name
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def _get_credential(self):
        try:
            from azure.identity import ClientSecretCredential, DefaultAzureCredential
            if self.tenant_id and self.client_id and self.client_secret:
                return ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
            return DefaultAzureCredential()
        except ImportError:
            raise ImportError("azure-identity package required: pip install azure-identity")

    def _get_adf_client(self):
        try:
            from azure.mgmt.datafactory import DataFactoryManagementClient
            cred = self._get_credential()
            return DataFactoryManagementClient(cred, self.subscription_id)
        except ImportError:
            raise ImportError("azure-mgmt-datafactory required: pip install azure-mgmt-datafactory")

    def extract(self) -> dict[str, Any]:
        """Main extraction entry point with layered strategy."""
        result: dict[str, Any] = {
            "pipeline_name": self.pipeline_name,
            "framework": "Azure Data Factory",
            "source_config": {},
            "ingestion_config": {},
            "dq_config": {},
            "raw_metadata": {},
        }
        try:
            client = self._get_adf_client()
            pipeline = client.pipelines.get(
                self.resource_group, self.factory_name, self.pipeline_name
            )
            raw = pipeline.as_dict()
            result["raw_metadata"] = {
                "description": raw.get("description", ""),
                "activities_count": len(raw.get("activities", [])),
                "parameters": raw.get("parameters", {}),
                "annotations": raw.get("annotations", []),
            }
            result["source_config"] = self._extract_source(raw)
            result["ingestion_config"] = self._extract_ingestion(raw)
            result["dq_config"] = self._extract_dq(raw)
        except Exception as exc:
            logger.warning("ADF extraction failed: %s", exc)
            result["raw_metadata"]["extraction_error"] = str(exc)
        return result

    def _extract_source(self, raw: dict) -> dict:
        source: dict = {}
        for activity in raw.get("activities", []):
            if activity.get("type") == "Copy":
                inputs = activity.get("inputs", [])
                if inputs:
                    ref = inputs[0].get("referenceName", "")
                    source["dataset"] = ref
                    source["type"] = activity.get("typeProperties", {}).get("source", {}).get("type", "Unknown")
        return source

    def _extract_ingestion(self, raw: dict) -> dict:
        ingestion: dict = {"activities": []}
        for activity in raw.get("activities", []):
            ingestion["activities"].append({
                "name": activity.get("name"),
                "type": activity.get("type"),
            })
        return ingestion

    def _extract_dq(self, raw: dict) -> dict:
        dq: dict = {"rules": []}
        for activity in raw.get("activities", []):
            if _DQ_PATTERNS.search(activity.get("name", "")):
                dq["rules"].append({
                    "activity": activity.get("name"),
                    "type": activity.get("type"),
                })
        return dq
