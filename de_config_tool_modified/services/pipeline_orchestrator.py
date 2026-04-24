"""
pipeline_orchestrator.py — Universal Pipeline Config Orchestrator
-----------------------------------------------------------------
Routes extraction requests to the correct extractor based on platform.

Extraction priority chain:
  1. Direct config (JSON/YAML from metadata)
  2. Service metadata (Lambda env vars, Glue args, ADF JSON, etc.)
  3. Code parsing (regex + AST)
  4. LLM-based inference (last fallback)

Supports:
  - AWS (Lambda, Glue, EMR, Step Functions)
  - Azure (ADF, Synapse)
  - Databricks (Jobs, DLT, Notebooks)
  - Snowflake (Tasks, Streams, Stored Procs)
  - Unknown frameworks (LLM fallback)
"""

from __future__ import annotations
import logging
from typing import Any, Optional, Literal

logger = logging.getLogger(__name__)

Platform = Literal["aws", "azure", "databricks", "snowflake", "unknown"]


def detect_platform(pipeline_name: str, hints: Optional[dict] = None) -> Platform:
    """
    Auto-detect platform from pipeline name patterns and hints.
    """
    name_lower = pipeline_name.lower()
    hints = hints or {}
    
    # Explicit hint takes priority
    if hints.get("platform"):
        return hints["platform"].lower()

    # Name-based detection
    aws_patterns = ["lambda", "glue", "emr", "step-fn", "stepfn", "sfn", "s3-etl", "redshift",
                    "ingestion", "pipeline-prod", "pipeline-dev", "etl-prod", "etl-dev",
                    "postgres-cdc", "kafka-processor", "clickstream", "snowflake-daily"]
    azure_patterns = ["adf", "synapse", "adls", "azure", "datafactory", "dataflow", "adf-api"]
    databricks_patterns = ["databricks", "dbt-spark", "dlt", "notebook", "spark-job", "delta-live", "spark-batch"]
    snowflake_patterns = ["snowflake", "sf-", "dbt-sf", "snow", "task-dag", "stream-task"]

    for p in aws_patterns:
        if p in name_lower:
            return "aws"
    for p in azure_patterns:
        if p in name_lower:
            return "azure"
    for p in databricks_patterns:
        if p in name_lower:
            return "databricks"
    for p in snowflake_patterns:
        if p in name_lower:
            return "snowflake"
    
    return "unknown"


class PipelineOrchestrator:
    """
    Unified orchestrator that delegates to platform-specific extractors
    and applies LLM fallback when needed.
    """

    def __init__(
        self,
        pipeline_name: str,
        platform: Optional[Platform] = None,
        demo_mode: bool = True,
        credentials: Optional[dict] = None,
        hints: Optional[dict] = None,
    ):
        self.pipeline_name = pipeline_name
        self.demo_mode = demo_mode
        self.credentials = credentials or {}
        self.hints = hints or {}
        self.platform = platform or detect_platform(pipeline_name, hints)

    def extract(self) -> dict[str, Any]:
        """
        Main extraction entry point. Returns unified new-schema structure.
        """
        logger.info("Extracting config for '%s' on platform '%s'", self.pipeline_name, self.platform)

        if self.platform == "aws":
            raw = self._extract_aws()
        elif self.platform == "azure":
            raw = self._extract_azure()
        elif self.platform == "databricks":
            raw = self._extract_databricks()
        elif self.platform == "snowflake":
            raw = self._extract_snowflake()
        else:
            raw = self._extract_unknown()

        return to_new_schema(raw)

    def _extract_aws(self) -> dict[str, Any]:
        """AWS extraction — demo stub (Lambda/Glue extractor removed)."""
        return {
            "pipeline_name": self.pipeline_name,
            "platform": "AWS",
            "framework": "AWS Glue / Lambda",
            "source_config": {
                "type": "s3",
                "connection": {"bucket": "source-bucket", "prefix": "incoming/"},
                "extraction_mode": "full_load",
            },
            "ingestion_config": {
                "pipeline_type": "glue",
                "mode": "batch",
                "output": {"bucket": "output-bucket", "format": "parquet"},
            },
            "dq_config": {"framework": "custom", "rules": []},
            "_demo": True,
        }

    def _extract_azure(self) -> dict[str, Any]:
        from services.azure_extractor import AzureExtractor, extract_adf_mock
        if self.demo_mode:
            return extract_adf_mock(self.pipeline_name)
        
        extractor = AzureExtractor(
            pipeline_name=self.pipeline_name,
            subscription_id=self.credentials.get("subscription_id"),
            resource_group=self.credentials.get("resource_group"),
            factory_name=self.credentials.get("factory_name"),
            tenant_id=self.credentials.get("tenant_id"),
            client_id=self.credentials.get("client_id"),
            client_secret=self.credentials.get("client_secret"),
        )
        result = extractor.extract()
        result["platform"] = "Azure"
        return result

    def _extract_databricks(self) -> dict[str, Any]:
        from services.databricks_extractor import DatabricksExtractor, extract_databricks_mock
        if self.demo_mode:
            return extract_databricks_mock(self.pipeline_name)
        
        extractor = DatabricksExtractor(
            job_name=self.pipeline_name,
            host=self.credentials.get("host"),
            token=self.credentials.get("token"),
        )
        result = extractor.extract()
        result["platform"] = "Databricks"
        return result

    def _extract_snowflake(self) -> dict[str, Any]:
        from services.snowflake_extractor import SnowflakeExtractor, extract_snowflake_mock
        if self.demo_mode:
            return extract_snowflake_mock(self.pipeline_name)
        
        extractor = SnowflakeExtractor(
            pipeline_name=self.pipeline_name,
            account=self.credentials.get("account"),
            user=self.credentials.get("user"),
            password=self.credentials.get("password"),
            warehouse=self.credentials.get("warehouse"),
            database=self.credentials.get("database"),
            schema=self.credentials.get("schema"),
            role=self.credentials.get("role"),
        )
        result = extractor.extract()
        result["platform"] = "Snowflake"
        return result

    def _extract_unknown(self) -> dict[str, Any]:
        """
        For unknown frameworks: return structured placeholder and trigger LLM inference
        if code is provided in hints.
        """
        code = self.hints.get("code") or self.hints.get("config_text", "")
        if code:
            # Use sync wrapper so this works in both sync and async contexts
            from services.llm_inference import infer_config_sync
            inferred = infer_config_sync(code, context=str(self.hints), pipeline_name=self.pipeline_name)
            inferred["pipeline_name"] = self.pipeline_name
            inferred["platform"] = "Unknown (LLM Inferred)"
            inferred["_demo"] = False
            return inferred
        
        return {
            "pipeline_name": self.pipeline_name,
            "platform": "Unknown",
            "framework": "Unknown — provide code/config for LLM inference",
            "source_config": {"type": "unknown", "inferred_from": "none"},
            "ingestion_config": {"pipeline_type": "unknown"},
            "dq_config": {"framework": "none", "rules": []},
            "raw_metadata": {"hint": "Paste code/config in the LLM Inference tab to extract config from unknown pipelines"},
            "_demo": False,
        }


def _try_int(val: Any) -> Optional[int]:
    try:
        return int(val) if val else None
    except (TypeError, ValueError):
        return None


# ── New schema converter ────────────────────────────────────────────────────────

def to_new_schema(result: dict[str, Any]) -> dict[str, Any]:
    """
    Convert any extractor output (source_config / ingestion_config / dq_config)
    to the new unified schema:

      source_configs, ingestion_configs, dq_rules, flow, missing_fields_analysis
    """
    src  = result.get("source_config") or {}
    ing  = result.get("ingestion_config") or {}
    dq   = result.get("dq_config") or {}
    meta = result.get("raw_metadata") or {}

    # ── source_configs ─────────────────────────────────────────────────────────
    conn = src.get("connection") or {}

    # Determine service_name
    service_name = (
        src.get("service_name") or
        src.get("linked_service") or
        ing.get("pipeline_type") or
        result.get("framework") or
        result.get("platform") or
        "Unknown"
    )

    # Determine authentication_type
    auth_type = (
        conn.get("auth_type") or
        src.get("auth_type") or
        src.get("authentication_type") or
        "Unknown"
    )

    # Merge connection_details from both src-level and conn dict
    connection_details: dict[str, Any] = {}
    for key in ("workspaceId", "workspace", "artifactId", "schema", "table",
                "endpoint", "path", "host", "port", "database", "bucket",
                "prefix", "brokers", "topic", "account", "warehouse",
                "container", "folder_path", "base_url", "uri"):
        val = conn.get(key) or src.get(key)
        if val:
            connection_details[key] = val

    source_configs = {
        "source_type": src.get("type") or src.get("source_type") or "Unknown",
        "service_name": service_name,
        "connection_details": connection_details if connection_details else {},
        "authentication_type": auth_type,
    }

    # ── ingestion_configs ──────────────────────────────────────────────────────
    # data_format
    fmt_raw = src.get("format") or src.get("file_format") or ing.get("format") or ""
    data_format: list[str] = []
    if fmt_raw:
        if isinstance(fmt_raw, list):
            data_format = fmt_raw
        else:
            data_format = [fmt_raw]

    # destination
    output = ing.get("output") or ing.get("target") or {}
    destination = (
        output.get("type") or
        output.get("bucket") or
        src.get("type") or
        "Unknown"
    )

    ingestion_configs = {
        "mode": (
            src.get("extraction_mode") or
            ing.get("mode") or
            ing.get("batch_mode") or
            "batch"
        ),
        "trigger_type": ing.get("trigger_type") or None,
        "frequency": (
            (ing.get("schedule") or {}).get("frequency") if isinstance(ing.get("schedule"), dict)
            else ing.get("frequency") or ing.get("schedule") or None
        ),
        "data_format": data_format,
        "destination": destination,
    }

    # ── dq_rules ───────────────────────────────────────────────────────────────
    rules_raw = dq.get("rules") or []
    dq_rules: list[str] = []
    seen: set[str] = set()

    # Normalise each rule to a string name
    for r in rules_raw:
        if isinstance(r, dict):
            rule_name = (
                r.get("rule_type") or
                r.get("type") or
                r.get("name") or
                "custom_check"
            )
        else:
            rule_name = str(r)
        if rule_name and rule_name not in seen:
            dq_rules.append(rule_name)
            seen.add(rule_name)

    # Always include baseline checks when no explicit rules were found
    if not dq_rules:
        dq_rules = ["row_count_check", "schema_check"]

    # ── flow ───────────────────────────────────────────────────────────────────
    steps = ing.get("steps") or []
    pipeline_name = result.get("pipeline_name") or "pipeline"
    platform = result.get("platform") or "Unknown"
    framework = result.get("framework") or platform

    # Build nodes from steps + source/destination
    nodes: list[dict] = [{"id": "source_1", "type": "source"}]
    edges: list[dict] = []
    prev_id = "source_1"
    for i, step in enumerate(steps):
        node_id = f"step_{i + 1}"
        nodes.append({"id": node_id, "type": "process", "label": step})
        edges.append({"from": prev_id, "to": node_id})
        prev_id = node_id
    nodes.append({"id": "sink_1", "type": "sink"})
    edges.append({"from": prev_id, "to": "sink_1"})

    flow_text = f"{framework}: {' -> '.join([n['id'] for n in nodes])}"

    flow = {
        "text": flow_text,
        "graph": {"nodes": nodes, "edges": edges},
    }

    # ── missing_fields_analysis ────────────────────────────────────────────────
    missing: list[dict] = []
    checks = {
        "source_configs.connection_details.artifactId":  not connection_details.get("artifactId"),
        "source_configs.connection_details.schema":       not connection_details.get("schema"),
        "source_configs.connection_details.table":        not connection_details.get("table"),
        "source_configs.connection_details.endpoint":     not connection_details.get("endpoint") and not connection_details.get("base_url"),
        "source_configs.connection_details.path":         not connection_details.get("path") and not connection_details.get("prefix") and not connection_details.get("folder_path"),
        "ingestion_configs.trigger_type":                  ingestion_configs["trigger_type"] is None,
        "ingestion_configs.frequency":                     ingestion_configs["frequency"] is None,
    }
    reasons = {
        "source_configs.connection_details.artifactId":  "No lakehouse or warehouse artifact identifier was present in the pipeline metadata.",
        "source_configs.connection_details.schema":       "No explicit schema field was found in the source dataset settings.",
        "source_configs.connection_details.table":        "No explicit table name was found in the source dataset settings.",
        "source_configs.connection_details.endpoint":     "No explicit URL or endpoint field was found in the pipeline activities.",
        "source_configs.connection_details.path":         "No folder path, file path, or storage path was found in the source dataset settings.",
        "ingestion_configs.trigger_type":                  "No trigger metadata, recurrence block, or schedule definition was found in the pipeline JSON.",
        "ingestion_configs.frequency":                     "No recurrence frequency, interval, cron, or schedule metadata was found in the pipeline JSON.",
    }
    for field, is_missing in checks.items():
        if is_missing:
            missing.append({"field": field, "reason": reasons[field]})

    return {
        "source_configs": source_configs,
        "ingestion_configs": ingestion_configs,
        "dq_rules": dq_rules,
        "flow": flow,
        "missing_fields_analysis": missing,
        # preserve top-level metadata
        "pipeline_name": result.get("pipeline_name"),
        "platform": result.get("platform"),
        "framework": result.get("framework"),
        "_demo": result.get("_demo", False),
    }
