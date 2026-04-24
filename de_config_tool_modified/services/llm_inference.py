"""
llm_inference.py — LLM-Based Config Inference Engine
------------------------------------------------------
Uses Anthropic Claude API to:
  1. Analyze pipeline code (Python/PySpark/SQL/YAML) and extract structured config
  2. Infer missing configuration fields from partial config
  3. Convert unstructured/unknown pipeline logic to standard JSON config
  4. Classify framework type from code snippets

Credential modes:
  - ANTHROPIC_API_KEY env var  → production use
  - No key present             → returns a helpful mock/placeholder response
"""

from __future__ import annotations
import json
import logging
import os
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Anthropic API setup ────────────────────────────────────────────────────────

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL   = "claude-sonnet-4-20250514"

def _get_api_key() -> Optional[str]:
    return os.environ.get("ANTHROPIC_API_KEY", "").strip() or None

def _build_headers() -> dict[str, str]:
    key = _get_api_key()
    headers = {
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }
    if key:
        headers["x-api-key"] = key
    return headers

def _api_available() -> bool:
    return bool(_get_api_key())


# ── System prompt ──────────────────────────────────────────────────────────────

EXTRACTION_SYSTEM_PROMPT = """You are a senior Data Platform Engineer specializing in reverse-engineering data pipelines.

Your task is to analyze pipeline code, configuration files, or partial configs and extract structured configuration.

You MUST return ONLY valid JSON in this exact format (no markdown, no explanation, just JSON):
{
  "source_configs": {
    "source_type": "<DB|API|File|Stream|etc>",
    "service_name": "<linked service or framework name>",
    "connection_details": {
      "workspaceId": "<workspace id if found>",
      "workspace": "<workspace name if found>",
      "artifactId": "<artifact/table/dataset id if found>",
      "schema": "<schema if found>",
      "table": "<table name if found>",
      "endpoint": "<URL or endpoint if found>",
      "path": "<file or folder path if found>"
    },
    "authentication_type": "<Connection Reference|IAM Role|Service Principal|OAuth|API Key|etc>"
  },
  "ingestion_configs": {
    "mode": "<batch|streaming|micro_batch>",
    "trigger_type": "<ScheduleTrigger|EventTrigger|ManualTrigger or null>",
    "frequency": "<Daily|Hourly|cron expression or null>",
    "data_format": ["<JSON|CSV|Parquet|Delta|etc>"],
    "destination": "<target system name>"
  },
  "dq_rules": ["<rule_name_1>", "<rule_name_2>"],
  "flow": {
    "text": "<human-readable flow description>",
    "graph": {
      "nodes": [{"id": "<node_id>", "type": "<source|process|sink|notification>"}],
      "edges": [{"from": "<node_id>", "to": "<node_id>"}]
    }
  },
  "missing_fields_analysis": [
    {"field": "<dotted.field.path>", "reason": "<why this field could not be extracted>"}
  ],
  "confidence": "<high|medium|low>",
  "notes": "<key observations about the pipeline>"
}

Rules:
- Use null for trigger_type and frequency when not found
- dq_rules is a flat list of rule name strings (e.g. "row_count_check", "schema_check", "not_null")
- Extract ALL DQ rules found, even implicit ones (assert, filter, validate, constraint)
- If credentials are present, DO NOT include actual values — use "***MASKED***"
- connection_details should only include fields actually found; omit unknowns entirely
- missing_fields_analysis must list every connection_details or ingestion_configs field that could NOT be determined, with a clear reason
- confidence: "high" = explicit config found, "medium" = inferred, "low" = guessing
"""

CLASSIFY_PROMPT = """Analyze this code/config snippet and determine its pipeline framework.
Return ONLY JSON (no markdown):
{
  "framework": "<framework name e.g. ADF|Databricks|Snowflake|Glue|Spark|dbt>",
  "source_type": "<DB|API|File|Stream|etc>",
  "pattern": "<etl|elt|streaming|batch|cdc|micro_batch>",
  "cloud_provider": "<aws|azure|gcp|snowflake|multi|unknown>",
  "confidence": "<high|medium|low>",
  "reasoning": "<one sentence>"
}"""


# ── Mock fallback for no-API-key mode ──────────────────────────────────────────

def _mock_infer(code_or_config: str, pipeline_name: Optional[str]) -> dict[str, Any]:
    """
    Pattern-based heuristic extraction used when no API key is set.
    Covers the most common cases well enough for demo purposes.
    """
    code_lower = code_or_config.lower()

    # Detect source
    if "psycopg2" in code_lower or "postgresql" in code_lower or "pg_host" in code_lower:
        src_type = "postgresql"
        conn = {"host": _extract_env(code_or_config, ["PG_HOST","POSTGRES_HOST","DB_HOST"]),
                "database": _extract_env(code_or_config, ["PG_DATABASE","POSTGRES_DATABASE","DB_NAME"]),
                "schema": _extract_env(code_or_config, ["PG_SCHEMA","POSTGRES_SCHEMA","DB_SCHEMA"]) or "public",
                "table": _extract_env(code_or_config, ["PG_TABLE","POSTGRES_TABLE","DB_TABLE"])}
    elif "kafka" in code_lower or "bootstrap.servers" in code_lower or "msk" in code_lower:
        src_type = "kafka"
        conn = {"brokers": _extract_env(code_or_config, ["KAFKA_BROKERS","BOOTSTRAP_SERVERS"]),
                "topic": _extract_env(code_or_config, ["KAFKA_TOPIC","TOPIC"])}
    elif "snowflake" in code_lower and ("connector" in code_lower or "snowflake.connect" in code_lower):
        src_type = "snowflake"
        conn = {"account": _extract_env(code_or_config, ["SNOWFLAKE_ACCOUNT","SF_ACCOUNT"]),
                "warehouse": _extract_env(code_or_config, ["SNOWFLAKE_WAREHOUSE","SF_WAREHOUSE"]),
                "database": _extract_env(code_or_config, ["SNOWFLAKE_DATABASE","SF_DATABASE"])}
    elif "requests.get" in code_lower or "httpx" in code_lower or "api_url" in code_lower or "base_url" in code_lower:
        src_type = "rest_api"
        conn = {"base_url": _extract_env(code_or_config, ["API_BASE_URL","API_URL","BASE_URL","ENDPOINT_URL"])}
    elif "s3" in code_lower and ("boto3" in code_lower or "s3://" in code_lower):
        src_type = "s3"
        conn = {"bucket": _extract_env(code_or_config, ["SOURCE_BUCKET","S3_BUCKET","BUCKET"])}
    elif "adls" in code_lower or "dfs.core.windows.net" in code_lower or "abfss://" in code_lower:
        src_type = "adls_gen2"
        conn = {"account": _extract_env(code_or_config, ["ADLS_ACCOUNT","STORAGE_ACCOUNT"])}
    elif "sftp" in code_lower or "paramiko" in code_lower:
        src_type = "sftp"
        conn = {"host": _extract_env(code_or_config, ["SFTP_HOST","FTP_HOST"])}
    elif "mysql" in code_lower or "pymysql" in code_lower:
        src_type = "mysql"
        conn = {"host": _extract_env(code_or_config, ["MYSQL_HOST","DB_HOST"])}
    elif "mongodb" in code_lower or "pymongo" in code_lower:
        src_type = "mongodb"
        conn = {"uri": _extract_env(code_or_config, ["MONGO_URI","MONGODB_URI"])}
    else:
        src_type = "unknown"
        conn = {}

    # Detect framework
    if "awsglue" in code_lower or "gluecontext" in code_lower:
        framework = "AWS_Glue"
        pipeline_type = "glue"
    elif "lambda_handler" in code_lower or "def handler" in code_lower:
        framework = "AWS_Lambda"
        pipeline_type = "lambda"
    elif "statemachine" in code_lower or "stepfunctions" in code_lower:
        framework = "AWS_StepFunctions"
        pipeline_type = "step_functions"
    elif "dlt." in code_lower or "delta live" in code_lower:
        framework = "Databricks_DLT"
        pipeline_type = "dlt"
    elif "sparksession" in code_lower or "pyspark" in code_lower:
        framework = "Databricks_Spark" if "databricks" in code_lower else "AWS_EMR"
        pipeline_type = "spark"
    elif "{{ config(" in code_lower or "{{ ref(" in code_lower:
        framework = "dbt"
        pipeline_type = "dbt"
    elif "create task" in code_lower or "create stream" in code_lower:
        framework = "Snowflake"
        pipeline_type = "snowflake_task"
    elif "datafactory" in code_lower or "linkedservice" in code_lower:
        framework = "ADF"
        pipeline_type = "adf"
    else:
        framework = "unknown"
        pipeline_type = "unknown"

    # Detect extraction mode
    watermark = _extract_env(code_or_config, ["WATERMARK_COLUMN","WATERMARK","INCREMENTAL_COLUMN"])
    if "cdc" in code_lower or "stream" in code_lower and "kafka" in code_lower:
        extraction_mode = "streaming" if "kafka" in code_lower else "cdc"
    elif watermark or "watermark" in code_lower or "incremental" in code_lower:
        extraction_mode = "incremental"
    else:
        extraction_mode = "full_load"

    # DQ rules
    rules: list[dict] = []
    # Detect assert-based checks
    for line in code_or_config.split("\n"):
        line_s = line.strip()
        if line_s.startswith("assert ") or line_s.startswith("assert("):
            rules.append({"column": "inferred", "rule_type": "custom", "condition": line_s[:80], "severity": "high", "action": "reject"})
        elif "is not null" in line_s.lower() or "isnull" in line_s.lower() or "isnotnull" in line_s.lower():
            col_match = re.search(r'["\'](\w+)["\']', line_s)
            col = col_match.group(1) if col_match else "unknown"
            rules.append({"column": col, "rule_type": "not_null", "condition": line_s[:80], "severity": "critical", "action": "reject"})
        elif "dlt.expect" in line_s.lower():
            exp_match = re.search(r'dlt\.expect\w*\(["\'](\w+)["\']', line_s)
            rules.append({"column": exp_match.group(1) if exp_match else "unknown", "rule_type": "dlt_expectation", "condition": line_s[:80], "severity": "high", "action": "drop"})

    # Detect DQ framework
    if "great_expectations" in code_lower or "great-expectations" in code_lower or "expectation_suite" in code_lower:
        dq_framework = "great_expectations"
    elif "deequ" in code_lower:
        dq_framework = "deequ"
    elif "soda" in code_lower and "scan" in code_lower:
        dq_framework = "soda"
    elif "dlt.expect" in code_lower:
        dq_framework = "dlt_expectations"
    elif "awsgluedq" in code_lower or "evaluatedataquality" in code_lower:
        dq_framework = "glue_dq"
    elif rules:
        dq_framework = "custom"
    else:
        dq_framework = "none"

    # Detect target
    target_bucket = _extract_env(code_or_config, ["OUTPUT_BUCKET","SINK_BUCKET","TARGET_BUCKET","DEST_BUCKET"])
    target_table  = _extract_env(code_or_config, ["OUTPUT_TABLE","TARGET_TABLE","SINK_TABLE"])
    if ".writestream" in code_lower or ".write.format" in code_lower or ".write.mode" in code_lower:
        target_type = "delta" if "delta" in code_lower else "parquet"
    elif "redshift" in code_lower:
        target_type = "redshift"
    elif "snowflake" in code_lower and "write" in code_lower:
        target_type = "snowflake"
    elif target_bucket:
        target_type = "s3"
    elif "adls" in code_lower or "abfss://" in code_lower:
        target_type = "adls_gen2"
    else:
        target_type = "unknown"

    schedule = _extract_env(code_or_config, ["SCHEDULE","CRON","CRON_EXPRESSION"])
    batch_size_raw = _extract_env(code_or_config, ["BATCH_SIZE","CHUNK_SIZE","PAGE_SIZE"])
    try:
        batch_size = int(batch_size_raw) if batch_size_raw else None
    except ValueError:
        batch_size = None

    # Build dq_rules list from rule dicts
    dq_rule_names = list({r.get("rule_type", "custom_check") for r in rules[:20]}) or ["row_count_check", "schema_check"]

    # Build simple flow graph
    flow_nodes = [{"id": "source_1", "type": "source"}, {"id": "sink_1", "type": "sink"}]
    flow_edges = [{"from": "source_1", "to": "sink_1"}]

    # Determine missing fields
    missing: list[dict] = []
    conn_fields = {"artifactId", "schema", "table", "endpoint", "path"}
    for f in conn_fields:
        if not conn.get(f):
            missing.append({"field": f"source_configs.connection_details.{f}", "reason": f"No {f} found in code pattern matching."})
    if not schedule:
        missing.append({"field": "ingestion_configs.trigger_type", "reason": "No trigger or schedule definition found."})
        missing.append({"field": "ingestion_configs.frequency", "reason": "No frequency or cron expression found."})

    return {
        "source_configs": {
            "source_type": src_type,
            "service_name": framework,
            "connection_details": conn,
            "authentication_type": conn.get("auth_type", "Unknown"),
        },
        "ingestion_configs": {
            "mode": extraction_mode if extraction_mode != "full_load" else "batch",
            "trigger_type": None,
            "frequency": schedule or None,
            "data_format": [fmt for fmt in [conn.get("format")] if fmt],
            "destination": target_type,
        },
        "dq_rules": dq_rule_names,
        "flow": {
            "text": f"{framework}: source_1 -> sink_1",
            "graph": {"nodes": flow_nodes, "edges": flow_edges},
        },
        "missing_fields_analysis": missing,
        "confidence": "medium" if src_type != "unknown" else "low",
        "notes": "Extracted via pattern matching (no API key). Set ANTHROPIC_API_KEY for full AI inference.",
        "_llm_inferred": False,
        "_pattern_matched": True,
    }


def _extract_env(code: str, keys: list[str]) -> Optional[str]:
    """Extract the most likely value for an env var from code."""
    for key in keys:
        # Match os.environ["KEY"] or os.environ.get("KEY") or env["KEY"]
        m = re.search(rf'''(?:os\.environ(?:\.get)?\s*[\[(]\s*['"]){re.escape(key)}['"](?:\s*,\s*['"]([^'"]+)['"])?''', code)
        if m:
            return m.group(1) or f"${{{key}}}"
        # Match KEY = "value" patterns
        m2 = re.search(rf'''^{re.escape(key)}\s*[=:]\s*['"]([^'"]+)['"]''', code, re.MULTILINE)
        if m2:
            return m2.group(1)
    return None


# ── Core async inference functions ─────────────────────────────────────────────

async def infer_config_from_code(
    code_or_config: str,
    context: Optional[str] = None,
    pipeline_name: Optional[str] = None,
) -> dict[str, Any]:
    """
    Extract pipeline config from arbitrary code using Claude API.
    Falls back to pattern matching if no API key is configured.
    """
    if not _api_available():
        logger.info("No ANTHROPIC_API_KEY found — using pattern matching fallback")
        result = _mock_infer(code_or_config, pipeline_name)
        return result

    try:
        import httpx

        user_message = "Analyze this pipeline and extract the configuration:\n\n"
        if pipeline_name:
            user_message += f"Pipeline name: {pipeline_name}\n\n"
        if context:
            user_message += f"Context/Environment:\n{context}\n\n"
        user_message += f"Code/Config to analyze:\n```\n{code_or_config[:8000]}\n```"

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                ANTHROPIC_API_URL,
                headers=_build_headers(),
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 2000,
                    "system": EXTRACTION_SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": user_message}],
                },
            )
            resp.raise_for_status()
            data = resp.json()

        raw_text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        raw_text = re.sub(r"^```[a-z]*\n?", "", raw_text)
        raw_text = re.sub(r"\n?```$", "", raw_text).strip()

        parsed = json.loads(raw_text)
        parsed["_llm_inferred"] = True
        parsed["_model"] = ANTHROPIC_MODEL
        return parsed

    except json.JSONDecodeError as e:
        logger.error("LLM returned invalid JSON: %s", e)
        return _fallback_config(pipeline_name or "unknown", f"JSON parse error: {e}")
    except Exception as exc:
        logger.error("LLM inference failed: %s", exc)
        return _fallback_config(pipeline_name or "unknown", str(exc))


async def infer_missing_fields(
    partial_config: dict[str, Any],
    raw_metadata: dict[str, Any],
) -> dict[str, Any]:
    """Fill in missing fields in a partial config using LLM."""
    if not _api_available():
        partial_config["_note"] = "Set ANTHROPIC_API_KEY for AI-powered field enrichment"
        return partial_config

    try:
        import httpx

        prompt = (
            f"I have a partial pipeline config and raw metadata. "
            f"Fill in the MISSING fields and return the COMPLETE config as JSON.\n\n"
            f"Partial Config:\n{json.dumps(partial_config, indent=2)}\n\n"
            f"Raw Metadata:\n{json.dumps(raw_metadata, indent=2)[:4000]}\n\n"
            f"Return only the filled-in version of the same JSON structure. Keep existing values unchanged."
        )

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                ANTHROPIC_API_URL,
                headers=_build_headers(),
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 2000,
                    "system": EXTRACTION_SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()

        raw_text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        raw_text = re.sub(r"^```[a-z]*\n?", "", raw_text)
        raw_text = re.sub(r"\n?```$", "", raw_text).strip()
        enriched = json.loads(raw_text)
        enriched["_llm_enriched"] = True
        return enriched

    except Exception as exc:
        logger.error("LLM field inference failed: %s", exc)
        partial_config["_llm_enrichment_error"] = str(exc)
        return partial_config


async def classify_framework(code_snippet: str) -> dict[str, str]:
    """Classify pipeline framework type from code snippet."""
    if not _api_available():
        # Quick pattern-based classification
        cl = code_snippet.lower()
        if "awsglue" in cl or "gluecontext" in cl:
            fw, cloud, pat = "AWS_Glue", "aws", "etl"
        elif "lambda_handler" in cl:
            fw, cloud, pat = "AWS_Lambda", "aws", "etl"
        elif "sparksession" in cl and "databricks" in cl:
            fw, cloud, pat = "Databricks_Spark", "azure", "etl"
        elif "dlt." in cl:
            fw, cloud, pat = "Databricks_DLT", "azure", "streaming"
        elif "{{ config(" in cl:
            fw, cloud, pat = "dbt", "multi", "elt"
        elif "create task" in cl or "create stream" in cl:
            fw, cloud, pat = "Snowflake", "snowflake", "batch"
        elif "readstream" in cl or "writestream" in cl:
            fw, cloud, pat = "Spark_Streaming", "multi", "streaming"
        elif "datafactory" in cl:
            fw, cloud, pat = "ADF", "azure", "etl"
        else:
            fw, cloud, pat = "unknown", "unknown", "unknown"
        return {"framework": fw, "source_type": "unknown", "pattern": pat, "cloud_provider": cloud, "confidence": "medium", "reasoning": "Pattern matching (no API key set)"}

    try:
        import httpx

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                ANTHROPIC_API_URL,
                headers=_build_headers(),
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 400,
                    "system": CLASSIFY_PROMPT,
                    "messages": [{"role": "user", "content": f"Classify this pipeline:\n```\n{code_snippet[:3000]}\n```"}],
                },
            )
            resp.raise_for_status()
            data = resp.json()

        raw_text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        raw_text = re.sub(r"^```[a-z]*\n?", "", raw_text)
        raw_text = re.sub(r"\n?```$", "", raw_text).strip()
        return json.loads(raw_text)

    except Exception as exc:
        logger.error("Framework classification failed: %s", exc)
        return {"framework": "unknown", "source_type": "unknown", "pattern": "unknown", "cloud_provider": "unknown", "confidence": "low", "reasoning": f"Classification failed: {exc}"}


def _fallback_config(pipeline_name: str, error: str) -> dict[str, Any]:
    return {
        "source_configs": {
            "source_type": "Unknown",
            "service_name": "Unknown",
            "connection_details": {},
            "authentication_type": "Unknown",
        },
        "ingestion_configs": {
            "mode": "batch",
            "trigger_type": None,
            "frequency": None,
            "data_format": [],
            "destination": "Unknown",
        },
        "dq_rules": ["row_count_check", "schema_check"],
        "flow": {
            "text": "Unknown pipeline — LLM inference failed",
            "graph": {"nodes": [{"id": "source_1", "type": "source"}], "edges": []},
        },
        "missing_fields_analysis": [],
        "confidence": "low",
        "notes": f"LLM inference failed: {error}",
        "_llm_inferred": False,
        "_error": error,
    }


# ── Sync wrapper ──────────────────────────────────────────────────────────────

def infer_config_sync(
    code_or_config: str,
    context: Optional[str] = None,
    pipeline_name: Optional[str] = None,
) -> dict[str, Any]:
    """Synchronous wrapper — safe to call from sync contexts."""
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, infer_config_from_code(code_or_config, context, pipeline_name))
                return future.result(timeout=90)
        return loop.run_until_complete(infer_config_from_code(code_or_config, context, pipeline_name))
    except Exception as exc:
        return _fallback_config(pipeline_name or "unknown", str(exc))
