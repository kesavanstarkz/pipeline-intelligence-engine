"""
LLM Inference Layer
────────────────────
Optional enrichment layer that sends payload + rule-based results to
an LLM (Anthropic Claude) for dynamic inference and gap-filling.

Only runs when:
  - settings.llm_enabled is True, OR
  - caller passes use_llm=True explicitly
  - settings.anthropic_api_key is set
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from config.settings import settings

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an expert Cloud Configuration Extraction AI. Your mission is to extract precise, evidence-backed SOURCE, INGESTION, and TARGET configurations from discovered cloud topology.

MISSION MANIFESTO:
1. Accuracy > Completeness.
2. DO NOT assume. DO NOT guess relationships. 
3. If data is not explicitly available in configurations or verified code analysis, use "UNKNOWN".
4. Only create connections (SOURCE -> INGESTION -> TARGET) if proven via trigger mappings, SDK calls, or IAM policies.

5. Ensure all raw configuration data from the scanner is preserved and structured in the "detailed_inventory" section for every discovered resource.

EXTRACTION SCHEMA:

1. SOURCE (API/Event Entry):
   - Use APIGW resources, methods, and integrations.
   - Extract: { "source_type": "API", "service": "API Gateway", "endpoint": "<invoke_url>", "method": [...], "integration_type": "Lambda Proxy", "target_lambda": "func_name", "auth": "IAM", "request_schema": "..." }

2. INGESTION (Compute):
   - Use Function config and event source mappings.
   - Extract: { "ingestion_type": "Lambda Processing", "function_name": "name", "runtime": "...", "timeout": 3, "memory": 128, "handler": "...", "event_source": "API Gateway", "downstream_targets": [...], "transformation": "UNKNOWN" }

3. TARGET (Storage):
   - Use Bucket location and validated write patterns.
   - Extract: { "target_type": "Object Storage", "service": "S3", "buckets": [...], "write_pattern": "PUT", "partitioning": "UNKNOWN", "format": "JSON" }

Respond ONLY with a valid JSON object matching this schema exactly.
{
  "confidence": {"framework": 0.9, "source": 0.8},
  "expert_extraction": {
      "source": {...},
      "ingestion": {...},
      "target": {...}
  },
  "detailed_inventory": [
      { "id": "...", "service": "...", "region": "...", "config": { ... } }
  ],
  "pipelines": [...],
  "nodes": [...],
  "validation": { "summary_badge": "Status: Verified" }
}
"""


def llm_infer(
    payload: Any,
    rule_based_results: Dict[str, List[str]],
) -> Optional[Dict[str, Any]]:
    """
    Call the local Ollama API (DeepSeek-R1) and return structured inference JSON.
    Returns None if LLM is disabled or call fails.
    """
    if not settings.llm_enabled:
        logger.info("LLM inference skipped — LLM_ENABLED is false.")
        return None

    import httpx

    user_message = _build_user_message(payload, rule_based_results)

    try:
        # Ollama API supports formatting output as json
        ollama_url = f"{settings.ollama_base_url.rstrip('/')}/api/generate"
        logger.info(f"Pinging Local LLM at {ollama_url} using model {settings.llm_model}...")
        
        response = httpx.post(
            ollama_url,
            json={
                "model": settings.llm_model,
                "prompt": f"{_SYSTEM_PROMPT}\n\n{user_message}",
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "num_ctx": 4096
                }
            },
            timeout=600.0 # Increased from 300s to 600s: DeepSeek reasoning takes heavy computation for deep config topologies.
        )
        response.raise_for_status()
        raw_text = response.json().get("response", "").strip()
        logger.info("Local LLM generated response successfully.")
        return _safe_parse_json(raw_text)
    except Exception as exc:
        logger.warning(f"Local LLM inference failed: {exc}")
        return None

def _build_user_message(payload: Any, rule_based: Dict[str, List[str]]) -> str:
    context = {
        "metadata": getattr(payload, "metadata", {}),
        "config": getattr(payload, "config", {}),
        "raw_json_sample": _truncate(getattr(payload, "raw_json", {})),
        "rule_based_results": rule_based
    }
    return (
        "Here is the pipeline payload and rule-based detection results.\n"
        "Please validate and enhance the analysis:\n\n"
        + json.dumps(context, indent=2, default=str)
    )

def _truncate(obj: Any, max_chars: int = 15000) -> Any:
    """Truncate large objects to avoid huge LLM prompts."""
    serialized = json.dumps(obj, default=str)
    if len(serialized) > max_chars:
        return serialized[:max_chars] + "... [truncated]"
    return obj

def _safe_parse_json(text: str) -> Optional[Dict[str, Any]]:
    # Strip markdown fences if present
    text = text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
    # Handle DeepSeek thoughts <think>...</think> which might bleed if format:json isn't perfectly respected
    import re
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    
    try:
        # Use strict=False to allow control characters (like unescaped newlines) 
        # which small models often output in JSON strings.
        return json.loads(text, strict=False)
    except json.JSONDecodeError as exc:
        logger.warning(f"LLM response is not valid JSON: {exc} | raw={text[:200]}")
        return {"raw_response": text}
