"""
routers/extract.py — Unified Multi-Platform Config Extraction Router
---------------------------------------------------------------------
Endpoints:
  POST /api/extract/config       — Extract from any platform (auto-detect)
  POST /api/extract/infer        — LLM inference from code/config text
  POST /api/extract/classify     — Classify pipeline framework from code
  GET  /api/extract/platforms    — List supported platforms + demo pipelines
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Any
import logging
import os

from services.pipeline_orchestrator import PipelineOrchestrator, detect_platform

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Request Models ─────────────────────────────────────────────────────────────

class UnifiedExtractRequest(BaseModel):
    pipeline_name: str
    platform: Optional[str] = None          # aws|azure|databricks|snowflake|unknown (auto-detect if omitted)
    demo_mode: bool = True
    credentials: Optional[dict[str, Any]] = None
    hints: Optional[dict[str, Any]] = None  # e.g. {"code": "...", "region": "us-east-1"}


class LLMInferRequest(BaseModel):
    code_or_config: str                     # Python/PySpark/SQL/YAML/JSON code or config
    context: Optional[str] = None           # Extra context (env vars, descriptions)
    pipeline_name: Optional[str] = None


class ClassifyRequest(BaseModel):
    code_snippet: str


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/config")
async def extract_config(req: UnifiedExtractRequest):
    """
    Universal config extraction endpoint.
    Auto-detects platform from pipeline name or use explicit platform param.
    Supports demo mode for all platforms.
    """
    try:
        orchestrator = PipelineOrchestrator(
            pipeline_name=req.pipeline_name,
            platform=req.platform,
            demo_mode=req.demo_mode,
            credentials=req.credentials or {},
            hints=req.hints or {},
        )
        result = orchestrator.extract()
        return JSONResponse(content=result)
    except Exception as exc:
        logger.exception("Extraction failed for %s", req.pipeline_name)
        raise HTTPException(status_code=400, detail={
            "error": str(exc),
            "hint": "Enable demo_mode=true to test without credentials.",
        })


@router.post("/infer")
async def infer_from_code(req: LLMInferRequest):
    """
    Use Claude LLM to extract structured pipeline config from arbitrary code or config text.
    Works on: Python, PySpark, SQL, YAML, JSON, dbt models, Terraform, etc.
    """
    if not req.code_or_config.strip():
        raise HTTPException(status_code=400, detail="code_or_config cannot be empty")
    
    try:
        from services.llm_inference import infer_config_from_code
        result = await infer_config_from_code(
            code_or_config=req.code_or_config,
            context=req.context,
            pipeline_name=req.pipeline_name,
        )
        return JSONResponse(content=result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/classify")
async def classify_framework(req: ClassifyRequest):
    """
    Classify the pipeline framework type from a code snippet.
    Returns: framework, source_type, pattern, cloud_provider, confidence.
    """
    try:
        from services.llm_inference import classify_framework
        result = await classify_framework(req.code_snippet)
        return JSONResponse(content=result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/platforms")
def list_platforms():
    """
    Returns all supported platforms and their available demo pipelines.
    """
    from services.azure_extractor import ADF_PIPELINE_LIST
    from services.databricks_extractor import DATABRICKS_JOB_LIST
    from services.snowflake_extractor import SNOWFLAKE_PIPELINE_LIST

    return {
        "platforms": [
            {
                "id": "azure",
                "name": "Azure",
                "icon": "🔷",
                "description": "ADF, Synapse Analytics, ADLS",
                "demo_pipelines": [
                    {"name": p["name"], "type": p["type"], "description": ""}
                    for p in ADF_PIPELINE_LIST
                ],
                "credential_fields": ["tenant_id", "client_id", "client_secret", "subscription_id", "resource_group", "factory_name"],
            },
            {
                "id": "databricks",
                "name": "Databricks",
                "icon": "🧱",
                "description": "Jobs, DLT Pipelines, Notebooks",
                "demo_pipelines": [
                    {"name": p["name"], "type": p["type"], "description": ""}
                    for p in DATABRICKS_JOB_LIST
                ],
                "credential_fields": ["host", "token"],
            },
            {
                "id": "snowflake",
                "name": "Snowflake",
                "icon": "❄️",
                "description": "Tasks, Streams, dbt, Dynamic Tables",
                "demo_pipelines": [
                    {"name": p["name"], "type": p["type"], "description": ""}
                    for p in SNOWFLAKE_PIPELINE_LIST
                ],
                "credential_fields": ["account", "user", "password", "warehouse", "database", "schema", "role"],
            },
            {
                "id": "unknown",
                "name": "Unknown / Custom",
                "icon": "🔍",
                "description": "Any custom or unknown pipeline — uses LLM inference from code",
                "demo_pipelines": [],
                "credential_fields": [],
            },
        ]
    }


@router.get("/health")
def health_check():
    """System health + dependency status check."""
    from services.health import get_health_status
    return get_health_status()


@router.get("/llm-status")
def llm_status():
    """Check if LLM inference is available."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    return {
        "api_key_configured": bool(key),
        "mode": "claude_api" if key else "pattern_matching_fallback",
        "model": "claude-sonnet-4-20250514",
        "hint": "" if key else "Set ANTHROPIC_API_KEY env var for full AI-powered inference. Pattern matching works without it.",
    }
