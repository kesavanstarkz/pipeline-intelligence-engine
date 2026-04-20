"""
API Models
──────────
Pydantic v2 schemas for POST /analyze request and response.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from engine.detectors.base import CloudEnvironment


class AnalyzeRequest(BaseModel):
    """
    POST /analyze request body.

    All three fields are optional — submit whatever is available.
    The engine will infer as much as possible from partial data.
    """

    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="DataHub entity attrs, platform tags, or free-form pipeline metadata",
        examples=[{"platform": "glue", "name": "customer_etl", "env": "PROD"}],
    )
    config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Pipeline configuration block (connections, job definitions, resource blocks)",
        examples=[{"connections": ["s3://bucket/path", "redshift://cluster/db"]}],
    )
    raw_json: Dict[str, Any] = Field(
        default_factory=dict,
        description="Any additional JSON (Terraform, parsed YAML, Glue job JSON, etc.)",
        examples=[{"type": "GlueJob", "dq": {"suite_name": "customer_suite"}}],
    )
    use_llm: bool = Field(
        default=False,
        description="Enable LLM inference layer (requires ANTHROPIC_API_KEY)",
    )


class WorkspaceDiscoverRequest(BaseModel):
    """POST /discover/workspace — scan a folder on the API host (repo, pipelines, configs)."""

    root_path: str = Field(
        ...,
        description="Absolute or relative path to a directory to inspect (must be under allowed roots)",
    )
    max_depth: int = Field(default=6, ge=1, le=20)
    max_files_recorded: int = Field(default=400, ge=10, le=5000)
    use_llm: bool = Field(
        default=False,
        description="If true and server LLM is enabled, run LLM synthesis after rule-based detection",
    )


class ConfidenceScores(BaseModel):
    framework: Optional[float] = None
    source: Optional[float] = None
    ingestion: Optional[float] = None
    dq_rules: Optional[float] = None


class AnalyzeResponse(BaseModel):
    """
    POST /analyze response body — matches the spec exactly.
    """

    framework:       List[str] = Field(description="Detected data platform frameworks")
    source:          List[str] = Field(description="Detected data sources")
    ingestion:       List[str] = Field(description="Detected ingestion engines / orchestrators")
    dq_rules:        List[str] = Field(description="Detected data quality rules / frameworks")
    confidence:      Dict[str, Optional[float]] = Field(description="Per-category confidence 0–1")
    llm_inference:   Optional[Dict[str, Any]] = Field(
        default=None, description="LLM-enhanced inference (null if disabled)"
    )
    datahub_lineage: List[Dict[str, Any]] = Field(
        default_factory=list, description="Upstream lineage edges from DataHub"
    )
    pipelines:       Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Distinct pipelines mapped out logically by the LLM"
    )
    evidence:        Optional[Dict[str, List[str]]] = Field(
        default=None, description="Detection evidence trail per category"
    )
    
    # Deep Inference Config Maps
    nodes:           Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Categorized inferred pipeline nodes with config details"
    )
    flow:            Optional[Dict[str, Any]] = Field(
        default=None, description="Detected triggers and routing logic"
    )
    source_config:   Optional[Dict[str, Any]] = Field(
        default=None, description="Extracted properties for dynamic source integrations"
    )
    ingestion_config: Optional[Dict[str, Any]] = Field(
        default=None, description="Extracted properties for compute transformations"
    )
    storage_config:  Optional[Dict[str, Any]] = Field(
        default=None, description="Extracted properties for data sinks and zones"
    )
    dq_config:       Optional[Dict[str, Any]] = Field(
        default=None, description="Automatically inferred validations and rule checks"
    )
    validation:      Optional[Dict[str, Any]] = Field(
        default=None, description="Identified pipeline errors and broken edges"
    )
    cloud_environments: Optional[List[CloudEnvironment]] = Field(
        default=None, description="Pipeline nodes grouped by cloud account/region"
    )
    expert_extraction: Optional[Dict[str, Any]] = Field(
        default=None, description="Strict, evidence-backed cloud configuration extraction (Expert AI Mode)"
    )
    detailed_inventory: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Detailed configuration inventory for all discovered cloud resources"
    )

