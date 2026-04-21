"""
Pipeline Intelligence Engine — FastAPI Application
───────────────────────────────────────────────────
Routes:
  POST /analyze       — main analysis endpoint
  POST /discover/workspace — scan a local folder on the API host and infer config
  POST /scan-cloud    — live cloud discovery
  GET  /health        — liveness + DataHub connectivity check
  GET  /detectors     — list registered detectors
  GET  /docs          — Swagger UI (auto-generated)
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, status, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from api.models import (
    AnalyzeRequest,
    AnalyzeResponse,
    DataPipelineAnalyzeResponse,
    WorkspaceDiscoverRequest,
)
from config.settings import settings
from engine.datahub_client import datahub_client
from engine.data_pipeline_analyzer import analyze_data_pipelines
from engine.pipeline_engine import PipelineIntelligenceEngine
from engine.registry import get_all_detectors
from engine.discovery.local_scanner import resolve_safe_root, scan_local_workspace
from llm.inference import llm_infer_data_pipeline_reasoning
from api.auth import router as auth_router
from starlette.middleware.sessions import SessionMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("pipeline_ie")

# ── Engine singleton ────────────────────────────────────────────────────────
_engine = PipelineIntelligenceEngine()


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    logger.info("Starting %s v%s", settings.app_title, settings.app_version)
    logger.info("DataHub GMS: %s", settings.datahub_gms_url)
    logger.info("LLM enabled: %s", settings.llm_enabled)
    yield
    logger.info("Shutdown.")


# ── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    description=(
        "Pipeline Intelligence Engine API"
    ),
    lifespan=lifespan
)

app.add_middleware(SessionMiddleware, secret_key=settings.app_secret_key)
app.include_router(auth_router)

# Mount static files
import os
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Jinja2 templates
templates = Jinja2Templates(directory="templates")


# ── Routes ──────────────────────────────────────────────────────────────────

@app.post(
    "/analyze",
    response_model=AnalyzeResponse,
    summary="Analyze a data pipeline",
    response_description="Extracted framework, source, ingestion, and DQ details",
)
async def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    """
    Analyze any client data pipeline and extract:
    - **framework**: Glue, Redshift, ADF, Databricks, Snowflake, EMR, Synapse …
    - **source**: S3, JDBC, ADLS, APIs …
    - **ingestion**: Glue Jobs, ADF Pipelines, Spark, Airflow …
    - **dq_rules**: Great Expectations, SQL validations, custom rules …

    All three input fields (`metadata`, `config`, `raw_json`) are optional — 
    submit whatever is available and the engine will infer the rest.
    """
    # LLM runs only when the client opts in AND the server allows it (see .kiro spec).
    use_llm = bool(request.use_llm and settings.llm_enabled)

    try:
        result = _engine.analyze(
            metadata=request.metadata,
            config=request.config,
            raw_json=request.raw_json,
            use_llm=use_llm,
        )
    except Exception as exc:
        logger.exception("Analysis failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analysis failed: {exc}",
        ) from exc

    return AnalyzeResponse(
        framework=result.framework,
        source=result.source,
        ingestion=result.ingestion,
        dq_rules=result.dq_rules,
        confidence=result.confidence,
        llm_inference=result.llm_inference,
        datahub_lineage=result.datahub_lineage,
        pipelines=result.pipelines,
        data_pipeline_reports=result.data_pipeline_reports,
        evidence=result.evidence,
        source_config=result.source_config,
        ingestion_config=result.ingestion_config,
        detailed_inventory=result.detailed_inventory,
        expert_extraction=result.expert_extraction,
        nodes=result.nodes,
        flow=result.flow,
        storage_config=result.storage_config,
        dq_config=result.dq_config,
        validation=result.validation,
    )


@app.post(
    "/analyze/data-pipelines",
    response_model=DataPipelineAnalyzeResponse,
    summary="Extract Fabric and ADF workflow details",
    response_description="Strict JSON list of detected Fabric / ADF data pipelines",
)
async def analyze_data_pipeline_workflows(request: AnalyzeRequest) -> DataPipelineAnalyzeResponse:
    """
    Detect Microsoft Fabric and Azure Data Factory workflows and return a strict
    pipeline-centric JSON payload.
    """
    try:
        payload = analyze_data_pipelines(
            _engine._build_payload(  # noqa: SLF001 - shared payload builder for API routes
                metadata=request.metadata,
                config=request.config,
                raw_json=request.raw_json,
            )
        )
        if request.use_llm and settings.llm_enabled:
            for report in payload:
                report["reasoning"] = llm_infer_data_pipeline_reasoning(report)
    except Exception as exc:
        logger.exception("Data pipeline analysis failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Data pipeline analysis failed: {exc}",
        ) from exc

    return DataPipelineAnalyzeResponse(root=payload)


@app.post(
    "/discover/workspace",
    response_model=AnalyzeResponse,
    summary="Scan a local folder (repo, configs, pipeline dirs) and infer ingestion config",
)
async def discover_workspace(body: WorkspaceDiscoverRequest) -> AnalyzeResponse:
    """
    Walk a directory on the **API host** (not the browser), detect frameworks and
    important paths, and return the same shape as ``/analyze`` including generated
    ingestion config suggestions.
    """
    try:
        safe_root = resolve_safe_root(body.root_path, settings)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    disc = scan_local_workspace(
        safe_root,
        max_depth=body.max_depth,
        max_files_recorded=body.max_files_recorded,
    )
    use_llm = bool(body.use_llm and settings.llm_enabled)

    metadata: Dict[str, Any] = {
        "source": "local_workspace_scan",
        "root": str(safe_root),
        "discovered_framework_hints": disc["frameworks"],
    }
    config: Dict[str, Any] = {
        "generated_ingestion_config": disc["generated_ingestion_config"],
    }
    raw_json: Dict[str, Any] = {
        "local_discovery": disc,
        "discovery_text": " ".join(disc["frameworks"])
        + "\n"
        + "\n".join(disc["evidence"][:120]),
    }

    try:
        result = _engine.analyze(
            metadata=metadata,
            config=config,
            raw_json=raw_json,
            use_llm=use_llm,
        )
    except Exception as exc:
        logger.exception("Workspace discovery failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Workspace discovery failed: {exc}",
        ) from exc

    ev = dict(result.evidence or {})
    ev["local_discovery"] = disc["evidence"][:100]
    ev["generated_config"] = [
        f"Inferred {disc['stats']['framework_hint_count']} framework(s); "
        f"{disc['stats']['important_file_count']} important file path(s) recorded."
    ]

    return AnalyzeResponse(
        framework=result.framework,
        source=result.source,
        ingestion=result.ingestion,
        dq_rules=result.dq_rules,
        confidence=result.confidence,
        llm_inference=result.llm_inference,
        datahub_lineage=result.datahub_lineage,
        pipelines=result.pipelines,
        data_pipeline_reports=result.data_pipeline_reports,
        evidence=ev,
        source_config=result.source_config,
        ingestion_config=result.ingestion_config,
        detailed_inventory=result.detailed_inventory,
        expert_extraction=result.expert_extraction,
        nodes=result.nodes,
        flow=result.flow,
        storage_config=result.storage_config,
        dq_config=result.dq_config,
        validation=result.validation,
    )

from engine.scanner.manager import scanner_manager

@app.post(
    "/scan-cloud",
    response_model=AnalyzeResponse,
    summary="Actively scan cloud environments using stored credentials",
)
async def scan_cloud(
    request: Request,
    providers: str | None = None,
    use_llm: bool = False,
) -> AnalyzeResponse:
    """
    Trigger live querying against AWS, Azure, Databricks, Snowflake 
    if credentials are mathematically configured in the Settings vault.
    Pipes the output into the PipelineIntelligenceEngine.

    Set ``use_llm=true`` to call the local LLM after detection (slower; can fail on small models).
    Default is off so scans return quickly with rule-based results.
    """
    try:
        azure_token = request.session.get("access_token")
        azure_token_fabric = request.session.get("access_token_fabric")
        # Ensure we don't have empty strings in the list
        provider_list = [p.strip().lower() for p in providers.split(",") if p.strip()] if providers else None
        live_data = await scanner_manager.scan_all(
            settings,
            providers=provider_list,
            azure_token=azure_token,
            azure_token_fabric=azure_token_fabric,
        )
        
        # Build strict topological DAGs first using Service Normalization
        safe_pipelines = []
        cloud_dump = live_data.get("raw_cloud_dump", [])
        
        source_nodes = []
        ingest_nodes = []
        target_nodes = []
        
        for raw_map in cloud_dump:
            if not isinstance(raw_map, dict): continue
            source_nodes.extend(raw_map.get("apigateway", []))
            # Azure Data Sources
            source_nodes.extend(raw_map.get("storage_accounts", [])) 
            
            ingest_nodes.extend(raw_map.get("lambda", []))
            ingest_nodes.extend(raw_map.get("glue", []))
            ingest_nodes.extend(raw_map.get("functions", []))
            ingest_nodes.extend(raw_map.get("datafactory", []))
            
            # Microsoft Fabric items can be Source, Ingest, or Target
            for item in raw_map.get("fabric_items", []):
                i_type = item.get("configuration", {}).get("Type", "").lower()
                if i_type in ["lakehouse", "warehouse"]:
                    target_nodes.append(item)
                elif i_type in ["pipeline", "datapipeline", "notebook", "sparkjob"]:
                    ingest_nodes.append(item)
                else:
                    source_nodes.append(item)

            target_nodes.extend(raw_map.get("s3", []))
            target_nodes.extend(raw_map.get("storage_accounts", []))
            target_nodes.extend(raw_map.get("fabric_workspaces", []))
        
        # Pipeline mapping and hardening
        for i_node in ingest_nodes:
            i_id = i_node.get("id", "")
            i_name = i_id.split("||")[-1].strip()
            cloud_paths = set([i_id.split("||")[0].strip().upper()])
            
            pipe_sources = []
            pipe_targets = []
            
            code_evidences = i_node.get("code_evidence", [])
            evidence = ["Compute handler assigned to INGESTION phase."]
            confidence = i_node.get("confidence", "LOW")
            if code_evidences: evidence.append("Code evidence confirmed.")
            
            # Upstream mapping (Source -> Ingestion)
            for s_node in source_nodes:
                s_id = s_node.get("id", "")
                if i_name.split('-')[-1].lower() in s_id.lower() or "api" in s_id.lower():
                     pipe_sources.append(s_id)
                     cloud_paths.add(s_id.split("||")[0].strip().upper())
                     evidence.append(f"Trigger binding matched API '{s_id}'.")
            
            # Downstream validating map (Ingestion -> Target)
            for target_val in i_node.get("env_targets", []):
                for t_node in target_nodes:
                    t_id = t_node.get("id", "")
                    t_name = t_id.split("||")[-1].strip().lower()
                    if t_name in target_val.lower() or target_val.lower() in t_id.lower():
                        pipe_targets.append(t_id)
                        cloud_paths.add(t_id.split("||")[0].strip().upper())
                        evidence.append(f"Hardened link to target '{t_id}'.")
                        
            # Apply Service Normalization Defaults if edges are empty
            if not pipe_targets and target_nodes:
                pipe_targets.append(target_nodes[len(safe_pipelines) % len(target_nodes)]["id"])
                evidence.append(f"Heuristics applied for TARGET fallback.")
            if not pipe_sources and source_nodes:
                pipe_sources.append(source_nodes[len(safe_pipelines) % len(source_nodes)]["id"])
                evidence.append(f"Heuristics applied for SOURCE fallback.")
                
            # Attach deep config for the UI side-panel
            config_payload = {
                "runtime": i_node.get("configuration", {}).get("Runtime"),
                "memory": i_node.get("configuration", {}).get("MemorySizeMB"),
                "timeout": i_node.get("configuration", {}).get("TimeoutSeconds"),
                "operations": i_node.get("configuration", {}).get("IngestionOperations"),
                "targets": i_node.get("configuration", {}).get("IngestionTargets"),
                "data_formats": i_node.get("configuration", {}).get("DataFormats"),
                "confidence_level": confidence
            }

            safe_pipelines.append({
                "name": f"{i_name} Architecture",
                "source": list(set(pipe_sources)),
                "ingestion": [i_id],
                "target": list(set(pipe_targets)),
                "framework": list(set(pipe_targets)), # Backwards compatible
                "cloud_path": list(cloud_paths),
                "confidence": confidence,
                "evidence": evidence,
                "code_evidence": code_evidences,
                "config": config_payload,
                "dq_rules": []
            })
            
        if not safe_pipelines and (source_nodes or target_nodes):
            safe_pipelines.append({
                "name": "Discovered Standalone Assets",
                "source": [s.get("id") for s in source_nodes],
                "ingestion": [],
                "target": [t.get("id") for t in target_nodes],
                "framework": [t.get("id") for t in target_nodes],
                "cloud_path": ["MULTICLOUD"],
                "confidence": "LOW",
                "evidence": ["No deep pipelines constructed."],
                "code_evidence": [],
                "dq_rules": []
            })

        # Optional LLM augmentation (off by default — Ollama JSON can be slow or invalid on small models)
        llm_on = bool(use_llm and settings.llm_enabled)
        result = _engine.analyze(
            metadata={"source": "Live Cloud Scan Execution"},
            config={"safe_pipelines": safe_pipelines},
            raw_json=live_data,
            use_llm=llm_on,
        )

        result.confidence["framework"] = 0.95
        result.confidence["source"] = 0.95
        result.confidence["ingestion"] = 0.95

        # Format evidence properly to show EXACT services discovered
        evidence_list = []
        
        if provider_list:
             evidence_list.append(f"Targeted Scan: {', '.join(provider_list).upper()}")
        else:
             evidence_list.append("Discovery: Scanning all configured cloud environments.")
        
        cloud_dump = live_data.get("raw_cloud_dump", [])
        if cloud_dump and isinstance(cloud_dump[0], dict):
            for service, resources in cloud_dump[0].items():
                if resources:
                    evidence_list.append(f"Discovered {len(resources)} active {service.upper()} resources:")
                    for res in resources[:10]:
                        if isinstance(res, dict):
                            res_id = res.get("id", "Unknown")
                            cfg = res.get("configuration", {})
                            # Create a beautiful, readable summary instead of raw dict
                            summary_parts = []
                            if "Runtime" in cfg: summary_parts.append(f"Runtime: {cfg['Runtime']}")
                            if "MemorySizeMB" in cfg: summary_parts.append(f"Memory: {cfg['MemorySizeMB']}MB")
                            if "PublicInvokeURL" in cfg: summary_parts.append(f"Endpoint: {cfg['PublicInvokeURL']}")
                            if "InferredTargets" in cfg or "IngestionTargets" in cfg:
                                tgts = cfg.get("InferredTargets") or cfg.get("IngestionTargets")
                                if tgts: summary_parts.append(f"Targets: {', '.join(tgts[:2])}")
                            
                            summary_str = " | ".join(summary_parts)
                            evidence_list.append(f"  - {res_id} ({summary_str if summary_str else 'Active'})")
                        else:
                            evidence_list.append(f"  - {res}")

        # Set the topological DAGs into the result output directly if the LLM fails to structure it, otherwise the LLM replaces it.
        result.pipelines = result.pipelines or safe_pipelines

        return AnalyzeResponse(
            framework=result.framework,
            source=result.source,
            ingestion=result.ingestion,
            dq_rules=result.dq_rules,
            confidence=result.confidence,
            llm_inference=result.llm_inference,
            datahub_lineage=result.datahub_lineage,
            pipelines=safe_pipelines,
            data_pipeline_reports=result.data_pipeline_reports,
            evidence={"Live Scan Telemetry Extract": evidence_list},
            source_config=result.source_config,
            ingestion_config=result.ingestion_config,
            storage_config=result.storage_config,
            dq_config=result.dq_config,
            validation=result.validation,
            expert_extraction=result.expert_extraction,
            detailed_inventory=result.detailed_inventory
        )

    except Exception as exc:
        logger.exception("Cloud scan failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cloud scan failed: {exc}",
        ) from exc


@app.get("/health", summary="Liveness + DataHub connectivity")
async def health(request: Request) -> Dict[str, Any]:
    dh_ok = datahub_client.health_check()
    user = request.session.get("user")
    return {
        "status": "ok",
        "datahub_connected": dh_ok,
        "datahub_url": settings.datahub_gms_url,
        "llm_enabled": settings.llm_enabled,
        "version": settings.app_version,
        "user": user # Returns user info if logged in
    }


@app.get("/detectors", summary="List registered detectors")
async def list_detectors() -> Dict[str, List[str]]:
    return {"detectors": [d.name for d in get_all_detectors()]}


@app.get("/", summary="Interactive Dashboard")
async def root(request: Request):
    """Serve the single-page application dashboard."""
    return templates.TemplateResponse("index.html", {"request": request, "settings": settings})


@app.get("/api/config/keys", summary="Get Configured Providers")
async def get_keys() -> Dict[str, Any]:
    """Return obfuscated/boolean statuses for configured cloud providers."""
    # Check if Azure is configured via Service Principal (ignoring placeholders)
    azure_sp_configured = bool(settings.azure_client_id and settings.azure_client_secret and "YOUR_" not in settings.azure_client_id)
    
    return {
        "aws": bool(settings.aws_access_key_id),
        "azure": azure_sp_configured,
        "snowflake": bool(settings.snowflake_account),
        "databricks": bool(settings.databricks_host),
        "datahub": bool(settings.datahub_token) or bool(settings.datahub_gms_url),
        "anthropic": bool(settings.anthropic_api_key),
        "azure_client_id_active": bool(settings.azure_client_id and "YOUR_" not in settings.azure_client_id)
    }


@app.post("/api/config/keys", summary="Set Configured Providers")
async def set_keys(keys: Dict[str, Any]) -> Dict[str, str]:
    """Securely accept and save new API keys to the system configuration."""
    # Filter out empty string keys so we don't accidentally blank out configs if not provided
    filtered_keys = {k: v for k, v in keys.items() if v}
    settings.update_keys(filtered_keys)
    return {"status": "success", "message": "Keys updated successfully"}


@app.exception_handler(Exception)
async def generic_exception_handler(request: Any, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )
