from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Optional
import json

from services.etl_config_manager import ETLConfigManager
from app.models.schemas import ETLConfigRequest, ETLConfigResponse

router = APIRouter()


class GenerateConfigRequest(BaseModel):
    source_id: str
    source_label: str
    fields: dict[str, Any]


@router.post("/generate")
async def generate_config(req: GenerateConfigRequest):
    """
    Accepts field values from the frontend and returns a structured
    source_configuration JSON blob.
    """
    field_aliases = {
        "source_name": "name",
        "base_path": "path",
        "bucket_container": "bucket",
        "auth": "authentication",
        "storage_type": "storage_type",
        "file_format": "format",
        "partition_pattern": "partition_pattern",
        "ingest_mode": "mode",
        "volume_per_day": "volume_per_day",
    }

    normalized_fields: dict[str, Any] = {}
    for key, value in req.fields.items():
        if value is None or value == "":
            continue
        normalized_key = field_aliases.get(key, key)
        normalized_fields[normalized_key] = value

    conn_keys = {
        "server", "host", "port", "database", "schema", "collection",
        "namespace", "eventhub", "brokers", "topic", "consumer_group",
        "bucket", "path", "base_url", "endpoint", "account",
        "warehouse", "instance_url", "object", "storage_type",
        "authentication", "network", "auth",
    }
    ext_keys = {"mode", "query", "offset_reset", "partition_pattern", "pagination", "rate_limit", "watermark_column", "state_store"}
    prof_keys = {"volume_per_day", "format", "encoding", "peak_throughput", "partitions", "throughput_units", "retention_days"}

    source_key = f"{req.source_id}_source"
    source_block: dict[str, Any] = {
        "name": normalized_fields.get("name", source_key),
        "type": req.source_label,
        "connection": {},
        "extraction": {},
        "data_profile": {},
    }

    for k, v in req.fields.items():
        if not v or k == "name":
            continue
        if k in conn_keys:
            source_block["connection"][k] = v
        elif k in ext_keys:
            source_block["extraction"][k] = v
        elif k in prof_keys:
            source_block["data_profile"][k] = v
        else:
            source_block["connection"][k] = v  # fallback

    return JSONResponse(content=source_block)


@router.post("/etl-config", response_model=ETLConfigResponse)
async def create_etl_config(req: ETLConfigRequest):
    """
    Unified ETL config endpoint: merges source, ingestion, and azure configs
    into a complete runtime payload with ADF parameters and KV secrets.
    """
    try:
        manager = ETLConfigManager.from_request(
            req.source_config,
            req.ingestion_config,
            req.azure_config
        )

        payload = manager.build_runtime_payload()
        adf_params = manager.to_adf_parameters(payload)
        kv_secret = manager.get_kv_secret_payload(payload.job_id)

        return ETLConfigResponse(
            job_id=payload.job_id,
            runtime_payload=payload.model_dump(),
            adf_parameters=adf_params,
            kv_secret=kv_secret
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/upload-azure-config")
async def upload_azure_config(file: UploadFile = File(...)):
    """
    Upload Azure config JSON file for baseline comparison.
    """
    try:
        content = await file.read()
        config = json.loads(content.decode("utf-8"))
        return JSONResponse(content={"message": "Azure config uploaded", "config": config})
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")


@router.post("/upload-azure")
async def upload_azure_config_alt(file: UploadFile = File(...)):
    """
    Accept an uploaded JSON file as the Azure reference config.
    Returns the parsed config back to the client for in-browser storage.
    """
    if not file.filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted.")
    raw = await file.read()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {e}")
    return {"azure_config": parsed, "filename": file.filename}


@router.post("/parse-azure-text")
async def parse_azure_text(payload: dict):
    """
    Accept raw JSON text pasted by the user and return the parsed config.
    Payload: { "text": "<raw json string>" }
    """
    raw = payload.get("text", "")
    if not raw.strip():
        raise HTTPException(status_code=400, detail="No text provided.")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {e}")
    return {"azure_config": parsed}
