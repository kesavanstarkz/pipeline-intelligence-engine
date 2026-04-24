from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.models.schemas import GenerateConfigRequest, AzureConfigUpload
from app.services.config_generator import build_config

router = APIRouter()

# In-memory store for the current session's Azure/baseline config
_baseline_store: dict = {}


@router.post("/upload-baseline")
async def upload_baseline(payload: AzureConfigUpload):
    """Accept a pasted or uploaded Azure/baseline config JSON."""
    _baseline_store.clear()
    _baseline_store.update(payload.config)
    return {"status": "ok", "keys_loaded": list(_baseline_store.keys())}


@router.get("/baseline")
async def get_baseline():
    """Return the currently loaded baseline config."""
    if not _baseline_store:
        raise HTTPException(status_code=404, detail="No baseline config loaded yet.")
    return _baseline_store


@router.post("/generate")
async def generate_config(req: GenerateConfigRequest):
    """Generate a structured nested config JSON from form fields."""
    cfg = build_config(req.source_type, req.fields)
    return cfg


@router.delete("/baseline")
async def clear_baseline():
    _baseline_store.clear()
    return {"status": "cleared"}
