import json
from fastapi import APIRouter
from fastapi.responses import StreamingResponse, JSONResponse
from app.models.schemas import ReconcileRequest, ReconcileResult
from app.services.reconcile_engine import reconcile

router = APIRouter()


@router.post("", response_model=ReconcileResult)
async def run_reconcile(req: ReconcileRequest) -> ReconcileResult:
    """Merge baseline + candidate with a chosen strategy and optional manual overrides."""
    return reconcile(
        baseline=req.baseline,
        candidate=req.candidate,
        strategy=req.strategy,
        overrides=req.overrides,
    )


@router.post("/export")
async def export_merged(req: ReconcileRequest):
    """
    Run reconcile and return the merged config as a downloadable JSON file.
    """
    result = reconcile(
        baseline=req.baseline,
        candidate=req.candidate,
        strategy=req.strategy,
        overrides=req.overrides,
    )
    payload = json.dumps(result.merged, indent=2)
    filename = "merged_de_config.json"
    return StreamingResponse(
        iter([payload]),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
