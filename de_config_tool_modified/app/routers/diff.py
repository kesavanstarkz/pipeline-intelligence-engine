from fastapi import APIRouter
from app.models.schemas import DiffRequest, DiffResult
from app.services.diff_engine import deep_diff

router = APIRouter()


@router.post("", response_model=DiffResult)
async def run_diff(req: DiffRequest) -> DiffResult:
    """
    Deep-diff two configs. Handles arbitrarily nested JSON.
    Returns a flat list of path-keyed diff entries.
    """
    return deep_diff(req.baseline, req.candidate)
