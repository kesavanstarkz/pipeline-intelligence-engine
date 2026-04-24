from fastapi import APIRouter
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Any
from datetime import datetime, timezone
import json

router = APIRouter()


class ReconcileRequest(BaseModel):
    generated: dict[str, Any]
    azure: dict[str, Any]
    strategy: str = "generated_wins"   # "generated_wins" | "azure_wins" | "deep_merge"
    filename: str = "merged_config"


@router.post("/reconcile")
async def reconcile(req: ReconcileRequest):
    """
    Merges generated config with Azure config using the chosen strategy,
    injects provenance metadata, and returns the final merged JSON.
    """
    merged = _merge(req.generated, req.azure, req.strategy)
    merged["_meta"] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy": req.strategy,
        "tool": "DE Config Management Tool v1.0",
    }
    return {"merged": merged}


@router.post("/download")
async def download(req: ReconcileRequest):
    """
    Same as /reconcile but streams the result as a downloadable .json file.
    """
    merged = _merge(req.generated, req.azure, req.strategy)
    merged["_meta"] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy": req.strategy,
        "tool": "DE Config Management Tool v1.0",
    }
    filename = req.filename.replace(" ", "_").rstrip(".json") + ".json"
    content = json.dumps(merged, indent=2)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ──────────────────────────────────────────────────────────────────────────────
# Merge strategies
# ──────────────────────────────────────────────────────────────────────────────

def _merge(generated: dict, azure: dict, strategy: str) -> dict:
    if strategy == "generated_wins":
        return _deep_merge(azure, generated)      # generated overwrites azure
    elif strategy == "azure_wins":
        return _deep_merge(generated, azure)      # azure overwrites generated
    elif strategy == "deep_merge":
        return _deep_merge_additive(generated, azure)
    else:
        return _deep_merge(azure, generated)


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. Override wins on conflicts."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _deep_merge_additive(a: dict, b: dict) -> dict:
    """
    Deep merge where list values are unioned, dicts are recursed,
    and scalar conflicts take the non-None value (a wins ties).
    """
    result = dict(a)
    for k, v in b.items():
        if k not in result:
            result[k] = v
        elif isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge_additive(result[k], v)
        elif isinstance(result[k], list) and isinstance(v, list):
            # Union lists, preserve order, dedupe primitives
            seen = []
            for item in result[k] + v:
                if item not in seen:
                    seen.append(item)
            result[k] = seen
        # else: a wins
    return result
