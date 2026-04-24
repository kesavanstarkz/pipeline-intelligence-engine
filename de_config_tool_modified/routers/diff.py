from fastapi import APIRouter
from pydantic import BaseModel
from typing import Any
from services.deep_diff import deep_diff

router = APIRouter()


class DiffRequest(BaseModel):
    generated: dict[str, Any]
    azure: dict[str, Any]


@router.post("/compare")
async def compare_configs(req: DiffRequest):
    """
    Runs a recursive deep-diff between the generated source config
    and the uploaded Azure reference config. Returns a structured
    diff tree with match/mismatch/added/removed at every nesting level.
    """
    result = deep_diff(req.generated, req.azure, path="root")
    summary = _summarise(result)
    return {"diff": result, "summary": summary}


def _summarise(nodes: list[dict]) -> dict:
    counts = {"match": 0, "mismatch": 0, "added": 0, "removed": 0, "nested": 0}
    for n in nodes:
        t = n.get("type")
        if t in counts:
            counts[t] += 1
        children = n.get("children", [])
        if children:
            counts["nested"] += 1
            sub = _summarise(children)
            for k in ("match", "mismatch", "added", "removed"):
                counts[k] += sub[k]
    return counts
